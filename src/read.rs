use crate::descriptors::{FieldDescriptorProtoExt, FileDescriptorSetExt};
use duckdb::ffi;
use duckdb::ffi::{
    duckdb_list_entry, duckdb_list_vector_get_child, duckdb_list_vector_reserve,
    duckdb_list_vector_set_size, duckdb_vector,
};
use duckdb::vtab::{DataChunk, FlatVector, Inserter, LogicalType, LogicalTypeId};
use prost::bytes::Buf;
use prost::encoding::{check_wire_type, decode_key, decode_varint, DecodeContext, WireType};
use prost::DecodeError;
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorSet};
use std::collections::{HashMap, HashSet};
use std::error::Error;

pub fn into_logical_type(
    field: &FieldDescriptorProto,
    descriptors: &FileDescriptorSet,
) -> Result<LogicalType, Box<dyn Error>> {
    match field.label() {
        Label::Repeated => Ok(LogicalType::list(&into_logical_type_single(
            field,
            descriptors,
        )?)),
        Label::Optional | Label::Required => Ok(into_logical_type_single(field, descriptors)?),
    }
}

pub fn into_logical_type_single(
    field: &FieldDescriptorProto,
    descriptors: &FileDescriptorSet,
) -> Result<LogicalType, Box<dyn Error>> {
    let value = match field.r#type() {
        Type::Message => {
            let type_name = field.fully_qualified_type_name()?;

            let message_descriptor = descriptors
                .message_matching(type_name)
                .ok_or(format!("message type not found: {}", type_name))?;

            LogicalType::struct_type(
                &message_descriptor
                    .field
                    .iter()
                    .map(|field| Ok((field.name(), into_logical_type(field, descriptors)?)))
                    .collect::<Result<Vec<(&str, LogicalType)>, Box<dyn Error>>>()?,
            )
        }
        Type::Enum => LogicalType::new(LogicalTypeId::Varchar),
        Type::String => LogicalType::new(LogicalTypeId::Varchar),
        Type::Uint32 => LogicalType::new(LogicalTypeId::UInteger),
        Type::Uint64 => LogicalType::new(LogicalTypeId::UBigint),
        Type::Double => LogicalType::new(LogicalTypeId::Double),
        Type::Float => LogicalType::new(LogicalTypeId::Float),
        Type::Int32 => LogicalType::new(LogicalTypeId::Integer),
        Type::Int64 => LogicalType::new(LogicalTypeId::Bigint),
        Type::Bool => LogicalType::new(LogicalTypeId::Boolean),
        logical_type => {
            return Err(format!(
                "unhandled field: {}, type: {}",
                field.name(),
                logical_type.as_str_name()
            )
            .into())
        }
    };

    Ok(value)
}

#[derive(Hash, Eq, PartialEq, Clone)]
pub enum ColumnKeyElement {
    Field { field_tag: i32 },
    List,
}

#[derive(Hash, Eq, PartialEq)]
pub struct ColumnKey {
    pub elements: Vec<ColumnKeyElement>,
}

impl ColumnKey {
    pub fn extending(&self, key: ColumnKeyElement) -> ColumnKey {
        let mut elements = self.elements.clone();
        elements.push(key);

        ColumnKey { elements }
    }
    pub fn empty() -> ColumnKey {
        ColumnKey { elements: vec![] }
    }
}

pub trait VectorAccessor {
    fn get_vector(&self, column_idx: usize) -> duckdb::ffi::duckdb_vector;
}

impl VectorAccessor for DataChunk {
    fn get_vector(&self, column_idx: usize) -> duckdb::ffi::duckdb_vector {
        let chunk = self.get_ptr();

        unsafe { ffi::duckdb_data_chunk_get_vector(chunk, column_idx as u64) }
    }
}

struct StructVector(duckdb::ffi::duckdb_vector);

impl StructVector {
    unsafe fn new(value: duckdb_vector) -> Self {
        Self(value)
    }
}

impl VectorAccessor for StructVector {
    fn get_vector(&self, idx: usize) -> duckdb_vector {
        unsafe { ffi::duckdb_struct_vector_get_child(self.0, idx as u64) }
    }
}

pub struct ProtobufMessageWriter<'a, V: VectorAccessor> {
    pub base_column_key: ColumnKey,
    pub column_information: &'a mut HashMap<ColumnKey, u64>,
    pub seen_repeated_fields: HashSet<usize>,
    pub descriptors: &'a FileDescriptorSet,
    pub message_descriptor: &'a DescriptorProto,
    pub output: &'a V,
    pub output_row_idx: usize,
}

impl<V> ProtobufMessageWriter<'_, V>
where
    V: VectorAccessor,
{
    pub fn merge(&mut self, mut buf: impl Buf, ctx: DecodeContext) -> Result<(), DecodeError> {
        let mut seen_tags = HashSet::new();

        while buf.has_remaining() {
            let (tag, wire_type) = decode_key(&mut buf)?;
            self.merge_field(tag, wire_type, &mut buf, ctx.clone())?;

            seen_tags.insert(tag as i32);
        }

        for (field_idx, _field) in self
            .message_descriptor
            .field
            .iter()
            .enumerate()
            .filter(|(_, it)| !seen_tags.contains(&it.number()))
        {
            // todo: use protobuf default values
            let mut column_vector = FlatVector::from(self.output.get_vector(field_idx));
            column_vector.set_null(self.output_row_idx);
        }

        Ok(())
    }

    fn merge_field<B>(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut B,
        ctx: DecodeContext,
    ) -> Result<(), DecodeError>
    where
        B: Buf,
        Self: Sized,
    {
        let (field_idx, field) = match self
            .message_descriptor
            .field
            .iter()
            .enumerate()
            .find(|(_idx, field)| field.number() == tag as i32)
        {
            Some((idx, field)) => (idx, field),
            None => {
                prost::encoding::skip_field(wire_type, tag, buf, ctx)?;
                return Ok(());
            }
        };

        match field.label() {
            Label::Repeated => {
                let ffi_list_vector = self.output.get_vector(field_idx);

                let mut list_entries_vector = FlatVector::from(ffi_list_vector);
                let list_entry_items = list_entries_vector.as_mut_slice::<duckdb_list_entry>();

                // Whether this repeated field has been seen before when handling this message. Used
                // to initialize list entry values.
                let has_seen = self.seen_repeated_fields.get(&field_idx).is_some();
                self.seen_repeated_fields.insert(field_idx);
                let column_key = self
                    .base_column_key
                    .extending(ColumnKeyElement::Field {
                        field_tag: field.number(),
                    })
                    .extending(ColumnKeyElement::List);
                if !has_seen {
                    let next_offset = self
                        .column_information
                        .get(&column_key)
                        .map(|it| *it)
                        .unwrap_or(0);
                    let list_entry = &mut list_entry_items[self.output_row_idx];
                    list_entry.offset = next_offset;
                    list_entry.length = 0;
                }

                let list_entry = &mut list_entry_items[self.output_row_idx];

                let row_idx = list_entry.offset as usize + list_entry.length as usize;
                let needed_length = row_idx + 1;

                unsafe { duckdb_list_vector_reserve(ffi_list_vector, needed_length as _) };
                unsafe { duckdb_list_vector_set_size(ffi_list_vector, needed_length as _) };
                let ffi_list_child_vector =
                    unsafe { duckdb_list_vector_get_child(ffi_list_vector) };

                list_entry.length += 1;
                self.column_information
                    .insert(column_key, needed_length as _);

                self.merge_single_field(
                    field,
                    wire_type,
                    buf,
                    ctx,
                    ffi_list_child_vector,
                    row_idx,
                )?;
            }
            Label::Optional | Label::Required => {
                let row_idx: usize = self.output_row_idx;
                let output_vector = self.output.get_vector(field_idx);
                self.merge_single_field(field, wire_type, buf, ctx, output_vector, row_idx)?;
            }
        };

        Ok(())
    }

    fn merge_single_field<B: Buf>(
        &mut self,
        field: &FieldDescriptorProto,
        wire_type: WireType,
        buf: &mut B,
        ctx: DecodeContext,
        output_vector: duckdb_vector,
        row_idx: usize,
    ) -> Result<(), DecodeError> {
        macro_rules! generate_match_arm {
            ($merge_fn:path, $slice_type:ty) => {{
                let mut value = <$slice_type>::default();
                $merge_fn(wire_type, &mut value, buf, ctx)?;

                let mut vector = FlatVector::from(output_vector);
                vector.as_mut_slice::<$slice_type>()[row_idx] = value;
            }};
        }

        match field.r#type() {
            Type::Message => {
                let output = unsafe { StructVector::new(output_vector) };

                let message_type_name = field
                    .fully_qualified_type_name()
                    .map_err(|err| DecodeError::new(err))?;

                let mut writer = ProtobufMessageWriter {
                    seen_repeated_fields: Default::default(),
                    base_column_key: self.base_column_key.extending(ColumnKeyElement::Field {
                        field_tag: field.number(),
                    }),
                    column_information: self.column_information,
                    descriptors: self.descriptors,
                    message_descriptor: self
                        .descriptors
                        .message_matching(message_type_name)
                        .ok_or_else(|| {
                            DecodeError::new(format!(
                                "message type not found in descriptor: {}",
                                message_type_name
                            ))
                        })?,
                    output_row_idx: row_idx,
                    output: &output,
                };

                check_wire_type(WireType::LengthDelimited, wire_type)?;

                let len = decode_varint(buf)?;
                let remaining = buf.remaining();
                if len > remaining as u64 {
                    return Err(DecodeError::new("buffer underflow"));
                }

                let mut seen_tags = HashSet::new();

                let limit = remaining - len as usize;
                while buf.remaining() > limit {
                    let (tag, wire_type) = decode_key(buf)?;
                    writer.merge_field(tag, wire_type, buf, Default::default())?;

                    seen_tags.insert(tag as i32);
                }

                if buf.remaining() != limit {
                    return Err(DecodeError::new("delimited length exceeded"));
                }

                for (field_idx, _field) in self
                    .message_descriptor
                    .field
                    .iter()
                    .enumerate()
                    .filter(|(_, it)| !seen_tags.contains(&it.number()))
                {
                    // todo: use protobuf default values
                    let mut column_vector = FlatVector::from(self.output.get_vector(field_idx));
                    column_vector.set_null(self.output_row_idx);
                }
            }
            Type::Enum => {
                let enum_type_name = field
                    .fully_qualified_type_name()
                    .map_err(|err| DecodeError::new(err))?;

                let enum_descriptor =
                    self.descriptors
                        .enum_matching(enum_type_name)
                        .ok_or_else(|| {
                            DecodeError::new(format!(
                                "enum type not found in descriptor: {}",
                                field.type_name()
                            ))
                        })?;

                let mut enum_value = <i32>::default();
                prost::encoding::int32::merge(wire_type, &mut enum_value, buf, ctx)?;

                let vector = FlatVector::from(output_vector);

                match enum_descriptor
                    .value
                    .iter()
                    .find(|value| value.number() == enum_value)
                {
                    None => {
                        vector.insert(row_idx, format!("unknown={}", enum_value).as_str());
                    }
                    Some(value) => {
                        vector.insert(row_idx, value.name());
                    }
                };
            }
            Type::String => {
                let mut value = <String>::default();
                prost::encoding::string::merge(wire_type, &mut value, buf, ctx)?;

                let vector = FlatVector::from(output_vector);
                vector.insert(row_idx, value.as_str());
            }
            Type::Uint32 => generate_match_arm!(prost::encoding::uint32::merge, u32),
            Type::Uint64 => generate_match_arm!(prost::encoding::uint64::merge, u64),
            Type::Double => generate_match_arm!(prost::encoding::double::merge, f64),
            Type::Float => generate_match_arm!(prost::encoding::float::merge, f32),
            Type::Int64 => generate_match_arm!(prost::encoding::int64::merge, i64),
            Type::Int32 => generate_match_arm!(prost::encoding::int32::merge, i32),
            Type::Bool => generate_match_arm!(prost::encoding::bool::merge, bool),
            field_type => {
                return Err(DecodeError::new(format!(
                    "unhandled field type: {}",
                    field_type.as_str_name()
                )));
            }
        };

        Ok(())
    }
}
