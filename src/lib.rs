use core::fmt;
use std::collections::HashSet;
use std::error::Error;
use std::ffi::{c_char, c_void};
use std::fmt::Formatter;
use std::fs::File;
use std::io;
use std::io::Read;
use std::ops::{Deref, DerefMut};

use byteorder::{BigEndian, ReadBytesExt};
use duckdb::Connection;
use duckdb::ffi;
use duckdb::ffi::{
    duckdb_list_entry, duckdb_list_vector_get_child, duckdb_list_vector_reserve,
    duckdb_list_vector_set_size, duckdb_vector,
};
use duckdb::vtab::{BindInfo, DataChunk, FlatVector, Free, FunctionInfo, InitInfo, Inserter, LogicalType, LogicalTypeId, VTab};
use duckdb_loadable_macros::duckdb_entrypoint;
use prost::{DecodeError, Message};
use prost::bytes::{Buf, BufMut};
use prost::encoding::{DecodeContext, message, WireType};
use prost_types::{DescriptorProto, EnumDescriptorProto, FieldDescriptorProto, FileDescriptorSet};
use prost_types::field_descriptor_proto::{Label, Type};

struct ProtobufVTab;

#[repr(C)]
struct Handle<T> {
    inner: *mut T,
}

impl <T> Handle<T> {
    pub fn assign(&mut self, inner: T) {
        self.inner = Box::into_raw(Box::new(inner));
    }
}

impl <T> Deref for Handle<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        if self.inner.is_null() {
            panic!("unable to deref non-null handle")
        }

        unsafe {
            &*self.inner
        }
    }
}

impl <T> DerefMut for Handle<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            &mut *self.inner
        }
    }
}

impl <T> Free for Handle<T> {
    fn free(&mut self) {
        unsafe {
            if self.inner.is_null() {
                return;
            }

            drop(Box::from_raw(self.inner));
        }
    }
}

struct RecordsReader {
    files_iterator: glob::Paths,
    current_file: Option<LengthDelimitedRecordsReader<File>>,
}

impl RecordsReader {
    pub fn new(params: &Parameters) -> Result<RecordsReader, Box<dyn Error>> {
        Ok(RecordsReader {
            files_iterator: glob::glob(params.files.as_str())?,
            current_file: None,
        })
    }

    fn next_message(&mut self) -> Result<Option<Vec<u8>>, Box<dyn Error>> {
        let file_reader = if let Some(reader) = &mut self.current_file {
            reader
        } else {
            let Some(next_file_path) = self.files_iterator.next() else {
                return Ok(None);
            };

            let next_file_path = next_file_path?;
            let next_file = File::open(&next_file_path)?;

            self.current_file = Some(LengthDelimitedRecordsReader::new(next_file));

            self.current_file.as_mut().unwrap()
        };

        let Some(next_message) = file_reader.try_get_next()? else {
            self.current_file = None;
            return Ok(None);
        };

        Ok(Some(next_message))
    }
}

struct LengthDelimitedRecordsReader<R: io::Read> {
    reader: R,
}

impl<R> LengthDelimitedRecordsReader<R>
where
    R: io::Read,
{
    pub fn new(reader: R) -> Self {
        Self { reader }
    }

    pub fn get_next(&mut self) -> Result<Vec<u8>, io::Error> {
        let len = self.reader.read_u32::<BigEndian>()?;
        let mut buf = vec![0; len as usize];
        self.reader.read_exact(&mut buf)?;

        Ok(buf)
    }

    pub fn try_get_next(&mut self) -> Result<Option<Vec<u8>>, io::Error> {
        match self.get_next() {
            Ok(it) => Ok(Some(it)),
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
            Err(err) => Err(err.into()),
        }
    }
}

struct Parameters {
    files: String,
    message_descriptor: DescriptorProto,
    descriptors: FileDescriptorSet,
}

pub fn get_message_matching<'a>(
    descriptor_set: &'a FileDescriptorSet,
    name: &str,
) -> Option<&'a DescriptorProto> {
    for file_descriptor in &descriptor_set.file {
        let package_name = file_descriptor.package();

        for message_descriptor in &file_descriptor.message_type {
            if &format!("{}.{}", package_name, message_descriptor.name()) == name {
                return Some(message_descriptor);
            }
        }
    }

    None
}

pub fn get_enum_matching<'a>(
    descriptor_set: &'a FileDescriptorSet,
    name: &str,
) -> Option<&'a EnumDescriptorProto> {
    for file_descriptor in &descriptor_set.file {
        let package_name = file_descriptor.package();

        for enum_descriptor in &file_descriptor.enum_type {
            if &format!("{}.{}", package_name, enum_descriptor.name()) == name {
                return Some(enum_descriptor);
            }
        }
    }

    None
}

impl Parameters {
    pub fn from_bind_info(bind: &BindInfo) -> Result<Self, Box<dyn Error>> {
        let files = bind
            .get_named_parameter("files")
            .ok_or("missing argument files")?
            .to_string();

        let descriptors = {
            let descriptor = bind
                .get_named_parameter("descriptors")
                .ok_or("missing argument descriptor")?
                .to_string();

            let mut file = File::open(descriptor)?;
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer)?;

            FileDescriptorSet::decode(buffer.as_slice())?
        };

        let message_name = bind
            .get_named_parameter("message_type")
            .ok_or("missing argument message_type")?
            .to_string();

        let message_descriptor = get_message_matching(&descriptors, &message_name.as_str())
            .ok_or("message type not found in descriptor")?;

        Ok(Self {
            files,
            message_descriptor: message_descriptor.clone(),
            descriptors,
        })
    }

    pub fn values() -> Vec<(String, LogicalType)> {
        vec![
            (
                "files".to_string(),
                LogicalType::new(LogicalTypeId::Varchar),
            ),
            (
                "message_type".to_string(),
                LogicalType::new(LogicalTypeId::Varchar),
            ),
            (
                "descriptors".to_string(),
                LogicalType::new(LogicalTypeId::Varchar),
            ),
        ]
    }
}

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

pub fn get_type_name(field: &FieldDescriptorProto) -> Result<&str, String> {
    let type_name = field.type_name();
    let (prefix, absolute_type_name) = type_name.split_at(1);
    if prefix != "." {
        return Err(format!("invalid type name: {}", type_name));
    }

    Ok(absolute_type_name)
}

pub fn into_logical_type_single(
    field: &FieldDescriptorProto,
    descriptors: &FileDescriptorSet,
) -> Result<LogicalType, Box<dyn Error>> {
    let value = match field.r#type() {
        Type::Message => {
            let type_name = get_type_name(field)?;

            let message_descriptor = get_message_matching(descriptors, type_name)
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

impl VTab for ProtobufVTab {
    type InitData = Handle<RecordsReader>;
    type BindData = Handle<Parameters>;

    fn bind(bind: &BindInfo, data: *mut Self::BindData) -> duckdb::Result<(), Box<dyn Error>> {
        let data = unsafe {&mut *data};

        let params = Parameters::from_bind_info(bind)?;

        for field_descriptor in &params.message_descriptor.field {
            bind.add_result_column(
                field_descriptor.json_name().as_ref(),
                into_logical_type(field_descriptor, &params.descriptors)?,
            );
        }

        data.assign(params);

        Ok(())
    }

    fn init(init_info: &InitInfo, data: *mut Self::InitData) -> duckdb::Result<(), Box<dyn Error>> {
        let data = unsafe { &mut *data };
        let bind_data = unsafe { &*init_info.get_bind_data::<Self::BindData>() };

        data.assign(RecordsReader::new(&bind_data)?);

        Ok(())
    }

    fn func(func: &FunctionInfo, output: &mut DataChunk) -> duckdb::Result<(), Box<dyn Error>> {
        let bind_data = unsafe { &mut *func.get_bind_data::<Self::BindData>() };
        let init_data = unsafe { &mut *func.get_init_data::<Self::InitData>() };

        let parameters: &Parameters = bind_data.deref();

        let available_chunk_size = output.flat_vector(0).capacity();
        let mut items = 0;

        for output_row_idx in 0..available_chunk_size {
            let bytes = match init_data.next_message()? {
                None => break,
                Some(bytes) => bytes,
            };

            let message_descriptor = &parameters.message_descriptor;

            let mut protobuf_message_writer = ProtobufMessageWriter {
                seen_repeated_fields: Default::default(),
                descriptors: &parameters.descriptors,
                message_descriptor,
                output_row_idx,
                output,
            };

            protobuf_message_writer.merge(bytes.as_slice())?;

            items += 1;
        }

        output.set_len(items);

        Ok(())
    }

    fn named_parameters() -> Option<Vec<(String, LogicalType)>> {
        Some(Parameters::values())
    }
}

struct ProtobufMessageWriter<'a, V: VectorAccessor> {
    seen_repeated_fields: HashSet<usize>,
    descriptors: &'a FileDescriptorSet,
    message_descriptor: &'a DescriptorProto,
    output: &'a V,
    output_row_idx: usize,
}

unsafe impl<V: VectorAccessor> Sync for ProtobufMessageWriter<'_, V> {}
unsafe impl<V: VectorAccessor> Send for ProtobufMessageWriter<'_, V> {}

impl<V: VectorAccessor> fmt::Debug for ProtobufMessageWriter<'_, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProtobufMessageWriter")
            .finish_non_exhaustive()
    }
}

trait VectorAccessor {
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

impl<V: VectorAccessor> ProtobufMessageWriter<'_, V> {
    fn merge_dynamic_field<B: Buf>(
        &self,
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

                let message_type_name =
                    get_type_name(field).map_err(|err| DecodeError::new(err))?;

                let mut writer = ProtobufMessageWriter {
                    seen_repeated_fields: Default::default(),
                    descriptors: self.descriptors,
                    message_descriptor: get_message_matching(&self.descriptors, message_type_name)
                        .ok_or_else(|| {
                            DecodeError::new(format!(
                                "message type not found in descriptor: {}",
                                message_type_name
                            ))
                        })?,
                    output_row_idx: row_idx,
                    output: &output,
                };

                message::merge(
                    WireType::LengthDelimited,
                    &mut writer,
                    buf,
                    DecodeContext::default(),
                )?;
            }
            Type::Enum => {
                let enum_type_name = get_type_name(field).map_err(|err| DecodeError::new(err))?;

                let enum_descriptor = get_enum_matching(&self.descriptors, enum_type_name)
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

impl<V> prost::Message for ProtobufMessageWriter<'_, V>
where
    V: VectorAccessor,
{
    fn encode_raw<B>(&self, _buf: &mut B)
    where
        B: BufMut,
        Self: Sized,
    {
        unimplemented!("encode_raw not implemented for protobuf message writer");
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

                fn find_next_offset(
                    items: &[duckdb_list_entry],
                    output_row_idx: usize,
                ) -> Option<u64> {
                    if output_row_idx == 0 {
                        return None;
                    }

                    let Some(it) = items[0..output_row_idx]
                        .iter()
                        .rev()
                        .find(|it| it.length != 0)
                    else {
                        return None;
                    };

                    Some(it.offset + it.length)
                }

                // Whether this repeated field has been seen before when handling this message. Used
                // to initialize list entry values.
                let has_seen = self.seen_repeated_fields.get(&field_idx).is_some();
                self.seen_repeated_fields.insert(field_idx);
                // todo: assumption that first memory value
                if !has_seen {
                    let next_offset =
                        find_next_offset(list_entry_items, self.output_row_idx).unwrap_or(0);
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

                self.merge_dynamic_field(
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
                self.merge_dynamic_field(field, wire_type, buf, ctx, output_vector, row_idx)?;
            }
        };

        Ok(())
    }

    fn encoded_len(&self) -> usize {
        unimplemented!("encoding not implemented for protobuf message writer");
    }

    fn clear(&mut self) {
        unimplemented!("clear not implemented for protobuf message writer")
    }
}

#[duckdb_entrypoint]
fn libduckdb_protobuf_init(conn: Connection) -> Result<(), Box<dyn Error>> {
    conn.register_table_function::<ProtobufVTab>("protobuf")?;

    Ok(())
}
