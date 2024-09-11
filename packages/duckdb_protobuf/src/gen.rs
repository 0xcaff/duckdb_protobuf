use crate::read::{ColumnKey, VectorAccessor};
use crate::varint::{decode_varint, DecodeVarint, IncorrectVarintError};
use anyhow::format_err;
use prost_reflect::{Cardinality, Kind, MessageDescriptor};
use protobuf::rt::WireType;
use std::collections::hash_map::Entry;
use std::collections::HashMap;

pub struct LocalRepeatedFieldsState {
    state: HashMap<u32, LocalRepeatedFieldState>,
}

impl LocalRepeatedFieldsState {
    pub fn new() -> LocalRepeatedFieldsState {
        LocalRepeatedFieldsState {
            state: HashMap::new(),
        }
    }
}

struct LocalRepeatedFieldState {
    length: u64,
    offset: u64,
}

pub struct ParseContext<'a> {
    bytes: &'a [u8],
    parser_state: &'a mut ParserState,
}

pub struct ParserState {
    column_state: HashMap<ColumnKey, u64>,
}

impl ParserState {
    pub fn new() -> ParserState {
        ParserState {
            column_state: Default::default(),
        }
    }
}

impl ParseContext<'_> {
    pub fn new<'a>(bytes: &'a [u8], parser_state: &'a mut ParserState) -> ParseContext<'a> {
        ParseContext {
            bytes,
            parser_state,
        }
    }
}

impl ParseContext<'_> {
    #[inline]
    pub fn consume(&mut self, n: usize) {
        self.bytes = &self.bytes[n..];
    }

    pub fn next(&mut self, limit: usize) -> ParseContext {
        ParseContext {
            bytes: &self.bytes[..limit],
            parser_state: self.parser_state,
        }
    }

    #[inline]
    pub fn read_varint<D: DecodeVarint>(&mut self) -> Result<Option<D>, IncorrectVarintError> {
        let Some((value, consumed)) = decode_varint::<D>(self.bytes)? else {
            return Ok(None);
        };

        self.consume(consumed);

        Ok(Some(value))
    }

    #[inline]
    pub fn must_read_varint<D: DecodeVarint>(&mut self) -> anyhow::Result<D> {
        Ok(self
            .read_varint::<D>()?
            .ok_or_else(|| format_err!("unexpected eof"))?)
    }

    pub fn skip_tag(&mut self, tag: u32) -> anyhow::Result<()> {
        let wire_type_value = tag & 0b111;
        let Some(wire_type) = WireType::new(wire_type_value) else {
            return Err(format_err!("unknown wire type {:#b}", wire_type_value));
        };

        self.skip_wire_type(wire_type)?;

        Ok(())
    }

    fn skip_wire_type(&mut self, wire_type: WireType) -> anyhow::Result<()> {
        match wire_type {
            WireType::Varint => {
                self.read_varint::<u64>()?;
            }
            WireType::Fixed64 => self.consume(8),
            WireType::Fixed32 => self.consume(4),
            WireType::LengthDelimited => {
                let len = self.must_read_varint::<u64>()?;
                self.consume(len as _);
            }
            WireType::StartGroup | WireType::EndGroup => {
                return Err(format_err!("sgroup and egroup not implemented"));
            }
        }

        Ok(())
    }

    pub fn read_string(
        &mut self,
        output_vector: duckdb::ffi::duckdb_vector,
        row_idx: usize,
    ) -> anyhow::Result<()> {
        let len = self.must_read_varint::<u64>()? as usize;

        unsafe {
            duckdb::ffi::duckdb_vector_assign_string_element_len(
                output_vector,
                row_idx as u64,
                self.bytes[..len].as_ptr() as _,
                len as _,
            );
        };

        self.consume(len);

        Ok(())
    }

    pub fn read_fixed_bytes<const N: usize>(
        &mut self,
        output_vector: duckdb::ffi::duckdb_vector,
        row_idx: usize,
    ) -> anyhow::Result<()> {
        unsafe {
            let value = duckdb::ffi::duckdb_vector_get_data(output_vector)
                .cast::<[u8; N]>()
                .add(row_idx as _);

            (*value).clone_from_slice(&self.bytes[0..N]);
            self.consume(N);
        };

        Ok(())
    }

    pub fn read_varint_value<D: DecodeVarint>(
        &mut self,
        output_vector: duckdb::ffi::duckdb_vector,
        row_idx: usize,
    ) -> anyhow::Result<()> {
        let value = self.must_read_varint::<D>()?;

        unsafe {
            let ptr = duckdb::ffi::duckdb_vector_get_data(output_vector)
                .cast::<D>()
                .add(row_idx as _);
            *ptr = value;
        };

        Ok(())
    }

    pub fn read_bool_value(
        &mut self,
        output_vector: duckdb::ffi::duckdb_vector,
        row_idx: usize,
    ) -> anyhow::Result<()> {
        let value = self.must_read_varint::<u64>()?;

        unsafe {
            let ptr = duckdb::ffi::duckdb_vector_get_data(output_vector)
                .cast::<bool>()
                .add(row_idx as _);
            *ptr = value != 0;
        };

        Ok(())
    }

    pub fn handle_repeated_field(
        &mut self,
        local_repeated_field_state: &mut LocalRepeatedFieldsState,
        field_idx: u32,
        output_vector: duckdb::ffi::duckdb_vector,
        row_idx: usize,
        column_key: &ColumnKey,
        func: impl FnOnce(
            &mut ParseContext,
            &ColumnKey,
            duckdb::ffi::duckdb_vector,
            usize,
        ) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let local_field_state = local_repeated_field_state.state.entry(field_idx);
        let (offset, length) = match local_field_state {
            Entry::Occupied(mut it) => {
                let value = it.get_mut();
                value.length += 1;
                (value.offset, value.length)
            }
            Entry::Vacant(..) => (
                self.parser_state
                    .column_state
                    .get(&column_key)
                    .map(|it| *it)
                    .unwrap_or_default(),
                1,
            ),
        };

        let list_entry = unsafe {
            &mut *duckdb::ffi::duckdb_vector_get_data(output_vector)
                .cast::<duckdb::ffi::duckdb_list_entry>()
                .add(row_idx)
        };

        list_entry.offset = offset;
        list_entry.length = length;

        let new_root_length = offset + length;
        unsafe { duckdb::ffi::duckdb_list_vector_reserve(output_vector, new_root_length) };
        unsafe { duckdb::ffi::duckdb_list_vector_set_size(output_vector, new_root_length) };

        let child_vector = unsafe { duckdb::ffi::duckdb_list_vector_get_child(output_vector) };

        func(self, &column_key, child_vector, (new_root_length - 1) as _)
    }

    pub fn consume_local_fields(
        &mut self,
        column_key: &ColumnKey,
        local_repeated_field_state: LocalRepeatedFieldsState,
    ) {
        for (field_idx, field_state) in local_repeated_field_state.state.into_iter() {
            let column_key = column_key.field(field_idx as _);
            self.parser_state
                .column_state
                .insert(column_key, field_state.offset + field_state.length);
        }
    }
}

pub fn parse_message(
    descriptor: &MessageDescriptor,
    ctx: &mut ParseContext,
    row_idx: usize,
    column_key: &ColumnKey,
    target: &impl VectorAccessor,
) -> anyhow::Result<()> {
    let mut local_repeated_fields_state = LocalRepeatedFieldsState::new();

    while let Some(tag) = ctx.read_varint::<u32>()? {
        let field_number = tag >> 3;
        let Some(field) = descriptor.get_field(field_number) else {
            ctx.skip_tag(tag)?;
            continue;
        };

        let (field_idx, _) = descriptor
            .fields()
            .enumerate()
            .find(|(a, v)| v == &field)
            .unwrap();

        let output_vector = target.get_vector(field_idx);
        let column_key = column_key.field(field_number);

        match field.cardinality() {
            Cardinality::Optional | Cardinality::Required => {
                if !parse_field(ctx, row_idx, &column_key, output_vector, field.kind())? {
                    ctx.skip_tag(tag)?;
                }
            }
            Cardinality::Repeated => ctx.handle_repeated_field(
                &mut local_repeated_fields_state,
                field_number,
                output_vector,
                row_idx,
                &column_key,
                |ctx, column_key, output_vector, row_idx| {
                    if !parse_field(ctx, row_idx, &column_key, output_vector, field.kind())? {
                        ctx.skip_tag(tag)?;
                    };

                    Ok(())
                },
            )?,
        }
    }

    ctx.consume_local_fields(column_key, local_repeated_fields_state);

    Ok(())
}

fn parse_field(
    ctx: &mut ParseContext,
    row_idx: usize,
    column_key: &ColumnKey,
    output_vector: duckdb::ffi::duckdb_vector,
    kind: Kind,
) -> anyhow::Result<bool> {
    match kind {
        Kind::Message(message) => {
            let target = unsafe { crate::read::StructVector::new(output_vector) };
            let len = ctx.must_read_varint::<u64>()?;

            parse_message(
                &message,
                &mut ctx.next(len as _),
                row_idx,
                &column_key,
                &target,
            )?;

            ctx.consume(len as _);
        }
        Kind::Enum(descriptor) => {
            let value = ctx.must_read_varint::<u32>()? as i32;

            let value_descriptor = descriptor
                .get_value(value)
                .unwrap_or_else(|| descriptor.default_value());

            let name = value_descriptor.name();

            let name_bytes = name.as_bytes();

            unsafe {
                duckdb::ffi::duckdb_vector_assign_string_element_len(
                    output_vector,
                    row_idx as u64,
                    name_bytes.as_ptr() as _,
                    name_bytes.len() as _,
                );
            }
        }
        Kind::String => {
            ctx.read_string(output_vector, row_idx)?;
        }
        Kind::Double => {
            ctx.read_fixed_bytes::<8>(output_vector, row_idx)?;
        }
        Kind::Float => {
            ctx.read_fixed_bytes::<4>(output_vector, row_idx)?;
        }
        Kind::Int64 | Kind::Uint64 => {
            ctx.read_varint_value::<u64>(output_vector, row_idx)?;
        }
        Kind::Int32 | Kind::Uint32 => {
            ctx.read_varint_value::<u32>(output_vector, row_idx)?;
        }
        Kind::Bool => {
            ctx.read_bool_value(output_vector, row_idx)?;
        }
        _ => return Ok(false),
    };

    Ok(true)
}
