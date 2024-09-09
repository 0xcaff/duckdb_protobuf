use crate::read::{ColumnKey, ColumnKeyElement, VectorAccessor};
use crate::varint::{decode_varint, DecodeVarint, IncorrectVarintError};
use anyhow::format_err;
use protobuf::rt::WireType;
use std::collections::HashMap;

include!(concat!(env!("OUT_DIR"), "/generated.rs"));

struct ParseContext<'a> {
    bytes: &'a [u8],
    parser_state: &'a mut ParserState,
    current_key: ColumnKey,
}

struct ParserState {
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
            current_key: ColumnKey::empty(),
        }
    }
}

impl<'a> ParseContext<'a> {
    #[inline]
    pub fn consume(&mut self, n: usize) {
        self.bytes = &self.bytes[n..];
    }

    pub fn next(&mut self, limit: usize, field_tag: usize) -> ParseContext {
        ParseContext {
            bytes: &self.bytes[..limit],
            parser_state: self.parser_state,
            // todo: remove the cast
            current_key: self.current_key.field(field_tag as _),
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
            duckdb::ffi::duckdb_vector_assign_string_element(
                output_vector,
                row_idx as u64,
                self.bytes[..len].as_ptr() as _,
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
        output_vector: duckdb::ffi::duckdb_vector,
        row_idx: usize,
        func: impl FnOnce(&mut ParseContext, duckdb::ffi::duckdb_vector, usize) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let column_key = self.current_key.extending(ColumnKeyElement::List);

        let list_entry = unsafe {
            &mut *duckdb::ffi::duckdb_vector_get_data(output_vector)
                .cast::<duckdb::ffi::duckdb_list_entry>()
                .add(row_idx)
        };

        let next_offset_ref = self.parser_state.column_state.get_mut(&column_key);
        let (next_offset, next_length) = if let Some(it) = &next_offset_ref {
            (**it, list_entry.length + 1)
        } else {
            (0, 1)
        };

        list_entry.offset = next_offset;
        list_entry.length = next_length;

        let new_next_offset = next_offset + next_length;

        if let Some(it) = next_offset_ref {
            *it = new_next_offset;
        } else {
            self.parser_state
                .column_state
                .insert(column_key.clone(), new_next_offset);
        }

        let new_length = new_next_offset;
        unsafe { duckdb::ffi::duckdb_list_vector_reserve(output_vector, new_length) };
        unsafe { duckdb::ffi::duckdb_list_vector_set_size(output_vector, new_length) };

        let child_vector = unsafe { duckdb::ffi::duckdb_list_vector_get_child(output_vector) };

        let mut context = ParseContext {
            bytes: &self.bytes,
            parser_state: self.parser_state,
            current_key: column_key,
        };

        func(&mut context, child_vector, (next_offset + next_length - 1) as _)?;

        Ok(())
    }
}

trait ParseIntoDuckDB {
    fn parse(
        ctx: &mut ParseContext,
        row_idx: usize,
        target: &impl VectorAccessor,
    ) -> anyhow::Result<()>;
}
