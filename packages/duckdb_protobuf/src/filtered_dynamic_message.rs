use prost::bytes::{Buf, BufMut};
use prost::encoding::{DecodeContext, WireType};
use prost::{DecodeError, Message};
use prost_reflect::{DynamicMessage, UnknownField};
use std::collections::HashSet;

#[derive(Debug, Clone)]
pub struct FilteredDynamicMessage {
    message: DynamicMessage,
    accepted_fields: HashSet<u32>,
}

impl FilteredDynamicMessage {
    pub fn new(message: DynamicMessage, accepted_fields: HashSet<u32>) -> FilteredDynamicMessage {
        FilteredDynamicMessage {
            message,
            accepted_fields,
        }
    }

    pub fn into(self) -> DynamicMessage {
        self.message
    }
}

impl Message for FilteredDynamicMessage {
    fn encode_raw(&self, buf: &mut impl BufMut)
    where
        Self: Sized,
    {
        self.message.encode_raw(buf)
    }

    fn merge_field(
        &mut self,
        number: u32,
        wire_type: WireType,
        buf: &mut impl Buf,
        ctx: DecodeContext,
    ) -> Result<(), DecodeError>
    where
        Self: Sized,
    {
        if !self.accepted_fields.contains(&number) {
            let _field = UnknownField::decode_value(number, wire_type, buf, ctx)?;
            return Ok(());
        }

        self.message.merge_field(number, wire_type, buf, ctx)
    }

    fn encoded_len(&self) -> usize {
        self.message.encoded_len()
    }

    fn clear(&mut self) {
        self.message.clear()
    }
}
