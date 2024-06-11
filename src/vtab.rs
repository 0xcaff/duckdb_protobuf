use crate::descriptors::FileDescriptorSetExt;
use crate::io::RecordsReader;
use duckdb::vtab::{
    BindInfo, DataChunk, Free, FunctionInfo, InitInfo, LogicalType, LogicalTypeId, VTab,
};
use prost::encoding::DecodeContext;
use prost::Message;
use prost_types::{DescriptorProto, FileDescriptorSet};
use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::ops::{Deref, DerefMut};
use crate::read::{ColumnKey, into_logical_type, ProtobufMessageWriter};

pub struct Parameters {
    pub files: String,
    pub message_descriptor: DescriptorProto,
    pub descriptors: FileDescriptorSet,
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

        let message_descriptor = descriptors
            .message_matching(&message_name.as_str())
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

pub struct ProtobufVTab;

impl VTab for ProtobufVTab {
    type InitData = Handle<RecordsReader>;
    type BindData = Handle<Parameters>;

    fn bind(bind: &BindInfo, data: *mut Self::BindData) -> duckdb::Result<(), Box<dyn Error>> {
        let data = unsafe { &mut *data };

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

        let mut column_information = Default::default();

        for output_row_idx in 0..available_chunk_size {
            let bytes = match init_data.next_message()? {
                None => break,
                Some(bytes) => bytes,
            };

            let message_descriptor = &parameters.message_descriptor;

            let mut protobuf_message_writer = ProtobufMessageWriter {
                seen_repeated_fields: Default::default(),
                base_column_key: ColumnKey::empty(),
                column_information: &mut column_information,
                descriptors: &parameters.descriptors,
                message_descriptor,
                output_row_idx,
                output,
            };

            let decode_context = DecodeContext::default();

            protobuf_message_writer.merge(bytes.as_slice(), decode_context)?;

            items += 1;
        }

        output.set_len(items);

        Ok(())
    }

    fn named_parameters() -> Option<Vec<(String, LogicalType)>> {
        Some(Parameters::values())
    }
}

#[repr(C)]
pub struct Handle<T> {
    inner: *mut T,
}

impl<T> Handle<T> {
    pub fn assign(&mut self, inner: T) {
        self.inner = Box::into_raw(Box::new(inner));
    }
}

impl<T> Deref for Handle<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        if self.inner.is_null() {
            panic!("unable to deref non-null handle")
        }

        unsafe { &*self.inner }
    }
}

impl<T> DerefMut for Handle<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.inner }
    }
}

impl<T> Free for Handle<T> {
    fn free(&mut self) {
        unsafe {
            if self.inner.is_null() {
                return;
            }

            drop(Box::from_raw(self.inner));
        }
    }
}
