use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::ops::{Deref, DerefMut};

use duckdb::vtab::{
    BindInfo, DataChunk, Free, FunctionInfo, InitInfo, LogicalType, LogicalTypeId, VTab,
};
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};

use crate::io::RecordsReader;
use crate::read::write_to_output;
use crate::types::into_logical_type;

pub struct Parameters {
    pub files: String,
    pub message_descriptor: MessageDescriptor,
    pub pool: DescriptorPool,
}

impl Parameters {
    pub fn from_bind_info(bind: &BindInfo) -> Result<Self, Box<dyn Error>> {
        let files = bind
            .get_named_parameter("files")
            .ok_or("missing argument files")?
            .to_string();

        let pool = {
            let descriptor = bind
                .get_named_parameter("descriptors")
                .ok_or("missing argument descriptor")?
                .to_string();

            let mut file = File::open(descriptor)?;
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer)?;

            DescriptorPool::decode(buffer.as_slice())?
        };

        let message_name = bind
            .get_named_parameter("message_type")
            .ok_or("missing argument message_type")?
            .to_string();

        let message_descriptor = pool
            .get_message_by_name(&message_name.as_str())
            .ok_or("message type not found in descriptor")?;

        Ok(Self {
            files,
            message_descriptor,
            pool,
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

        for field_descriptor in params.message_descriptor.fields() {
            bind.add_result_column(
                field_descriptor.name().as_ref(),
                into_logical_type(&field_descriptor)?,
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

            let message =
                DynamicMessage::decode(parameters.message_descriptor.clone(), bytes.as_slice())?;

            write_to_output(
                &mut column_information,
                &message,
                output,
                available_chunk_size,
                output_row_idx,
            )?;

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
