use anyhow::{format_err, Context};
use crossbeam::queue::ArrayQueue;
use duckdb::vtab::{
    BindInfo, DataChunk, Free, FunctionInfo, InitInfo, LogicalType, LogicalTypeId, VTab,
    VTabLocalData,
};
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;

use crate::io::{parse, DelimitedLengthKind, LengthDelimitedRecordsReader, LengthKind};
use crate::read::write_to_output;
use crate::types::into_logical_type;

pub struct Parameters {
    pub files: String,
    pub descriptor_bytes: Vec<u8>,
    pub message_name: String,
    pub shared_message_descriptor: MessageDescriptor,
    pub length_kind: LengthKind,
}

impl Parameters {
    pub fn from_bind_info(bind: &BindInfo) -> Result<Self, anyhow::Error> {
        let files = bind
            .get_named_parameter("files")
            .ok_or_else(|| format_err!("missing argument `files`"))?
            .to_string();

        let descriptor = bind
            .get_named_parameter("descriptors")
            .ok_or_else(|| format_err!("missing parameter `descriptor`"))?
            .to_string();

        let descriptor_bytes = (|| -> Result<Vec<u8>, anyhow::Error> {
            let mut file = File::open(descriptor)?;
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer)?;

            Ok(buffer)
        })()
        .with_context(|| format_err!("field `descriptors`"))?;

        let shared_descriptor_pool = DescriptorPool::decode(descriptor_bytes.as_slice())?;

        let message_name = bind
            .get_named_parameter("message_type")
            .ok_or_else(|| format_err!("missing parameter `message_type`"))?
            .to_string();

        let message_descriptor = shared_descriptor_pool
            .get_message_by_name(&message_name.as_str())
            .ok_or_else(|| format_err!("message type not found in `descriptor`"))?;

        let length_kind = bind
            .get_named_parameter("delimiter")
            .ok_or_else(|| format_err!("missing parameter `delimiter`"))?;

        let length_kind = parse::<LengthKind>(&length_kind.to_string())
            .map_err(|err| format_err!("when parsing parameter delimiter: {}", err))?;

        Ok(Self {
            files,
            descriptor_bytes,
            message_name,
            shared_message_descriptor: message_descriptor,
            length_kind,
        })
    }

    pub fn message_descriptor(&self) -> Result<MessageDescriptor, anyhow::Error> {
        let descriptor_pool =
            DescriptorPool::decode(self.descriptor_bytes.as_slice())?;

        let message_descriptor = descriptor_pool
            .get_message_by_name(&self.message_name)
            .unwrap();

        Ok(message_descriptor)
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
            (
                "delimiter".to_string(),
                LogicalType::new(LogicalTypeId::Varchar),
            ),
        ]
    }
}

pub struct GlobalState {
    queue: ArrayQueue<PathBuf>,
}

impl GlobalState {
    pub fn new(params: &Parameters) -> Result<GlobalState, anyhow::Error> {
        let tasks = {
            let mut tasks = vec![];
            let items = glob::glob(params.files.as_str())?;
            for item in items {
                let item = item?;
                tasks.push(item);
            }

            tasks
        };

        let queue = {
            let queue = ArrayQueue::new(tasks.len());

            for item in tasks {
                queue.push(item).unwrap();
            }

            queue
        };

        Ok(GlobalState { queue })
    }
}

pub struct ProtobufVTab;

impl VTab for ProtobufVTab {
    type InitData = Handle<GlobalState>;
    type BindData = Handle<Parameters>;

    unsafe fn bind(
        bind: &BindInfo,
        data: *mut Self::BindData,
    ) -> duckdb::Result<(), Box<dyn Error>> {
        Ok(Self::bind(bind, data).map_err(format_error_with_causes)?)
    }

    unsafe fn init(
        init_info: &InitInfo,
        data: *mut Self::InitData,
    ) -> duckdb::Result<(), Box<dyn Error>> {
        Ok(Self::init(init_info, data).map_err(format_error_with_causes)?)
    }

    unsafe fn func(
        func: &FunctionInfo,
        output: &mut DataChunk,
    ) -> duckdb::Result<(), Box<dyn Error>> {
        Ok(Self::func(func, output).map_err(format_error_with_causes)?)
    }

    fn named_parameters() -> Option<Vec<(String, LogicalType)>> {
        Some(Parameters::values())
    }
}

impl ProtobufVTab {
    fn bind(bind: &BindInfo, data: *mut <Self as VTab>::BindData) -> Result<(), anyhow::Error> {
        let data = unsafe { &mut *data };

        let params = Parameters::from_bind_info(bind)?;

        for field_descriptor in params.shared_message_descriptor.fields() {
            bind.add_result_column(
                field_descriptor.name().as_ref(),
                into_logical_type(&field_descriptor)?,
            );
        }

        data.assign(params);

        Ok(())
    }

    fn init(
        init_info: &InitInfo,
        data: *mut <Self as VTab>::InitData,
    ) -> Result<(), anyhow::Error> {
        let data = unsafe { &mut *data };
        let bind_data = unsafe { &*init_info.get_bind_data::<<Self as VTab>::BindData>() };

        let new_global_state = GlobalState::new(bind_data)?;
        init_info.set_max_threads(new_global_state.queue.len() as _);
        data.assign(new_global_state);

        Ok(())
    }

    fn func(func: &FunctionInfo, output: &mut DataChunk) -> duckdb::Result<(), anyhow::Error> {
        let bind_data = unsafe { &mut *func.get_bind_data::<<Self as VTab>::BindData>() };
        let init_data = unsafe { &mut *func.get_init_data::<<Self as VTab>::InitData>() };
        let local_init_data =
            unsafe { &mut *func.get_local_init_data::<<Self as VTabLocalData>::LocalInitData>() };

        let parameters: &Parameters = bind_data.deref();

        let local_descriptor = local_init_data.local_descriptor.clone();

        let mut state_container = StateContainer {
            local_state: local_init_data,
            global_state: init_data,
            parameters,
        };

        let available_chunk_size = output.flat_vector(0).capacity();
        let mut items = 0;

        let mut column_information = Default::default();

        for output_row_idx in 0..available_chunk_size {
            let bytes = match state_container.next_message()? {
                None => break,
                Some(bytes) => bytes,
            };

            let message =
                DynamicMessage::decode(local_descriptor.clone(), bytes.as_slice())?;

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
}

struct StateContainer<'a> {
    local_state: &'a mut LocalState,
    global_state: &'a GlobalState,
    parameters: &'a Parameters,
}

impl StateContainer<'_> {
    fn next_message(&mut self) -> Result<Option<Vec<u8>>, anyhow::Error> {
        let file_reader = if let Some(reader) = &mut self.local_state.current {
            reader
        } else {
            let Some(next_file_path) = self.global_state.queue.pop() else {
                return Ok(None);
            };

            let mut next_file = File::open(&next_file_path)?;
            match self.parameters.length_kind {
                LengthKind::BigEndianFixed => {
                    self.local_state.current = Some(LengthDelimitedRecordsReader::create(
                        next_file,
                        DelimitedLengthKind::BigEndianFixed,
                    ));

                    self.local_state.current.as_mut().unwrap()
                }
                LengthKind::Varint => {
                    self.local_state.current = Some(LengthDelimitedRecordsReader::create(
                        next_file,
                        DelimitedLengthKind::Varint,
                    ));

                    self.local_state.current.as_mut().unwrap()
                }
                LengthKind::SingleMessagePerFile => {
                    let mut bytes = Vec::new();
                    next_file.read_to_end(&mut bytes)?;
                    return Ok(Some(bytes));
                }
            }
        };

        let Some(next_message) = file_reader.try_get_next()? else {
            self.local_state.current = None;
            return Ok(None);
        };

        Ok(Some(next_message))
    }
}

#[repr(C)]
pub struct LocalState {
    current: Option<LengthDelimitedRecordsReader>,
    local_descriptor: MessageDescriptor,
}

impl VTabLocalData for ProtobufVTab {
    type LocalInitData = Handle<LocalState>;

    fn local_init(
        init_info: &InitInfo,
        data: *mut Self::LocalInitData,
    ) -> duckdb::Result<(), Box<dyn Error>> {
        let bind_data = unsafe { &*init_info.get_bind_data::<<Self as VTab>::BindData>() };
        let local_descriptor = bind_data.message_descriptor()?;

        let data = unsafe { &mut *data };

        data.assign(LocalState {
            current: None,
            local_descriptor: local_descriptor,
        });

        Ok(())
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

fn format_error_with_causes(error: anyhow::Error) -> anyhow::Error {
    format_err!(
        "{}",
        error
            .chain()
            .map(|cause| cause.to_string())
            .collect::<Vec<_>>()
            .join(": ")
    )
}
