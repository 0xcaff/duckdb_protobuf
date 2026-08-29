use crate::filtered_dynamic_message::FilteredDynamicMessage;
use crate::io::{parse, DelimitedLengthKind, LengthDelimitedRecordsReader, LengthKind, Record};
use crate::read::{write_to_output, MyFlatVector, VectorAccessor};
use crate::types::into_logical_type;
use anyhow::{format_err, Context};
use crossbeam::queue::ArrayQueue;
use duckdb::{
    core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab, VTabLocalData},
};
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, ReflectMessage};
use std::error::Error;
use std::ffi::CString;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

pub struct Parameters {
    pub files: String,
    pub descriptor_bytes: Vec<u8>,
    pub message_name: String,
    pub shared_message_descriptor: MessageDescriptor,
    pub length_kind: LengthKind,
    pub include_filename: bool,
    pub include_position: bool,
    pub include_size: bool,
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

        let include_filename = bind
            .get_named_parameter("filename")
            .map(|value| value.to_int64() != 0)
            .unwrap_or(false);

        let include_position = bind
            .get_named_parameter("position")
            .map(|value| value.to_int64() != 0)
            .unwrap_or(false);

        let include_size = bind
            .get_named_parameter("size")
            .map(|value| value.to_int64() != 0)
            .unwrap_or(false);

        Ok(Self {
            files,
            descriptor_bytes,
            message_name,
            shared_message_descriptor: message_descriptor,
            length_kind,
            include_filename,
            include_position,
            include_size,
        })
    }

    pub fn message_descriptor(&self) -> Result<MessageDescriptor, anyhow::Error> {
        let descriptor_pool = DescriptorPool::decode(self.descriptor_bytes.as_slice())?;

        let message_descriptor = descriptor_pool
            .get_message_by_name(&self.message_name)
            .unwrap();

        Ok(message_descriptor)
    }

    pub fn values() -> Vec<(String, LogicalTypeHandle)> {
        vec![
            (
                "files".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
            (
                "message_type".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
            (
                "descriptors".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
            (
                "delimiter".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
            (
                "filename".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Boolean),
            ),
            (
                "position".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Boolean),
            ),
            (
                "size".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Boolean),
            ),
        ]
    }
}
pub struct GlobalState {
    queue: ArrayQueue<PathBuf>,
    column_indices: Vec<duckdb::ffi::idx_t>,
}

impl GlobalState {
    pub fn new(
        params: &Parameters,
        column_indices: Vec<duckdb::ffi::idx_t>,
    ) -> Result<GlobalState, anyhow::Error> {
        let tasks = {
            let mut tasks = vec![];
            let items = glob::glob(params.files.as_str())?;
            for item in items {
                let item = item?;
                tasks.push(item);
            }

            tasks
        };

        if tasks.is_empty() {
            return Err(format_err!("no files matching glob found {}", params.files));
        }

        let queue = {
            let queue = ArrayQueue::new(tasks.len());

            for item in tasks {
                queue.push(item).unwrap();
            }

            queue
        };

        Ok(GlobalState {
            queue,
            column_indices,
        })
    }
}

pub struct ProtobufVTab;

impl VTab for ProtobufVTab {
    type InitData = GlobalState;
    type BindData = Parameters;

    fn bind(bind: &BindInfo) -> duckdb::Result<Self::BindData, Box<dyn Error>> {
        Self::bind(bind).map_err(box_error_with_causes)
    }

    fn init(init_info: &InitInfo) -> duckdb::Result<Self::InitData, Box<dyn Error>> {
        Self::init(init_info).map_err(box_error_with_causes)
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> duckdb::Result<(), Box<dyn Error>> {
        Self::func(func, output).map_err(box_error_with_causes)
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(Parameters::values())
    }

    fn supports_pushdown() -> bool {
        true
    }
}

impl ProtobufVTab {
    fn bind(bind: &BindInfo) -> Result<Parameters, anyhow::Error> {
        let params = Parameters::from_bind_info(bind)?;

        for field_descriptor in params.shared_message_descriptor.fields() {
            bind.add_result_column(
                field_descriptor.name().as_ref(),
                into_logical_type(&field_descriptor)?,
            );
        }

        if params.include_filename {
            bind.add_result_column("filename", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        }

        if params.include_position {
            bind.add_result_column("position", LogicalTypeHandle::from(LogicalTypeId::UBigint));
        }

        if params.include_size {
            bind.add_result_column("size", LogicalTypeHandle::from(LogicalTypeId::UBigint));
        }

        Ok(params)
    }

    fn init(init_info: &InitInfo) -> Result<GlobalState, anyhow::Error> {
        let bind_data = unsafe { &*init_info.get_bind_data::<Parameters>() };
        let column_indices = init_info.get_column_indices();

        let new_global_state = GlobalState::new(bind_data, column_indices)?;
        init_info.set_max_threads(new_global_state.queue.len() as _);
        Ok(new_global_state)
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> duckdb::Result<(), anyhow::Error> {
        let parameters = func.get_bind_data();
        let init_data = func.get_init_data();
        let local_init_data =
            unsafe { &mut *func.get_local_init_data::<<Self as VTabLocalData>::LocalInitData>() };

        let local_descriptor = local_init_data.local_descriptor.clone();

        let mut state_container = StateContainer {
            local_state: local_init_data,
            global_state: init_data,
            parameters,
        };

        let available_chunk_size = output.flat_vector(0).capacity();
        let mut items = 0;

        let mut column_information = Default::default();

        let message = {
            let message = DynamicMessage::new(local_descriptor.clone());
            let fields: Vec<_> = local_descriptor.fields().collect();

            let message = FilteredDynamicMessage::new(
                message,
                init_data
                    .column_indices
                    .iter()
                    .filter_map(|it| {
                        let it = *it as usize;
                        if it >= fields.len() {
                            return None;
                        }

                        Some(fields[it].number())
                    })
                    .collect(),
            );

            message
        };

        for output_row_idx in 0..available_chunk_size {
            let StateContainerValue {
                path_reference,
                size,
                bytes,
                position,
            } = match state_container.next_message()? {
                None => break,
                Some(message_info) => message_info,
            };

            let mut message = message.clone();
            message.merge(bytes.as_slice())?;
            let message = message.into();

            write_to_output(
                &init_data.column_indices,
                &mut column_information,
                &message,
                output,
                available_chunk_size,
                output_row_idx,
            )?;

            let mut field_offset = message.descriptor().fields().len();

            if parameters.include_filename {
                if let Some((field_offset, _)) = init_data
                    .column_indices
                    .iter()
                    .enumerate()
                    .find(|(_, it)| (**it as usize) == (field_offset))
                {
                    let it = (|| -> Option<CString> {
                        let value = CString::new(path_reference.path().to_str()?).ok()?;
                        Some(value)
                    })();

                    let column = output.get_vector(field_offset);

                    match it {
                        None => unsafe {
                            let validity = duckdb::ffi::duckdb_vector_get_validity(column);
                            duckdb::ffi::duckdb_validity_set_row_invalid(
                                validity,
                                output_row_idx as _,
                            );
                        },
                        Some(value) => unsafe {
                            duckdb::ffi::duckdb_vector_assign_string_element(
                                column,
                                output_row_idx as _,
                                value.as_ptr(),
                            )
                        },
                    }
                }

                field_offset += 1;
            }

            if parameters.include_position {
                if let Some((field_offset, _)) = init_data
                    .column_indices
                    .iter()
                    .enumerate()
                    .find(|(_, it)| (**it as usize) == (field_offset))
                {
                    let column = output.get_vector(field_offset);
                    let mut vector =
                        unsafe { MyFlatVector::<u64>::with_capacity(column, available_chunk_size) };
                    vector.as_mut_slice()[output_row_idx] = position as _;
                }

                field_offset += 1;
            }

            if parameters.include_size {
                if let Some((field_offset, _)) = init_data
                    .column_indices
                    .iter()
                    .enumerate()
                    .find(|(_, it)| (**it as usize) == (field_offset))
                {
                    let column = output.get_vector(field_offset);
                    let mut vector =
                        unsafe { MyFlatVector::<u64>::with_capacity(column, available_chunk_size) };
                    vector.as_mut_slice()[output_row_idx] = size as _;
                }

                field_offset += 1;
            }

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

enum PathReference<'a> {
    Borrowed(&'a Path),
    Owned(PathBuf),
}

impl<'a> PathReference<'a> {
    pub fn path(&self) -> &Path {
        match self {
            PathReference::Borrowed(it) => *it,
            PathReference::Owned(it) => it.as_path(),
        }
    }
}

struct StateContainerValue<'a> {
    path_reference: PathReference<'a>,
    bytes: Vec<u8>,
    size: usize,
    position: u64,
}

impl StateContainer<'_> {
    fn next_message(&mut self) -> Result<Option<StateContainerValue<'_>>, anyhow::Error> {
        let mut value = match self.local_state.current.take() {
            Some(it) => it,
            None => {
                let Some(next_file_path) = self.global_state.queue.pop() else {
                    return Ok(None);
                };

                let mut next_file = File::open(&next_file_path)?;
                match self.parameters.length_kind {
                    LengthKind::BigEndianFixed => LengthDelimitedRecordsReader::create(
                        next_file,
                        DelimitedLengthKind::BigEndianFixed,
                        next_file_path,
                    ),
                    LengthKind::Varint => LengthDelimitedRecordsReader::create(
                        next_file,
                        DelimitedLengthKind::Varint,
                        next_file_path,
                    ),
                    LengthKind::SingleMessagePerFile => {
                        let mut bytes = Vec::new();
                        next_file.read_to_end(&mut bytes)?;
                        let size = bytes.len();
                        return Ok(Some(StateContainerValue {
                            bytes,
                            path_reference: PathReference::Owned(next_file_path),
                            position: 0,
                            size,
                        }));
                    }
                }
            }
        };

        let Some(Record {
            position,
            size,
            bytes: next_message,
        }) = value.try_get_next()?
        else {
            return Ok(None);
        };

        self.local_state.current = Some(value);
        Ok(Some(StateContainerValue {
            path_reference: PathReference::Borrowed(
                self.local_state.current.as_ref().unwrap().path(),
            ),
            bytes: next_message,
            size: size as _,
            position,
        }))
    }
}

#[repr(C)]
pub struct LocalState {
    current: Option<LengthDelimitedRecordsReader>,
    local_descriptor: MessageDescriptor,
}

impl VTabLocalData for ProtobufVTab {
    type LocalInitData = LocalState;

    fn local_init(init_info: &InitInfo) -> duckdb::Result<Self::LocalInitData, Box<dyn Error>> {
        let bind_data = unsafe { &*init_info.get_bind_data::<Parameters>() };
        let local_descriptor = bind_data
            .message_descriptor()
            .map_err(box_error_with_causes)?;

        Ok(LocalState {
            current: None,
            local_descriptor,
        })
    }
}

fn box_error_with_causes(error: anyhow::Error) -> Box<dyn Error> {
    Box::new(std::io::Error::other(
        format_error_with_causes(error).to_string(),
    ))
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
