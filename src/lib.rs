use byteorder::{BigEndian, ReadBytesExt};
use std::error::Error;
use std::ffi::{c_char, c_void, CString};
use std::fs::File;
use std::io;
use std::io::{ErrorKind, Read};

use duckdb::vtab::{BindInfo, DataChunk, Free, FunctionInfo, InitInfo, LogicalType, LogicalTypeId, VTab};
use duckdb::Connection;
use duckdb::ffi;
use duckdb_loadable_macros::duckdb_entrypoint;
use prost::Message;
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{DescriptorProto, EnumDescriptorProto, FieldDescriptorProto, FileDescriptorSet};

struct ProtobufVTab;

#[repr(C)]
struct ProtobufBindData {
    parameters: *mut Parameters,
}

impl Free for ProtobufBindData {
    fn free(&mut self) {
        unsafe {
            if self.parameters.is_null() {
                return;
            }

            drop(Box::from_raw(self.parameters));
        }
    }
}

#[repr(C)]
struct ProtobufInitData {
    files_iterator: glob::Paths,
    current_file: Option<LengthDelimitedRecordsReader<File>>,
}

struct LengthDelimitedRecordsReader<R: io::Read> {
    reader: R,
}

impl<R> Iterator for LengthDelimitedRecordsReader<R>
where
    R: io::Read,
{
    type Item = Result<Vec<u8>, io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.try_get_next() {
            Ok(value) => Some(Ok(value)),
            Err(err) => {
                if err.kind() == ErrorKind::UnexpectedEof {
                    None
                } else {
                    Some(Err(err))
                }
            }
        }
    }
}

impl<R> LengthDelimitedRecordsReader<R>
where
    R: io::Read,
{
    pub fn new(reader: R) -> Self {
        Self { reader }
    }

    pub fn try_get_next(&mut self) -> Result<Vec<u8>, io::Error> {
        let len = self.reader.read_u32::<BigEndian>()?;
        let mut buf = vec![0; len as usize];
        self.reader.read_exact(&mut buf)?;

        Ok(buf)
    }
}

impl ProtobufInitData {}

fn init(bind_params: &Parameters) -> Result<ProtobufInitData, Box<dyn Error>> {
    Ok(ProtobufInitData {
        files_iterator: glob::glob(bind_params.files.as_str())?,
        current_file: None,
    })
}

impl Free for ProtobufInitData {}

struct Parameters {
    files: String,
    message_descriptor: DescriptorProto,
    descriptors: FileDescriptorSet,
}

pub fn get_message_matching<'a>(
    descriptor_set: &'a FileDescriptorSet,
    name: &str,
) -> Result<Option<&'a DescriptorProto>, Box<dyn Error>> {
    for file_descriptor in &descriptor_set.file {
        let package_name = file_descriptor.package();

        for message_descriptor in &file_descriptor.message_type {
            if &format!("{}.{}", package_name, message_descriptor.name()) == name {
                return Ok(Some(message_descriptor));
            }
        }
    }

    Ok(None)
}

pub fn get_enum_matching<'a>(
    descriptor_set: &'a FileDescriptorSet,
    name: &str,
) -> Result<Option<&'a EnumDescriptorProto>, Box<dyn Error>> {
    for file_descriptor in &descriptor_set.file {
        let package_name = file_descriptor.package();

        for enum_descriptor in &file_descriptor.enum_type {
            if &format!("{}.{}", package_name, enum_descriptor.name()) == name {
                return Ok(Some(enum_descriptor));
            }
        }
    }

    Ok(None)
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

        let message_descriptor = get_message_matching(&descriptors, &message_name.as_str())?
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
        Label::Optional | Label::Required => Ok(into_logical_type_single(field, descriptors)?),
        Label::Repeated => Ok(LogicalType::list(&into_logical_type_single(
            field,
            descriptors,
        )?)),
    }
}

pub fn into_logical_type_enum(
    enum_descriptor: &EnumDescriptorProto,
) -> Result<LogicalType, Box<dyn Error>> {
    let mut values = enum_descriptor.value.clone();
    values.sort_by_key(|it| it.number());

    if values[0].number() != 0 {
        return Err("enum values must start at 0".into());
    }

    if values.len() != values.last().unwrap().number() as usize + 1 {
        return Err("enum values must be contiguous".into());
    }

    Ok(create_enum_type(
        &values
            .iter()
            .map(|it| it.name())
            .collect::<Vec<&str>>()
            .as_slice(),
    ))
}

pub fn get_type_name(field: &FieldDescriptorProto) -> Result<&str, Box<dyn Error>> {
    let type_name = field.type_name();
    let (prefix, absolute_type_name) = type_name.split_at(1);
    if prefix != "." {
        return Err(format!("invalid type name: {}", type_name).into());
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

            let message_descriptor = get_message_matching(descriptors, type_name)?
                .ok_or(format!("message type not found: {}", type_name))?;

            LogicalType::struct_type(
                &message_descriptor
                    .field
                    .iter()
                    .map(|field| Ok((field.json_name(), into_logical_type(field, descriptors)?)))
                    .collect::<Result<Vec<(&str, LogicalType)>, Box<dyn Error>>>()?,
            )
        }
        Type::Enum => {
            let type_name = get_type_name(field)?;
            let enum_descriptor = get_enum_matching(descriptors, type_name)?
                .ok_or(format!("enum type not found: {}", type_name))?;

            into_logical_type_enum(enum_descriptor)?
        }
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

pub fn create_enum_type(variants: &[&str]) -> LogicalType {
    let variants = variants
        .iter()
        .map(|it| CString::new(*it).unwrap())
        .collect::<Vec<_>>();
    let mut variants = variants.iter().map(|it| it.as_ptr()).collect::<Vec<_>>();

    From::from(unsafe {
        ffi::duckdb_create_enum_type(variants.as_mut_ptr(), variants.len() as _)
    })
}

impl VTab for ProtobufVTab {
    type InitData = ProtobufInitData;
    type BindData = ProtobufBindData;

    fn bind(bind: &BindInfo, data: *mut Self::BindData) -> duckdb::Result<(), Box<dyn Error>> {
        let params = Parameters::from_bind_info(bind)?;

        for (idx, field_descriptor) in params.message_descriptor.field.iter().enumerate() {
            bind.add_result_column(
                field_descriptor
                    .json_name
                    .as_ref()
                    .ok_or(format!("no field json_name for index: {}", idx))?,
                into_logical_type(field_descriptor, &params.descriptors)?,
            );
        }

        unsafe {
            (*data).parameters = Box::into_raw(Box::new(params));
        }

        Ok(())
    }

    fn init(init_info: &InitInfo, data: *mut Self::InitData) -> duckdb::Result<(), Box<dyn Error>> {
        let bind_date = init_info.get_bind_data::<ProtobufBindData>();

        unsafe {
            let parameters = &*(&*bind_date).parameters;
            *data = init(parameters)?;
        }

        Ok(())
    }

    fn func(func: &FunctionInfo, output: &mut DataChunk) -> duckdb::Result<(), Box<dyn Error>> {
        let bind_info = unsafe {
            let bind_info = func.get_bind_data::<ProtobufBindData>();

            &mut *bind_info
        };

        let mut init_info = unsafe {
            let init_info = func.get_init_data::<ProtobufInitData>();

            &mut *init_info
        };

        // unsafe {
        //     if (*init_info).done {
        //     } else {
        //         (*init_info).done = true;
        //         let vector = output.flat_vector(0);
        //         let it = output.struct_vector(0);
        //         let name = CString::from_raw((*bind_info).name);
        //         let result = CString::new(format!("Hello {}", name.to_str()?))?;
        //         // Can't consume the CString
        //         (*bind_info).name = CString::into_raw(name);
        //         vector.insert(0, result);
        //         output.set_len(1);
        //     }
        // }

        Ok(())
    }

    fn named_parameters() -> Option<Vec<(String, LogicalType)>> {
        Some(Parameters::values())
    }
}

#[duckdb_entrypoint]
pub fn libduckdb_protobuf_init(conn: Connection) -> Result<(), Box<dyn Error>> {
    conn.register_table_function::<ProtobufVTab>("protobuf")?;

    Ok(())
}
