use anyhow::format_err;
use duckdb::vtab::{LogicalType, LogicalTypeId};
use protobuf::reflect::{FieldDescriptor, RuntimeFieldType, RuntimeType};

pub fn into_logical_type(field: &FieldDescriptor) -> Result<LogicalType, anyhow::Error> {
    Ok(match field.runtime_field_type() {
        RuntimeFieldType::Singular(field) => into_logical_type_single(field)?,
        RuntimeFieldType::Repeated(field) => LogicalType::list(&into_logical_type_single(field)?),
        RuntimeFieldType::Map(_, _) => return Err(format_err!("map unimplemented")),
    })
}

fn into_logical_type_single(field: RuntimeType) -> Result<LogicalType, anyhow::Error> {
    let value = match field {
        RuntimeType::Message(message_descriptor) => {
            if message_descriptor.full_name() == "google.protobuf.Timestamp" {
                LogicalType::new(LogicalTypeId::Timestamp)
            } else {
                let fields = message_descriptor
                    .fields()
                    .collect::<Vec<FieldDescriptor>>();

                let fields = fields
                    .iter()
                    .map(|field| Ok((field.name(), into_logical_type(&field)?)))
                    .collect::<Result<Vec<(&str, LogicalType)>, anyhow::Error>>()?;

                LogicalType::struct_type(fields.as_slice())
            }
        }
        RuntimeType::Enum(enum_descriptor) => {
            let names = enum_descriptor.values().collect::<Vec<_>>();
            let names = names.iter().map(|it| it.name()).collect::<Vec<_>>();
            LogicalType::enumeration(names.as_slice())
        }
        RuntimeType::F64 => LogicalType::new(LogicalTypeId::Double),
        RuntimeType::F32 => LogicalType::new(LogicalTypeId::Float),
        RuntimeType::I32 => LogicalType::new(LogicalTypeId::Integer),
        RuntimeType::I64 => LogicalType::new(LogicalTypeId::Bigint),
        RuntimeType::U32 => LogicalType::new(LogicalTypeId::UInteger),
        RuntimeType::U64 => LogicalType::new(LogicalTypeId::UBigint),
        RuntimeType::Bool => LogicalType::new(LogicalTypeId::Boolean),
        RuntimeType::String => LogicalType::new(LogicalTypeId::Varchar),
        RuntimeType::VecU8 => LogicalType::new(LogicalTypeId::Blob),
    };

    Ok(value)
}
