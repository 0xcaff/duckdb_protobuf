use anyhow::format_err;
use duckdb::vtab::{LogicalType, LogicalTypeId};
use protobuf::reflect::{FieldDescriptor, RuntimeFieldType, RuntimeType};

pub fn into_logical_type(
    field: &FieldDescriptor,
    with_location: bool,
) -> Result<LogicalType, anyhow::Error> {
    Ok(match field.runtime_field_type() {
        RuntimeFieldType::Singular(field) => into_logical_type_single(field, with_location)?,
        RuntimeFieldType::Repeated(field) => {
            LogicalType::list(&into_logical_type_single(field, with_location)?)
        }
        RuntimeFieldType::Map(_, _) => return Err(format_err!("map unimplemented")),
    })
}

fn into_logical_type_single(
    field: RuntimeType,
    with_location: bool,
) -> Result<LogicalType, anyhow::Error> {
    let value = match field {
        RuntimeType::Message(message_descriptor) => {
            if message_descriptor.full_name() == "google.protobuf.Timestamp" {
                LogicalType::new(LogicalTypeId::Timestamp)
            } else {
                let fields = message_descriptor
                    .fields()
                    .collect::<Vec<FieldDescriptor>>();

                let mut fields = fields
                    .iter()
                    .map(|field| Ok((field.name(), into_logical_type(&field, with_location)?)))
                    .collect::<Result<Vec<(&str, LogicalType)>, anyhow::Error>>()?;

                if with_location {
                    fields.push(("position", LogicalType::new(LogicalTypeId::UBigint)));
                    fields.push(("length", LogicalType::new(LogicalTypeId::UBigint)))
                }

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
