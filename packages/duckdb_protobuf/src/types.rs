use anyhow::format_err;
use duckdb::core::{LogicalTypeHandle, LogicalTypeId};
use prost_reflect::{Cardinality, FieldDescriptor, Kind};

pub fn into_logical_type(field: &FieldDescriptor) -> Result<LogicalTypeHandle, anyhow::Error> {
    Ok(match field.cardinality() {
        Cardinality::Optional | Cardinality::Required => into_logical_type_single(field)?,
        Cardinality::Repeated => LogicalTypeHandle::list(&into_logical_type_single(field)?),
    })
}

fn into_logical_type_single(field: &FieldDescriptor) -> Result<LogicalTypeHandle, anyhow::Error> {
    let value = match field.kind() {
        Kind::Message(message_descriptor)
            if message_descriptor.full_name() == "google.protobuf.Timestamp" =>
        {
            LogicalTypeHandle::from(LogicalTypeId::Timestamp)
        }
        Kind::Message(message_descriptor) => {
            let fields = message_descriptor
                .fields()
                .collect::<Vec<FieldDescriptor>>();

            let fields = fields
                .iter()
                .map(|field| Ok((field.name(), into_logical_type(&field)?)))
                .collect::<Result<Vec<(&str, LogicalTypeHandle)>, anyhow::Error>>()?;

            LogicalTypeHandle::struct_type(fields.as_slice())
        }
        Kind::Enum(descriptor) => {
            let names = descriptor.values().collect::<Vec<_>>();
            let names = names.iter().map(|it| it.name()).collect::<Vec<_>>();
            LogicalTypeHandle::enumeration(names.as_slice())
        }
        Kind::Double => LogicalTypeHandle::from(LogicalTypeId::Double),
        Kind::Float => LogicalTypeHandle::from(LogicalTypeId::Float),
        Kind::Int32 => LogicalTypeHandle::from(LogicalTypeId::Integer),
        Kind::Int64 => LogicalTypeHandle::from(LogicalTypeId::Bigint),
        Kind::Uint32 => LogicalTypeHandle::from(LogicalTypeId::UInteger),
        Kind::Uint64 => LogicalTypeHandle::from(LogicalTypeId::UBigint),
        Kind::Bool => LogicalTypeHandle::from(LogicalTypeId::Boolean),
        Kind::String => LogicalTypeHandle::from(LogicalTypeId::Varchar),
        logical_type => {
            return Err(format_err!(
                "unhandled field: {}, type: {:?}",
                field.name(),
                logical_type,
            )
            .into())
        }
    };

    Ok(value)
}
