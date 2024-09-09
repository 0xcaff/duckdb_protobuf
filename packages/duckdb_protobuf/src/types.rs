use anyhow::format_err;
use duckdb::vtab::{LogicalType, LogicalTypeId};
use prost_reflect::{Cardinality, FieldDescriptor, Kind};

pub fn into_logical_type(field: &FieldDescriptor) -> Result<LogicalType, anyhow::Error> {
    Ok(match field.cardinality() {
        Cardinality::Optional | Cardinality::Required => into_logical_type_single(field)?,
        Cardinality::Repeated => LogicalType::list(&into_logical_type_single(field)?),
    })
}

fn into_logical_type_single(field: &FieldDescriptor) -> Result<LogicalType, anyhow::Error> {
    let value = match field.kind() {
        // todo: turn this back on
        // Kind::Message(message_descriptor)
        //     if message_descriptor.full_name() == "google.protobuf.Timestamp" =>
        // {
        //     LogicalType::new(LogicalTypeId::Timestamp)
        // }
        Kind::Message(message_descriptor) => {
            let fields = message_descriptor
                .fields()
                .collect::<Vec<FieldDescriptor>>();

            let fields = fields
                .iter()
                .map(|field| Ok((field.name(), into_logical_type(&field)?)))
                .collect::<Result<Vec<(&str, LogicalType)>, anyhow::Error>>()?;

            LogicalType::struct_type(fields.as_slice())
        }
        Kind::Enum(..) => LogicalType::new(LogicalTypeId::Varchar),
        Kind::Double => LogicalType::new(LogicalTypeId::Double),
        Kind::Float => LogicalType::new(LogicalTypeId::Float),
        Kind::Int32 => LogicalType::new(LogicalTypeId::Integer),
        Kind::Int64 => LogicalType::new(LogicalTypeId::Bigint),
        Kind::Uint32 => LogicalType::new(LogicalTypeId::UInteger),
        Kind::Uint64 => LogicalType::new(LogicalTypeId::UBigint),
        Kind::Bool => LogicalType::new(LogicalTypeId::Boolean),
        Kind::String => LogicalType::new(LogicalTypeId::Varchar),
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
