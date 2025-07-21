use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::Deref;
use std::slice;

use anyhow::{bail, format_err};
use duckdb::vtab::{DataChunk, LogicalTypeId};
use protobuf::reflect::{FieldDescriptor, MessageDescriptor, MessageRef, ReflectFieldRef, ReflectOptionalRef, ReflectValueRef, RuntimeFieldType, RuntimeType};
use protobuf::MessageDyn;

pub fn write_to_output(
    mappings: &[u64],
    columns_state: &mut HashMap<ColumnKey, u64>,
    value: &dyn MessageDyn,
    output: &DataChunk,
    max_rows: usize,
    row_idx: usize,
) -> Result<(), anyhow::Error> {
    let column_key = &ColumnKey::empty();
    let message_descriptor = value.descriptor_dyn();
    let fields: Vec<FieldDescriptor> = message_descriptor.fields().collect();
    for (output_field_idx, field_idx) in mappings.iter().enumerate() {
        let field_idx = *field_idx as usize;
        if field_idx >= fields.len() {
            continue;
        }

        let field_descriptor = &fields[field_idx];
        let column_vector = output.get_vector(output_field_idx);
        let field_ref = field_descriptor.get_reflect(value);

        let column_key = column_key.field(&field_descriptor);

        write_column(
            columns_state,
            &column_key,
            Some(field_ref),
            &field_descriptor,
            column_vector,
            max_rows,
            row_idx,
        )?;
    }

    Ok(())
}

pub fn write_message(
    columns_state: &mut HashMap<ColumnKey, u64>,
    column_key: &ColumnKey,
    message_descriptor: &MessageDescriptor,
    value: Option<MessageRef>,
    output: &impl VectorAccessor,
    max_rows: usize,
    row_idx: usize,
) -> Result<(), anyhow::Error> {
    for (field_idx, field_descriptor) in message_descriptor.fields().enumerate() {
        let column_vector = output.get_vector(field_idx);
        let field_ref = value.as_ref().map(|it| field_descriptor.get_reflect(it.deref()));

        let column_key = column_key.field(&field_descriptor);

        write_column(
            columns_state,
            &column_key,
            field_ref,
            &field_descriptor,
            column_vector,
            max_rows,
            row_idx,
        )?;
    }

    Ok(())
}

pub struct MyFlatVector<T> {
    _phantom_data: PhantomData<T>,
    ptr: duckdb::ffi::duckdb_vector,
    capacity: usize,
}

impl<T> MyFlatVector<T> {
    pub unsafe fn with_capacity(ptr: duckdb::ffi::duckdb_vector, capacity: usize) -> Self {
        Self {
            _phantom_data: Default::default(),
            ptr,
            capacity,
        }
    }

    fn as_mut_ptr(&self) -> *mut T {
        unsafe { duckdb::ffi::duckdb_vector_get_data(self.ptr).cast() }
    }

    pub fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { slice::from_raw_parts_mut(self.as_mut_ptr(), self.capacity) }
    }
}

pub fn write_column(
    columns_state: &mut HashMap<ColumnKey, u64>,
    column_key: &ColumnKey,
    field_ref: Option<ReflectFieldRef>,
    field_descriptor: &FieldDescriptor,
    column: duckdb::ffi::duckdb_vector,
    max_rows: usize,
    row_idx: usize,
) -> Result<(), anyhow::Error> {
    match field_descriptor.runtime_field_type() {
        RuntimeFieldType::Repeated(values) => {
            let column_key = column_key.extending(ColumnKeyElement::List);

            let mut list_entries_vector = unsafe {
                MyFlatVector::<duckdb::ffi::duckdb_list_entry>::with_capacity(column, max_rows)
            };
            let list_entry = &mut list_entries_vector.as_mut_slice()[row_idx];

            let next_offset_ref = columns_state.get_mut(&column_key);
            let next_offset = if let Some(it) = &next_offset_ref {
                **it
            } else {
                0
            };

            let len_u64 = if let Some(ReflectFieldRef::Repeated(values)) = &field_ref {
                u64::try_from(values.len())?
            } else {
                0
            };

            list_entry.offset = next_offset;
            list_entry.length = len_u64;

            let new_next_offset = next_offset + len_u64;

            if let Some(it) = next_offset_ref {
                *it = new_next_offset;
            } else {
                columns_state.insert(column_key.clone(), new_next_offset);
            }

            let new_length = new_next_offset;

            unsafe { duckdb::ffi::duckdb_list_vector_reserve(column, new_length) };
            unsafe { duckdb::ffi::duckdb_list_vector_set_size(column, new_length) };

            if let Some(ReflectFieldRef::Repeated(values)) = field_ref {
                let child_vector = unsafe { duckdb::ffi::duckdb_list_vector_get_child(column) };
                
                let element_type = values.element_type();

                for (idx, value) in values.into_iter().enumerate() {
                    let row_idx = next_offset as usize + idx;

                    write_single_column(
                        columns_state,
                        &column_key,
                        ReflectOptionalRef::some(value),
                        field_descriptor,
                        &element_type,
                        child_vector,
                        new_length as usize,
                        row_idx,
                    )?;
                }
            }
        }
        RuntimeFieldType::Singular(runtime_type) => {
            write_single_column(
                columns_state,
                column_key,
                if let Some(ReflectFieldRef::Optional(values)) = field_ref { values } else { ReflectOptionalRef::none(runtime_type.clone()) },
                field_descriptor,
                &runtime_type,
                column,
                max_rows,
                row_idx,
            )?;
        }
        _ => return Err(format_err!("unknown type")),
    }

    Ok(())
}

pub fn write_single_column(
    columns_state: &mut HashMap<ColumnKey, u64>,
    column_key: &ColumnKey,
    value: ReflectOptionalRef,
    field_descriptor: &FieldDescriptor,
    runtime_type: &RuntimeType,
    column: duckdb::ffi::duckdb_vector,
    max_rows: usize,
    row_idx: usize,
) -> Result<(), anyhow::Error> {
    match runtime_type {
        RuntimeType::Message(message_descriptor)
            if message_descriptor.full_name() == "google.protobuf.Timestamp" =>
        {
            let mut vector = unsafe { MyFlatVector::<i64>::with_capacity(column, max_rows) };
            vector.as_mut_slice()[row_idx] = if let Some(ReflectValueRef::Message(message_value)) =
                value.value()
            {
                let seconds = message_descriptor
                    .field_by_number(1)
                    .ok_or_else(|| format_err!("expected field 1 for google.protobuf.Timestamp"))?
                    .get_singular_field_or_default(message_value.deref())
                    .to_i64()
                    .ok_or_else(|| format_err!("expected i64"))?;

                let nanos = message_descriptor
                    .field_by_number(2)
                    .ok_or_else(|| format_err!("expected field 2 for google.protobuf.Timestamp"))?
                    .get_singular_field_or_default(message_value.deref())
                    .to_i32()
                    .ok_or_else(|| format_err!("expected i64"))?;

                seconds * 1000000 + (nanos as i64 / 1000)
            } else {
                0
            }
        }
        RuntimeType::Message(message_descriptor) => {
            let source = unsafe { StructVector::new(column) };

            write_message(
                columns_state,
                column_key,
                &message_descriptor,
                if let Some(ReflectValueRef::Message(message)) = value.value() { Some(message) } else { None },
                &source,
                max_rows,
                row_idx,
            )?;
        }
        RuntimeType::Enum(enum_descriptor) => {
            let enum_value_descriptor =
                if let Some(ReflectValueRef::Enum(.., value)) = value.value() {
                    enum_descriptor.value_by_number_or_default(value)
                } else {
                    enum_descriptor.default_value()
                };
            let idx = enum_value_descriptor.value();

            let column_type = unsafe { duckdb::ffi::duckdb_vector_get_column_type(column) };

            let logical_type =
                LogicalTypeId::from(unsafe { duckdb::ffi::duckdb_enum_internal_type(column_type) });

            match logical_type {
                LogicalTypeId::UTinyint => {
                    let mut vector = unsafe { MyFlatVector::<u8>::with_capacity(column, max_rows) };
                    vector.as_mut_slice()[row_idx] = idx as _;
                }
                LogicalTypeId::USmallint => {
                    let mut vector =
                        unsafe { MyFlatVector::<u16>::with_capacity(column, max_rows) };
                    vector.as_mut_slice()[row_idx] = idx as _;
                }
                LogicalTypeId::UInteger => {
                    let mut vector =
                        unsafe { MyFlatVector::<u32>::with_capacity(column, max_rows) };
                    vector.as_mut_slice()[row_idx] = idx as _;
                }
                _ => bail!("unknown enum column type {:?}", logical_type),
            }
        }
        RuntimeType::U32 => {
            let mut vector = unsafe { MyFlatVector::<u32>::with_capacity(column, max_rows) };
            vector.as_mut_slice()[row_idx] =
                if let Some(ReflectValueRef::U32(value)) = value.value() {
                    value
                } else {
                    u32::default()
                };
        }
        RuntimeType::U64 => {
            let mut vector = unsafe { MyFlatVector::<u64>::with_capacity(column, max_rows) };
            vector.as_mut_slice()[row_idx] =
                if let Some(ReflectValueRef::U64(value)) = value.value() {
                    value
                } else {
                    u64::default()
                };
        }
        RuntimeType::I32 => {
            let mut vector = unsafe { MyFlatVector::<i32>::with_capacity(column, max_rows) };
            vector.as_mut_slice()[row_idx] =
                if let Some(ReflectValueRef::I32(value)) = value.value() {
                    value
                } else {
                    i32::default()
                };
        }
        RuntimeType::I64 => {
            let mut vector = unsafe { MyFlatVector::<i64>::with_capacity(column, max_rows) };
            vector.as_mut_slice()[row_idx] =
                if let Some(ReflectValueRef::I64(value)) = value.value() {
                    value
                } else {
                    i64::default()
                };
        }
        RuntimeType::F32 => {
            let mut vector = unsafe { MyFlatVector::<f32>::with_capacity(column, max_rows) };
            vector.as_mut_slice()[row_idx] =
                if let Some(ReflectValueRef::F32(value)) = value.value() {
                    value
                } else {
                    f32::default()
                };
        }
        RuntimeType::F64 => {
            let mut vector = unsafe { MyFlatVector::<f64>::with_capacity(column, max_rows) };
            vector.as_mut_slice()[row_idx] =
                if let Some(ReflectValueRef::F64(value)) = value.value() {
                    value
                } else {
                    f64::default()
                };
        }
        RuntimeType::Bool => {
            let mut vector = unsafe { MyFlatVector::<bool>::with_capacity(column, max_rows) };
            vector.as_mut_slice()[row_idx] =
                if let Some(ReflectValueRef::Bool(value)) = value.value() {
                    value
                } else {
                    bool::default()
                };
        }
        RuntimeType::String => {
            let value = if let Some(ReflectValueRef::String(value)) = value.value() {
                value
            } else {
                ""
            };
            let value = value.as_bytes();

            unsafe {
                duckdb::ffi::duckdb_vector_assign_string_element_len(
                    column,
                    row_idx as u64,
                    value.as_ptr() as _,
                    value.len() as _,
                )
            };
        }
        RuntimeType::VecU8 => {
            let value = if let Some(ReflectValueRef::Bytes(value)) = value.value() {
                value
            } else {
                &[]
            };

            unsafe {
                duckdb::ffi::duckdb_vector_assign_string_element_len(
                    column,
                    row_idx as u64,
                    value.as_ptr() as _,
                    value.len() as _,
                )
            };
        }
    }

    Ok(())
}

#[derive(Hash, Eq, PartialEq, Clone)]
pub enum ColumnKeyElement {
    Field { field_tag: u32 },
    List,
}

#[derive(Hash, Eq, PartialEq, Clone)]
pub struct ColumnKey {
    pub elements: Vec<ColumnKeyElement>,
}

impl ColumnKey {
    pub fn field(&self, field: &FieldDescriptor) -> ColumnKey {
        self.extending(ColumnKeyElement::Field {
            field_tag: field.proto().number() as u32,
        })
    }

    pub fn extending(&self, key: ColumnKeyElement) -> ColumnKey {
        let mut elements = self.elements.clone();
        elements.push(key);

        ColumnKey { elements }
    }
    pub fn empty() -> ColumnKey {
        ColumnKey { elements: vec![] }
    }
}

pub trait VectorAccessor {
    fn get_vector(&self, column_idx: usize) -> duckdb::ffi::duckdb_vector;
}

impl VectorAccessor for DataChunk {
    fn get_vector(&self, column_idx: usize) -> duckdb::ffi::duckdb_vector {
        let chunk = self.get_ptr();

        unsafe { duckdb::ffi::duckdb_data_chunk_get_vector(chunk, column_idx as u64) }
    }
}

struct StructVector(duckdb::ffi::duckdb_vector);

impl StructVector {
    unsafe fn new(value: duckdb::ffi::duckdb_vector) -> Self {
        Self(value)
    }
}

impl VectorAccessor for StructVector {
    fn get_vector(&self, idx: usize) -> duckdb::ffi::duckdb_vector {
        unsafe { duckdb::ffi::duckdb_struct_vector_get_child(self.0, idx as u64) }
    }
}
