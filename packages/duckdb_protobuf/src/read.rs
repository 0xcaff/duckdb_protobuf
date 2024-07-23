use std::collections::HashMap;
use std::ffi::CString;
use std::marker::PhantomData;
use std::slice;

use anyhow::{bail, format_err};
use duckdb::vtab::DataChunk;
use prost_reflect::{Cardinality, DynamicMessage, FieldDescriptor, Kind, ReflectMessage, Value};

pub fn write_to_output(
    columns_state: &mut HashMap<ColumnKey, u64>,
    value: &DynamicMessage,
    output: &DataChunk,
    max_rows: usize,
    row_idx: usize,
) -> Result<(), anyhow::Error> {
    write_message(
        columns_state,
        &ColumnKey::empty(),
        value,
        output,
        max_rows,
        row_idx,
    )
}

pub fn write_message(
    columns_state: &mut HashMap<ColumnKey, u64>,
    column_key: &ColumnKey,
    value: &DynamicMessage,
    output: &impl VectorAccessor,
    max_rows: usize,
    row_idx: usize,
) -> Result<(), anyhow::Error> {
    for (field_idx, field_descriptor) in value.descriptor().fields().enumerate() {
        let column_vector = output.get_vector(field_idx);
        let value = value.get_field(&field_descriptor);

        let column_key = column_key.field(&field_descriptor);

        write_column(
            columns_state,
            &column_key,
            &value,
            &field_descriptor,
            column_vector,
            max_rows,
            row_idx,
        )?;
    }

    Ok(())
}

struct MyFlatVector<T> {
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
    value: &Value,
    field_descriptor: &FieldDescriptor,
    column: duckdb::ffi::duckdb_vector,
    max_rows: usize,
    row_idx: usize,
) -> Result<(), anyhow::Error> {
    match field_descriptor.cardinality() {
        Cardinality::Repeated => {
            let column_key = column_key.extending(ColumnKeyElement::List);

            let mut list_entries_vector = unsafe {
                MyFlatVector::<duckdb::ffi::duckdb_list_entry>::with_capacity(column, max_rows)
            };
            let list_entry = &mut list_entries_vector.as_mut_slice()[row_idx];

            let values = value
                .as_list()
                .ok_or_else(|| format_err!("expected list"))?;

            let next_offset_ref = columns_state.get_mut(&column_key);
            let next_offset = if let Some(it) = &next_offset_ref {
                **it
            } else {
                0
            };

            let len_u64 = u64::try_from(values.len())?;

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

            let child_vector = unsafe { duckdb::ffi::duckdb_list_vector_get_child(column) };

            for (idx, value) in values.iter().enumerate() {
                let row_idx = next_offset as usize + idx;

                write_single_column(
                    columns_state,
                    &column_key,
                    value,
                    field_descriptor,
                    child_vector,
                    new_length as usize,
                    row_idx,
                )?;
            }
        }
        Cardinality::Optional | Cardinality::Required => {
            write_single_column(
                columns_state,
                column_key,
                value,
                field_descriptor,
                column,
                max_rows,
                row_idx,
            )?;
        }
    }

    Ok(())
}

pub fn write_single_column(
    columns_state: &mut HashMap<ColumnKey, u64>,
    column_key: &ColumnKey,
    value: &Value,
    field_descriptor: &FieldDescriptor,
    column: duckdb::ffi::duckdb_vector,
    max_rows: usize,
    row_idx: usize,
) -> Result<(), anyhow::Error> {
    match field_descriptor.kind() {
        Kind::Message(message_descriptor)
            if message_descriptor.full_name() == "google.protobuf.Timestamp" =>
        {
            let message = value
                .as_message()
                .ok_or_else(|| format_err!("expected message"))?;
            let seconds =
                message
                    .get_field(&message_descriptor.get_field(1).ok_or_else(|| {
                        format_err!("expected field 1 for google.protobuf.Timestamp")
                    })?)
                    .as_i64()
                    .ok_or_else(|| format_err!("expected i64"))?;

            let nanos =
                message
                    .get_field(&message_descriptor.get_field(2).ok_or_else(|| {
                        format_err!("expected field 2 for google.protobuf.Timestamp")
                    })?)
                    .as_i32()
                    .ok_or_else(|| format_err!("expected i32"))?;

            let mut vector = unsafe { MyFlatVector::<i64>::with_capacity(column, max_rows) };
            vector.as_mut_slice()[row_idx] = seconds * 1000000 + (nanos as i64 / 1000);
        }
        Kind::Message(..) => {
            let message = value
                .as_message()
                .ok_or_else(|| format_err!("expected message"))?;

            let source = unsafe { StructVector::new(column) };

            write_message(
                columns_state,
                column_key,
                message,
                &source,
                max_rows,
                row_idx,
            )?;
        }
        Kind::Enum(enum_descriptor) => {
            let enum_value = value
                .as_enum_number()
                .ok_or_else(|| format_err!("expected enum value"))?;

            let enum_value_descriptor = enum_descriptor
                .get_value(enum_value)
                .unwrap_or_else(|| enum_descriptor.default_value());
            let name = CString::new(enum_value_descriptor.name())?;

            unsafe {
                duckdb::ffi::duckdb_vector_assign_string_element(
                    column,
                    row_idx as u64,
                    name.as_ptr(),
                )
            };
        }
        Kind::String => {
            let value = value
                .as_str()
                .ok_or_else(|| format_err!("expected string"))?;
            let value = CString::new(value)?;

            unsafe {
                duckdb::ffi::duckdb_vector_assign_string_element(
                    column,
                    row_idx as u64,
                    value.as_ptr(),
                )
            };
        }
        Kind::Double => {
            let value = value
                .as_f64()
                .ok_or_else(|| format_err!("expected double"))?;
            let mut vector = unsafe { MyFlatVector::<f64>::with_capacity(column, max_rows) };
            vector.as_mut_slice()[row_idx] = value;
        }
        Kind::Float => {
            let value = value
                .as_f32()
                .ok_or_else(|| format_err!("expected float"))?;
            let mut vector = unsafe { MyFlatVector::<f32>::with_capacity(column, max_rows) };
            vector.as_mut_slice()[row_idx] = value;
        }
        Kind::Int32 => {
            let value = value
                .as_i32()
                .ok_or_else(|| format_err!("expected int32"))?;
            let mut vector = unsafe { MyFlatVector::<i32>::with_capacity(column, max_rows) };
            vector.as_mut_slice()[row_idx] = value;
        }
        Kind::Int64 => {
            let value = value
                .as_i64()
                .ok_or_else(|| format_err!("expected int64"))?;
            let mut vector = unsafe { MyFlatVector::<i64>::with_capacity(column, max_rows) };
            vector.as_mut_slice()[row_idx] = value;
        }
        Kind::Uint32 => {
            let value = value
                .as_u32()
                .ok_or_else(|| format_err!("expected uint32"))?;
            let mut vector = unsafe { MyFlatVector::<u32>::with_capacity(column, max_rows) };
            vector.as_mut_slice()[row_idx] = value;
        }
        Kind::Uint64 => {
            let value = value
                .as_u64()
                .ok_or_else(|| format_err!("expected uint64"))?;
            let mut vector = unsafe { MyFlatVector::<u64>::with_capacity(column, max_rows) };
            vector.as_mut_slice()[row_idx] = value;
        }
        Kind::Bool => {
            let value = value
                .as_bool()
                .ok_or_else(|| format_err!("expected bool"))?;
            let mut vector = unsafe { MyFlatVector::<bool>::with_capacity(column, max_rows) };
            vector.as_mut_slice()[row_idx] = value;
        }
        _ => {
            bail!("unhandled field type");
        }
    };

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
            field_tag: field.number(),
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
