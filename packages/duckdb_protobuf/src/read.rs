use std::marker::PhantomData;
use std::slice;

use duckdb::vtab::DataChunk;

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

#[derive(Hash, Eq, PartialEq, Clone)]
pub struct ColumnKey {
    pub elements: Vec<u32>,
}

impl ColumnKey {
    pub fn field(&self, field_tag: u32) -> ColumnKey {
        let mut elements = self.elements.clone();
        elements.push(field_tag);

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

pub struct StructVector(duckdb::ffi::duckdb_vector);

impl StructVector {
    pub unsafe fn new(value: duckdb::ffi::duckdb_vector) -> Self {
        Self(value)
    }
}

impl VectorAccessor for StructVector {
    fn get_vector(&self, idx: usize) -> duckdb::ffi::duckdb_vector {
        unsafe { duckdb::ffi::duckdb_struct_vector_get_child(self.0, idx as u64) }
    }
}
