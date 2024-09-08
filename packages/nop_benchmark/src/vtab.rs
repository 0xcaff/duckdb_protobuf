use anyhow::format_err;
use duckdb::vtab::{
    BindInfo, DataChunk, Free, FunctionInfo, InitInfo, LogicalType, LogicalTypeId,
    VTab,
};
use std::error::Error;
use std::io::Read;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::slice;

pub struct Parameters {
    pub count: u32,
}

impl Parameters {
    pub fn from_bind_info(bind: &BindInfo) -> Result<Self, anyhow::Error> {
        let count = bind
            .get_named_parameter("count")
            .ok_or_else(|| format_err!("missing argument `count`"))?
            .to_int64();

        Ok(Self { count: count as _ })
    }

    pub fn values() -> Vec<(String, LogicalType)> {
        vec![(
            "count".to_string(),
            LogicalType::new(LogicalTypeId::UInteger),
        )]
    }
}
pub struct GlobalState {
    remaining: u32,
}

impl GlobalState {
    pub fn new(params: &Parameters) -> Result<GlobalState, anyhow::Error> {
        Ok(GlobalState {
            remaining: params.count,
        })
    }
}

pub struct NopBenchmark;

impl VTab for NopBenchmark {
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

impl NopBenchmark {
    fn bind(bind: &BindInfo, data: *mut <Self as VTab>::BindData) -> Result<(), anyhow::Error> {
        let data = unsafe { &mut *data };

        let params = Parameters::from_bind_info(bind)?;

        bind.add_result_column("count", LogicalType::new(LogicalTypeId::UInteger));

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
        data.assign(new_global_state);

        Ok(())
    }

    fn func(func: &FunctionInfo, output: &mut DataChunk) -> duckdb::Result<(), anyhow::Error> {
        let init_data = unsafe { &mut *func.get_init_data::<<Self as VTab>::InitData>() };

        let available_chunk_size = output.flat_vector(0).capacity();
        let mut items = 0;

        let column = output.get_vector(0);
        let mut vector =
            unsafe { MyFlatVector::<u32>::with_capacity(column, available_chunk_size) };

        for output_row_idx in 0..available_chunk_size {
            if (init_data.remaining == 0) {
                break;
            }

            vector.as_mut_slice()[output_row_idx] = init_data.remaining;
            init_data.remaining -= 1;

            items += 1;
        }

        output.set_len(items);

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

pub trait VectorAccessor {
    fn get_vector(&self, column_idx: usize) -> duckdb::ffi::duckdb_vector;
}

impl VectorAccessor for DataChunk {
    fn get_vector(&self, column_idx: usize) -> duckdb::ffi::duckdb_vector {
        let chunk = self.get_ptr();

        unsafe { duckdb::ffi::duckdb_data_chunk_get_vector(chunk, column_idx as u64) }
    }
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
