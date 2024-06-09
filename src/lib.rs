use duckdb::ffi;
use duckdb::vtab::{
    BindInfo, DataChunk, Free, FunctionInfo, InitInfo, Inserter, LogicalType, LogicalTypeId, VTab,
};
use duckdb::Connection;
use duckdb_loadable_macros::duckdb_entrypoint;
use std::error::Error;
use std::ffi::{c_char, c_void, CString};

struct ProtobufVTab;

#[repr(C)]
struct ProtobufBindData {
    name: *mut c_char,
}

impl Free for ProtobufBindData {
    fn free(&mut self) {
        unsafe {
            if self.name.is_null() {
                return;
            }
            drop(CString::from_raw(self.name));
        }
    }
}

#[repr(C)]
struct ProtobufInitData {
    done: bool,
}

impl Free for ProtobufInitData {}

impl VTab for ProtobufVTab {
    type InitData = ProtobufInitData;
    type BindData = ProtobufBindData;

    fn bind(bind: &BindInfo, data: *mut Self::BindData) -> duckdb::Result<(), Box<dyn Error>> {
        bind.add_result_column("column0", LogicalType::new(LogicalTypeId::Varchar));

        let param = bind.get_named_parameter("test").unwrap().to_string();
        unsafe {
            (*data).name = CString::new(param).unwrap().into_raw();
        }

        Ok(())
    }

    fn init(_init: &InitInfo, data: *mut Self::InitData) -> duckdb::Result<(), Box<dyn Error>> {
        unsafe {
            (*data).done = false;
        }

        Ok(())
    }

    fn func(func: &FunctionInfo, output: &mut DataChunk) -> duckdb::Result<(), Box<dyn Error>> {
        let init_info = func.get_init_data::<ProtobufInitData>();
        let bind_info = func.get_bind_data::<ProtobufBindData>();

        unsafe {
            if (*init_info).done {
                output.set_len(0);
            } else {
                (*init_info).done = true;
                let vector = output.flat_vector(0);
                let name = CString::from_raw((*bind_info).name);
                let result = CString::new(format!("Hello {}", name.to_str()?))?;
                // Can't consume the CString
                (*bind_info).name = CString::into_raw(name);
                vector.insert(0, result);
                output.set_len(1);
            }
        }

        Ok(())
    }

    fn named_parameters() -> Option<Vec<(String, LogicalType)>> {
        Some(vec![
            ("test".to_string(), LogicalType::new(LogicalTypeId::Varchar)),
            // ("descriptor".to_string(), LogicalType::new(LogicalTypeId::Varchar)),
        ])
    }
}

#[duckdb_entrypoint]
pub fn libduckdb_protobuf_init(conn: Connection) -> Result<(), Box<dyn Error>> {
    conn.register_table_function::<ProtobufVTab>("protobuf")?;

    Ok(())
}
