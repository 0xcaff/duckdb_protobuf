use anyhow::format_err;
use byteorder::{BigEndian, ReadBytesExt};
use ouroboros::self_referencing;
use protobuf::CodedInputStream;
use std::error::Error;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use strum::{AsRefStr, EnumIter, EnumString, IntoEnumIterator};

#[derive(Copy, Clone, EnumString, EnumIter, AsRefStr)]
pub enum LengthKind {
    BigEndianFixed,
    Varint,
    SingleMessagePerFile,
}

pub fn parse<T: std::str::FromStr<Err = impl Error> + IntoEnumIterator + AsRef<str>>(
    value: &str,
) -> Result<T, anyhow::Error> {
    Ok(T::from_str(value).map_err(|err| {
        format_err!(
            "{}: expected one of: {}, got: {}",
            err,
            T::iter()
                .map(|it| format!("{}", it.as_ref()))
                .collect::<Vec<_>>()
                .join(", "),
            value
        )
    })?)
}

#[derive(Copy, Clone)]
pub enum DelimitedLengthKind {
    BigEndianFixed,
    Varint,
}

#[self_referencing]
pub struct LengthDelimitedRecordsReader {
    length_kind: DelimitedLengthKind,
    path: PathBuf,
    inner: File,

    #[borrows(mut inner)]
    #[not_covariant]
    reader: CodedInputStream<'this>,
}

pub struct Record {
    pub bytes: Vec<u8>,
    pub position: u64,
    pub size: u32,
}

impl LengthDelimitedRecordsReader {
    pub fn create(inner: File, length_kind: DelimitedLengthKind, path: PathBuf) -> Self {
        LengthDelimitedRecordsReaderBuilder {
            length_kind,
            path,
            inner,
            reader_builder: |it| CodedInputStream::new(it),
        }
        .build()
    }

    fn get_next(&mut self) -> Result<Record, io::Error> {
        let length_kind = *self.borrow_length_kind();
        Ok(self.with_reader_mut(move |reader| {
            let position = reader.pos();
            let len = match length_kind {
                DelimitedLengthKind::BigEndianFixed => reader.read_u32::<BigEndian>()?,
                DelimitedLengthKind::Varint => reader.read_raw_varint32()?,
            };

            let mut buf = vec![0; len as usize];
            <CodedInputStream as io::Read>::read_exact(reader, &mut buf)?;

            Ok::<_, io::Error>(Record {
                bytes: buf,
                position,
                size: len,
            })
        })?)
    }

    pub fn try_get_next(&mut self) -> Result<Option<Record>, io::Error> {
        match self.get_next() {
            Ok(it) => Ok(Some(it)),
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    pub fn path(&self) -> &Path {
        self.borrow_path().as_path()
    }
}
