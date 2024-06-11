use byteorder::{BigEndian, ReadBytesExt};
use std::error::Error;
use std::fs::File;
use std::io;
use crate::vtab::Parameters;

pub struct RecordsReader {
    files_iterator: glob::Paths,
    current_file: Option<LengthDelimitedRecordsReader<File>>,
}

impl RecordsReader {
    pub fn new(params: &Parameters) -> Result<RecordsReader, Box<dyn Error>> {
        Ok(RecordsReader {
            files_iterator: glob::glob(params.files.as_str())?,
            current_file: None,
        })
    }

    pub fn next_message(&mut self) -> Result<Option<Vec<u8>>, Box<dyn Error>> {
        let file_reader = if let Some(reader) = &mut self.current_file {
            reader
        } else {
            let Some(next_file_path) = self.files_iterator.next() else {
                return Ok(None);
            };

            let next_file_path = next_file_path?;
            let next_file = File::open(&next_file_path)?;

            self.current_file = Some(LengthDelimitedRecordsReader::new(next_file));

            self.current_file.as_mut().unwrap()
        };

        let Some(next_message) = file_reader.try_get_next()? else {
            self.current_file = None;
            return Ok(None);
        };

        Ok(Some(next_message))
    }
}

pub struct LengthDelimitedRecordsReader<R: io::Read> {
    reader: R,
}

impl<R> LengthDelimitedRecordsReader<R>
where
    R: io::Read,
{
    pub fn new(reader: R) -> Self {
        Self { reader }
    }

    pub fn get_next(&mut self) -> Result<Vec<u8>, io::Error> {
        let len = self.reader.read_u32::<BigEndian>()?;
        let mut buf = vec![0; len as usize];
        self.reader.read_exact(&mut buf)?;

        Ok(buf)
    }

    pub fn try_get_next(&mut self) -> Result<Option<Vec<u8>>, io::Error> {
        match self.get_next() {
            Ok(it) => Ok(Some(it)),
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
            Err(err) => Err(err.into()),
        }
    }
}
