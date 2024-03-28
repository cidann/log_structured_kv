use std::{cell::RefCell, collections::BTreeMap, ffi::OsString, fs::{read_dir, remove_file, DirBuilder, File, OpenOptions}, io::{BufReader, Read, Seek, Write}, ops::DerefMut, path::PathBuf, rc::Rc, result, str::FromStr};


use crate::{util::{ OffsetStreamSerializer, KILOBYTE}, KVError, Result};

const _: () = assert!(std::mem::size_of::<u64>()<=std::mem::size_of::<usize>());

type FileReadBufRef=Rc<RefCell<BufReader<File>>>;

pub struct LogStorage{
    directory:PathBuf,
    cur_file_size:usize,
    file_size_limit:usize,

    cur_storage_size:usize,

    cur_write_file:File,
    read_file_buffers:BTreeMap<usize,FileReadBufRef>,
}



pub struct LogPointer{
    file_buf:FileReadBufRef,
    file_serial:usize,
    offset:u64
}

struct FileReadBufRefWrapper(FileReadBufRef);

impl LogStorage {
    pub fn load(directory:PathBuf)->Result<LogStorage>{
        assert!(directory.is_dir(),"Expected directory for database");
        DirBuilder::new().recursive(true).create(directory.clone()).map_err(|_|KVError::IOError("LogStorage::load1"))?;
        
        let (cur_storage_size,mut read_file_pool)=Self::load_persisted_files(&directory)?;
        let new_file_serial=read_file_pool
        .last_entry()
        .map(|entry|entry.key()+1)
        .unwrap_or(0);

        let cur_file_path=directory.join(new_file_serial.to_string());
        let cur_write_file=Self::new_log_file(cur_file_path.clone())?;
        let cur_read_file=new_file_read_buf_ref(Self::get_log_file(cur_file_path)?);

        read_file_pool.insert(new_file_serial,cur_read_file.clone());

        Ok(LogStorage{
            directory,
            cur_file_size:0,
            file_size_limit:4*KILOBYTE,
            cur_storage_size,
            cur_write_file,
            read_file_buffers:read_file_pool
        })
    }

    pub fn write<T>(&mut self,data:T)->Result<LogPointer>
    where 
        T:serde::ser::Serialize
    {
        let output_bytes=serde_json::to_vec(&data).map_err(|_|KVError::ParseError("LogStorage::write1"))?;
        self.write_bytes(&output_bytes)
    }
    
    pub fn write_iter<T,D>(&mut self,iter:T)->Result<Vec<(LogPointer,D)>>
    where
        T: Iterator<Item = D>,
        D: serde::ser::Serialize
    {
        let parsed=iter
        .map(|data|serde_json::to_vec(&data).map(|bytes|(bytes,data)))
        .collect::<result::Result<Vec<_>,_>>()
        .map_err(|_|KVError::ParseError("LogStorage::write_iter1"))?;

        parsed
        .into_iter()
        .map(|(parsed,data)|self.write_bytes(&parsed).map(|log_ptr|(log_ptr,data)))
        .collect()
    }

    pub fn merge<T,D>(&mut self,file_serials:&[usize],merged_data:T)->Result<Vec<(LogPointer,D)>>
    where
        T: IntoIterator<Item = D>,
        D: serde::ser::Serialize
    {
        self.replace_write_file()?;
        let res=self.write_iter(merged_data.into_iter());
        for serial in file_serials{
            let path=self.directory.join(serial.to_string());
            self.read_file_buffers.remove(serial);
            remove_file(path).map_err(|_|KVError::IOError("LogStorage::merge1"))?
        }

        res
    }

    pub fn iter_entries<'a,T>(&'a self)->impl Iterator<Item = Result<(LogPointer,T)>>+'a
    where
        T: serde::de::DeserializeOwned+'a
    {
        self.read_file_buffers
        .iter()
        .flat_map(|(serial,file_buf)|{
            let wrapped_buf_ref=FileReadBufRefWrapper(file_buf.clone());
            let stream=serde_json::Deserializer::from_reader(wrapped_buf_ref);
            let offset_stream=OffsetStreamSerializer::new(stream.into_iter::<T>());
            
            offset_stream
            .map(|res|
                res.map(|(offset,parsed)|(LogPointer::new(*serial,offset,file_buf.clone()),parsed))
            )
        })
    }

    pub fn iter_read_files(&self)->impl Iterator<Item = (&usize,&FileReadBufRef)>{
        self.read_file_buffers.iter()
    }
    
    pub fn storage_size(&self)->usize{
        self.cur_storage_size
    }

    fn write_bytes(&mut self,bytes:&[u8])->Result<LogPointer>{
        let data_size=bytes.len();
        if data_size+self.cur_file_size>self.file_size_limit{
            self.replace_write_file()?
        }

        let (file_serial,read_buf_ref)=self.read_file_buffers.last_key_value().expect("Always at least 1 file");
        let offset=self.cur_write_file.stream_position().map_err(|_|KVError::IOError("LogStorage::write_bytes1"))?;
        self.cur_write_file.write_all(bytes).map_err(|_|KVError::WriteError("LogStorage::write_bytes2"))?;
        self.cur_file_size+=data_size;
        self.cur_storage_size+=data_size;

        Ok(
            LogPointer::new(
                *file_serial,
                offset,
                read_buf_ref.clone()
            )
        )
    }

    fn replace_write_file(&mut self)->Result<()>{
        let new_file_serial=self.read_file_buffers.last_entry().expect("Always at least 1 file").key()+1;
        let new_file_path=self.directory.join(new_file_serial.to_string());

        let new_write_file=Self::new_log_file(new_file_path.clone())?;

        let new_read_file_buf=new_file_read_buf_ref(Self::get_log_file(new_file_path)?);

        self.read_file_buffers.insert(new_file_serial,new_read_file_buf);
        self.cur_write_file=new_write_file;
        self.cur_file_size=0;

        Ok(())
    }


    fn load_persisted_files(directory:&PathBuf)->Result<(usize,BTreeMap<usize,FileReadBufRef>)>{
        let sorted_file_names=get_sorted_file_names(directory)?;
        let mut size=0;

        let mut read_file_pool=BTreeMap::new();
        
        for (num,sorted_file_names) in sorted_file_names.iter(){
            let file_name=directory.join(sorted_file_names);
            let file=OpenOptions::new()
            .read(true)
            .open(file_name)
            .map_err(|_|KVError::IOError("LogStorage::load_persited_files1"))?;
            size+=file.metadata().map_err(|_|KVError::IOError("LogStorage::load_persisted_files2"))?.len() as usize;

            let file_buf=Rc::new(RefCell::new(BufReader::new(file)));
            read_file_pool.insert(*num,file_buf);
        }

        Ok((size,read_file_pool))
    }
    
    fn new_log_file(path:PathBuf)->Result<File>{
        OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)
        .map_err(|_|KVError::IOError("LogStorage::new_log_file"))
    }

    fn get_log_file(path:PathBuf)->Result<File>{
        OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(|_|KVError::IOError("LogStorage::get_log_file"))
    }
    
}

impl LogPointer {
    fn new(file_serial:usize,offset:u64,file_buf:FileReadBufRef)->LogPointer{
        LogPointer{
            file_buf,
            offset,
            file_serial
        }
    }
    pub fn read<T: serde::de::DeserializeOwned>(&self)->Result<T>{
        self.file_buf.borrow_mut().seek(std::io::SeekFrom::Start(self.offset)).map_err(|_|KVError::IOError("LogPointer::read"))?;
        
        let mut cell_ref=self.file_buf.borrow_mut();
        let deserializer=serde_json::Deserializer::from_reader(cell_ref.deref_mut());
        let mut stream_deserializer=deserializer.into_iter();
        
        let operation:T=stream_deserializer
        .next()
        .ok_or(KVError::ReadError("LogPointer::read2"))?
        .map_err(|_|KVError::ParseError("LogPointer::read"))?;

        Ok(operation)
    }
}

impl Read for FileReadBufRefWrapper{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.0.borrow_mut().read(buf)
    }
}


fn new_file_read_buf_ref(file:File)->FileReadBufRef{
    Rc::new(RefCell::new(BufReader::new(file)))
}

pub fn osstring_parse<T>(osstring:&OsString)->Result<T>
    where T:FromStr
{
    let string=osstring.clone().into_string().map_err(|_|KVError::ParseError("osstring_parsea"))?;
    let res=string.parse().map_err(|_|KVError::ParseError("osstring_parseb"))?;

    Ok(res)
}

fn get_sorted_file_names(dir_path:&PathBuf)->Result<Vec<(usize,OsString)>>{
    let dir=read_dir(dir_path).map_err(|_|KVError::IOError("KvStore::get_sorted_file_names"))?;
    let mut file_names=dir
    .into_iter()
    .map(|entry| 
        entry
        .map(|entry|entry.file_name())
        .map_err(|_|KVError::ReadError("KvStore::get_sorted_file_names"))
        .and_then(|name|Ok((osstring_parse(&name)?,name)))
    )
    .collect::<Result<Vec<(usize,OsString)>>>()?;
    
    file_names.sort();

    Ok(file_names)
}