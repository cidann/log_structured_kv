
use std::{fs::OpenOptions, io::BufReader, path::PathBuf};

use serde::Deserialize;

use crate::common::{KILOBYTE, MEGABYTE};
#[derive(Deserialize)]
pub struct Config{
    pub db_dir:String,
    pub file_size:usize,
    pub merge_size:usize,
}


impl Default for Config{
    fn default() -> Self {
        Self { db_dir: ".".to_string(), file_size: 4*MEGABYTE, merge_size: 10*KILOBYTE }
    }
}

impl Config {
    pub fn open(config_path:PathBuf)->Config{
        let file=OpenOptions::new().read(true).open(config_path);
        match file {
            Ok(file) => {
                serde_json::from_reader(BufReader::new(file)).unwrap_or_default()
            },
            Err(_) => Config::default(),
        }
    }
}