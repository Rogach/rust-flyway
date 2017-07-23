extern crate flyway;

use flyway::{Reader, MigrationFile};
use std::path::PathBuf;
use std::fs;
use std::fs::File;
use std::io;
use std::io::Read;

pub struct DirectoryReader {
    directory: PathBuf
}

impl DirectoryReader {
    pub fn new(directory: PathBuf) -> DirectoryReader {
        DirectoryReader { directory }
    }
}

fn read_migrations_from_directory(directory: &PathBuf) -> io::Result<Vec<MigrationFile>> {
    if directory.is_dir() {
        let mut files = Vec::new();
        for entry in fs::read_dir(directory)? {
            let path = entry?.path();
            if path.is_file() {
                let mut migration_file = File::open(&path)?;
                let mut contents = String::new();
                migration_file.read_to_string(&mut contents)?;
                files.push(MigrationFile {
                    name: path.file_name().and_then(|n| n.to_str()).unwrap_or("").to_owned(),
                    contents: contents
                });
            }
        }
        Ok(files)
    } else {
        Err(io::Error::new(io::ErrorKind::Other, format!("path '{:?}' is not a directory", directory)))
    }
}

impl Reader for DirectoryReader {
    fn read_migrations(&self) -> Result<Vec<MigrationFile>, String> {
        match read_migrations_from_directory(&self.directory) {
            Ok(files) => Ok(files),
            Err(err) => Err(err.to_string())
        }
    }
}

#[test] fn test_dir_reading() {
    assert_eq!(
        DirectoryReader::new(PathBuf::from("assets")).read_migrations(),
        Ok(vec![
            MigrationFile {
                name: "V1.0.0__a.sql".into(),
                contents: "select user();\n".into()
            }
        ])
    )
}
