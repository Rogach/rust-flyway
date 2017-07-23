extern crate flyway;

use flyway::{Reader, MigrationFile};
use std::collections::HashMap;

pub struct EmbedReader {
    files: HashMap<Vec<u8>, Vec<u8>>
}

impl EmbedReader {
    pub fn new(files: HashMap<Vec<u8>, Vec<u8>>) -> EmbedReader {
        EmbedReader { files }
    }
}

impl Reader for EmbedReader {
    fn read_migrations(&self) -> Result<Vec<MigrationFile>, String> {
        let mut files = Vec::new();
        for (name_bytes, contents_bytes) in self.files.clone() {
            files.push(MigrationFile {
                name: match String::from_utf8(name_bytes) {
                    Ok(s) => s,
                    Err(err) => return Err(err.to_string())
                },
                contents: match String::from_utf8(contents_bytes) {
                    Ok(s) => s,
                    Err(err) => return Err(err.to_string())
                }
            })
        }
        Ok(files)
    }
}

#[test] fn test_embed_reading() {
    let mut files = HashMap::new();
    files.insert(
        "V1.0.0__a.sql".as_bytes().to_owned(),
        "select 1;".as_bytes().to_owned()
    );
    files.insert(
        "V1.0.1__b.sql".as_bytes().to_owned(),
        "select 2;".as_bytes().to_owned(),
    );


    let expected = vec![
        MigrationFile {
            name: "V1.0.0__a.sql".into(),
            contents: "select 1;".into()
        },
        MigrationFile {
            name: "V1.0.1__b.sql".into(),
            contents: "select 2;".into()
        },
    ];

    let migration_files = EmbedReader::new(files).read_migrations();

    match migration_files.clone() {
        Ok(mut migration_files) => {
            migration_files.sort_by_key(|m| m.name.clone());
            assert_eq!(expected, migration_files);
        },
        Err(_) => assert_eq!(Ok(expected), migration_files)
    }

}
