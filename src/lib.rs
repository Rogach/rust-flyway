#![feature(proc_macro)]
#[cfg(test)] extern crate mockers;
#[cfg(test)] extern crate mockers_derive;
extern crate itertools;
extern crate regex;
#[macro_use] extern crate lazy_static;
extern crate crc;
extern crate semver;

#[cfg(test)] mod tests;

use itertools::join;
#[cfg(test)] use mockers_derive::derive_mock;
use regex::Regex;
use crc::crc32;
use std::collections::HashMap;
use semver::Version;
use std::cmp::Ordering;
use std::time::Instant;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Migration {
    pub version: String,
    pub description: String,
    pub migration_type: String,
    pub script: String,
    pub checksum: i32,
    pub execution_time: i32,
    pub success: bool,
    pub contents: String
}

#[derive(Clone, Debug)]
pub struct MigrationFile {
    pub name: String,
    pub contents: String
}

#[cfg_attr(test, derive_mock)]
pub trait Driver {
    fn ensure_schema_version_exists(&self) -> Result<(), String>;
    fn get_failed_migrations(&self) -> Result<Vec<Migration>, String>;
    fn get_existing_migrations(&self) -> Result<Vec<Migration>, String>;
    fn execute_migration(&self, sql: String) -> Result<(), String>;
    fn save_migration(&self, migration: Migration) -> Result<(), String>;
}

#[cfg_attr(test, derive_mock)]
pub trait Reader {
    fn read_migrations(&self) -> Result<Vec<MigrationFile>, String>;
}

pub struct Flyway {
    reader: Box<Reader>,
    driver: Box<Driver>
}
lazy_static! {
    static ref MIGRATION_NAME_REGEX: Regex = Regex::new(r"V([\.0-9]+)__(.*).sql").unwrap();
}
impl Flyway {
    pub fn new(reader: Box<Reader>, driver: Box<Driver>) -> Flyway {
        Flyway { reader, driver }
    }

    fn parse_migration_name(name: &str) -> Option<(String, String)> {
        MIGRATION_NAME_REGEX.captures(name).map(|c| {
            (c[1].to_owned(), c[2].to_owned())
        })
    }

    fn read_migration(file: MigrationFile) -> Result<Migration, String> {
        Flyway::parse_migration_name(&file.name).map(|(version, description)| {
            Migration {
                version: version,
                description: description,
                migration_type: String::from("SQL"),
                script: file.name.clone(),
                checksum: crc32::checksum_ieee(file.contents.as_bytes()) as i32,
                execution_time: 0,
                success: false,
                contents: file.contents.clone()
            }
        }).ok_or(format!("Failed to parse migration file name: {}", file.name))
    }

    pub fn execute(&self) -> Result<(), String> {
        let failed_migrations = self.driver.get_failed_migrations()?;
        if !failed_migrations.is_empty() {
            return Err(format!("Failed migrations detected! Roll back your database and start from a fresh backup. Failed migrations: {}", join(failed_migrations.iter().map(|m| &m.version), ", ")));
        }

        let migration_files = self.reader.read_migrations()?;
        let mut incoming_migrations = Vec::new();
        for migration_file in migration_files.into_iter() {
            let migration = Flyway::read_migration(migration_file)?;
            incoming_migrations.push(migration);
        }

        let migration_comparator = |a: &Migration, b: &Migration| {
            Version::parse(&a.version).unwrap().cmp(&Version::parse(&b.version).unwrap())
        };
        incoming_migrations.sort_by(&migration_comparator);

        let mut existing_migrations = self.driver.get_existing_migrations()?;
        for existing_migration in &existing_migrations {
            match incoming_migrations.iter().find(|m| m.version == existing_migration.version) {
                Some(incoming_migration) => {
                    if incoming_migration.checksum != existing_migration.checksum {
                        return Err(format!("Checksum mismatch for migration {}: existing migration {}, incoming migration {}", existing_migration.version, existing_migration.checksum, incoming_migration.checksum))
                    }
                },
                None => return Err(format!("Incoming migrations do not contain migration {} - seems you are running code that is older than database contents.", existing_migration.version))
            }
        }
        existing_migrations.sort_by(&migration_comparator);

        let existing_migrations_idx: HashMap<String, &Migration> =
            existing_migrations.iter().map(|m| (m.script.clone(), m)).collect();

        let new_migrations: Vec<&Migration> =
            incoming_migrations.iter().filter(|m| !existing_migrations_idx.contains_key(&m.script)).collect();

        if let Some(newest_existing_migration) = existing_migrations.iter().last() {
            if let Some(older_incoming_migration) = new_migrations.iter().find(|m| migration_comparator(newest_existing_migration, m) != Ordering::Less) {
                return Err(format!("Incoming new migration is older than existing: {}", older_incoming_migration.script));
            }
        }

        for new_migration in new_migrations {
            let mut new_migration = new_migration.to_owned();
            let start = Instant::now();
            let result = self.driver.execute_migration(new_migration.contents.clone());
            let elapsed = start.elapsed();
            new_migration.execution_time = (elapsed.as_secs() * 1000 + (elapsed.subsec_nanos() / 1_000_000) as u64) as i32;
            match result {
                Ok(()) => {
                    new_migration.success = true;
                    self.driver.save_migration(new_migration)?;
                },
                Err(error) => {
                    new_migration.success = false;
                    self.driver.save_migration(new_migration)?;
                    return Err(error)
                }
            }
        }

        Ok(())
    }
}
