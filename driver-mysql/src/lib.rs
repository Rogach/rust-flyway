extern crate flyway;
#[macro_use] extern crate mysql;
extern crate regex;

use flyway::{Driver, Migration};

pub struct MysqlDriver {
    connection_pool: mysql::Pool
}

impl MysqlDriver {
    pub fn new(connection_pool: mysql::Pool) -> MysqlDriver {
        MysqlDriver { connection_pool }
    }

    fn query_migrations(&self) -> Result<Vec<Migration>, mysql::error::Error> {
        let rows = self.connection_pool.prep_exec(
            r#"
select
  `version`,
  `description`,
  `type`,
  `script`,
  `checksum`,
  `execution_time`,
  `success`
from `schema_version`;
"#,
            ()
        )?;
        let mut migrations = Vec::new();
        for row in rows {
            let mut row = row?;
            migrations.push(Migration {
                version: row.take("version").unwrap(),
                description: row.take("description").unwrap(),
                migration_type: row.take("type").unwrap(),
                script: row.take("script").unwrap(),
                checksum: row.take("checksum").unwrap(),
                execution_time: row.take("execution_time").unwrap(),
                success: row.take::<u8, &str>("success").unwrap() == 1,
                contents: String::new()
            });
        }
        Ok(migrations)
    }

    fn mysql_execute_migration(&self, sql: String) -> Result<(), mysql::error::Error> {
        let mut transaction = self.connection_pool.start_transaction(false, None, None)?;
        for statement in regex::Regex::new("; *\n").unwrap().split(&sql) {
            transaction.prep_exec(statement, ())?;
        }
        Ok(())
    }

    fn mysql_save_migration(&self, migration: Migration) -> Result<(), mysql::error::Error> {
        let rank: i32 = self.connection_pool.first_exec(
            "select ifnull(max(`installed_rank`), 0) as max_rank from `schema_version`;", ()
        )?.and_then(|mut r| r.take::<i32, &str>("max_rank")).unwrap_or(0) + 1;
        self.connection_pool.prep_exec(
            r#"
insert into `schema_version`(
  `installed_rank`,
  `version`,
  `description`,
  `type`,
  `script`,
  `checksum`,
  `installed_by`,
  `execution_time`,
  `success`
)
values(
  :rank,
  :version,
  :description,
  :type,
  :script,
  :checksum,
  user(),
  :execution_time,
  :success
)
"#,
            params!{
                "rank" => rank,
                "version" => migration.version,
                "description" => migration.description,
                "type" => migration.migration_type,
                "script" => migration.script,
                "checksum" => migration.checksum,
                "execution_time" => migration.execution_time,
                "success" => if migration.success { 1 } else { 0 }
            }
        )?;
        Ok(())
    }
}

fn rewrap_mysql_result<T>(res: Result<T, mysql::error::Error>) -> Result<T, String> {
    match res {
        Ok(r) => Ok(r),
        Err(err) => Err(err.to_string())
    }
}

impl Driver for MysqlDriver {
    fn ensure_schema_version_exists(&self) -> Result<(), String> {
        rewrap_mysql_result(self.connection_pool.first_exec(
            r#"
CREATE TABLE IF NOT EXISTS `schema_version` (
  `installed_rank` int(11) NOT NULL,
  `version` varchar(50) DEFAULT NULL,
  `description` varchar(200) NOT NULL,
  `type` varchar(20) NOT NULL,
  `script` varchar(1000) NOT NULL,
  `checksum` int(11) DEFAULT NULL,
  `installed_by` varchar(100) NOT NULL,
  `installed_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `execution_time` int(11) NOT NULL,
  `success` tinyint(1) NOT NULL,
  PRIMARY KEY (`installed_rank`),
  KEY `schema_version_s_idx` (`success`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"#,
            ()
        )).map(|_| ())
    }

    fn get_existing_migrations(&self) -> Result<Vec<Migration>, String> {
        rewrap_mysql_result(self.query_migrations())
    }

    fn execute_migration(&self, sql: String) -> Result<(), String> {
        rewrap_mysql_result(self.mysql_execute_migration(sql))
    }

    fn save_migration(&self, migration: Migration) -> Result<(), String> {
        rewrap_mysql_result(self.mysql_save_migration(migration))
    }
}
