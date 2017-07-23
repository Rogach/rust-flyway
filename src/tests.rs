use super::*;
use mockers::Scenario;

fn test_migration() -> Migration {
    Migration {
        version: "1.0.0".into(),
        description: "".into(),
        migration_type: "SQL".into(),
        script: "V1.0.0__.sql".into(),
        checksum: 0,
        execution_time: 0,
        success: true
    }
}

#[test] fn test_fail_if_got_failed_migrations() {
    let sc = Scenario::new();
    let reader = sc.create_mock_for::<Reader>();
    let driver = sc.create_mock_for::<Driver>();
    sc.expect(driver.get_failed_migrations_call().and_return(Ok(vec![
        { let mut m = test_migration(); m.success = false; m }
    ])));
    let f = Flyway::new(Box::new(reader), Box::new(driver));
    assert_eq!(f.execute(), Err("Failed migrations detected! Roll back your database and start from a fresh backup. Failed migrations: 1.0.0".into()));
}

#[test] fn test_migration_name_parse() {
    assert_eq!(Flyway::parse_migration_name("V1__a.sql"),
               Some(("1".into(), "a".into())));
    assert_eq!(Flyway::parse_migration_name("V1.0__a.sql"),
               Some(("1.0".into(), "a".into())));
    assert_eq!(Flyway::parse_migration_name("V1.0__ab.sql"),
               Some(("1.0".into(), "ab".into())));
    assert_eq!(Flyway::parse_migration_name("V1.0.2__ab__23.sql"),
               Some(("1.0.2".into(), "ab__23".into())));
}

#[test] fn test_database_newer() {
    let sc = Scenario::new();
    let reader = sc.create_mock_for::<Reader>();
    let driver = sc.create_mock_for::<Driver>();
    sc.expect(driver.get_failed_migrations_call().and_return(Ok(vec![])));
    sc.expect(reader.read_migrations_call().and_return(Ok(vec![])));
    sc.expect(driver.get_existing_migrations_call().and_return(Ok(vec![
        test_migration()
    ])));

    let f = Flyway::new(Box::new(reader), Box::new(driver));
    assert_eq!(f.execute(), Err("Incoming migrations do not contain migration 1.0.0 - seems you are running code that is older than database contents.".into()));
}

#[test] fn test_checksum_mismatch() {
    let sc = Scenario::new();
    let reader = sc.create_mock_for::<Reader>();
    let driver = sc.create_mock_for::<Driver>();
    sc.expect(driver.get_failed_migrations_call().and_return(Ok(vec![])));
    sc.expect(reader.read_migrations_call().and_return(Ok(vec![
        MigrationFile { name: "V1.0.0__a.sql".into(), contents: "42".into() }
    ])));
    sc.expect(driver.get_existing_migrations_call().and_return(Ok(vec![
        test_migration()
    ])));

    let f = Flyway::new(Box::new(reader), Box::new(driver));
    assert_eq!(f.execute(), Err("Checksum mismatch for migration 1.0.0: existing migration 0, incoming migration 841265288".into()));
}

#[test] fn test_older_incoming_migration() {
    let sc = Scenario::new();
    let reader = sc.create_mock_for::<Reader>();
    let driver = sc.create_mock_for::<Driver>();
    sc.expect(driver.get_failed_migrations_call().and_return(Ok(vec![])));
    sc.expect(reader.read_migrations_call().and_return(Ok(vec![
        MigrationFile { name: "V0.2.0__a.sql".into(), contents: "42".into() },
        MigrationFile { name: "V1.0.0__.sql".into(), contents: "".into() }
    ])));
    sc.expect(driver.get_existing_migrations_call().and_return(Ok(vec![
        test_migration()
    ])));

    let f = Flyway::new(Box::new(reader), Box::new(driver));
    assert_eq!(f.execute(), Err("Incoming new migration is older than existing: V0.2.0__a.sql".into()));
}
