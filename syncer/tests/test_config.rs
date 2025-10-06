#[cfg(test)]
mod test_config {
    use chrono::NaiveDate;
    use std::collections::HashMap;
    use std::fs;
    use std::path::PathBuf;
    use syncer::{LocalConfig, RemoteConfig};
    use tempfile::NamedTempFile;

    #[test]
    fn test_local_config_from_toml() {
        let toml_content = r#"
tables = ["table_a", "table_b"]
start_time = "2025-10-01"
local_storage_path = "/data/exports"

[table_event_mappings]
table_a = "EventTypeA"
table_b = "EventTypeB"

[remote_server]
address = "192.168.1.100"
port = 22
username = "datauser"
private_key_path = "/home/user/.ssh/id_rsa"
remote_path = "/remote/data/imports"
"#;

        let temp_file = NamedTempFile::new().unwrap();
        fs::write(temp_file.path(), toml_content).unwrap();

        let config = LocalConfig::from_file(temp_file.path().to_str().unwrap()).unwrap();

        assert_eq!(config.tables, vec!["table_a", "table_b"]);
        assert_eq!(
            config.table_event_mappings.get("table_a").unwrap(),
            "EventTypeA"
        );
        assert_eq!(
            config.start_time,
            NaiveDate::from_ymd_opt(2025, 10, 1).unwrap()
        );
        assert_eq!(config.local_storage_path, PathBuf::from("/data/exports"));
        assert_eq!(config.remote_server.address, "192.168.1.100");
        assert_eq!(config.remote_server.port, 22);
    }

    #[test]
    fn test_remote_config_from_toml() {
        let toml_content = r#"
remote_storage_path = "/remote/data/imports"

[import_mappings]
source_a = "target_a"
source_b = "target_b"

[table_event_mappings]
source_a = "EventTypeA"
source_b = "EventTypeB"
"#;

        let temp_file = NamedTempFile::new().unwrap();
        fs::write(temp_file.path(), toml_content).unwrap();

        let config = RemoteConfig::from_file(temp_file.path().to_str().unwrap()).unwrap();

        assert_eq!(
            config.remote_storage_path,
            PathBuf::from("/remote/data/imports")
        );
        assert_eq!(config.import_mappings.get("source_a").unwrap(), "target_a");
        assert_eq!(
            config.table_event_mappings.get("source_a").unwrap(),
            "EventTypeA"
        );
    }

    #[test]
    fn test_local_config_invalid_date() {
        let toml_content = r#"
tables = ["table_a"]
start_time = "invalid-date"
local_storage_path = "/data/exports"

[table_event_mappings]
table_a = "EventTypeA"

[remote_server]
address = "192.168.1.100"
port = 22
username = "datauser"
private_key_path = "/home/user/.ssh/id_rsa"
remote_path = "/remote/data/imports"
"#;

        let temp_file = NamedTempFile::new().unwrap();
        fs::write(temp_file.path(), toml_content).unwrap();

        let result = LocalConfig::from_file(temp_file.path().to_str().unwrap());
        assert!(result.is_err());
    }

    #[test]
    fn test_local_config_missing_field() {
        let toml_content = r#"
tables = ["table_a"]
local_storage_path = "/data/exports"

[table_event_mappings]
table_a = "EventTypeA"

[remote_server]
address = "192.168.1.100"
port = 22
username = "datauser"
private_key_path = "/home/user/.ssh/id_rsa"
remote_path = "/remote/data/imports"
"#;

        let temp_file = NamedTempFile::new().unwrap();
        fs::write(temp_file.path(), toml_content).unwrap();

        let result = LocalConfig::from_file(temp_file.path().to_str().unwrap());
        assert!(result.is_err());
    }

    #[test]
    fn test_remote_config_empty_mappings() {
        let toml_content = r#"
remote_storage_path = "/remote/data/imports"

[import_mappings]

[table_event_mappings]
"#;

        let temp_file = NamedTempFile::new().unwrap();
        fs::write(temp_file.path(), toml_content).unwrap();

        let config = RemoteConfig::from_file(temp_file.path().to_str().unwrap()).unwrap();

        assert!(config.import_mappings.is_empty());
        assert!(config.table_event_mappings.is_empty());
    }

    #[test]
    fn test_local_config_serialization() {
        let mut table_event_mappings = HashMap::new();
        table_event_mappings.insert("table_a".to_string(), "EventTypeA".to_string());

        let config = LocalConfig {
            tables: vec!["table_a".to_string()],
            table_event_mappings,
            start_time: NaiveDate::from_ymd_opt(2025, 10, 1).unwrap(),
            local_storage_path: PathBuf::from("/data/exports"),
            remote_server: syncer::RemoteServerConfig {
                address: "192.168.1.100".to_string(),
                port: 22,
                username: "datauser".to_string(),
                private_key_path: PathBuf::from("/home/user/.ssh/id_rsa"),
                remote_path: PathBuf::from("/remote/data/imports"),
            },
        };

        let toml_str = toml::to_string(&config).unwrap();
        assert!(toml_str.contains("tables"));
        assert!(toml_str.contains("table_a"));
        assert!(toml_str.contains("2025-10-01"));
    }

    #[test]
    fn test_date_format_parsing() {
        let toml_content = r#"
tables = ["table_a"]
start_time = "2025-12-31"
local_storage_path = "/data/exports"

[table_event_mappings]
table_a = "EventTypeA"

[remote_server]
address = "192.168.1.100"
port = 22
username = "datauser"
private_key_path = "/home/user/.ssh/id_rsa"
remote_path = "/remote/data/imports"
"#;

        let temp_file = NamedTempFile::new().unwrap();
        fs::write(temp_file.path(), toml_content).unwrap();

        let config = LocalConfig::from_file(temp_file.path().to_str().unwrap()).unwrap();

        assert_eq!(
            config.start_time,
            NaiveDate::from_ymd_opt(2025, 12, 31).unwrap()
        );
    }
}
