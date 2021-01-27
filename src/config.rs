use std::env;
use core::str::FromStr;

// DEFAULT DB CONFIG
pub const SOURCE_DB_DEFAULT_HOST:&str = "localhost";
pub const SOURCE_DB_DEFAULT_PORT:&str = "5432";
pub const SOURCE_DB_DEFAULT_DATABASE:&str = "postgres";
pub const SOURCE_DB_DEFAULT_USER:&str = "postgres";
pub const SOURCE_DB_DEFAULT_PASS:&str = "";

pub const TARGET_DB_DEFAULT_HOST:&str = "localhost";
pub const TARGET_DB_DEFAULT_PORT:&str = "5555";
pub const TARGET_DB_DEFAULT_DATABASE:&str = "postgres";
pub const TARGET_DB_DEFAULT_USER:&str = "postgres";
pub const TARGET_DB_DEFAULT_PASS:&str = "";

// OTHER DEFAULT CONFIG
pub const DEFAULT_MAX_THREADS:i64 = 8;
pub const DEFAULT_ROWS_TO_EXECUTE_INSERT:i64 = 1000;
pub const DEFAULT_MAX_ROWS_FOR_SELECT:i64 = 10000;
pub const ERROR_LOG_ENABLED:bool = false;
pub const DEFAULT_IMPORTER_IMPL:&str = "COPY";

// Encapsulates all DB and config info needed for a worker thread to do an import
pub struct ImportConfig {
    pub schema:String,
    pub table:String,
    pub where_clause:String,
    pub source_db_url:String,
    pub target_db_url:String,    
    pub importer_impl:String
}

pub enum ConfigProperty {
    SourceDBHost,
    SourceDBPort,
    SourceDBUser,
    SourceDBPass,
    SourceDBDatabase,
    TargetDBHost,
    TargetDBPort,
    TargetDBUser,
    TargetDBPass,
    TargetDBDatabase,
    MaxThreads,
    RowsToExecuteInsert,
    MaxRowsForSelect,
    ErrorLogEnabled,
    ImporterImplementation
}

// TODO: Make this methods private and publish a map instead
// Populate that map once the app is starting, using the following sources (in this order):
// - If batch mode => Read config from batch file (if available. If existing, all parameters must be there)
// - params from command line
// - Environment vars
// - Default values

pub fn get_config_property<T>(property : ConfigProperty, default_value: T) -> T where T : FromStr {
    match property {
        ConfigProperty::SourceDBHost => environment_or_default(&"SOURCE_DB_HOST", default_value),
        ConfigProperty::SourceDBPort => environment_or_default(&"SOURCE_DB_PORT", default_value),
        ConfigProperty::SourceDBUser => environment_or_default(&"SOURCE_DB_USER", default_value),
        ConfigProperty::SourceDBPass => environment_or_default(&"SOURCE_DB_PASS", default_value),
        ConfigProperty::SourceDBDatabase => environment_or_default(&"SOURCE_DB_DATABASE", default_value),
        ConfigProperty::TargetDBHost => environment_or_default(&"TARGET_DB_HOST", default_value),
        ConfigProperty::TargetDBPort => environment_or_default(&"TARGET_DB_PORT", default_value),
        ConfigProperty::TargetDBUser => environment_or_default(&"TARGET_DB_USER", default_value),
        ConfigProperty::TargetDBPass => environment_or_default(&"TARGET_DB_PASS", default_value),
        ConfigProperty::TargetDBDatabase => environment_or_default(&"TARGET_DB_DATABASE", default_value),
        ConfigProperty::MaxThreads => environment_or_default(&"MAX_THREADS", default_value),
        ConfigProperty::RowsToExecuteInsert => environment_or_default(&"ROWS_FOR_INSERT", default_value),
        ConfigProperty::MaxRowsForSelect => environment_or_default(&"ROWS_FOR_SELECT", default_value),
        ConfigProperty::ErrorLogEnabled => environment_or_default(&"ERROR_LOG", default_value),
        ConfigProperty::ImporterImplementation => environment_or_default(&"IMPORTER_IMPL", default_value)
    }
}

pub fn get_source_db_url() -> String {
    return get_source_db_url_with_hiding(false);
}

pub fn get_target_db_url() -> String {
    return get_target_db_url_with_hiding(false);
}

pub fn get_source_db_url_with_hiding(hide_pass:bool) -> String {
    let host:String = get_config_property(ConfigProperty::SourceDBHost, SOURCE_DB_DEFAULT_HOST.to_owned());
    let port:String = get_config_property(ConfigProperty::SourceDBPort, SOURCE_DB_DEFAULT_PORT.to_owned());
    let database:String = get_config_property(ConfigProperty::SourceDBDatabase, SOURCE_DB_DEFAULT_DATABASE.to_owned());
    let user:String = get_config_property(ConfigProperty::SourceDBUser, SOURCE_DB_DEFAULT_USER.to_owned());
    let mut pass:String = get_config_property(ConfigProperty::SourceDBPass, SOURCE_DB_DEFAULT_PASS.to_owned());
    if hide_pass {
        pass = String::from("**HIDDEN**");
    }
    format!("host='{}' port='{}' dbname='{}' user='{}' password='{}'", host , port, database, user, pass)
}

pub fn get_target_db_url_with_hiding(hide_pass:bool) -> String {
    let host:String = get_config_property(ConfigProperty::TargetDBHost, TARGET_DB_DEFAULT_HOST.to_owned());
    let port:String = get_config_property(ConfigProperty::TargetDBPort, TARGET_DB_DEFAULT_PORT.to_owned());
    let database:String = get_config_property(ConfigProperty::TargetDBDatabase, TARGET_DB_DEFAULT_DATABASE.to_owned());
    let user:String = get_config_property(ConfigProperty::TargetDBUser, TARGET_DB_DEFAULT_USER.to_owned());
    let mut pass:String = get_config_property(ConfigProperty::TargetDBPass, TARGET_DB_DEFAULT_PASS.to_owned());
    if hide_pass {
        pass = String::from("**HIDDEN**");
    }
    format!("host='{}' port='{}' dbname='{}' user='{}' password='{}'", host , port, database, user, pass)
}

fn environment_or_default<T> (key:&str, default_value: T) -> T where T: FromStr {
    match env::var(key) {
        Ok(env_value) => { 
            if let Ok(value) = env_value.parse::<T>() {
             return value 
            }
            else{
                panic!("Error: Couldn't parse the value of env var {}={} to the proper type", key, env_value);
            }
        },
        Err(_e) => default_value
    }
}