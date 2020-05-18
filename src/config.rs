use std::env;
use core::str::FromStr;

// DEFAULT DB CONFIG
pub const SOURCE_DB_DEFAULT_HOST:&str = "localhost";
pub const SOURCE_DB_DEFAULT_PORT:&str = "5432";
pub const SOURCE_DB_DEFAULT_USER:&str = "postgres";
pub const SOURCE_DB_DEFAULT_PASS:&str = "";

pub const TARGET_DB_DEFAULT_HOST:&str = "localhost";
pub const TARGET_DB_DEFAULT_PORT:&str = "5555";
pub const TARGET_DB_DEFAULT_USER:&str = "postgres";
pub const TARGET_DB_DEFAULT_PASS:&str = "";

// OTHER DEFAULT CONFIG
pub const DEFAULT_MAX_THREADS:i64 = 8;
pub const DEFAULT_ROWS_TO_EXECUTE_INSERT:i64 = 100;
pub const DEFAULT_MAX_ROWS_FOR_SELECT:i64 = 1000;
pub const ERROR_LOG_ENABLED:bool = false;

pub enum ConfigProperty {
    SourceDBHost,
    SourceDBPort,
    SourceDBUser,
    SourceDBPass,
    TargetDBHost,
    TargetDBPort,
    TargetDBUser,
    TargetDBPass,
    MaxThreads,
    RowsToExecuteInsert,
    MaxRowsForSelect,
    ErrorLogEnabled
}

pub fn get_config_property<T>(property : ConfigProperty, default_value: T) -> T where T : FromStr {
    match property {
        ConfigProperty::SourceDBHost => environment_or_default(&"SOURCE_DB_HOST", default_value),
        ConfigProperty::SourceDBPort => environment_or_default(&"SOURCE_DB_PORT", default_value),
        ConfigProperty::SourceDBUser => environment_or_default(&"SOURCE_DB_USER", default_value),
        ConfigProperty::SourceDBPass => environment_or_default(&"SOURCE_DB_PASS", default_value),
        ConfigProperty::TargetDBHost => environment_or_default(&"TARGET_DB_HOST", default_value),
        ConfigProperty::TargetDBPort => environment_or_default(&"TARGET_DB_PORT", default_value),
        ConfigProperty::TargetDBUser => environment_or_default(&"TARGET_DB_USER", default_value),
        ConfigProperty::TargetDBPass => environment_or_default(&"TARGET_DB_PASS", default_value),
        ConfigProperty::MaxThreads => environment_or_default(&"MAX_THREADS", default_value),
        ConfigProperty::RowsToExecuteInsert => environment_or_default(&"ROWS_FOR_INSERT", default_value),
        ConfigProperty::MaxRowsForSelect => environment_or_default(&"ROWS_FOR_SELECT", default_value),
        ConfigProperty::ErrorLogEnabled => environment_or_default(&"ERROR_LOG", default_value)
    }
}

pub fn get_source_db_url() -> String {
    let host:String = get_config_property(ConfigProperty::SourceDBHost, SOURCE_DB_DEFAULT_HOST.to_owned());
    let port:String = get_config_property(ConfigProperty::SourceDBPort, SOURCE_DB_DEFAULT_PORT.to_owned());
    let user:String = get_config_property(ConfigProperty::SourceDBUser, SOURCE_DB_DEFAULT_USER.to_owned());
    let pass:String = get_config_property(ConfigProperty::SourceDBPass, SOURCE_DB_DEFAULT_PASS.to_owned());
    format!("host='{}' port='{}' user='{}' password='{}'", host , port, user, pass)
}

pub fn get_target_db_url() -> String {
    let host:String = get_config_property(ConfigProperty::TargetDBHost, TARGET_DB_DEFAULT_HOST.to_owned());
    let port:String = get_config_property(ConfigProperty::TargetDBPort, TARGET_DB_DEFAULT_PORT.to_owned());
    let user:String = get_config_property(ConfigProperty::TargetDBUser, TARGET_DB_DEFAULT_USER.to_owned());
    let pass:String = get_config_property(ConfigProperty::TargetDBPass, TARGET_DB_DEFAULT_PASS.to_owned());
    format!("host='{}' port='{}' user='{}' password='{}'", host , port, user, pass)
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