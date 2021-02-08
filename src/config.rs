use core::str::FromStr;
use clap::Clap;
use lazy_static::lazy_static;
use regex::{Regex, Error};
use std::collections::HashMap;
use std::env;

// [user[:password]@][netloc][:port][/dbname]

// DEFAULT DB CONFIG
pub const SOURCE_DB_CONNECTION:&str = "postgres@localhost:5432/postgres";
pub const SOURCE_DB_DEFAULT_HOST:&str = "localhost";
pub const SOURCE_DB_DEFAULT_PORT:&str = "5432";
pub const SOURCE_DB_DEFAULT_DATABASE:&str = "postgres";
pub const SOURCE_DB_DEFAULT_USER:&str = "postgres";
pub const SOURCE_DB_DEFAULT_PASS:&str = "";

pub const TARGET_DB_CONNECTION:&str = "postgres@localhost:5555/postgres";
pub const TARGET_DB_DEFAULT_HOST:&str = "localhost";
pub const TARGET_DB_DEFAULT_PORT:&str = "5555";
pub const TARGET_DB_DEFAULT_DATABASE:&str = "postgres";
pub const TARGET_DB_DEFAULT_USER:&str = "postgres";
pub const TARGET_DB_DEFAULT_PASS:&str = "";

// OTHER DEFAULT CONFIG
pub const DEFAULT_MAX_THREADS:i64 = 8;
pub const DEFAULT_ROWS_TO_EXECUTE_INSERT:i64 = 1000;
pub const DEFAULT_MAX_ROWS_FOR_SELECT:i64 = 10000;
pub const ERROR_LOG_ENABLED_BY_DEFAULT:bool = false;
pub const DEFAULT_IMPORTER_IMPL:&str = "COPY";

// Creates a singleton with all config values
lazy_static! {
    pub static ref CONFIG_MAP: HashMap<ConfigKey, ConfigProperty> = populate_properties_map();
}

// Encapsulates all DB and config info needed for a worker thread to do an import
pub struct ImportConfig {
    pub schema:String,
    pub table:String,
    pub where_clause:String,
    pub source_db_url:String,
    pub target_db_url:String,    
    pub importer_impl:String
}

#[derive(PartialEq, Eq, Hash, Debug)]
pub struct ConnectionParams {
    user:String,
    pass:String,
    host:String,
    port:String,
    dbname:String
}

impl FromStr for ConnectionParams {
    type Err=Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let regex = Regex::new("([^@:]+)(:[^@]+)?@([^:]+):([^/]+)(/.+)?")?;
        if !regex.is_match(s) {
            panic!("Connection URL is invalid: {}", s);
        }
        let captures = regex.captures(s).unwrap();
        let user = captures.get(1).map_or("", |m| m.as_str());
        let pass = captures.get(2).map_or("", |m| &m.as_str()[1..]);
        let host = captures.get(3).map_or("", |m| m.as_str());
        let port = captures.get(4).map_or("", |m| m.as_str());
        let dbname = captures.get(5).map_or("", |m| &m.as_str()[1..]);

        return Ok(ConnectionParams {user:user.to_owned(), pass:pass.to_owned(), host:host.to_owned(), port:port.to_owned(), dbname:dbname.to_owned()});
    }
}

#[derive(PartialEq, Eq, Hash, Debug)]
pub enum ConfigKey {
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

    SourceDBConnection,
    TargetDBConnection,
    MaxThreads,
    RowsToExecuteInsert,
    MaxRowsForSelect,
    ErrorLogEnabled,
    ImporterImplementation,
    BatchFileName
}

#[derive(PartialEq, Eq, Hash, Debug)]
pub enum ConfigProperty {
    SourceDBHost(String),
    SourceDBPort(String),
    SourceDBUser(String),
    SourceDBPass(String),
    SourceDBDatabase(String),
    TargetDBHost(String),
    TargetDBPort(String),
    TargetDBUser(String),
    TargetDBPass(String),
    TargetDBDatabase(String),

    SourceDBConnection(ConnectionParams),
    TargetDBConnection(ConnectionParams),
    MaxThreads(i64),
    RowsToExecuteInsert(i64),
    MaxRowsForSelect(i64),
    ErrorLogEnabled(bool),
    ImporterImplementation(String),
    BatchFileName(String)
}

/// This doc string acts as a help message when the user runs '--help'
/// as do all doc strings on fields
#[derive(Clap)]
#[clap(version = env!("CARGO_PKG_VERSION"), author = "Miguel Rivero")]
struct Opts {
    /// Source DB URL  user:secret@host:port/dbname
    #[clap(long)]
    source: Option<String>,
    /// Target DB URL  user:secret@host:port/dbname
    #[clap(long)]
    target: Option<String>,
    // Max worker threads for the import
    #[clap(long)]
    max_threads: Option<i64>,
    // Min number of rows read to trigger insert
    #[clap(long)]
    rows_insert: Option<i64>,
    // Max number of rows on each select/copy
    #[clap(long)]
    rows_select: Option<i64>,
    // Enable parse errors logging
    #[clap(long)]
    error_log: Option<bool>,
    // Use SELECT or COPY implementation
    #[clap(long)]
    importer_impl: Option<String>,
    // Batch file to process
    #[clap(long)]
    batch_filename: Option<String>
}

// TODO: Make this methods private and publish a map instead
// Populate that map once the app is starting, using the following sources (in this order):
// - If batch mode => Read config from batch file (if available. If existing, all parameters must be there)
// - params from command line
// - Environment vars
// - Default values

// DEPRECATED
pub fn get_config_property<T>(property : ConfigProperty, default_value: T) -> T where T : FromStr {
    match property {
        ConfigProperty::SourceDBHost(_) => environment_or_default(&"SOURCE_DB_HOST", default_value),
        ConfigProperty::SourceDBPort(_) => environment_or_default(&"SOURCE_DB_PORT", default_value),
        ConfigProperty::SourceDBUser(_) => environment_or_default(&"SOURCE_DB_USER", default_value),
        ConfigProperty::SourceDBPass(_) => environment_or_default(&"SOURCE_DB_PASS", default_value),
        ConfigProperty::SourceDBDatabase(_) => environment_or_default(&"SOURCE_DB_DATABASE", default_value),
        ConfigProperty::TargetDBHost(_) => environment_or_default(&"TARGET_DB_HOST", default_value),
        ConfigProperty::TargetDBPort(_) => environment_or_default(&"TARGET_DB_PORT", default_value),
        ConfigProperty::TargetDBUser(_) => environment_or_default(&"TARGET_DB_USER", default_value),
        ConfigProperty::TargetDBPass(_) => environment_or_default(&"TARGET_DB_PASS", default_value),
        ConfigProperty::TargetDBDatabase(_) => environment_or_default(&"TARGET_DB_DATABASE", default_value),
        ConfigProperty::MaxThreads(_) => environment_or_default(&"MAX_THREADS", default_value),
        ConfigProperty::RowsToExecuteInsert(_) => environment_or_default(&"ROWS_FOR_INSERT", default_value),
        ConfigProperty::MaxRowsForSelect(_) => environment_or_default(&"ROWS_FOR_SELECT", default_value),
        ConfigProperty::ErrorLogEnabled(_) => environment_or_default(&"ERROR_LOG", default_value),
        ConfigProperty::ImporterImplementation(_) => environment_or_default(&"IMPORTER_IMPL", default_value),
        ConfigProperty::BatchFileName(_) => environment_or_default(&"BATCH_FILENAME", default_value),

        ConfigProperty::SourceDBConnection(_) => default_value,
        ConfigProperty::TargetDBConnection(_) => default_value,
    }
}

pub fn get_source_db_url() -> String {
    return get_source_db_url_with_hiding(false);
}

pub fn get_target_db_url() -> String {
    return get_target_db_url_with_hiding(false);
}

pub fn get_source_db_url_with_hiding(hide_pass:bool) -> String {
    let host:String = get_config_property(ConfigProperty::SourceDBHost(SOURCE_DB_DEFAULT_HOST.to_owned()), SOURCE_DB_DEFAULT_HOST.to_owned());
    let port:String = get_config_property(ConfigProperty::SourceDBPort(SOURCE_DB_DEFAULT_PORT.to_owned()), SOURCE_DB_DEFAULT_PORT.to_owned());
    let database:String = get_config_property(ConfigProperty::SourceDBDatabase(SOURCE_DB_DEFAULT_DATABASE.to_owned()), SOURCE_DB_DEFAULT_DATABASE.to_owned());
    let user:String = get_config_property(ConfigProperty::SourceDBUser(SOURCE_DB_DEFAULT_USER.to_owned()), SOURCE_DB_DEFAULT_USER.to_owned());
    let mut pass:String = get_config_property(ConfigProperty::SourceDBPass(SOURCE_DB_DEFAULT_PASS.to_owned()), SOURCE_DB_DEFAULT_PASS.to_owned());
    if hide_pass {
        pass = String::from("**HIDDEN**");
    }
    format!("host='{}' port='{}' dbname='{}' user='{}' password='{}'", host , port, database, user, pass)
}

pub fn get_target_db_url_with_hiding(hide_pass:bool) -> String {
    let host:String = get_config_property(ConfigProperty::TargetDBHost(TARGET_DB_DEFAULT_HOST.to_owned()), TARGET_DB_DEFAULT_HOST.to_owned());
    let port:String = get_config_property(ConfigProperty::TargetDBPort(TARGET_DB_DEFAULT_PORT.to_owned()), TARGET_DB_DEFAULT_PORT.to_owned());
    let database:String = get_config_property(ConfigProperty::TargetDBDatabase(TARGET_DB_DEFAULT_DATABASE.to_owned()), TARGET_DB_DEFAULT_DATABASE.to_owned());
    let user:String = get_config_property(ConfigProperty::TargetDBUser(TARGET_DB_DEFAULT_USER.to_owned()), TARGET_DB_DEFAULT_USER.to_owned());
    let mut pass:String = get_config_property(ConfigProperty::TargetDBPass(TARGET_DB_DEFAULT_PASS.to_owned()), TARGET_DB_DEFAULT_PASS.to_owned());
    if hide_pass {
        pass = String::from("**HIDDEN**");
    }
    format!("host='{}' port='{}' dbname='{}' user='{}' password='{}'", host , port, database, user, pass)
}

fn environment_or_default<T> (env_key:&str, default_value: T) -> T where T: FromStr {
    match env::var(env_key) {
        Ok(env_value) => { 
            if let Ok(value) = env_value.parse::<T>() {
             return value 
            }
            else{
                panic!("Error: Couldn't parse the value of env var {}={} to the proper type", env_key, env_value);
            }
        },
        Err(_e) => default_value
    }
}

// Populate the config params, overriding the values in the following order:
// 1 - Add all default values
// 2 - Override previous values with any existing ENVIRONMENT VARS
// 3 - Override previous values with any existing command line params
fn populate_properties_map() -> HashMap<ConfigKey, ConfigProperty> {
    let mut properties_set = HashMap::new();

    // Legacy properties: DEPRECATED
    properties_set.insert(ConfigKey::SourceDBHost ,ConfigProperty::SourceDBHost(environment_or_default(&"SOURCE_DB_HOST",SOURCE_DB_DEFAULT_HOST.to_string())));   
    properties_set.insert(ConfigKey::SourceDBPort, ConfigProperty::SourceDBPort(environment_or_default(&"SOURCE_DB_PORT",SOURCE_DB_DEFAULT_PORT.to_string())));
    properties_set.insert(ConfigKey::SourceDBDatabase, ConfigProperty::SourceDBDatabase(environment_or_default(&"SOURCE_DB_DATABASE",SOURCE_DB_DEFAULT_DATABASE.to_string())));
    properties_set.insert(ConfigKey::SourceDBUser, ConfigProperty::SourceDBUser(environment_or_default(&"SOURCE_DB_USER",SOURCE_DB_DEFAULT_USER.to_string())));
    properties_set.insert(ConfigKey::SourceDBUser, ConfigProperty::SourceDBPass(environment_or_default(&"SOURCE_DB_PASS",SOURCE_DB_DEFAULT_PASS.to_string())));
    properties_set.insert(ConfigKey::TargetDBHost, ConfigProperty::TargetDBHost(environment_or_default(&"TARGET_DB_HOST",TARGET_DB_DEFAULT_HOST.to_string())));
    properties_set.insert(ConfigKey::TargetDBPort, ConfigProperty::TargetDBPort(environment_or_default(&"TARGET_DB_PORT",TARGET_DB_DEFAULT_PORT.to_string())));
    properties_set.insert(ConfigKey::TargetDBDatabase, ConfigProperty::TargetDBDatabase(environment_or_default(&"TARGET_DB_DATABASE",TARGET_DB_DEFAULT_DATABASE.to_string())));
    properties_set.insert(ConfigKey::TargetDBUser, ConfigProperty::TargetDBUser(environment_or_default(&"TARGET_DB_USER",TARGET_DB_DEFAULT_USER.to_string())));
    properties_set.insert(ConfigKey::TargetDBPass, ConfigProperty::TargetDBPass(environment_or_default(&"TARGET_DB_PASS",TARGET_DB_DEFAULT_PASS.to_string())));

    // Current properties
    properties_set.insert(ConfigKey::SourceDBConnection, get_most_prioritary_value(&"SOURCE_DB_CONNECTION"));
    properties_set.insert(ConfigKey::TargetDBConnection, get_most_prioritary_value(&"TARGET_DB_CONNECTION"));
    properties_set.insert(ConfigKey::MaxThreads, get_most_prioritary_value(&"MAX_THREADS"));
    properties_set.insert(ConfigKey::RowsToExecuteInsert, get_most_prioritary_value(&"ROWS_FOR_INSERT"));
    properties_set.insert(ConfigKey::MaxRowsForSelect, get_most_prioritary_value(&"ROWS_FOR_SELECT"));
    properties_set.insert(ConfigKey::ErrorLogEnabled, get_most_prioritary_value(&"ERROR_LOG"));
    properties_set.insert(ConfigKey::ImporterImplementation, get_most_prioritary_value(&"IMPORTER_IMPL"));
    properties_set.insert(ConfigKey::BatchFileName, get_most_prioritary_value(&"BATCH_FILENAME"));

    return properties_set;
}

// Look in order into all possible sources for the value of a config property
fn get_most_prioritary_value(env_key:&str) -> ConfigProperty {
    // Parse command line params
    let opts: Opts = Opts::parse();

    match env_key {
        "SOURCE_DB_CONNECTION" => 
            ConfigProperty::SourceDBConnection(parse_connection_params_from(&opts.source, "SOURCE_DB_CONNECTION", SOURCE_DB_CONNECTION.to_owned())),
        "TARGET_DB_CONNECTION" => 
            ConfigProperty::TargetDBConnection(parse_connection_params_from(&opts.target, "TARGET_DB_CONNECTION", TARGET_DB_CONNECTION.to_owned())),    
        "MAX_THREADS" => ConfigProperty::MaxThreads(get_numeric_value_from(&opts.max_threads, "MAX_THREADS", DEFAULT_MAX_THREADS)),
        "ROWS_FOR_INSERT" => ConfigProperty::RowsToExecuteInsert(get_numeric_value_from(&opts.rows_insert, "ROWS_FOR_INSERT", DEFAULT_ROWS_TO_EXECUTE_INSERT)),
        "ROWS_FOR_SELECT" => ConfigProperty::MaxRowsForSelect(get_numeric_value_from(&opts.rows_select, "ROWS_FOR_SELECT", DEFAULT_MAX_ROWS_FOR_SELECT)),
        "ERROR_LOG" =>  ConfigProperty::ErrorLogEnabled(get_bool_value_from(&opts.error_log, "ERROR_LOG", ERROR_LOG_ENABLED_BY_DEFAULT)),
        "IMPORTER_IMPL" =>  ConfigProperty::ImporterImplementation(get_string_value_from(&opts.importer_impl, "IMPORTER_IMPL", DEFAULT_IMPORTER_IMPL.to_owned())),
        "BATCH_FILENAME" =>  ConfigProperty::BatchFileName(get_string_value_from(&opts.batch_filename, "BATCH_FILENAME", "".to_owned())),
        _ => panic!("Config parameter key not recognized")
    }

}

fn parse_connection_params_from(command_line_param:&Option<String>, env_key:&str, default_url:String) -> ConnectionParams {
    let from_env_or_default = environment_or_default(env_key, default_url);
    
    return ConnectionParams::from_str(&command_line_param.to_owned().unwrap_or(from_env_or_default)).unwrap();
}

fn get_numeric_value_from(command_line_param:&Option<i64>, env_key:&str, default:i64) -> i64{
    return command_line_param.unwrap_or(environment_or_default(env_key, default));
}

fn get_string_value_from(command_line_param:&Option<String>, env_key:&str, default:String) -> String{
    return command_line_param.to_owned().unwrap_or(environment_or_default(env_key, default));
}

fn get_bool_value_from(command_line_param:&Option<bool>, env_key:&str, default:bool) -> bool{
    return command_line_param.unwrap_or(environment_or_default(env_key, default));
}