use core::str::FromStr;
use clap::Parser;
use lazy_static::lazy_static;
use regex::{Regex, Error};
use std::env;

// DEFAULT DB CONFIG
pub const SOURCE_DB_CONNECTION:&str = "postgres:postgres@localhost:5432/postgres";
pub const TARGET_DB_CONNECTION:&str = "postgres:postgres@localhost:5555/postgres";

// OTHER DEFAULT CONFIG VALUES
pub const DEFAULT_MAX_THREADS:i64 = 8;
pub const DEFAULT_ROWS_FOR_INSERT:i64 = 10000;
pub const DEFAULT_ROWS_FOR_SELECT:i64 = 50000;
pub const ERROR_LOG_ENABLED_BY_DEFAULT:bool = false;
pub const DEFAULT_IMPORTER_IMPL:&str = "COPY";

// Creates a global shared static singleton with all config values
lazy_static! {
    pub static ref CONFIG_PROPERTIES: ConfigProperties = populate_properties();
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
    pub user:String,
    pub pass:String,
    pub host:String,
    pub port:String,
    pub dbname:String
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
pub enum ConfigProperty {
    SourceDBConnection(ConnectionParams),
    TargetDBConnection(ConnectionParams),
    MaxThreads(i64),
    RowsToExecuteInsert(i64),
    RowsToExecuteSelect(i64),
    ErrorLogEnabled(bool),
    ImporterImplementation(String),
    BatchFileName(String)
}

pub struct ConfigProperties {
    pub source: ConnectionParams,
    pub target: ConnectionParams,
    pub max_threads: i64,
    pub rows_insert: i64,
    pub rows_select: i64,
    pub error_log: bool,
    pub importer_impl: String,
    pub batch_filename: String
}

const ABOUT_MSG:&str = "Command line tool to export data from a Postgres DB and insert it to another one";
const AFTER_HELP_MSG:&str = 
"You can use enviroment variables instead of passing the options by command line.
The name of the env vars is the same as the options, but using Upper case and underscores

  e.g.: --rows-insert 25 ==> ROWS_INSERT=25\n";

/// This doc string acts as a help message when the user runs '--help'
/// as do all doc strings on fields
#[derive(Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"), author = "Miguel Rivero", about = ABOUT_MSG, after_help = AFTER_HELP_MSG)]
struct Opts {
    /// Source DB URL:  user:secret@host:port/dbname
    #[clap(long, short)]
    source: Option<String>,
    /// Target DB URL:  user:secret@host:port/dbname
    #[clap(long, short)]
    target: Option<String>,
    /// Max worker threads for the import
    #[clap(long)]
    max_threads: Option<i64>,
    /// Min number of rows read to trigger insert
    #[clap(long)]
    rows_insert: Option<i64>,
    /// Max number of rows on each select/copy
    #[clap(long)]
    rows_select: Option<i64>,
    /// Enable parse errors logging
    #[clap(long)]
    error_log: Option<bool>,
    /// Use SELECT or COPY implementation
    #[clap(long)]
    importer_impl: Option<String>,
    /// Batch file to process
    #[clap(long)]
    batch_filename: Option<String>
}

pub fn get_source_db_url() -> String {
    return get_source_db_url_with_hiding(false);
}

pub fn get_target_db_url() -> String {
    return get_target_db_url_with_hiding(false);
}

pub fn get_source_db_url_with_hiding(hide_pass:bool) -> String {
    to_postgres_driver_params(&CONFIG_PROPERTIES.source , hide_pass)
}

pub fn get_target_db_url_with_hiding(hide_pass:bool) -> String {
    to_postgres_driver_params(&CONFIG_PROPERTIES.target, hide_pass)
}

fn to_postgres_driver_params(connection_params:&ConnectionParams, hide_pass:bool) -> String {

    let host:String = connection_params.host.to_owned();
    let port:String = connection_params.port.to_owned();
    let database:String = connection_params.dbname.to_owned();
    let user:String = connection_params.user.to_owned();
    let mut pass:String = connection_params.pass.to_owned();
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

fn populate_properties() -> ConfigProperties {

    // Current properties
    let source_connection = match get_most_prioritary_value(&"SOURCE") {
        ConfigProperty::SourceDBConnection(conn) => conn,
        _ => panic!("Wrong enum type") 
    };
    let target_connection = match get_most_prioritary_value(&"TARGET") {
        ConfigProperty::TargetDBConnection(conn) => conn,
        _ => panic!("Wrong enum type") 
    };
    let max_threads = match get_most_prioritary_value(&"MAX_THREADS") {
        ConfigProperty::MaxThreads(t) => t,
        _ => panic!("Wrong enum type") 
    };
    let rows_insert = match get_most_prioritary_value(&"ROWS_INSERT") {
        ConfigProperty::RowsToExecuteInsert(r) => r,
        _ => panic!("Wrong enum type") 
    };
    let rows_select = match get_most_prioritary_value(&"ROWS_SELECT") {
        ConfigProperty::RowsToExecuteSelect(r) => r,
        _ => panic!("Wrong enum type") 
    };
    let error_log = match get_most_prioritary_value(&"ERROR_LOG") {
        ConfigProperty::ErrorLogEnabled(e) => e,
        _ => panic!("Wrong enum type") 
    };
    let importer_impl = match get_most_prioritary_value(&"IMPORTER_IMPL") {
        ConfigProperty::ImporterImplementation(i) => i,
        _ => panic!("Wrong enum type") 
    };
    let batch_filename = match get_most_prioritary_value(&"BATCH_FILENAME") {
        ConfigProperty::BatchFileName(b) => b,
        _ => panic!("Wrong enum type") 
    };

    return ConfigProperties { source: source_connection, target: target_connection, max_threads: max_threads, rows_insert: rows_insert,
        rows_select:rows_select, error_log: error_log, importer_impl: importer_impl, batch_filename: batch_filename };
}

// Get the config param, looking for the value in the following order:
// 1 - If present, get it from command line params
// 2 - Otherwise, look in ENVIRONMENT VARS
// 3 - If no value found in 1 or 2, then use default value (if it isn't optional)
fn get_most_prioritary_value(env_key:&str) -> ConfigProperty {
    // Parse command line params
    let opts: Opts = Opts::parse();

    match env_key {
        "SOURCE" => 
            ConfigProperty::SourceDBConnection(parse_connection_params_from(&opts.source, "SOURCE", SOURCE_DB_CONNECTION.to_owned())),
        "TARGET" => 
            ConfigProperty::TargetDBConnection(parse_connection_params_from(&opts.target, "TARGET", TARGET_DB_CONNECTION.to_owned())),    
        "MAX_THREADS" => ConfigProperty::MaxThreads(get_value_from(opts.max_threads, "MAX_THREADS", DEFAULT_MAX_THREADS)),
        "ROWS_INSERT" => ConfigProperty::RowsToExecuteInsert(get_value_from(opts.rows_insert, "ROWS_INSERT", DEFAULT_ROWS_FOR_INSERT)),
        "ROWS_SELECT" => ConfigProperty::RowsToExecuteSelect(get_value_from(opts.rows_select, "ROWS_SELECT", DEFAULT_ROWS_FOR_SELECT)),
        "ERROR_LOG" =>  ConfigProperty::ErrorLogEnabled(get_value_from(opts.error_log, "ERROR_LOG", ERROR_LOG_ENABLED_BY_DEFAULT)),
        "IMPORTER_IMPL" =>  ConfigProperty::ImporterImplementation(get_value_from(opts.importer_impl, "IMPORTER_IMPL", DEFAULT_IMPORTER_IMPL.to_owned())),
        "BATCH_FILENAME" =>  ConfigProperty::BatchFileName(get_value_from(opts.batch_filename, "BATCH_FILENAME", "".to_owned())),
        _ => panic!("Config parameter key requested not recognized: {}", env_key)
    }

}

fn parse_connection_params_from(command_line_param:&Option<String>, env_key:&str, default_url:String) -> ConnectionParams {
    let from_env_or_default = environment_or_default(env_key, default_url);
    
    return ConnectionParams::from_str(&command_line_param.to_owned().unwrap_or(from_env_or_default)).unwrap();
}

fn get_value_from<T>(command_line_param:Option<T>, env_key:&str, default:T) -> T where T: FromStr {
    match command_line_param {
        Some(v) => v,
        None => environment_or_default(env_key, default)
    }
}