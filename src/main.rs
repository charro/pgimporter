// External libs
extern crate dialoguer;
extern crate chrono;
extern crate indicatif;
extern crate resolve;
extern crate rust_decimal;
extern crate log;
extern crate simple_logging;

// This program's modules
mod db;
mod utils;
mod config;

use dialoguer::{theme::ColorfulTheme, MultiSelect, Select, Input, Confirm};
use log::LevelFilter;
use chrono::{Utc};
use std::env;

fn main() {
    println!("Postgres Data Importer - v{}", env!("CARGO_PKG_VERSION"));
    println!();

    let args: Vec<String> = env::args().collect();
    if args.len() > 1 {
        show_help_and_end_program();
    }

    if config::get_config_property(config::ConfigProperty::ErrorLogEnabled, config::ERROR_LOG_ENABLED) {
        let error_log_filename = format!("pgimport_errors_{}.log", Utc::now().to_rfc3339());
        simple_logging::log_to_file(error_log_filename, LevelFilter::Error).unwrap();    
    }

    println!("Checking DB connections...");
    // Check if provided URL is correct
    if !utils::check_postgres_source_target_servers() {
        std::process::exit(1);
    }

    let schemas = db::get_available_schemas();

    let selection = Select::with_theme(&ColorfulTheme::default())
        .with_prompt("Choose an Schema")
        .default(0)
        .items(&schemas[..])
        .interact()
        .unwrap();
    
    let selected_schema:String = schemas[selection].to_owned();

    let tables = db::get_available_tables_in_schema(&selected_schema);

    if tables.is_empty() {
        println!("Selected schema doesn't contain any table");
        std::process::exit(1);
    }

    let selected_tables = create_options_with(&tables[..], &[], "Choose tables to import");

    if selected_tables.is_empty() {
        println!("You must select at least one table to import");
        std::process::exit(1);
    }

    let where_clause:String = Input::with_theme(&ColorfulTheme::default())
    .with_prompt("WHERE: [Optional]")
    .allow_empty(true)
    .interact()
    .unwrap();

    let target_host_port = format!("{}:{}", 
        config::get_config_property(config::ConfigProperty::TargetDBHost, config::TARGET_DB_DEFAULT_HOST.to_owned()),
        config::get_config_property(config::ConfigProperty::ErrorLogEnabled, config::TARGET_DB_DEFAULT_PORT.to_owned())
    );

    let confirm_msg = format!("Do you want to TRUNCATE tables in target DB [{}] ? 
    (WARNING, YOU'LL REMOVE ALL ROWS OF ALL SELECTED TABLES)", target_host_port);

    let truncate = Confirm::with_theme(&ColorfulTheme::default())
        .with_prompt(confirm_msg.to_owned())
        .default(false)
        .interact()
        .unwrap();

    for table_index in selected_tables {
        db::import_table_from_env(selected_schema.to_owned(), tables[table_index].to_owned(), where_clause.to_owned(), truncate);
    }
}

fn create_options_with<T:ToString>(options:&[T], defaults:&[bool], prompt:&str) -> Vec<usize> {

    let selections_result = MultiSelect::with_theme(&ColorfulTheme::default())
        .with_prompt(prompt)
        .items(&options[..])
        .defaults(&defaults[..])
        .interact();

    match selections_result {
        Ok(selections) => {
            return selections;
        },
        Err(error) => {
            println!("Couldn't get your option: {}", error);
        }
    }

    // Didn't get any option.
    return vec!();
}

fn show_help_and_end_program(){
    println!("   Imports data from one or more tables from a Source DB to a Target DB. (Chosen Schemas and Tables must exist in Target DB)");
    println!();
    println!("By default, DB connection properties are:");
    println!("Source DB: {}", config::get_source_db_url());
    println!("Target DB: {}", config::get_target_db_url());
    println!();
    println!("To override these properties you can set following env vars before calling the importer:");
    println!("***************************************************************************************");
    println!("SOURCE_DB_HOST : The IP or host of the DB where the data will be fetched from");
    println!("SOURCE_DB_PORT : The port of the DB where the data will be fetched from");
    println!("SOURCE_DB_USER : The username of the DB where the data will be fetched from");
    println!("SOURCE_DB_PASS : The password of the DB where the data will be fetched from");
    println!("TARGET_DB_HOST : The IP or host of the DB where the data will be inserted to");
    println!("TARGET_DB_PORT : The port  of the DB where the data will be inserted to");
    println!("TARGET_DB_USER : The username of the DB where the data will be inserted to");
    println!("TARGET_DB_PASS : The password of the DB where the data will be inserted to");
    println!("MAX_THREADS : Number of threads to be used (Will affect the performance)");
    println!("ROWS_FOR_INSERT : How many rows will be inserted at once to target DB (Will affect the performance and memory consumed by process)");
    println!("ROWS_FOR_SELECT : How many rows to request at once from source DB (Will affect the performance and memory consumed by process)");
    println!("ERROR_LOG_ENABLED: If set to true, will log to a file all errors found during execution");
    std::process::exit(1);  
}