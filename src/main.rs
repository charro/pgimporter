// This program's modules
mod db;
mod utils;
mod config;
mod batch;
mod query;
mod copy;

use dialoguer::{theme::ColorfulTheme, MultiSelect, Select, Input, Confirm};
use log::LevelFilter;
use chrono::{Utc};
use std::env;

fn main() {
    println!("Postgres Data Importer - v{}", env!("CARGO_PKG_VERSION"));
    println!();

    if config::get_config_property(config::ConfigProperty::ErrorLogEnabled, config::ERROR_LOG_ENABLED) {
        let error_log_filename = format!("pgimport_errors_{}.log", Utc::now().to_rfc3339());
        simple_logging::log_to_file(error_log_filename, LevelFilter::Error).unwrap();    
    }

    let args: Vec<String> = env::args().collect();
    if args.len() > 1 {
        let first_arg = &args[1];
        if first_arg == "--help" || first_arg == "-h" {
            show_help_and_end_program();
        }
        else{
          batch::execute_batch_file(first_arg);
          std::process::exit(0);
        }
    }
    else{
        execute_interactive();
    }

}

fn execute_interactive(){
    // Check if DB connection URLs are correct
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

    let confirm_msg = format!("Do you want to TRUNCATE selected tables in target DB [{}] ? (WARNING: ALL DATA WILL BE LOST!)", target_host_port);

    let truncate = Confirm::with_theme(&ColorfulTheme::default())
        .with_prompt(confirm_msg.to_owned())
        .default(false)
        .interact()
        .unwrap();

    for table_index in selected_tables {
        db::import_table_from(selected_schema.to_owned(), tables[table_index].to_owned(), where_clause.to_owned(), truncate);
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
            std::process::exit(1);
        }
    }
}

fn show_help_and_end_program(){
    println!("   Imports data from one or more tables from a Source DB to a Target DB. (Chosen Schemas and Tables must exist in Target DB)");
    println!();
    println!("Current DB connection parameters are:");
    println!("Source DB: {}", config::get_source_db_url_with_hiding(true));
    println!("Target DB: {}", config::get_target_db_url_with_hiding(true));
    println!();
    println!("To override these properties you can set following env vars before calling the importer:");
    println!("***************************************************************************************");
    println!("SOURCE_DB_HOST : The IP or host of the DB where the data will be fetched from");
    println!("SOURCE_DB_PORT : The port of the DB where the data will be fetched from");
    println!("SOURCE_DB_DATABASE : The name of the database to look for the schemas to import from");
    println!("SOURCE_DB_USER : The username of the DB where the data will be fetched from");
    println!("SOURCE_DB_PASS : The password of the DB where the data will be fetched from");
    println!("TARGET_DB_HOST : The IP or host of the DB where the data will be inserted to");
    println!("TARGET_DB_PORT : The port  of the DB where the data will be inserted to");
    println!("TARGET_DB_DATABASE : The name of the database to insert the data to");
    println!("TARGET_DB_USER : The username of the DB where the data will be inserted to");
    println!("TARGET_DB_PASS : The password of the DB where the data will be inserted to");
    println!("MAX_THREADS : Number of threads to be used (Will affect the performance)");
    println!("ROWS_FOR_INSERT : How many rows will be inserted at once to target DB (Will affect the performance and memory consumed by process)");
    println!("ROWS_FOR_SELECT : How many rows to request at once from source DB (Will affect the performance and memory consumed by process)");
    println!("ERROR_LOG_ENABLED: If set to true, will log to a file all errors found during execution");
    println!("IMPORTER_IMPL: Choose the implementation for the import [QUERY|COPY]. COPY is used by default");
    std::process::exit(1);  
}