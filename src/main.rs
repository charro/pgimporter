// This program's modules
mod db;
mod utils;
mod config;
mod batch;
mod query;
mod copy;
mod single_import;
mod multi_import;

use dialoguer::{theme::ColorfulTheme, MultiSelect, Select, Input, Confirm};
use log::LevelFilter;
use chrono::{Utc};
use std::env;
use config::{CONFIG_PROPERTIES};

struct TableInfo{
    name: String,
    rows: u64
}

impl ToString for TableInfo {
    fn to_string(&self) -> String {
        return format!("{} - {} rows", &self.name, &self.rows);
    }
}

fn main() {
    println!("PostgreSQL Data Importer - v{}", env!("CARGO_PKG_VERSION"));
    println!();
    println!("Exporting from Source DB: {}", config::get_source_db_url_with_hiding(true));
    println!("Importing to Target DB: {}", config::get_target_db_url_with_hiding(true));
    println!();

    if CONFIG_PROPERTIES.error_log {
        let error_log_filename = format!("pgimport_errors_{}.log", Utc::now().to_rfc3339());
        simple_logging::log_to_file(error_log_filename, LevelFilter::Error).unwrap();        
    }

    if CONFIG_PROPERTIES.batch_filename.is_empty() {
        execute_interactive();
    }
    else {
        batch::execute_batch_file(&CONFIG_PROPERTIES.batch_filename);
        std::process::exit(0);
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

    let table_info_list = get_tables_info(selected_schema.as_str(), tables);

    let selected_tables = create_options_with(&table_info_list[..], &[], "Choose tables to import");

    if selected_tables.is_empty() {
        println!("You must select at least one table to import");
        std::process::exit(1);
    }

    let where_clause:String = Input::with_theme(&ColorfulTheme::default())
    .with_prompt("WHERE: [Optional]")
    .allow_empty(true)
    .interact()
    .unwrap();

    let target_db_connection = &CONFIG_PROPERTIES.target;

    let target_host_port = format!("{}:{}", target_db_connection.host, target_db_connection.port);

    let confirm_msg = format!("Do you want to TRUNCATE selected tables in target DB [{}] ? (WARNING: ALL DATA WILL BE LOST!)", target_host_port);

    let truncate = Confirm::with_theme(&ColorfulTheme::default())
        .with_prompt(confirm_msg.to_owned())
        .default(false)
        .interact()
        .unwrap();

    for table_index in selected_tables {
        db::import_table_from(selected_schema.to_owned(), table_info_list[table_index].name.to_owned(), where_clause.to_owned(), truncate);
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

fn get_tables_info(schema:&str, tables:Vec<String>) -> Vec<TableInfo> {
    let mut table_info_list = Vec::new();

    for table in tables {
        let rows = db::get_number_of_rows_for(schema, table.as_str());
        table_info_list.push(TableInfo{name: table, rows: rows});
    }

    return table_info_list;
}