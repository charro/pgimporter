use serde::{Serialize, Deserialize};
use serde_yaml::from_reader;
use std::fs::File;
use std::io::BufReader;

use crate::db;
use crate::utils;

#[derive(Serialize, Deserialize)]
struct SchemaImport {
    schema: String,
    tables: Vec<String>,
    where_clause: String,
    truncate: bool
}

#[derive(Serialize, Deserialize)]
struct Batch {
    imports: Vec<SchemaImport>
}

pub fn execute_batch_file(batch_file: &String) {
    println!("Processing batch file {}...", batch_file);

    match File::open(batch_file) {
        Ok(file) => {
            let buf_reader = BufReader::new(file);
            match from_reader(buf_reader) {
                Ok(b) => {
                    // Check if DB connection URLs are correct
                    if !utils::check_postgres_source_target_servers() {
                        std::process::exit(1);
                    }
                    
                    let batch:Batch = b;
                    for (i, schema_import) in batch.imports.iter().enumerate() {
                        println!("Job {}: Importing schema {}...", i, schema_import.schema);
                        execute_schema_import(&schema_import.schema, &schema_import.tables, 
                            &schema_import.where_clause, schema_import.truncate);   
                    }
                },
                Err(err) => {
                    println!("Error parsing batch file {} : {}", batch_file, err);
                }
            }
        },
        Err(err) => {
            println!("Couldn't open batch file {} : {}", batch_file, err);
        }
    }
}

fn execute_schema_import(schema:&String, tables:&Vec<String>, where_clause:&String, truncate:bool){
    let mut checked_where_clause = &String::from("");
    if where_clause != "~" {
        checked_where_clause = where_clause;
    }
    
    for table in tables {
        db::import_table_from(schema.to_owned(), table.to_owned(), checked_where_clause.to_owned(), truncate);
    }
}