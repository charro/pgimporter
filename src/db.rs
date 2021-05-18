use postgres::{Client, NoTls};
use std::time::{Instant};
use std::sync::Arc;

use crate::config;
use crate::config::{CONFIG_PROPERTIES, ImportConfig};

use crate::single_import;
use crate::multi_import;

pub struct DBClients {
    pub source_client:Client,
    pub target_client:Client
}

pub struct TableChunk {
    pub where_clause:String,
    pub offset:i64,
    pub limit:i64,
    pub order_by:String
}

pub trait TableImporter {
    fn import_table_chunk(&self, import_config:&ImportConfig, db_clients:&mut DBClients, chunk:&TableChunk);
}

pub fn get_available_schemas() -> Vec<String> {

    let mut client = match Client::connect(config::get_source_db_url().as_str(), NoTls) {
        Ok(client) => client,
        Err(error) => { println!("Couldn't connect to source DB. Error: {}", error);  std::process::exit(1); }
    };

    let mut schemas:Vec<String> = vec!();  
    
    for row in client.query("SELECT schema_name FROM information_schema.schemata where schema_name 
            not like 'pg_%' and schema_name <> 'information_schema'", &[]).unwrap(){
        let schema_name:String = row.try_get(0).unwrap();
        schemas.push(schema_name);
    }
    
    return schemas;
}

pub fn get_available_tables_in_schema(schema:&str) -> Vec<String> {

    let mut client = match Client::connect(config::get_source_db_url().as_str(), NoTls) {
        Ok(client) => client,
        Err(error) => { println!("Couldn't connect to source DB. Error: {}", error);  std::process::exit(1); }
    };

    let mut tables:Vec<String> = vec!();  
    
    // Get all tables from the schema that aren't partitions
    for row in client.query("select distinct table_name
                from information_schema.tables ist
                join pg_class pgc on ist.table_name = pgc.relname 
                where ist.table_schema = $1 and ist.table_type = 'BASE TABLE'
                and pgc.relispartition = false", &[&schema]).unwrap(){
        let table_name:String = row.try_get(0).unwrap();
        tables.push(table_name);
    }
    
    return tables;
}

pub fn get_any_unique_constraint_fields_for_table(schema:&str, table:&str) -> Option<String> {
    let mut client = match Client::connect(config::get_source_db_url().as_str(), NoTls) {
        Ok(client) => client,
        Err(error) => { println!("Couldn't connect to source DB. Error: {}", error);  std::process::exit(1); }
    };

    let unique_constraints = client.query(
        "select
            string_agg(ccu.column_name, ', ') as constraint_columns
        FROM
            INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
        JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS ccu ON ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
            and tc.constraint_schema  = ccu.constraint_schema and tc.constraint_catalog  = ccu.constraint_catalog 
        where
            tc.table_schema = $1 and 
            tc.table_name  = $2 and
            tc.constraint_type in ('PRIMARY KEY', 'UNIQUE')
        group by tc.constraint_name, tc.constraint_type ", &[&schema, &table]).unwrap();

    if unique_constraints.len() == 0 {
        return None;
    }
    else {
        // Just take the first one. All of them should be valid
        let constraint_fields:String = unique_constraints[0].try_get(0).unwrap();
        return Some(constraint_fields.to_owned());
    }
}

pub fn get_first_column_from_table(schema:&str, table:&str) -> String {
    let mut client = match Client::connect(config::get_source_db_url().as_str(), NoTls) {
        Ok(client) => client,
        Err(error) => { println!("Couldn't connect to source DB. Error: {}", error);  std::process::exit(1); }
    };

    let columns = client.query("SELECT column_name 
        FROM information_schema.columns WHERE table_schema = $1 AND table_name   = $2;", &[&schema, &table]).unwrap();

    let first_column:String = columns[0].try_get(0).unwrap();

    return first_column;
}

// TODO: Pass here the connection params as a single struct
pub fn import_table_from(schema:String, table:String, where_clause:String, truncate:bool) {
    // Get some properties from config
    let source_db_url:String = config::get_source_db_url();
    let target_db_url:String = config::get_target_db_url();
    let importer_impl = &CONFIG_PROPERTIES.importer_impl;

    let import_config = ImportConfig { schema: schema, table: table, where_clause: where_clause, 
        source_db_url: source_db_url, target_db_url: target_db_url, importer_impl: importer_impl.to_string()};

    println!();
    println!("Importing table {}.{} ...", import_config.schema, import_config.table);

    // Start measuring total time spent importing this table
    let start = Instant::now();

    // TRUNCATE target table if truncate is requested
    if truncate {
        println!("TRUNCATING table {}.{}...", import_config.schema, import_config.table);
        let mut target_client = match Client::connect(import_config.target_db_url.as_ref(), NoTls) {
            Ok(client) => client,
            Err(error) => { println!("Couldn't connect to target DB. Error: {}", error);  std::process::exit(1); }
        };

        let truncate_query = format!("TRUNCATE TABLE {}.{}", import_config.schema, import_config.table);
        target_client.execute(truncate_query.as_str(), &[]).unwrap();
    }

    let total_rows_to_import = count_total_rows_for_import(&import_config);

    println!("{} rows to insert in total", total_rows_to_import);
    
    // Use smart pointers to share the same common Boxed values between all potential Threads (not needed for unboxed types)
    let import_config = Arc::new(import_config);

    // If single thread is forced by config, just use it
    if CONFIG_PROPERTIES.max_threads < 2 {
        single_import::single_thread_import(&import_config, total_rows_to_import as u64);
    }
    else {
        // Check if there's any UNIQUE constraint in the source table so we can use it for the ORDER BY
        // If there's none we have to use single-thread version to make import results are correct
        match get_any_unique_constraint_fields_for_table(&import_config.schema, &import_config.table) {
            Some(order_by) => multi_import::multi_thread_import(&import_config, &order_by, total_rows_to_import),
            None => {
                println!("INFO: {}.{} doesn't have any UNIQUE constraint to order by. 
                    Switching to SINGLE Thread import", &import_config.schema, &import_config.table);
                single_import::single_thread_import(&import_config, total_rows_to_import as u64);
            }
        }
    }

    let duration = start.elapsed();
    println!("Finished importing {} rows from table {}.{} in {} secs", total_rows_to_import, import_config.schema, 
        import_config.table, duration.as_secs());
}


fn count_total_rows_for_import(import_config:&ImportConfig) -> i64 {
    let mut count_db_client = match Client::connect(import_config.source_db_url.as_str(), NoTls) {
        Ok(client) => client,
        Err(error) => { println!("Couldn't connect to source DB. Error: {}", error);  std::process::exit(1); }
    };
    
    // Count the rows to import
    let mut count_query = format!("SELECT count(1) FROM {}.{}", import_config.schema, import_config.table);
    if !import_config.where_clause.is_empty() {
        count_query = format!("{} WHERE {}", count_query, import_config.where_clause)
    }

    let total_rows_to_import:i64 = match count_db_client.query(count_query.as_str(), &[]) {
        Ok(count) => count[0].get(0),
        Err(error) => { println!("Couldn't execute query: {} | Error: {} ", count_query, error); std::process::exit(1); }
    };    

    if total_rows_to_import <= 0{
        println!("WARNING: No rows to import from query {}", count_query);
        return 0;
    }

    total_rows_to_import
}