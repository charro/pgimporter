use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use postgres::{Client, NoTls};
use std::time::{Instant};
use std::thread;

use crate::config;
use crate::config::{CONFIG_PROPERTIES, ImportConfig};

use crate::copy::CopyImporter;
use crate::query::QueryImporter;

pub struct DBClients {
    pub source_client:Client,
    pub target_client:Client
}

pub struct TableChunk {
    pub where_clause:String,
    pub offset:i64,
    pub limit:i64
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

// TODO: Pass here the connection params as a single struct
pub fn import_table_from(schema:String, table:String, where_clause:String, truncate:bool) {
    // Get DB properties from config
    let source_db_url:String = config::get_source_db_url();
    let target_db_url:String = config::get_target_db_url();

    let max_threads = CONFIG_PROPERTIES.max_threads;
    let max_rows_for_select = CONFIG_PROPERTIES.rows_select;
    let importer_impl = &CONFIG_PROPERTIES.importer_impl;

    let import_config = ImportConfig { schema: schema, table: table, where_clause: where_clause, 
        source_db_url: source_db_url, target_db_url: target_db_url, importer_impl: importer_impl.to_string()};

    // Use smart pointers to share the same common Boxed values between all Threads (not needed for unboxed types)
    let import_config = std::sync::Arc::new(import_config);

    println!();
    println!("Importing table {}.{} ...", import_config.schema, import_config.table);
    // Create the progression bars
    let multi_progress_bar = MultiProgress::new();
    let sty = ProgressStyle::default_bar()
        .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
        .progress_chars("##-");

    // Start measuring total time spent importing this table
    let start = Instant::now();

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
        return;
    }

    println!("{} rows to insert in total", total_rows_to_import);

    // Divide all rows to import by the number of threads to use
    let rows_per_thread = total_rows_to_import / max_threads;

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

    // START IMPORTING, SPAWNING WORKER THREADS
    let mut previous_thread_last_row = 0;
    // Remember that higher limit in for loop is exclusive in Rust so this is actually 0 to max_threads-1:
    for thread_num in 0..max_threads {
    
        let limit_for_this_thread;

        if thread_num == max_threads-1 {
            // Last thread inserts remaining rows
            limit_for_this_thread = total_rows_to_import - previous_thread_last_row;
        }
        else {
            limit_for_this_thread = rows_per_thread
        }
        
        // This thread working row starts from previous thread last row
        let mut offset_for_this_thread = previous_thread_last_row;
        if thread_num == 0 {
            offset_for_this_thread = previous_thread_last_row;
        }
        
        // Set last row for this thread (previous_thread for the next thread)
        previous_thread_last_row = offset_for_this_thread + limit_for_this_thread;
        
        // Create a new progress bar to show the progress of this thread
        let progress_bar = multi_progress_bar.add(ProgressBar::new(limit_for_this_thread as u64));
        progress_bar.set_style(sty.clone());

        // Clone the smart pointer so each thread has its own references to the DB values
        // Those references will be removed when the thread ends and when there are no references left the memory will be freed
        let import_config = import_config.clone();

        // NEW WORKER THREAD BEGINS
        thread::spawn(move || {
            
            let source_client = match Client::connect(import_config.source_db_url.as_ref(), NoTls) {
                Ok(client) => client,
                Err(error) => { println!("Couldn't connect to source DB. Error: {}", error);  std::process::exit(1); }
            };
            
            let target_client = match Client::connect(import_config.target_db_url.as_ref(), NoTls) {
                Ok(client) => client,
                Err(error) => { println!("Couldn't connect to target DB. Error: {}", error);  std::process::exit(1); }
            };

            let mut db_clients = DBClients { source_client: source_client, target_client: target_client};

            let mut rows_read_in_this_thread = 0;
            // Create select query
            let mut complete_where:String = import_config.where_clause.to_owned().to_string();
            if !import_config.where_clause.is_empty() {
                complete_where = format!("WHERE {}", import_config.where_clause);
            }

            // If number of rows to read in this thread are more than MAX_ROWS_FOR_SELECT, divide in several selects of max size
            // Doing this is specially important for big queries, as the memory consumption could even kill the process
            let mut limit = limit_for_this_thread;
            let mut offset = offset_for_this_thread;
            let max_offset = offset_for_this_thread + limit_for_this_thread;
            if limit_for_this_thread > max_rows_for_select {
                limit = max_rows_for_select;
            }
            
            // Iterate until finishing with all rows assigned to this thread
            while offset < max_offset {
  
                let table_chunk = TableChunk { where_clause: complete_where.to_owned(), offset: offset, limit: limit};

                if import_config.importer_impl == "QUERY" {
                    let importer = QueryImporter;                    
                    importer.import_table_chunk(&import_config, &mut db_clients, &table_chunk);
                }
                else {
                    let importer = CopyImporter;
                    importer.import_table_chunk(&import_config, &mut db_clients, &table_chunk);
                }
 
                // Update progress bar after execution
                progress_bar.inc(limit as u64);

                rows_read_in_this_thread = rows_read_in_this_thread + limit;

                // Increase the offset in the same amount as the rows read (limit)
                // If new offset + limit > max_offset
                // set the new limit as the difference between max_offset and current new offset
                offset += limit;
                if offset + limit > max_offset {
                    limit = max_offset - offset;
                }
            } // THREAD ENDS

            progress_bar.finish_with_message(
                format!("Thread {} finished reading rows from {} to {}",thread_num, offset_for_this_thread, max_offset).as_str());
            return limit_for_this_thread;
        });

    }

    // Wait for all the progress bars to finish. Also acts as a join for the child threads
    multi_progress_bar.join_and_clear().unwrap();

    let duration = start.elapsed();
    println!("Finished importing {} rows from table {}.{} in {} secs", total_rows_to_import, import_config.schema, 
        import_config.table, duration.as_secs());
}