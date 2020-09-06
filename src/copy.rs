use postgres::{Client, NoTls, Row, Column, Error};
use postgres::types::{Type};
use std::thread;
use std::io::{Read, Write};
use std::time::{Instant, SystemTime};
use chrono::offset::Utc;
use chrono::DateTime;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rust_decimal::Decimal;

use crate::config;
use crate::config::ConfigProperty;
use crate::utils::{log_error};
use crate::db::{TableImporter};

pub struct CopyImporter;

impl TableImporter for CopyImporter {

    fn import_table_from(&self, schema:String, table:String, where_clause:String, truncate:bool) {
        // Get DB properties from config
        let source_db_url:String = config::get_source_db_url();
        let target_db_url:String = config::get_target_db_url();
        let max_threads:i64 = config::get_config_property(ConfigProperty::MaxThreads, config::DEFAULT_MAX_THREADS);
        let max_rows_for_select:i64 = config::get_config_property(ConfigProperty::MaxRowsForSelect, config::DEFAULT_MAX_ROWS_FOR_SELECT);
        let min_rows_for_insert:i64 = config::get_config_property(ConfigProperty::RowsToExecuteInsert, config::DEFAULT_ROWS_TO_EXECUTE_INSERT);

        // Use smart pointers to share the same common Boxed values between all Threads (not needed for unboxed types)
        let schema = std::sync::Arc::new(schema);
        let table = std::sync::Arc::new(table);
        let where_clause = std::sync::Arc::new(where_clause);
        let source_db_url = std::sync::Arc::new(source_db_url);
        let target_db_url = std::sync::Arc::new(target_db_url);

        println!();
        println!("Importing table {}.{} ...", schema, table);
        // Create the progression bars
        let m = MultiProgress::new();
        let sty = ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
            .progress_chars("##-");

        // Start measuring total time spent importing this table
        let start = Instant::now();

        let mut count_db_client = match Client::connect(source_db_url.as_str(), NoTls) {
            Ok(client) => client,
            Err(error) => { println!("Couldn't connect to source DB. Error: {}", error);  std::process::exit(1); }
        };
        
        // Count the rows to import
        let mut count_query = format!("SELECT count(1) FROM {}.{}", schema, table);
        if !where_clause.is_empty() {
            count_query = format!("{} WHERE {}", count_query, where_clause)
        }

        let rows_to_import:i64 = match count_db_client.query(count_query.as_str(), &[]) {
            Ok(count) => count[0].get(0),
            Err(error) => { println!("Couldn't execute query: {} | Error: {} ", count_query, error); std::process::exit(1); }
        };
        
        if rows_to_import <= 0{
            println!("WARNING: No rows to import from query {}", count_query);
            return;
        }

        println!("{} rows to insert in total", rows_to_import);

        // Divide all rows to import by the number of threads to use
        let rows_per_thread = rows_to_import / max_threads;

        // TRUNCATE target table if truncate is requested
        if truncate {
            println!("TRUNCATING table {}.{}...", schema, table);
            let mut target_client = match Client::connect(target_db_url.as_ref(), NoTls) {
                Ok(client) => client,
                Err(error) => { println!("Couldn't connect to target DB. Error: {}", error);  std::process::exit(1); }
            };

            let truncate_query = format!("TRUNCATE TABLE {}.{}", schema, table);
            target_client.execute(truncate_query.as_str(), &[]).unwrap();
        }

        // START IMPORTING
        let mut previous_thread_last_row = 0;
        // Remember that higher limit in for loop is exclusive in Rust so this is actually 0 to max_threads-1:
        for thread_num in 0..max_threads {
    
            let mut limit_for_this_thread = rows_per_thread;
            // Last thread inserts all remaining rows
            if thread_num == max_threads-1 {
                limit_for_this_thread = rows_to_import - previous_thread_last_row;
            };
            
            let mut offset_for_this_thread = previous_thread_last_row;
            if thread_num == 0 {
                offset_for_this_thread = previous_thread_last_row;
            }
            
            previous_thread_last_row = offset_for_this_thread + limit_for_this_thread;
            // Create a new progress bar to show the progress of this thread
            let pb = m.add(ProgressBar::new(limit_for_this_thread as u64));
            pb.set_style(sty.clone());
    
            // Clone the smart pointers so each thread has its own references to the schema + table
            // Those references will be removed when the thread ends and when there are no references left the memory will be freed
            let schema = schema.clone();
            let table = table.clone();
            let where_clause = where_clause.clone();
            let source_db_url = source_db_url.clone();
            let target_db_url = target_db_url.clone();
    
            // NEW WORKER THREAD BEGINS
            thread::spawn(move || {
                
                let mut source_client = match Client::connect(source_db_url.as_ref(), NoTls) {
                    Ok(client) => client,
                    Err(error) => { println!("Couldn't connect to source DB. Error: {}", error);  std::process::exit(1); }
                };
                
                let mut target_client = match Client::connect(target_db_url.as_ref(), NoTls) {
                    Ok(client) => client,
                    Err(error) => { println!("Couldn't connect to target DB. Error: {}", error);  std::process::exit(1); }
                };

                let mut rows_read_in_this_thread = 0;
                // Create select query
                let mut complete_where:String = where_clause.to_owned().to_string();
                if !where_clause.is_empty() {
                    complete_where = format!("WHERE {}", where_clause);
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
        
                    // Create copy query to extract data
                    let select_query = format!("SELECT * FROM {}.{} {} OFFSET {} LIMIT {}",
                        schema, table, complete_where, offset, limit);
                    let copy_out_query:String = format!("COPY ({}) TO STDOUT", select_query);

                    let mut reader = source_client.copy_out(copy_out_query.as_str()).unwrap();
                    let mut buf = vec![];
                    reader.read_to_end(&mut buf).unwrap();
        
                    // Create copy query to import data
                    let copy_in_query:String = format!("COPY {} FROM STDIN", table);
                    let mut writer = target_client.copy_in(copy_in_query.as_str()).unwrap();
                    writer.write_all(&buf).unwrap();
                    writer.finish().unwrap();

                    // Update progress bar after execution
                    pb.inc(limit as u64);

                    rows_read_in_this_thread = rows_read_in_this_thread + limit;

                    // Increase the offset in the same amount as the rows read (limit)
                    // If new offset + limit > max_offset
                    // set the new limit as the difference between max_offset and current new offset
                    offset += limit;
                    if offset + limit > max_offset {
                        limit = max_offset - offset;
                    }
                } // THREAD ENDS

                pb.finish_with_message(
                    format!("Thread {} finished reading rows from {} to {}",thread_num, offset_for_this_thread, max_offset).as_str());
                return limit_for_this_thread;
            });

        }

        // Wait for all the progress bars to finish. Also acts as a join for the child threads
        m.join_and_clear().unwrap();

        let duration = start.elapsed();
        println!("Finished importing {} rows from table {}.{} in {} secs", rows_to_import, schema, table, duration.as_secs());
    }

}