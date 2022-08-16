use crate::config::{ImportConfig, CONFIG_PROPERTIES};
use postgres::{Client, NoTls};
use std::sync::Arc;
use std::thread;
use indicatif::{ProgressBar, ProgressStyle, MultiProgress};

use crate::copy::CopyImporter;
use crate::query::QueryImporter;
use crate::db::{DBClients, TableChunk, TableImporter};

pub fn multi_thread_import(import_config:&Arc<ImportConfig>, order_by:&String, total_rows_to_import:i64) {

    let max_threads = CONFIG_PROPERTIES.max_threads;
    let max_rows_for_select = CONFIG_PROPERTIES.rows_select;

    // Divide all rows to import by the number of threads to use
    let rows_per_thread = total_rows_to_import / max_threads;

    // START IMPORTING, SPAWNING WORKER THREADS
    // Create the progression bars
    let multi_progress_bar = MultiProgress::new();
    let sty = ProgressStyle::default_bar()
        .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
        .progress_chars("##-");

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
        let order_by = order_by.clone();

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

            progress_bar.set_position(0);

            // Iterate until finishing with all rows assigned to this thread
            while offset < max_offset {
  
                let table_chunk = TableChunk { where_clause: complete_where.to_owned(), offset: offset, 
                    limit: limit, order_by: order_by.to_owned()};

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
}
