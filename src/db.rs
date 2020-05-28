use postgres::{Client, NoTls, Row, Column, Error};
use postgres::types::{Type};
use std::thread;
use std::time::{Instant, SystemTime};
use chrono::offset::Utc;
use chrono::DateTime;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rust_decimal::Decimal;

use crate::config;
use crate::config::ConfigProperty;
use crate::utils::{log_error};

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

pub fn import_table_from_env(schema:String, table:String, where_clause:String) {
    
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

    println!("Importing table {}.{} ...", schema, table);
    // Create the progression bars
    let m = MultiProgress::new();
    let sty = ProgressStyle::default_bar()
        .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
        .progress_chars("##-");

    // Start measuring total time for this table
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

            let mut column_names:Vec<String> = vec!();
            let mut query_values:String = String::from("");
            let mut rows_to_insert:i64 = 0;

            let mut rows_read_in_this_thread = 0;
            // Create select query
            let mut complete_where:String = where_clause.to_owned().to_string();
            if !where_clause.is_empty() {
                complete_where = format!("WHERE {}", where_clause);
            }

            // If number of rows to read in this thread are more than MAX_ROWS_FOR_SELECT, divide in several selects of max size
            // Doing this is specially important for big queries, as the memory consumption could even kill the process otherwise
            let mut limit = limit_for_this_thread;
            let mut offset = offset_for_this_thread;
            let max_offset = offset_for_this_thread + limit_for_this_thread;
            if limit_for_this_thread > max_rows_for_select {
                limit = max_rows_for_select;
            }

            // Iterate until finishing with all rows assigned to this thread
            while offset < max_offset {
    
                // Create the SELECT query for this iteration
                let select_query = format!("SELECT * FROM {}.{} {} OFFSET {} LIMIT {}",
                     schema, table, complete_where, offset, limit);
    
                // Read all values for previous query and insert them in the target DB
                for row in source_client.query(select_query.as_str(), &[]).unwrap() {
                    
                    rows_read_in_this_thread += 1;
                    
                    // Column names are always the same, do this only once
                    if column_names.is_empty() {
                        for column in row.columns() {
                            let column_name = column.name();
                            column_names.push(column_name.to_string());
                        }                    
                    }
    
                    let mut column_values:Vec<String> = vec!();
                    // Get a whole row of values
                    for column in row.columns() {
                        let value: String = string_value_for(&row, column);
                        column_values.push(value);
                    }
                    rows_to_insert = rows_to_insert + 1;
    
                    // Create as many values as needed for this row
                    let mut row_values = format!("({}", column_values[0]);
                    for i in 1..column_values.len() {
                        row_values = format!("{},{}", row_values, column_values[i]);
                    }
                    row_values = format!("{})", row_values);
    
                    // Add new row to insert to the query values
                    if query_values.is_empty() {
                        query_values = row_values;
                    }
                    else{
                        query_values = format!("{},{}", query_values, row_values);
                    }
    
                    // If we've reached the minimum number to insert, do it so and reset the insert query
                    if rows_to_insert == min_rows_for_insert || rows_read_in_this_thread == limit_for_this_thread {
                        let column_names_list:String = format!("{:?}", column_names);
                        let column_names_list = column_names_list.replace("[","(");
                        let column_names_list = column_names_list.replace("]",")");
                    
                        let query = format!("INSERT INTO {}.{} {} VALUES {}", schema, table, column_names_list, query_values);
                        
                        target_client.execute(query.as_str(), &[]).unwrap();
                        
                        // Update progress bar after execution
                        pb.inc(rows_to_insert as u64);
     
                        rows_to_insert = 0;
                        query_values = String::from("");
                    }
    
                }

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

// Convert any SQL type to a string. Not a extensive list. Just supporting the most common ones.
fn string_value_for(row:&Row, column:&Column) -> String {
    // Try to get the value. Will return null in case of error
     return match &*column.type_() {
        &Type::BOOL => { 
            match row.try_get(column.name()) {
                Ok(val) => { let val:bool = val; val.to_string() },
                Err(error) => handle_sql_error(error)
            }
        },
        &Type::INT2 => { 
            match row.try_get(column.name()) {
                Ok(val) => { let val:i16 = val; val.to_string() },
                Err(error) => handle_sql_error(error)
            }
        },
        &Type::INT4 => { 
            match row.try_get(column.name()) {
                Ok(val) => { let val:i32 = val; val.to_string() },
                Err(error) => handle_sql_error(error)
            }
        },
        &Type::INT8 => { 
            match row.try_get(column.name()) {
                Ok(val) => { let val:i64 = val; val.to_string() },
                Err(error) => handle_sql_error(error)
            }
        },
        &Type::VARCHAR | &Type::TEXT => { 
            match row.try_get(column.name()) {
                Ok(val) => { 
                    let val:String = val;
                    let val:String = val.replace("'", "''");
                    format!("'{}'",val.to_string()) 
                },
                Err(error) => handle_sql_error(error)
            }
        },
        &Type::FLOAT4 => { 
            match row.try_get(column.name()) {
                Ok(val) => { let val:f32 = val; val.to_string() },
                Err(error) => handle_sql_error(error)
            }
        },
        &Type::NUMERIC => { 
            match row.try_get(column.name()) {
                Ok(val) => { let val:Decimal = val; val.to_string() },
                Err(error) => handle_sql_error(error)
            }
        },
        &Type::TIMESTAMPTZ => {
            match row.try_get(column.name()) {
                Ok(timestamptz) => {
                    let value:SystemTime = timestamptz;
                    let datetime: DateTime<Utc> = value.into();
                    format!("'{}'",datetime.format("%Y-%m-%d %T"))
                },
                Err(error) => handle_sql_error(error)
            }
        },
        unknown => { 
            log_error(format!("Error. Postgres Type not supported yet by the pgimporter: {}", unknown).as_str()); 
            "NULL".to_owned() 
        }
    }
}

fn handle_sql_error(_error:Error) -> String {
    // FIXME: Re-enable error logging once we can identify when error comes from reading a NULL value.
    // Otherwise performance gets very affected
    //log_error(format!("{}", error).as_str()); 
    "NULL".to_owned()
}