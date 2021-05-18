use std::io::{BufRead, Write};
use crate::config::{ImportConfig, CONFIG_PROPERTIES};
use postgres::{Client, NoTls};
use indicatif::{ProgressBar, ProgressStyle};

pub fn single_thread_import(import_config:&ImportConfig, total_rows_to_import:u64) {
    let max_rows_per_batch = CONFIG_PROPERTIES.rows_select;

    let mut source_client = match Client::connect(import_config.source_db_url.as_ref(), NoTls) {
        Ok(client) => client,
        Err(error) => { println!("Couldn't connect to source DB. Error: {}", error);  std::process::exit(1); }
    };
    
    let mut target_client = match Client::connect(import_config.target_db_url.as_ref(), NoTls) {
        Ok(client) => client,
        Err(error) => { println!("Couldn't connect to target DB. Error: {}", error);  std::process::exit(1); }
    };

    // Create copy query to extract data
    let select_query = format!("SELECT * FROM {}.{} {}", import_config.schema, import_config.table, import_config.where_clause);
    let copy_out_query:String = format!("COPY ({}) TO STDOUT", select_query);
    
    let mut reader = source_client.copy_out(copy_out_query.as_str()).unwrap();

    // Create ProgressBar to show progress of import to user
    let pb = ProgressBar::new(total_rows_to_import);
    let sty = ProgressStyle::default_bar()
        .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
        .progress_chars("##-");
    pb.set_style(sty);
    pb.set_position(0);

    let mut buffer = vec!();
    let mut total_rows = 0;
    // Keep reading from source until reader is empty
    loop {
        let row = reader.fill_buf().unwrap();
        let row_bytes = row.len();
        
        // If we've reached EOF, end now, writing remaining rows on buffer
        if row_bytes == 0 {
            if buffer.len() > 0 {
                write_to_target(import_config, &mut target_client, &buffer);
                pb.finish_and_clear();
            }
            break;
        }
    
        buffer.extend(row);
        total_rows = total_rows + 1;

        if total_rows % max_rows_per_batch == 0 {
            write_to_target(import_config, &mut target_client, &buffer);
            pb.set_position(total_rows as u64);
            buffer = vec!();
        }

        // ensure the bytes we worked with aren't returned again later
        reader.consume(row_bytes);
    }

    println!("TOTAL ROWS READ: {}", total_rows);
}

fn write_to_target(import_config:&ImportConfig, target_client:&mut Client, buffer:&[u8]) {
    // Create copy query to import data
    let copy_in_query:String = format!("COPY {}.{} FROM STDIN", import_config.schema, import_config.table);
    let mut writer = target_client.copy_in(copy_in_query.as_str()).unwrap();
    writer.write_all(&buffer).unwrap();
    writer.finish().unwrap();
}