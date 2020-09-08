use std::io::{Read, Write};
use crate::config::ImportConfig;
use crate::db::{TableImporter, DBClients, TableChunk};

pub struct CopyImporter;

impl TableImporter for CopyImporter {

    fn import_table_chunk(&self, import_config:&ImportConfig, db_clients:&mut DBClients, chunk:&TableChunk) {
        // Create copy query to extract data
        let select_query = format!("SELECT * FROM {}.{} {} OFFSET {} LIMIT {}",
            import_config.schema, import_config.table, chunk.where_clause, chunk.offset, chunk.limit);
        let copy_out_query:String = format!("COPY ({}) TO STDOUT", select_query);
    
        let mut reader = db_clients.source_client.copy_out(copy_out_query.as_str()).unwrap();
        let mut buf = vec![];
        reader.read_to_end(&mut buf).unwrap();
        
        // Create copy query to import data
        let copy_in_query:String = format!("COPY {} FROM STDIN", import_config.table);
        let mut writer = db_clients.target_client.copy_in(copy_in_query.as_str()).unwrap();
        writer.write_all(&buf).unwrap();
        writer.finish().unwrap();    
    }

}