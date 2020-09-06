use postgres::{Client, NoTls};

use crate::config;

// TODO: Pass here the connection params as a single struct
pub trait TableImporter {
    fn import_table_from(&self, schema:String, table:String, where_clause:String, truncate:bool);
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
