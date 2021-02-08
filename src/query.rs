use chrono::offset::Utc;
use chrono::DateTime;
use postgres::types::Type;
use postgres::{Column, Error, Row};
use rust_decimal::Decimal;
use std::time::SystemTime;

use crate::config;
use crate::config::{ConfigProperty, ImportConfig};
use crate::db::{DBClients, TableChunk, TableImporter};
use crate::utils::log_error;

pub struct QueryImporter;

impl TableImporter for QueryImporter {

    fn import_table_chunk(&self, import_config: &ImportConfig, db_clients:&mut DBClients, chunk: &TableChunk) {
        let min_rows_for_insert: i64 = 
            config::get_config_property(ConfigProperty::RowsToExecuteInsert(config::DEFAULT_ROWS_TO_EXECUTE_INSERT), config::DEFAULT_ROWS_TO_EXECUTE_INSERT);

        let mut rows_read_in_this_thread = 0;
        let mut column_names: Vec<String> = vec![];
        let mut query_values: String = String::from("");
        let mut rows_to_insert: i64 = 0;

        // Create the SELECT query for this iteration
        let select_query = format!(
            "SELECT * FROM {}.{} {} OFFSET {} LIMIT {}",
            import_config.schema,
            import_config.table,
            chunk.where_clause,
            chunk.offset,
            chunk.limit
        );

        // Read all values for previous query and insert them in the target DB
        for row in db_clients.source_client.query(select_query.as_str(), &[]).unwrap() {
            rows_read_in_this_thread += 1;

            // Column names are always the same, do this only once
            if column_names.is_empty() {
                for column in row.columns() {
                    let column_name = column.name();
                    column_names.push(column_name.to_string());
                }
            }

            let mut column_values: Vec<String> = vec![];
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
            } else {
                query_values = format!("{},{}", query_values, row_values);
            }

            // If we've reached the minimum number to insert, do it so and reset the insert query
            if rows_to_insert == min_rows_for_insert || rows_read_in_this_thread == chunk.limit {
                let column_names_list: String = format!("{:?}", column_names);
                let column_names_list = column_names_list.replace("[", "(");
                let column_names_list = column_names_list.replace("]", ")");

                let query = format!(
                    "INSERT INTO {}.{} {} VALUES {}",
                    import_config.schema, import_config.table, column_names_list, query_values
                );

                db_clients.target_client.execute(query.as_str(), &[]).unwrap();

                rows_to_insert = 0;
                query_values = String::from("");
            }
        }
    }

}

// Convert any SQL type to a string. Not a extensive list. Just supporting the most common ones.
fn string_value_for(row: &Row, column: &Column) -> String {
    // Try to get the value. Will return null in case of error
    return match &*column.type_() {
        &Type::BOOL => match row.try_get(column.name()) {
            Ok(val) => {
                let val: bool = val;
                val.to_string()
            }
            Err(error) => handle_sql_error(error),
        },
        &Type::INT2 => match row.try_get(column.name()) {
            Ok(val) => {
                let val: i16 = val;
                val.to_string()
            }
            Err(error) => handle_sql_error(error),
        },
        &Type::INT4 => match row.try_get(column.name()) {
            Ok(val) => {
                let val: i32 = val;
                val.to_string()
            }
            Err(error) => handle_sql_error(error),
        },
        &Type::INT8 => match row.try_get(column.name()) {
            Ok(val) => {
                let val: i64 = val;
                val.to_string()
            }
            Err(error) => handle_sql_error(error),
        },
        &Type::VARCHAR | &Type::TEXT => match row.try_get(column.name()) {
            Ok(val) => {
                let val: String = val;
                let val: String = val.replace("'", "''");
                format!("'{}'", val.to_string())
            }
            Err(error) => handle_sql_error(error),
        },
        &Type::FLOAT4 => match row.try_get(column.name()) {
            Ok(val) => {
                let val: f32 = val;
                val.to_string()
            }
            Err(error) => handle_sql_error(error),
        },
        &Type::NUMERIC => match row.try_get(column.name()) {
            Ok(val) => {
                let val: Decimal = val;
                val.to_string()
            }
            Err(error) => handle_sql_error(error),
        },
        &Type::TIMESTAMPTZ => match row.try_get(column.name()) {
            Ok(timestamptz) => {
                let value: SystemTime = timestamptz;
                let datetime: DateTime<Utc> = value.into();
                format!("'{}'", datetime.format("%Y-%m-%d %T"))
            }
            Err(error) => handle_sql_error(error),
        },
        unknown => {
            log_error(
                format!(
                    "Error. Postgres Type not supported yet by the pgimporter: {}",
                    unknown
                )
                .as_str(),
            );
            "NULL".to_owned()
        }
    };
}

fn handle_sql_error(_error: Error) -> String {
    // FIXME: Re-enable error logging once we can identify when error comes from reading a NULL value.
    // Otherwise performance gets very affected
    //log_error(format!("{}", error).as_str());
    "NULL".to_owned()
}
