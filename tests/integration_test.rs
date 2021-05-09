use postgres::{Client, NoTls};

const SOURCE_URL: &str = "host='localhost' port='5432' dbname='postgres' user='postgres' password='postgres'";  
const TARGET_URL: &str = "host='localhost' port='5555' dbname='postgres' user='postgres' password='postgres'";

const QUERY: &str = "SELECT id, some_text, a_number FROM table1 ORDER BY id";

struct DBElement {
    id:i32,
    some_text:String,
    a_number: i32
}

#[test]
fn check_inserted_data(){

    let mut source_client = match Client::connect(SOURCE_URL, NoTls) {
        Ok(client) => client,
        Err(error) => { println!("Couldn't connect to source DB. Error: {}", error);  std::process::exit(1); }
    };

    let mut target_client = match Client::connect(TARGET_URL, NoTls) {
        Ok(client) => client,
        Err(error) => { println!("Couldn't connect to target DB. Error: {}", error);  std::process::exit(1); }
    };

    let mut source_results = Vec::new();
    for row in source_client.query(QUERY, &[]).unwrap() {
        let id: i32 = row.get(0);
        let some_text: String = row.get(1);
        let a_number: i32 = row.get(2);
    
        let element = DBElement { id: id, some_text: some_text, a_number: a_number };
        source_results.push(element);
    }

    let mut target_results = Vec::new();
    for row in target_client.query(QUERY, &[]).unwrap() {
        let id: i32 = row.get(0);
        let some_text: String = row.get(1);
        let a_number: i32 = row.get(2);
    
        let element = DBElement { id: id, some_text: some_text, a_number: a_number };
        target_results.push(element);
    }

    println!("Found {} elements in source and {} in target", source_results.len(), target_results.len());

    assert_eq!(source_results.len(), target_results.len());

    for index in 0..source_results.len() {
        assert_eq!(source_results[index].id, target_results[index].id);
        assert_eq!(source_results[index].some_text, target_results[index].some_text);
        assert_eq!(source_results[index].a_number, target_results[index].a_number);
    }
}