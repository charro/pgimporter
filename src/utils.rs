use std::net::{SocketAddr, IpAddr, TcpStream, Shutdown};
use std::time::Duration;
use resolve::resolve_host;
use log::{error};

use crate::config::{ get_config_property, ConfigProperty, SOURCE_DB_DEFAULT_HOST,
     SOURCE_DB_DEFAULT_PORT, TARGET_DB_DEFAULT_HOST, TARGET_DB_DEFAULT_PORT };

pub fn check_postgres_source_target_servers() -> bool {
    let source_host:String = get_config_property(ConfigProperty::SourceDBHost, SOURCE_DB_DEFAULT_HOST.to_owned());
    let source_port:String = get_config_property(ConfigProperty::SourceDBPort, SOURCE_DB_DEFAULT_PORT.to_owned());
    let target_host:String = get_config_property(ConfigProperty::TargetDBHost, TARGET_DB_DEFAULT_HOST.to_owned());
    let target_port:String = get_config_property(ConfigProperty::TargetDBPort, TARGET_DB_DEFAULT_PORT.to_owned());

    check_postgres_server("Source DB", source_host.as_str(), source_port.as_str()) && 
    check_postgres_server("Target DB", target_host.as_str(), target_port.as_str())
}

pub fn log_error(err_msg:&str){
    error!("{}", err_msg);
}

fn check_postgres_server(msg:&str, host:&str, port:&str) -> bool {
    print!("{}: Checking Postgres server {}:{}...", msg, host, port);

    // The provided host is an IP?
    if let Ok(ip) = host.parse::<IpAddr>() {
        return check_ip_port(&ip.to_string(), port)
    }
    // Provided host is a hostname. Needs DNS resolution?
    else{
        if let Ok(ips) = resolve_host(host) {
            for ip in ips {
                if check_ip_port(&ip.to_string(), port) {
                    println!("     OK");
                    return true;
                }
            }
        }
    }

    log_error("Testing the error log");
    println!("     FAILED. Couldn't reach server in {}:{}", host, port);
    false
}

fn check_ip_port(ip:&str, port:&str) -> bool{
    let ip_port = format!("{}:{}", ip, port);

    if let Ok(postgres_socket) = ip_port.parse() {
        let postgres_socket:SocketAddr = postgres_socket;
    
        // Try to connect to the TCP port. Fail after some seconds
        if let Ok(stream) = TcpStream::connect_timeout(&postgres_socket, Duration::from_secs(10)) {
            stream.shutdown(Shutdown::Both).unwrap();
            return true
        }
    }

    false
}