use quanta_cli::{QuantaCliClient, Result};
use clap::Parser;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use comfy_table::{Table, Cell, presets::UTF8_FULL};
use std::process;

#[derive(Parser)]
#[command(name = "quanta-cli")]
#[command(about = "QuantaDB Command Line Interface")]
#[command(version)]
struct Cli {
    /// Server host
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    
    /// Server port
    #[arg(short, long, default_value = "54321")]
    port: u16,
    
    /// Execute a single query and exit
    #[arg(short, long)]
    execute: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let address = format!("{}:{}", cli.host, cli.port);
    
    println!("🚀 QuantaDB CLI Client");
    println!("Connecting to {}...", address);
    
    let mut client = QuantaCliClient::new(&address);
    client.connect().await?;
    
    println!("✅ Connected to QuantaDB server!");
    
    if let Some(query) = cli.execute {
        // Execute single query and exit
        match client.execute(&query).await {
            Ok(result) => {
                println!("Query executed successfully!");
                print_result(&result);
            }
            Err(e) => {
                eprintln!("❌ Error: {}", e);
                process::exit(1);
            }
        }
    } else {
        // Interactive mode
        run_interactive(&mut client).await?;
    }
    
    client.disconnect().await?;
    Ok(())
}

async fn run_interactive(client: &mut QuantaCliClient) -> Result<()> {
    println!("\nType '\\help' for help, '\\quit' to exit.");
    println!("Enter SQL queries below:\n");
    
    let mut rl = DefaultEditor::new()?;
    
    loop {
        let readline = rl.readline("quanta=> ");
        match readline {
            Ok(line) => {
                let line = line.trim();
                
                if line.is_empty() {
                    continue;
                }
                
                // Handle CLI commands
                if line.starts_with('\\') {
                    match handle_cli_command(line, client).await {
                        Ok(should_continue) => {
                            if !should_continue {
                                break;
                            }
                        }
                        Err(e) => {
                            eprintln!("❌ Error: {}", e);
                        }
                    }
                    continue;
                }
                
                // Execute SQL query
                match client.execute(line).await {
                    Ok(result) => {
                        print_result(&result);
                    }
                    Err(e) => {
                        eprintln!("❌ Error: {}", e);
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("Use Ctrl+D or \\quit to exit.");
            }
            Err(ReadlineError::Eof) => {
                println!("Goodbye!");
                break;
            }
            Err(err) => {
                eprintln!("❌ Error: {:?}", err);
                break;
            }
        }
    }
    
    Ok(())
}

async fn handle_cli_command(cmd: &str, _client: &mut QuantaCliClient) -> Result<bool> {
    match cmd {
        "\\help" | "\\h" => {
            print_help();
            Ok(true)
        }
        "\\quit" | "\\q" | "\\exit" => {
            println!("Goodbye!");
            Ok(false)
        }
        "\\status" => {
            println!("✅ Connected to QuantaDB server");
            Ok(true)
        }
        _ => {
            println!("Unknown command: {}. Type \\help for help.", cmd);
            Ok(true)
        }
    }
}

fn print_help() {
    println!("\nQuantaDB CLI Commands:");
    println!("  \\help, \\h     Show this help message");
    println!("  \\quit, \\q     Quit the CLI");
    println!("  \\exit         Quit the CLI");
    println!("  \\status       Show connection status");
    println!("\nSQL Commands:");
    println!("  CREATE TABLE  Create a new table");
    println!("  INSERT INTO   Insert data into a table");
    println!("  SELECT        Query data from tables");
    println!("  DELETE FROM   Delete data from tables");
    println!("  DROP TABLE    Remove a table");
    println!();
}

fn print_result(result: &quanta_cli::QueryResult) {
    println!("Debug: result.success = {}", result.success);
    println!("Debug: result.message = {}", result.message);
    println!("Debug: result.data = {:?}", result.data);
    
    if !result.success {
        eprintln!("❌ Query failed: {}", result.message);
        return;
    }
    
    println!("✅ {}", result.message);
    
    if let Some(rows) = &result.data {
        if !rows.is_empty() {
            let mut table = Table::new();
            table.load_preset(UTF8_FULL);
            
            // Add header
            let header: Vec<Cell> = (0..rows[0].values.len())
                .map(|i| Cell::new(format!("Column {}", i + 1)))
                .collect();
            table.set_header(header);
            
            // Add rows
            for row in rows {
                let cells: Vec<Cell> = row.values.iter()
                    .map(|value| Cell::new(value.to_string()))
                    .collect();
                table.add_row(cells);
            }
            
            println!("\n{}", table);
            println!("({} rows)", rows.len());
        } else {
            println!("(0 rows)");
        }
    }
    
    if let Some(affected) = result.affected_rows {
        if affected > 0 {
            println!("({} rows affected)", affected);
        }
    }
    
    println!();
}
