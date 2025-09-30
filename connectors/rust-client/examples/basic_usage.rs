use quanta_client::QuantaClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔌 Connecting to QuantaDB...");
    
    let mut client = QuantaClient::new("127.0.0.1:5432");
    client.connect().await?;
    
    println!("✅ Connected to QuantaDB!");
    
    // Create a table
    println!("\n📝 Creating table 'users'...");
    let result = client.execute("CREATE TABLE users (id INT, name TEXT, age INT)").await?;
    println!("Result: {}", result.message);
    
    // Insert some data
    println!("\n➕ Inserting data...");
    let result = client.execute("INSERT INTO users VALUES (1, \"Alice\", 25)").await?;
    println!("Result: {}", result.message);
    
    let result = client.execute("INSERT INTO users VALUES (2, \"Bob\", 30)").await?;
    println!("Result: {}", result.message);
    
    // Query the data
    println!("\n🔍 Querying data...");
    let result = client.execute("SELECT * FROM users").await?;
    println!("Result: {}", result.message);
    
    if let Some(rows) = result.data {
        for (i, row) in rows.iter().enumerate() {
            println!("Row {}: {:?}", i + 1, row.values);
        }
    }
    
    // Query with WHERE clause
    println!("\n🔍 Querying with WHERE clause...");
    let result = client.execute("SELECT name FROM users WHERE age > 25").await?;
    println!("Result: {}", result.message);
    
    if let Some(rows) = result.data {
        for (i, row) in rows.iter().enumerate() {
            println!("Row {}: {:?}", i + 1, row.values);
        }
    }
    
    client.disconnect().await?;
    println!("\n👋 Disconnected from QuantaDB");
    
    Ok(())
}
