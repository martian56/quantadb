# PowerShell test client for QuantaDB server

Write-Host "Testing QuantaDB Server..." -ForegroundColor Green

try {
    # Create TCP client
    $client = New-Object System.Net.Sockets.TcpClient
    $client.Connect("127.0.0.1", 54321)
    
    Write-Host "Connected to QuantaDB server" -ForegroundColor Green
    
    # Get network stream
    $stream = $client.GetStream()
    $reader = New-Object System.IO.StreamReader($stream)
    $writer = New-Object System.IO.StreamWriter($stream)
    $writer.AutoFlush = $true
    
    # Read welcome message
    $welcome = $reader.ReadLine()
    Write-Host "Welcome: $welcome" -ForegroundColor Yellow
    
    # Test queries
    $testQueries = @(
        '{"query": "CREATE TABLE users (id INT, name TEXT, age INT)"}',
        '{"query": "INSERT INTO users VALUES (1, \"Alice\", 25)"}',
        '{"query": "INSERT INTO users VALUES (2, \"Bob\", 30)"}',
        '{"query": "SELECT * FROM users"}',
        '{"query": "SELECT name FROM users WHERE age > 25"}',
        '{"query": "DELETE FROM users WHERE id = 2"}',
        '{"query": "SELECT * FROM users"}'
    )
    
    foreach ($i in 1..$testQueries.Length) {
        $query = $testQueries[$i-1]
        Write-Host "`nTest $i`: $query" -ForegroundColor Cyan
        
        # Send query
        $writer.WriteLine($query)
        
        # Read response
        $response = $reader.ReadLine()
        $responseObj = $response | ConvertFrom-Json
        
        if ($responseObj.success) {
            Write-Host "Success: $($responseObj.message)" -ForegroundColor Green
            if ($responseObj.data) {
                Write-Host "Data: $($responseObj.data)" -ForegroundColor White
            }
        } else {
            Write-Host "Error: $($responseObj.error)" -ForegroundColor Red
        }
        
        Start-Sleep -Milliseconds 100
    }
    
    # Cleanup
    $reader.Close()
    $writer.Close()
    $stream.Close()
    $client.Close()
    
    Write-Host "`nAll tests completed!" -ForegroundColor Green
    
} catch {
    Write-Host "Test failed: $($_.Exception.Message)" -ForegroundColor Red
}
