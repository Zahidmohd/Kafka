import net from "net";

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

const server = net.createServer((connection) => {
  console.log("Client connected");
  
  connection.on("data", (data) => {
    console.log("Received request from client");
    console.log("Request data:", data.toString('hex'));
    
    // Parse request header v2
    // Offset 0: message_size (4 bytes)
    // Offset 4: request_api_key (2 bytes)
    // Offset 6: request_api_version (2 bytes)
    // Offset 8: correlation_id (4 bytes) <- We need this!
    
    const correlationId = data.readInt32BE(8);
    console.log("Extracted correlation_id:", correlationId);
    
    // Create response buffer (8 bytes total)
    const response = Buffer.alloc(8);
    
    // Write message_size (4 bytes, big-endian) - value: 0
    response.writeInt32BE(0, 0);
    
    // Write correlation_id (4 bytes, big-endian) - echo back from request
    response.writeInt32BE(correlationId, 4);
    
    console.log("Sending response:", response.toString('hex'));
    connection.write(response);
  });
  
  connection.on("end", () => {
    console.log("Client disconnected");
  });
});

server.listen(9092, "127.0.0.1", () => {
  console.log("Kafka broker listening on port 9092");
});
