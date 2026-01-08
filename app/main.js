import net from "net";

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

const server = net.createServer((connection) => {
  console.log("Client connected");
  
  connection.on("data", (data) => {
    console.log("Received request from client");
    
    // Create response buffer (8 bytes total)
    const response = Buffer.alloc(8);
    
    // Write message_size (4 bytes, big-endian) - value: 0
    response.writeInt32BE(0, 0);
    
    // Write correlation_id (4 bytes, big-endian) - value: 7
    response.writeInt32BE(7, 4);
    
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
