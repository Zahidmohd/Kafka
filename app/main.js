import net from "net";

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

// Handler for ApiVersions API (API key 18)
function handleApiVersions(connection, requestApiVersion, correlationId) {
  console.log("Handling ApiVersions request");
  
  // Check if API version is supported (we support 0-4)
  if (requestApiVersion < 0 || requestApiVersion > 4) {
    console.log("Unsupported version, returning error code 35");
    
    // For unsupported version, send minimal response
    const response = Buffer.alloc(10);
    response.writeInt32BE(0, 0); // message_size (placeholder)
    response.writeInt32BE(correlationId, 4);
    response.writeInt16BE(35, 8); // UNSUPPORTED_VERSION
    connection.write(response);
    return;
  }
  
  // Build ApiVersions v4 response body
  const responseBody = Buffer.alloc(22);
  let offset = 0;
  
  // error_code (INT16): 0
  responseBody.writeInt16BE(0, offset);
  offset += 2;
  
  // api_keys (COMPACT_ARRAY): 2 entries = 3 in compact encoding
  responseBody.writeUInt8(3, offset);
  offset += 1;
  
  // API key entry 1: ApiVersions (18)
  responseBody.writeInt16BE(18, offset); // api_key
  offset += 2;
  responseBody.writeInt16BE(0, offset);  // min_version
  offset += 2;
  responseBody.writeInt16BE(4, offset);  // max_version
  offset += 2;
  responseBody.writeUInt8(0, offset);    // TAG_BUFFER (empty)
  offset += 1;
  
  // API key entry 2: DescribeTopicPartitions (75)
  responseBody.writeInt16BE(75, offset); // api_key
  offset += 2;
  responseBody.writeInt16BE(0, offset);  // min_version
  offset += 2;
  responseBody.writeInt16BE(0, offset);  // max_version
  offset += 2;
  responseBody.writeUInt8(0, offset);    // TAG_BUFFER (empty)
  offset += 1;
  
  // throttle_time_ms (INT32): 0
  responseBody.writeInt32BE(0, offset);
  offset += 4;
  
  // TAG_BUFFER (empty)
  responseBody.writeUInt8(0, offset);
  offset += 1;
  
  // Calculate message_size: header (4 bytes) + body (22 bytes) = 26 bytes
  const messageSize = 4 + responseBody.length;
  
  // Create full response
  const response = Buffer.alloc(4 + messageSize);
  response.writeInt32BE(messageSize, 0);
  response.writeInt32BE(correlationId, 4);
  responseBody.copy(response, 8);
  
  console.log("Sending ApiVersions response:", response.toString('hex'));
  connection.write(response);
}

// Handler for DescribeTopicPartitions API (API key 75)
function handleDescribeTopicPartitions(connection, data, correlationId) {
  console.log("Handling DescribeTopicPartitions request");
  console.log("Full request hex:", data.toString('hex'));
  
  // Parse request body to extract topic names
  // Request Header v2 structure:
  //   Offset 0: message_size (4 bytes)
  //   Offset 4: request_api_key (2 bytes)
  //   Offset 6: request_api_version (2 bytes)
  //   Offset 8: correlation_id (4 bytes)
  //   Offset 12: client_id (COMPACT_NULLABLE_STRING)
  //   Offset X: TAG_BUFFER (1 byte minimum)
  //   Then body starts
  
  let offset = 12; // Start after message_size, api_key, api_version, correlation_id
  
  console.log("Starting offset:", offset);
  console.log("Bytes at offset 12:", data.toString('hex', 12, 20));
  
  // Skip client_id (COMPACT_NULLABLE_STRING)
  // COMPACT_NULLABLE_STRING: 0 = null, otherwise length+1
  const clientIdLengthByte = data.readUInt8(offset);
  console.log("Client ID length byte:", clientIdLengthByte);
  offset += 1;
  
  if (clientIdLengthByte > 0) {
    const clientIdLength = clientIdLengthByte - 1;
    const clientId = data.toString('utf8', offset, offset + clientIdLength);
    console.log("Client ID:", clientId, "length:", clientIdLength);
    offset += clientIdLength;
  } else {
    console.log("Client ID is null");
  }
  
  // Skip TAG_BUFFER from header (at least 1 byte)
  const headerTagBufferLength = data.readUInt8(offset);
  console.log("Header TAG_BUFFER length:", headerTagBufferLength);
  offset += 1;
  if (headerTagBufferLength > 0) {
    // Skip additional tag buffer bytes if present
    // For now, assuming it's just 0 (empty)
  }
  
  console.log("Body starts at offset:", offset);
  console.log("Bytes at body start:", data.toString('hex', offset, offset + 20));
  
  // Now we're at the body: topics (COMPACT_ARRAY)
  const topicsArrayLengthByte = data.readUInt8(offset);
  console.log("Topics array length byte:", topicsArrayLengthByte);
  const topicsArrayLength = topicsArrayLengthByte - 1; // Compact encoding: n+1
  offset += 1;
  
  console.log("Number of topics requested:", topicsArrayLength);
  
  // Extract topic name from first topic
  let topicName = "";
  if (topicsArrayLength > 0) {
    // Read topic name (COMPACT_STRING)
    const topicNameLengthByte = data.readUInt8(offset);
    const topicNameLength = topicNameLengthByte - 1;
    console.log("Topic name length byte:", topicNameLengthByte, "actual length:", topicNameLength);
    offset += 1;
    topicName = data.toString('utf8', offset, offset + topicNameLength);
    console.log("Requested topic:", topicName);
  }
  
  // Build DescribeTopicPartitions v0 response
  // Response uses header v1: correlation_id + TAG_BUFFER
  
  // Build response body first
  const topicNameBytes = Buffer.from(topicName, 'utf8');
  const bodySize = 4 + 1 + 2 + 1 + topicNameBytes.length + 16 + 1 + 1 + 4 + 1 + 1 + 1;
  const responseBody = Buffer.alloc(bodySize);
  let bodyOffset = 0;
  
  // throttle_time_ms (INT32): 0
  responseBody.writeInt32BE(0, bodyOffset);
  bodyOffset += 4;
  
  // topics (COMPACT_ARRAY): 1 element = 2 in compact encoding
  responseBody.writeUInt8(2, bodyOffset);
  bodyOffset += 1;
  
  // Topic entry:
  // error_code (INT16): 3 (UNKNOWN_TOPIC_OR_PARTITION)
  responseBody.writeInt16BE(3, bodyOffset);
  bodyOffset += 2;
  
  // topic_name (COMPACT_STRING): length+1, then bytes
  responseBody.writeUInt8(topicNameBytes.length + 1, bodyOffset);
  bodyOffset += 1;
  topicNameBytes.copy(responseBody, bodyOffset);
  bodyOffset += topicNameBytes.length;
  
  // topic_id (UUID): 16 bytes of zeros (00000000-0000-0000-0000-000000000000)
  responseBody.fill(0, bodyOffset, bodyOffset + 16);
  bodyOffset += 16;
  
  // is_internal (BOOLEAN): false (0)
  responseBody.writeUInt8(0, bodyOffset);
  bodyOffset += 1;
  
  // partitions (COMPACT_ARRAY): 0 elements = 1 in compact encoding
  responseBody.writeUInt8(1, bodyOffset);
  bodyOffset += 1;
  
  // topic_authorized_operations (INT32): 0
  responseBody.writeInt32BE(0, bodyOffset);
  bodyOffset += 4;
  
  // TAG_BUFFER (empty)
  responseBody.writeUInt8(0, bodyOffset);
  bodyOffset += 1;
  
  // next_cursor (NULLABLE_INT8): -1 (null)
  responseBody.writeInt8(-1, bodyOffset);
  bodyOffset += 1;
  
  // TAG_BUFFER (empty)
  responseBody.writeUInt8(0, bodyOffset);
  bodyOffset += 1;
  
  // Build response header v1: correlation_id + TAG_BUFFER
  const headerSize = 4 + 1; // correlation_id (4) + TAG_BUFFER (1)
  const messageSize = headerSize + responseBody.length;
  
  // Create full response
  const response = Buffer.alloc(4 + messageSize);
  response.writeInt32BE(messageSize, 0);
  response.writeInt32BE(correlationId, 4);
  response.writeUInt8(0, 8); // TAG_BUFFER (empty) for header v1
  responseBody.copy(response, 9);
  
  console.log("Sending DescribeTopicPartitions response:", response.toString('hex'));
  connection.write(response);
}

const server = net.createServer((connection) => {
  console.log("Client connected");
  
  connection.on("data", (data) => {
    console.log("Received request from client");
    console.log("Request data:", data.toString('hex'));
    
    // Parse request header v2
    // Offset 0: message_size (4 bytes)
    // Offset 4: request_api_key (2 bytes)
    // Offset 6: request_api_version (2 bytes)
    // Offset 8: correlation_id (4 bytes)
    
    const requestApiKey = data.readInt16BE(4);
    const requestApiVersion = data.readInt16BE(6);
    const correlationId = data.readInt32BE(8);
    
    console.log("Request API Key:", requestApiKey);
    console.log("Request API Version:", requestApiVersion);
    console.log("Correlation ID:", correlationId);
    
    // Route to appropriate API handler
    if (requestApiKey === 18) {
      // ApiVersions API
      handleApiVersions(connection, requestApiVersion, correlationId);
    } else if (requestApiKey === 75) {
      // DescribeTopicPartitions API
      handleDescribeTopicPartitions(connection, data, correlationId);
    } else {
      console.log("Unknown API key:", requestApiKey);
    }
  });
  
  connection.on("end", () => {
    console.log("Client disconnected");
  });
});

server.listen(9092, "127.0.0.1", () => {
  console.log("Kafka broker listening on port 9092");
});
