# Kafka Broker Implementation - Project Plan

## 🎯 Project Overview

This project is a simplified implementation of an Apache Kafka broker built from scratch in JavaScript (Node.js). The goal is to understand the inner workings of Kafka by implementing its core protocol and features step by step.

### What is Apache Kafka?

Apache Kafka is a distributed event streaming platform used for building real-time data pipelines and streaming applications. It's designed to handle high-throughput, fault-tolerant message streaming between different systems.

### What are we Building?

We're building a Kafka broker that can:
- Accept TCP connections from Kafka clients
- Parse and understand the Kafka wire protocol
- Respond to various Kafka API requests
- Manage topics, partitions, and messages
- Store and retrieve event data

---

## 📋 Implementation Stages

### ✅ Stage 1: Send Response with Correlation ID

**Status:** COMPLETED

**What it does:**
- Sets up a basic TCP server listening on port 9092 (standard Kafka port)
- Accepts client connections
- Sends a hardcoded 8-byte response with correlation ID = 7

**Purpose:**
This stage introduces the basic structure of Kafka messages and responses:
- `message_size` (4 bytes): Size of header + body
- `correlation_id` (4 bytes): Unique identifier for matching requests/responses

**Key Concepts:**
- **TCP Server**: Uses Node.js `net` module to create a server
- **Binary Protocol**: Kafka uses binary data (not text/JSON)
- **Big-Endian Integers**: All integers are encoded in big-endian byte order

**Code Highlights:**
```javascript
const response = Buffer.alloc(8);
response.writeInt32BE(0, 0);  // message_size
response.writeInt32BE(7, 4);  // correlation_id
```

---

### ✅ Stage 2: Extract and Echo Correlation ID

**Status:** COMPLETED

**What it does:**
- Parses incoming request headers (Request Header v2)
- Extracts the actual correlation ID from the request
- Echoes it back in the response

**Purpose:**
Demonstrates how to parse binary Kafka protocol messages and extract specific fields.

**Request Header v2 Structure:**
```
Offset  Size  Field
------  ----  -----
0       4     message_size
4       2     request_api_key
6       2     request_api_version
8       4     correlation_id          <- We extract this!
12+     var   client_id, TAG_BUFFER
```

**Key Concepts:**
- **Binary Parsing**: Reading specific byte offsets from buffers
- **Request-Response Matching**: Correlation IDs let clients match responses to their requests
- **Protocol Fields**: Understanding the structure of Kafka headers

**Code Highlights:**
```javascript
const correlationId = data.readInt32BE(8);  // Read from offset 8
response.writeInt32BE(correlationId, 4);    // Echo back in response
```

---

### ✅ Stage 3: API Version Validation and Error Codes

**Status:** COMPLETED

**What it does:**
- Validates the `request_api_version` field from the request
- Returns error code 35 (UNSUPPORTED_VERSION) for unsupported versions
- Returns error code 0 (no error) for supported versions (0-4)

**Purpose:**
Introduces error handling and API versioning in Kafka protocol.

**Supported Versions:**
- ApiVersions API: versions 0-4

**Error Codes:**
- `0`: No error (success)
- `35`: UNSUPPORTED_VERSION

**Response Structure:**
```
Offset  Size  Field
------  ----  -----
0       4     message_size
4       4     correlation_id
8       2     error_code              <- New field!
```

**Key Concepts:**
- **API Versioning**: Each Kafka API supports multiple versions
- **Error Codes**: Standardized error codes for different failure scenarios
- **Version Negotiation**: Clients and brokers must agree on protocol versions

**Code Highlights:**
```javascript
const requestApiVersion = data.readInt16BE(6);
let errorCode = 0;
if (requestApiVersion < 0 || requestApiVersion > 4) {
  errorCode = 35; // UNSUPPORTED_VERSION
}
```

---

### ✅ Stage 4: Full ApiVersions Response

**Status:** COMPLETED

**What it does:**
- Implements complete ApiVersions v4 response body
- Returns list of supported APIs with their version ranges
- Correctly calculates and sets the `message_size` field

**Purpose:**
The ApiVersions API is crucial for Kafka clients to discover what features a broker supports.

**ApiVersions Response Body (v4) Structure:**
```
Field               Type            Size    Description
-----               ----            ----    -----------
error_code          INT16           2       Error code (0 = success)
api_keys            COMPACT_ARRAY   var     Array of supported APIs
  ├─ api_key        INT16           2       API identifier (e.g., 18 = ApiVersions)
  ├─ min_version    INT16           2       Minimum supported version
  ├─ max_version    INT16           2       Maximum supported version
  └─ TAG_BUFFER     TAGGED_FIELDS   1       Optional tagged fields (empty)
throttle_time_ms    INT32           4       Throttle time in milliseconds
TAG_BUFFER          TAGGED_FIELDS   1       Optional tagged fields (empty)
```

**Currently Supported APIs:**
- **API 18 (ApiVersions)**: versions 0-4
  - Used to query broker capabilities
  - Must be supported by all Kafka brokers

**COMPACT_ARRAY Encoding:**
In Kafka's protocol, COMPACT_ARRAY uses special length encoding:
- For an array with `n` elements, the length prefix is `n + 1`
- Empty array (0 elements) is encoded as `0`
- Array with 1 element is encoded as `2` (not `1`)

**Complete Response Structure:**
```
00 00 00 13  // message_size:      19 bytes (header 4 + body 15)
XX XX XX XX  // correlation_id:    (echoed from request)
00 00        // error_code:        0 (no error)
02           // api_keys length:   2 = 1 element (COMPACT_ARRAY encoding)
00 12        // api_key:           18 (ApiVersions)
00 00        // min_version:       0
00 04        // max_version:       4
00           // TAG_BUFFER:        empty
00 00 00 00  // throttle_time_ms:  0
00           // TAG_BUFFER:        empty
```

**Key Concepts:**
- **Message Size Calculation**: `message_size` = header size + body size
- **COMPACT_ARRAY**: Special encoding format for arrays in Kafka protocol
- **TAG_BUFFER**: Extensibility mechanism for adding optional fields
- **API Discovery**: Clients use this to know what the broker supports

**Code Highlights:**
```javascript
// COMPACT_ARRAY: 1 element encoded as 2
responseBody.writeUInt8(2, offset);

// API entry
responseBody.writeInt16BE(18, offset);  // api_key: ApiVersions
responseBody.writeInt16BE(0, offset);   // min_version
responseBody.writeInt16BE(4, offset);   // max_version

// Correct message_size calculation
const messageSize = 4 + responseBody.length; // header + body
response.writeInt32BE(messageSize, 0);
```

---

## 🔮 Future Stages (To Be Implemented)

### Stage 5: DescribeTopicPartitions API
- Implement support for querying topic metadata
- Return information about topics and their partitions

### Stage 6: Topic Management
- Support for creating topics
- Managing partitions
- Topic configuration

### Stage 7: Produce API
- Accept messages from producers
- Write events to partitions
- Acknowledge successful writes

### Stage 8: Fetch API
- Allow consumers to read messages
- Support offset-based reading
- Implement batching

### Stage 9: Message Storage
- Persist messages to disk
- Implement log segments
- Support for log compaction

### Stage 10: Replication (Advanced)
- Multi-broker support
- Leader election
- Partition replication

---

## 🏗️ Architecture

### Current Implementation

```
┌─────────────────────────────────────────────────┐
│                Kafka Broker                      │
│                (Port 9092)                       │
└─────────────────────────────────────────────────┘
                      │
                      │ TCP Connection
                      │
┌─────────────────────┴──────────────────────────┐
│                                                 │
│  Client Request (Binary)                        │
│  ┌───────────────────────────────────────┐     │
│  │ message_size                          │     │
│  │ request_api_key                       │     │
│  │ request_api_version                   │     │
│  │ correlation_id                        │     │
│  │ ... (rest of header and body)         │     │
│  └───────────────────────────────────────┘     │
│                                                 │
└─────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────┐
│         Request Parser & Handler                │
│  - Extract header fields                        │
│  - Validate API version                         │
│  - Route to appropriate handler                 │
└─────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────┐
│              API Handlers                       │
│  - ApiVersions (implemented)                    │
│  - Produce (future)                             │
│  - Fetch (future)                               │
│  - CreateTopics (future)                        │
└─────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────┐
│  Broker Response (Binary)                       │
│  ┌───────────────────────────────────────┐     │
│  │ message_size                          │     │
│  │ correlation_id                        │     │
│  │ error_code                            │     │
│  │ ... (response body)                   │     │
│  └───────────────────────────────────────┘     │
└─────────────────────────────────────────────────┘
```

---

## 📚 Kafka Protocol Concepts

### 1. Binary Protocol
Kafka uses a custom binary protocol (not HTTP, not JSON). All data is sent as raw bytes over TCP.

**Why Binary?**
- **Performance**: Faster to parse than text formats
- **Compact**: Smaller message sizes
- **Precision**: No ambiguity in data representation

### 2. Request-Response Model
Each client request gets exactly one response from the broker.

**Flow:**
1. Client opens TCP connection to broker
2. Client sends request with unique correlation_id
3. Broker processes request
4. Broker sends response with same correlation_id
5. Client matches response to original request

### 3. Big-Endian Byte Order
All multi-byte integers use big-endian (network byte order).

**Example:**
- Integer `2` as 32-bit big-endian: `00 00 00 02`
- Integer `256` as 32-bit big-endian: `00 00 01 00`

### 4. Data Types

| Type | Size | Description |
|------|------|-------------|
| INT8 | 1 byte | Signed 8-bit integer |
| INT16 | 2 bytes | Signed 16-bit integer |
| INT32 | 4 bytes | Signed 32-bit integer |
| INT64 | 8 bytes | Signed 64-bit integer |
| STRING | Variable | Length-prefixed UTF-8 string |
| COMPACT_ARRAY | Variable | Array with special length encoding |
| TAGGED_FIELDS | Variable | Optional extensibility fields |

### 5. API Versioning
Each API (e.g., Produce, Fetch, ApiVersions) supports multiple versions:
- Versions allow protocol evolution without breaking compatibility
- Clients specify which version they want to use
- Brokers respond with same version or error if unsupported

### 6. Error Codes
Standardized error codes for different scenarios:
- `0`: No error
- `35`: UNSUPPORTED_VERSION
- `3`: UNKNOWN_TOPIC_OR_PARTITION
- `100`: UNKNOWN_TOPIC_ID
- And many more...

---

## 🛠️ Technologies Used

- **Node.js**: Runtime environment
- **net module**: Built-in TCP networking
- **Buffer API**: Binary data manipulation
- **Big-endian encoding**: Network byte order

---

## 🧪 Testing

### Manual Testing
```bash
# Start the broker
./your_program.sh

# Send a test request (in another terminal)
echo -n "0000001a0012000467890abc00096b61666b612d636c69000a6b61666b612d636c6904302e3100" | xxd -r -p | nc localhost 9092 | hexdump -C
```

### Automated Testing
The CodeCrafters platform provides automated tests that verify:
- Correct message structure
- Proper byte encoding
- Accurate field values
- Protocol compliance

---

## 📖 Learning Resources

### Official Kafka Documentation
- [Kafka Protocol Guide](https://kafka.apache.org/protocol.html)
- [API Reference](https://kafka.apache.org/protocol.html#protocol_api_keys)
- [Error Codes](https://kafka.apache.org/protocol.html#protocol_error_codes)

### Key Concepts to Understand
1. **TCP/IP Networking**: How TCP connections work
2. **Binary Data**: Working with buffers and byte arrays
3. **Protocol Design**: How to design efficient binary protocols
4. **Message Brokers**: Role of message brokers in distributed systems
5. **Event Streaming**: Kafka's use cases and architecture

---

## 🎓 What We've Learned So Far

1. **TCP Server Implementation**
   - Creating servers with Node.js `net` module
   - Handling connections and data events

2. **Binary Protocol Parsing**
   - Reading integers from specific byte offsets
   - Understanding byte order (big-endian vs little-endian)
   - Working with Node.js Buffers

3. **Kafka Wire Protocol**
   - Message structure (size, header, body)
   - Request header parsing
   - Response construction

4. **API Versioning**
   - How protocols evolve over time
   - Version negotiation between client and server
   - Error handling for unsupported versions

5. **Protocol Data Types**
   - INT16, INT32 encoding
   - COMPACT_ARRAY special encoding
   - TAG_BUFFER for extensibility

6. **Error Handling**
   - Returning appropriate error codes
   - Distinguishing between different error scenarios

---

## 🚀 Next Steps

1. Implement more Kafka APIs (Produce, Fetch, etc.)
2. Add topic and partition management
3. Implement message storage (in-memory first, then persistent)
4. Add consumer group support
5. Implement log compaction
6. Add replication support

---

## 📝 Notes

- This is a learning project - not production-ready
- Focuses on core protocol understanding
- Implements simplified versions of Kafka features
- Does not include clustering, replication, or persistence (yet)

---

**Last Updated:** January 8, 2026
**Current Stage:** Stage 4 - ApiVersions API Complete

