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

### ✅ Stage 5: Advertise DescribeTopicPartitions API

**Status:** COMPLETED

**What it does:**
- Adds a second API entry to the ApiVersions response
- Advertises support for the DescribeTopicPartitions API (API key 75)
- Updates the COMPACT_ARRAY to include 2 elements instead of 1

**Purpose:**
This stage prepares the broker to handle topic metadata requests by advertising the DescribeTopicPartitions API capability.

**Supported APIs (Now Advertised):**
1. **API 18 (ApiVersions)**: versions 0-4
2. **API 75 (DescribeTopicPartitions)**: version 0

**Updated Response Structure:**
```
00 00 00 1a  // message_size:      26 bytes (header 4 + body 22)
XX XX XX XX  // correlation_id:    (echoed from request)
00 00        // error_code:        0 (no error)
03           // api_keys length:   3 = 2 elements (COMPACT_ARRAY encoding)

// First API entry
00 12        // api_key:           18 (ApiVersions)
00 00        // min_version:       0
00 04        // max_version:       4
00           // TAG_BUFFER:        empty

// Second API entry
00 4b        // api_key:           75 (DescribeTopicPartitions)
00 00        // min_version:       0
00 00        // max_version:       0
00           // TAG_BUFFER:        empty

00 00 00 00  // throttle_time_ms:  0
00           // TAG_BUFFER:        empty
```

**Key Concepts:**
- **API Discovery**: Clients use ApiVersions to discover what the broker supports
- **Multiple API Support**: Broker can advertise support for multiple APIs
- **COMPACT_ARRAY Growth**: Length prefix changes from 2 (1 element) to 3 (2 elements)
- **Version Ranges**: Different APIs can have different supported version ranges

**Code Highlights:**
```javascript
// COMPACT_ARRAY: 2 elements encoded as 3
responseBody.writeUInt8(3, offset);

// First API: ApiVersions (18)
responseBody.writeInt16BE(18, offset);  // api_key
responseBody.writeInt16BE(0, offset);   // min_version: 0
responseBody.writeInt16BE(4, offset);   // max_version: 4

// Second API: DescribeTopicPartitions (75)
responseBody.writeInt16BE(75, offset);  // api_key
responseBody.writeInt16BE(0, offset);   // min_version: 0
responseBody.writeInt16BE(0, offset);   // max_version: 0
```

**What's DescribeTopicPartitions?**
The DescribeTopicPartitions API is used to query metadata about topics and their partitions:
- Topic names and IDs
- Number of partitions per topic
- Partition leaders and replicas
- Topic configuration
- Partition states

---

### ✅ Stage 6: Implement DescribeTopicPartitions for Unknown Topics

**Status:** COMPLETED

**What it does:**
- Routes requests to appropriate API handlers based on `request_api_key`
- Parses DescribeTopicPartitions request body to extract topic names
- Responds with error code 3 (UNKNOWN_TOPIC_OR_PARTITION) for unknown topics
- Uses Response Header v1 (includes TAG_BUFFER)

**Purpose:**
This stage implements the actual request handling for the DescribeTopicPartitions API, allowing clients to query topic metadata (though all topics are currently treated as unknown).

**Request Parsing:**
The DescribeTopicPartitions request body contains:
```
Field           Type            Description
-----           ----            -----------
topics          COMPACT_ARRAY   Array of topics to query
  └─ topic_name STRING          The topic name
  └─ TAG_BUFFER TAGGED_FIELDS   Optional fields
TAG_BUFFER      TAGGED_FIELDS   Optional fields
```

**Response Header v1 vs v0:**
- **v0**: Only `correlation_id` (4 bytes)
- **v1**: `correlation_id` (4 bytes) + `TAG_BUFFER` (1 byte)

**DescribeTopicPartitions Response Structure:**
```
00 00 00 2f  // message_size:                 47 bytes
ab cd ef 12  // correlation_id:               (from request)
00           // TAG_BUFFER:                   empty (header v1)
00 00 00 00  // throttle_time_ms:             0
02           // topics array:                 1 element
00 03        // error_code:                   3 (UNKNOWN_TOPIC_OR_PARTITION)
04           // topic_name length:            3+1 (compact string)
66 6f 6f     // topic_name:                   "foo"
00 00 00 00  // topic_id:                     00000000-0000-0000-
00 00 00 00  //                               0000-000000000000
00 00 00 00  //                               (16 bytes, all zeros)
00 00 00 00  //
00           // is_internal:                  false
01           // partitions array:             0 elements (empty)
00 00 00 00  // topic_authorized_operations:  0
00           // TAG_BUFFER:                   empty
ff           // next_cursor:                  -1 (null)
00           // TAG_BUFFER:                   empty
```

**Key Fields Explained:**

1. **error_code = 3**: UNKNOWN_TOPIC_OR_PARTITION
   - Indicates the requested topic doesn't exist
   - Client will know to create the topic or handle the error

2. **topic_id = all zeros**: 
   - UUID format: 00000000-0000-0000-0000-000000000000
   - Represents a null/invalid topic ID

3. **is_internal = false**:
   - Kafka has internal topics (e.g., `__consumer_offsets`)
   - Our topics are not internal

4. **partitions = empty**:
   - COMPACT_ARRAY with 0 elements (encoded as 1)
   - No partition data since topic doesn't exist

5. **next_cursor = -1**:
   - Used for pagination in responses with many results
   - -1 means null (no more pages)

**Code Architecture Changes:**
- **Refactored to handler pattern**: Separate functions for each API
- `handleApiVersions()`: Handles API key 18
- `handleDescribeTopicPartitions()`: Handles API key 75
- Main connection handler routes requests based on `request_api_key`

**Parsing COMPACT_STRING:**
```javascript
// COMPACT_STRING format: length_prefix (n+1) + bytes
const nameLength = data.readUInt8(offset) - 1;
offset += 1;
const name = data.toString('utf8', offset, offset + nameLength);
```

**Parsing COMPACT_ARRAY:**
```javascript
// COMPACT_ARRAY format: length_prefix (n+1) + elements
const arrayLength = data.readUInt8(offset) - 1;
offset += 1;
// Now read 'arrayLength' elements
```

**Key Concepts:**
- **API Routing**: Different request_api_key values route to different handlers
- **Request Body Parsing**: Extracting structured data from binary protocol
- **Response Header Versioning**: Different response header versions for different APIs
- **Error Codes**: Returning appropriate errors for unknown resources
- **UUID Encoding**: 16-byte binary UUID format
- **COMPACT_STRING**: Variable-length strings with length prefix
- **NULLABLE Fields**: Using special values (-1, all zeros) to represent null

**Code Highlights:**
```javascript
// API routing based on request_api_key
if (requestApiKey === 18) {
  handleApiVersions(connection, requestApiVersion, correlationId);
} else if (requestApiKey === 75) {
  handleDescribeTopicPartitions(connection, data, correlationId);
}

// Parse COMPACT_STRING for topic name
const topicNameLength = data.readUInt8(offset) - 1;
offset += 1;
const topicName = data.toString('utf8', offset, offset + topicNameLength);

// Write UUID (all zeros for unknown topic)
responseBody.fill(0, bodyOffset, bodyOffset + 16);

// Response Header v1 includes TAG_BUFFER
response.writeUInt8(0, 8); // TAG_BUFFER after correlation_id
```

---

### ✅ Stage 7: Handle Known Topics with Metadata Parsing

**Status:** COMPLETED

**What it does:**
- Reads and parses the Kafka cluster metadata log file
- Extracts topic names, UUIDs, and partition information
- Returns proper metadata for known topics (error_code=0, actual UUID, partition data)
- Still returns error for unknown topics

**Purpose:**
This stage implements full topic metadata support by parsing Kafka's internal `__cluster_metadata` log file, allowing the broker to respond with actual topic and partition information.

**Cluster Metadata Log:**
- **Location**: `/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log`
- **Format**: Kafka binary log format with record batches
- **Contains**: Topic records, partition records, and cluster configuration

**Binary Log Format Structure:**
```
Log File:
├─ Record Entry 1:
│  ├─ baseOffset (8 bytes, INT64)
│  ├─ batchLength (4 bytes, INT32)
│  └─ Record Batch:
│     ├─ Header (61 bytes):
│     │  ├─ partitionLeaderEpoch (4 bytes)
│     │  ├─ magic (1 byte) = 2
│     │  ├─ crc (4 bytes)
│     │  ├─ attributes (2 bytes)
│     │  ├─ lastOffsetDelta (4 bytes)
│     │  ├─ timestamps (16 bytes)
│     │  ├─ producer info (14 bytes)
│     │  ├─ baseSequence (4 bytes)
│     │  └─ recordsCount (4 bytes)
│     └─ Records (variable):
│        ├─ Record 1 (uses varints):
│        │  ├─ length (varint)
│        │  ├─ attributes (1 byte)
│        │  ├─ timestampDelta (varint)
│        │  ├─ offsetDelta (varint)
│        │  ├─ keyLength (varint)
│        │  ├─ key (bytes):
│        │  │  ├─ frameVersion (1 byte)
│        │  │  ├─ recordType (1 byte):
│        │  │  │  ├─ 2 = TopicRecord
│        │  │  │  └─ 3 = PartitionRecord
│        │  │  └─ type-specific data
│        │  ├─ valueLength (varint)
│        │  └─ value (bytes)
│        └─ Record 2, 3, ...
└─ Record Entry 2, 3, ...
```

**Record Types:**

1. **TopicRecord (type 2):**
   - Contains: topic name, topic UUID
   - Used to register new topics in the cluster

2. **PartitionRecord (type 3):**
   - Key: partition ID, topic UUID
   - Value: replicas, ISR (in-sync replicas), leader

**Varint Encoding:**
- Variable-length integer encoding (1-5 bytes)
- Uses zigzag encoding for signed integers
- MSB (bit 7) = 1 means more bytes follow
- LSB 7 bits contain data

**Response for Known Topics:**
```
00 00 00 4a  // message_size:                 74 bytes
ab cd ef 12  // correlation_id:               (from request)
00           // TAG_BUFFER:                   empty (header v1)
00 00 00 00  // throttle_time_ms:             0
02           // topics array:                 1 element
00 00        // error_code:                   0 (NO ERROR!) ✓
04           // name length:                  3 (compact string)
66 6f 6f     // name:                         "foo"
a1 b2 c3 d4  // topic_id:                     (ACTUAL UUID!) ✓
e5 f6 a7 b8  //                               (16 bytes)
c9 d0 e1 f2  //
a3 b4 c5 d6  //
00           // is_internal:                  false
02           // partitions array:             1 element ✓
00 00        //   error_code:                 0
00 00 00 00  //   partition_index:            0
00 00 00 01  //   leader_id:                  1
00 00 00 00  //   leader_epoch:               0
02           //   replica_nodes:              1 element
01           //     broker 1
02           //   isr_nodes:                  1 element
01           //     broker 1
01           //   eligible_leader_replicas:   0 elements
01           //   last_known_elr:             0 elements
01           //   offline_replicas:           0 elements
00           //   TAG_BUFFER:                 empty
00 00 00 00  // topic_authorized_operations:  0
00           // TAG_BUFFER:                   empty
ff           // next_cursor:                  -1 (null)
00           // TAG_BUFFER:                   empty
```

**Key Implementation Details:**

1. **Metadata Storage:**
   ```javascript
   const topicsMetadata = new Map();
   // Structure: Map<topicName, { name, id, partitions[] }>
   ```

2. **Varint Reading:**
   ```javascript
   function readVarint(buffer, offset) {
     // Reads variable-length integers
     // Handles zigzag encoding for signed values
     // Returns [value, bytesRead]
   }
   ```

3. **Dynamic Response Building:**
   ```javascript
   function buildDescribeTopicPartitionsBody(topicName, topicNameBytes, topicMetadata) {
     // Builds response based on whether topic exists
     // For known topics: includes actual UUID and partition data
     // For unknown topics: error_code=3, zero UUID, no partitions
   }
   ```

**Partition Information Fields:**
- **partition_index**: Partition ID (0, 1, 2, ...)
- **leader_id**: Broker ID that leads this partition
- **leader_epoch**: Leadership generation number
- **replica_nodes**: All brokers storing this partition
- **isr_nodes**: In-sync replicas (up-to-date copies)
- **eligible_leader_replicas**: Brokers eligible to become leader
- **last_known_elr**: Last known eligible leaders
- **offline_replicas**: Unavailable replicas

**Key Concepts:**
- **Kafka Log Format**: Binary format for storing records on disk
- **Record Batches**: Group of related records for efficiency
- **Varints**: Space-efficient integer encoding
- **Magic Byte**: Version indicator (v2 = flexible records)
- **CRC**: Checksum for data integrity
- **Topic UUID**: Unique identifier for topics (separate from name)
- **Replicas**: Copies of partition data across brokers
- **ISR**: In-sync replicas that are caught up with leader
- **Leader**: Primary broker handling reads/writes for a partition

**Code Highlights:**
```javascript
// Parse cluster metadata on startup
parseClusterMetadata();

// Read and parse binary log file
const logData = fs.readFileSync(logPath);
const baseOffset = logData.readBigInt64BE(offset);
const batchLength = logData.readInt32BE(offset);

// Parse varint-encoded records
const [length, lengthBytes] = readVarint(data, offset);

// Extract topic UUID from TopicRecord
const topicId = value.slice(offset, offset + 16);
topicsMetadata.set(topicName, { name, id: topicId, partitions: [] });

// Return actual metadata for known topics
if (topicExists) {
  responseBody.writeInt16BE(0, offset); // No error!
  topicMetadata.id.copy(responseBody, offset); // Actual UUID!
  // Include partition data...
}
```

**Challenges Solved:**
1. **Binary Format Parsing**: Decoding Kafka's custom log format
2. **Varint Handling**: Reading variable-length integers
3. **Record Type Identification**: Distinguishing TopicRecords from PartitionRecords
4. **UUID Management**: Matching partitions to topics via UUID
5. **Dynamic Response Building**: Different responses for known vs unknown topics

---

### ✅ Stage 8: Handle Multiple Topics

**Status:** COMPLETED

**What it does:**
- Parses multiple topic names from a single DescribeTopicPartitions request
- Looks up metadata for each requested topic
- **Sorts topics alphabetically by name** (required by protocol)
- Returns metadata for all topics in a single response

**Purpose:**
Allows clients to query multiple topics efficiently in one request instead of making separate requests for each topic.

**Key Implementation:**
```javascript
// Parse ALL topics from request
const requestedTopics = [];
for (let i = 0; i < topicsArrayLength; i++) {
  const topicName = data.toString('utf8', offset, offset + topicNameLength);
  requestedTopics.push(topicName);
}

// Look up metadata for each
const topicsWithMetadata = [];
for (const topicName of requestedTopics) {
  let topicMetadata = topicsMetadata.get(topicName) || findTopicInLog(topicName);
  topicsWithMetadata.push({ name: topicName, metadata: topicMetadata });
}

// Sort alphabetically (REQUIRED!)
topicsWithMetadata.sort((a, b) => a.name.localeCompare(b.name));

// Build response with all topics
const responseBody = buildDescribeTopicPartitionsBodyMultiple(topicsWithMetadata);
```

**Response Structure:**
```
throttle_time_ms: 0
topics array length: N+1 (COMPACT_ARRAY)
  Topic 1 (alphabetically sorted):
    error_code: 0 or 3
    topic_name: "apple"
    topic_id: <UUID>
    partitions: [...]
  Topic 2:
    topic_name: "zebra"
    ...
next_cursor: -1
```

**Key Concepts:**
- **Alphabetical Sorting**: Required by Kafka protocol for consistent ordering
- **Batch Queries**: More efficient than individual requests
- **Mixed Results**: Can include both known and unknown topics in response
- **Independent Lookup**: Each topic is looked up independently

---

### ✅ Stage 9: Advertise Fetch API

**Status:** COMPLETED

**What it does:**
- Adds Fetch API (API key 1) to the ApiVersions response
- Advertises support for Fetch versions 0-16

**Purpose:**
The Fetch API is how consumers read messages from Kafka topics. By advertising it in ApiVersions, clients know the broker supports message consumption.

**Updated ApiVersions Response:**
```
00 00 00 21  // message_size:      33 bytes (header 4 + body 29)
XX XX XX XX  // correlation_id:    (echoed from request)
00 00        // error_code:        0 (no error)
04           // api_keys length:   4 = 3 elements (COMPACT_ARRAY encoding)

// API entry 1: Fetch
00 01        // api_key:           1 (Fetch)
00 00        // min_version:       0
00 10        // max_version:       16
00           // TAG_BUFFER:        empty

// API entry 2: ApiVersions
00 12        // api_key:           18 (ApiVersions)
00 00        // min_version:       0
00 04        // max_version:       4
00           // TAG_BUFFER:        empty

// API entry 3: DescribeTopicPartitions
00 4b        // api_key:           75 (DescribeTopicPartitions)
00 00        // min_version:       0
00 00        // max_version:       0
00           // TAG_BUFFER:        empty

00 00 00 00  // throttle_time_ms:  0
00           // TAG_BUFFER:        empty
```

**Supported APIs (Now Advertised):**
1. **API 1 (Fetch)**: versions 0-16
   - Used by consumers to read messages
   - Supports various options (offset, partition, max bytes, etc.)
2. **API 18 (ApiVersions)**: versions 0-4
3. **API 75 (DescribeTopicPartitions)**: version 0

**What's the Fetch API?**
The Fetch API allows Kafka consumers to:
- Read messages from specific partitions
- Specify starting offset (where to begin reading)
- Control max bytes to fetch
- Read from multiple partitions in one request
- Get records in batches for efficiency

**Key Concepts:**
- **Consumer Operations**: Fetch is the primary API for consuming messages
- **Version 16**: Modern version with many features (isolation level, session ID, etc.)
- **API Discovery**: Clients check ApiVersions before using Fetch

**Code Highlights:**
```javascript
// Now advertise 3 APIs instead of 2
responseBody.writeUInt8(4, offset); // 3 elements + 1

// Fetch API (1): versions 0-16
responseBody.writeInt16BE(1, offset);   // api_key
responseBody.writeInt16BE(0, offset);   // min_version
responseBody.writeInt16BE(16, offset);  // max_version
```

---

### ✅ Stage 10: Implement Fetch API (Empty Response)

**Status:** COMPLETED

**What it does:**
- Handles Fetch requests (API key 1)
- Returns a valid Fetch v16 response for empty topic array
- Uses Response Header v1 (with TAG_BUFFER)

**Purpose:**
First step in implementing the Fetch API. Handles the basic response structure before adding actual message fetching logic.

**Fetch Response v16 Structure:**
```
00 00 00 11  // message_size:      17 bytes (header 5 + body 12)
XX XX XX XX  // correlation_id:    (echoed from request)
00           // TAG_BUFFER:        empty (response header v1)
00 00 00 00  // throttle_time_ms:  0
00 00        // error_code:        0 (NO_ERROR)
00 00 00 00  // session_id:        0
01           // responses:         1 = 0 elements (COMPACT_ARRAY, empty)
00           // TAG_BUFFER:        empty
```

**Fetch Response Fields Explained:**

1. **throttle_time_ms** (INT32): How long client should wait before next request
   - Set to 0 (no throttling)

2. **error_code** (INT16): Top-level error code
   - 0 = NO_ERROR
   - Other codes: OFFSET_OUT_OF_RANGE, UNKNOWN_TOPIC_OR_PARTITION, etc.

3. **session_id** (INT32): Fetch session ID for incremental fetching
   - Used in KIP-227 for efficient fetching
   - 0 means no session

4. **responses** (COMPACT_ARRAY): Array of per-topic responses
   - Empty for this stage (no topics requested)
   - Will contain topic data in future stages

**Key Concepts:**
- **Fetch API**: Primary API for consuming messages from Kafka
- **Response Header v1**: Same as DescribeTopicPartitions (includes TAG_BUFFER)
- **Empty Response**: Valid response when no topics are requested
- **Session-based Fetching**: Allows stateful fetching for efficiency

**Code Highlights:**
```javascript
function handleFetch(connection, requestApiVersion, correlationId, data) {
  // Build minimal Fetch v16 response
  const responseBody = Buffer.alloc(12);
  
  // throttle_time_ms: 0
  responseBody.writeInt32BE(0, offset);
  
  // error_code: 0  
  responseBody.writeInt16BE(0, offset);
  
  // session_id: 0
  responseBody.writeInt32BE(0, offset);
  
  // responses: empty array
  responseBody.writeUInt8(1, offset); // 0 elements + 1
  
  // TAG_BUFFER: empty
  responseBody.writeUInt8(0, offset);
}
```

**What's Next:**
Future stages will expand this to:
- Parse topic and partition requests
- Read actual messages from log files
- Return batches of records
- Handle offsets and max bytes limits

---

### ✅ Stage 11: Fetch API for Unknown Topic

**Status:** COMPLETED

**What it does:**
- Parses Fetch v16 request to extract topic_id (UUID)
- Returns error response for unknown topics
- Error code 100 (UNKNOWN_TOPIC_ID) at partition level

**Purpose:**
Handles Fetch requests for topics that don't exist, allowing clients to detect and handle missing topics appropriately.

**Fetch Request v16 Parsing:**
```javascript
// Skip standard header fields
// Skip: max_wait_ms, min_bytes, max_bytes, isolation_level
// Skip: session_id, session_epoch

// Parse topics array
const topicsArrayLength = data.readUInt8(offset) - 1;
for (let i = 0; i < topicsArrayLength; i++) {
  const topicId = Buffer.alloc(16);
  data.copy(topicId, 0, offset, offset + 16); // Read UUID
  requestedTopics.push({ topicId });
}
```

**Fetch Response v16 for Unknown Topic:**
```
00 00 00 XX  // message_size:      varies
XX XX XX XX  // correlation_id:    (from request)
00           // TAG_BUFFER:        empty (header v1)
00 00 00 00  // throttle_time_ms:  0
00 00        // error_code:        0 (NO_ERROR at top level)
00 00 00 00  // session_id:        0
02           // responses:         2 = 1 topic (COMPACT_ARRAY)

// Topic response:
XX XX XX XX  // topic_id:          (UUID, 16 bytes)
XX XX XX XX
XX XX XX XX
XX XX XX XX
02           // partitions:        2 = 1 partition (COMPACT_ARRAY)

// Partition response:
00 00 00 00  // partition_index:   0
00 64        // error_code:        100 (UNKNOWN_TOPIC_ID) ✓
00 00 00 00  // high_watermark:    0 (INT64)
00 00 00 00
00 00 00 00  // last_stable_offset: 0 (INT64)
00 00 00 00
00 00 00 00  // log_start_offset:  0 (INT64)
00 00 00 00
01           // aborted_transactions: empty
FF FF FF FF  // preferred_read_replica: -1 (no preference)
00           // records:           null (COMPACT_BYTES)
00           // TAG_BUFFER:        empty
00           // TAG_BUFFER (topic): empty

00           // TAG_BUFFER (response): empty
```

**Fetch Response Fields Explained:**

1. **throttle_time_ms** (INT32): 0 (no throttling)

2. **error_code** (INT16): 0 at top level
   - Top-level errors apply to the entire request
   - Per-topic/partition errors are in the responses

3. **session_id** (INT32): 0 (no session)

4. **responses** (COMPACT_ARRAY): Per-topic responses
   - **topic_id** (UUID): 16-byte topic identifier
   - **partitions** (COMPACT_ARRAY): Per-partition responses
     - **partition_index** (INT32): Partition number (0)
     - **error_code** (INT16): **100 = UNKNOWN_TOPIC_ID**
     - **high_watermark** (INT64): Highest offset (0 for unknown)
     - **last_stable_offset** (INT64): Last committed offset (0)
     - **log_start_offset** (INT64): Earliest offset (0)
     - **aborted_transactions** (COMPACT_ARRAY): Empty for unknown topic
     - **preferred_read_replica** (INT32): -1 (no preference)
     - **records** (COMPACT_BYTES): null/empty for unknown topic
     - **TAG_BUFFER**: Empty

**Error Code 100 - UNKNOWN_TOPIC_ID:**
- Indicates the requested topic UUID doesn't exist
- Client should handle by:
  - Creating the topic (if allowed)
  - Retrying with different topic
  - Reporting error to user

**Key Concepts:**
- **UUID-based Topic Identification**: Fetch uses UUIDs, not names
- **Per-Partition Errors**: Errors can be at request, topic, or partition level
- **Watermarks**: Track message offsets in partitions
- **Empty Records**: No data returned for unknown topics
- **INT64 Fields**: Large offsets for high-throughput topics

**Code Highlights:**
```javascript
// Parse topic_id (UUID - 16 bytes)
const topicId = Buffer.alloc(16);
data.copy(topicId, 0, offset, offset + 16);

// Return error for unknown topic
responseBody.writeInt16BE(100, offset); // UNKNOWN_TOPIC_ID

// Write INT64 values
responseBody.writeBigInt64BE(BigInt(0), offset);
```

---

### ✅ Stage 12: Fetch API for Known Topic (No Messages)

**Status:** COMPLETED

**What it does:**
- Looks up topics by UUID in cluster metadata
- Distinguishes between unknown topics and known topics without messages
- Returns error_code 0 for known topics (even if empty)
- Returns error_code 100 only for truly unknown topics

**Purpose:**
Properly handles topics that exist but have no messages yet, allowing consumers to wait for messages without error.

**Topic Lookup by UUID:**
```javascript
function findTopicByUUID(topicUUID) {
  // Check cache first
  for (const [name, metadata] of topicsMetadata.entries()) {
    if (metadata && metadata.id && metadata.id.equals(topicUUID)) {
      return metadata;
    }
  }
  
  // Search log file for UUID
  const uuidIndex = logData.indexOf(topicUUID);
  
  // Work backwards to find topic name before UUID
  // Then look up full metadata by name
  return findTopicInLog(topicName);
}
```

**Response Differences:**

**Unknown Topic (error 100):**
```
partition_index: 0
error_code: 100 (UNKNOWN_TOPIC_ID)
records: null
```

**Known Topic, No Messages (error 0):**
```
partition_index: 0
error_code: 0 (NO_ERROR) ✓
high_watermark: 0
last_stable_offset: 0
log_start_offset: 0
records: null (no messages yet)
```

**Key Difference:**
- **Unknown topic**: error_code = 100, consumer knows topic doesn't exist
- **Known topic (empty)**: error_code = 0, consumer knows topic exists but has no messages

**Consumer Behavior:**
- **Error 100**: Consumer should either create topic or report error
- **Error 0**: Consumer can wait/poll for messages to arrive

**Implementation:**
```javascript
// Look up metadata for each topic by UUID
const topicsWithMetadata = requestedTopics.map(topic => {
  const metadata = findTopicByUUID(topic.topicId);
  const exists = metadata !== null;
  
  return {
    topicId: topic.topicId,
    metadata: metadata,
    exists: exists
  };
});

// Set appropriate error code
const errorCode = topic.exists ? 0 : 100;
responseBody.writeInt16BE(errorCode, offset);
```

**Key Concepts:**
- **Topic Existence vs. Message Availability**: Different concepts
- **UUID Matching**: Topics identified by UUID in Fetch, by name in DescribeTopicPartitions
- **Empty Records**: Valid state for new/empty topics
- **Consumer Polling**: Consumers repeatedly fetch from empty topics waiting for messages

---

## 🔮 Future Stages (To Be Implemented)

### Stage 13: Fetch with Actual Messages
- Read messages from partition log files
- Return record batches
- Handle offsets correctly
- Parse and encode Kafka record format

### Stage 14: Topic Management  
- Support for creating topics
- Managing partitions
- Topic configuration

### Stage 15: Produce API
- Accept messages from producers
- Write events to partitions
- Acknowledge successful writes

### Stage 16: Message Storage
- Persist messages to disk
- Implement log segments
- Support for log compaction

### Stage 17: Replication (Advanced)
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
│  │ request_api_key     (18 or 75)        │     │
│  │ request_api_version                   │     │
│  │ correlation_id                        │     │
│  │ client_id, TAG_BUFFER (header)        │     │
│  │ ... (request body)                    │     │
│  └───────────────────────────────────────┘     │
│                                                 │
└─────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────┐
│         Request Parser & Router                 │
│  - Extract header fields                        │
│  - Parse request_api_key                        │
│  - Route based on API key                       │
└─────────────────────────────────────────────────┘
                      │
            ┌─────────┴─────────┐
            ▼                   ▼
┌─────────────────┐   ┌──────────────────────────┐
│ API Key 18      │   │ API Key 75               │
│ ApiVersions     │   │ DescribeTopicPartitions  │
│ Handler         │   │ Handler                  │
│                 │   │                          │
│ - Validate ver  │   │ - Parse request body     │
│ - Return list   │   │ - Extract topic names    │
│   of supported  │   │ - Return topic metadata  │
│   APIs          │   │   (or error if unknown)  │
└─────────────────┘   └──────────────────────────┘
            │                   │
            └─────────┬─────────┘
                      ▼
┌─────────────────────────────────────────────────┐
│  Broker Response (Binary)                       │
│  ┌───────────────────────────────────────┐     │
│  │ message_size                          │     │
│  │ correlation_id                        │     │
│  │ TAG_BUFFER (header v1 for API 75)     │     │
│  │ error_code or response data           │     │
│  │ ... (response body)                   │     │
│  └───────────────────────────────────────┘     │
└─────────────────────────────────────────────────┘
```

**Supported APIs:**
- **API 18 (ApiVersions)**: Returns list of supported APIs
- **API 75 (DescribeTopicPartitions)**: Returns topic metadata (currently unknown topics only)

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
   - Parsing variable-length strings (COMPACT_STRING)
   - Parsing arrays (COMPACT_ARRAY)

3. **Kafka Wire Protocol**
   - Message structure (size, header, body)
   - Request header parsing (v0, v1, v2)
   - Response construction with different header versions
   - Request body parsing

4. **API Versioning**
   - How protocols evolve over time
   - Version negotiation between client and server
   - Error handling for unsupported versions
   - Different APIs can have different version ranges

5. **Protocol Data Types**
   - INT8, INT16, INT32 encoding
   - COMPACT_ARRAY special encoding (n+1 for n elements)
   - COMPACT_STRING encoding (length+1 prefix + bytes)
   - TAG_BUFFER for extensibility
   - UUID format (16 bytes)
   - NULLABLE types (using special values like -1)
   - BOOLEAN encoding

6. **Error Handling**
   - Returning appropriate error codes
   - Distinguishing between different error scenarios
   - Error code 3: UNKNOWN_TOPIC_OR_PARTITION
   - Error code 35: UNSUPPORTED_VERSION

7. **API Routing and Architecture**
   - Routing requests based on `request_api_key`
   - Separating handlers for different APIs
   - Code organization with handler functions
   - Different response header versions for different APIs

8. **Kafka Concepts**
   - Topics and partitions (detailed understanding)
   - Topic metadata (name, ID, partitions)
   - Internal vs. external topics
   - Unknown topic handling
   - Topic UUIDs and identification
   - Cluster metadata storage
   - Partition replicas and ISR (in-sync replicas)
   - Leader election and epochs
   - Broker IDs and leadership

9. **Binary File Parsing**
   - Reading and parsing Kafka log files
   - Kafka binary log format
   - Record batches and individual records
   - Varint encoding/decoding (variable-length integers)
   - Zigzag encoding for signed integers
   - Magic bytes and format versioning
   - CRC checksums for data integrity

10. **Advanced Data Structures**
   - In-memory metadata storage (Map)
   - Topic and partition relationships
   - UUID handling (16-byte identifiers)
   - Dynamic response building based on metadata state

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
**Current Stage:** Stage 12 - Fetch API for Known Topic (No Messages) Complete

