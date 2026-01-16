# PyKV_A_Scalable_In_Memory_Key_value_store_with_persistence
PyKV is a custom-built, asynchronous key-Value database implemented in Python. It features a dual-layer storage architecture: a Persistent Database for durability and an LRU (Least Recently Used) Hot Cache for high-speed access.
## 🚀 Key Features
**Memory-First Architecture:** High-speed operations using in-memory data structures.

**Dual-Layer Storage:**

**The DB:** Stores the full dataset (Permanent).

**The LRU Cache:** Maintains "Hot" data with a configurable capacity (Performance).

**Persistence (AOF):** Append-Only Logging ensures no data loss during crashes, with a built-in Compaction Housekeeper to keep logs small.

**Leader-Follower Replication:** Real-time data synchronization across multiple nodes for high availability.

**TTL (Time-To-Live):** Support for self-expiring keys.

**Asynchronous Engine:** Built on asyncio to handle concurrent client connections efficiently.
## 🏗️ Architecture
LRU Cache LogicPyKV uses a Doubly Linked List combined with a Hash Map to achieve $O(1)$ time complexity for both hits and evictions. When the cache capacity is reached, "Cold" items are evicted from memory but remain available in the persistent database layer.
## Replication Flow
Leader receives a write command (SET, DEL, INCR).

Leader updates local memory and writes to the AOF.

Leader broadcasts the command to all connected Followers.

Followers apply the change to their own local state and AOF.
## 🛠️ Installation & Setup
**Prerequisites**

Python 3.10+

aiofiles library for non-blocking I/O.

```bash

pip install aiofiles
```
**1. Start the Leader Server**
```
# From the project root
python -m server.main
```
**2. Start a Follower (Optional)**
```
python server/follower.py
```
**3. Run the Client**
```
python client/client.py
```
## ⌨️ Supported Commands

| Command | Usage | Description |
| :--- | :--- | :--- |
| **SET** | `SET <key> <value> [EX <seconds>]` | Stores a key-value pair with optional expiry. |
| **GET** | `GET <key>` | Retrieves the value of a key. |
| **DEL** | `DEL <key>` | Removes a key from the database. |
| **INCR** | `INCR <key>` | Increments a numeric value by 1. |
| **INFO** | `INFO` | Returns stats (Hits, Misses, DB size, etc). |

## 📊 Monitoring the Performance
Use the INFO command to see the separation between your Database and your LRU Hot Cache:

**Cache Hit:** Key was found in the fast Linked List.

**Cache Miss:** Key was found in the Database but had to be promoted back to the Hot Cache.

## 📁 Project Structure
`core/`: Contains the LRU logic and memory management.

`persistence/`: Handles AOF logging and log compaction.

`server/`: Contains the Leader and Follower network logic.

`client/`: A command-line interface for interacting with PyKV.
