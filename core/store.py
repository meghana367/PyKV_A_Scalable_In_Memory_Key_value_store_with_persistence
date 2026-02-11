from .node import Node
import asyncio 
import time
from persistence.aof_logger import AOFLogger

class LRUCache:
    def __init__(self, capacity: int = 5):
        self.capacity = capacity
        self.db = {}       # The actual database (Stores ALL keys)
        self.cache = {}    # The "Hot" Cache (Points to nodes in the Linked List)
        self.expiries = {} # Stores expiration timestamps: {key: timestamp}
        
        # Linked List for LRU (The Hot Zone)
        self.head = Node()
        self.tail = Node()
        self.head.next = self.tail
        self.tail.prev = self.head
        
        self.lock = asyncio.Lock()
        self.aof = AOFLogger(filepath="persistence/appendonly.aof")

        self.stats = {
            "cache_hits": 0,    
            "cache_misses": 0,  
            "total_commands": 0,
            "start_time": time.time()
        }
        self._replay_aof()

    def _replay_aof(self):
        """Rebuilds the full database state and TTLs from disk on startup."""
        logs = self.aof.read_logs()
        for line in logs:
            parts = line.split()
            if not parts: continue
            cmd = parts[0].upper()
            
            if cmd == "SET" and len(parts) >= 3:
                key, value = parts[1], parts[2]
                self.db[key] = value
                
                # Rebuild TTL if EX was in the log
                parts_upper = [p.upper() for p in parts]
                if "EX" in parts_upper:
                    try:
                        ex_idx = parts_upper.index("EX")
                        ttl = int(parts[ex_idx + 1])
                        self.expiries[key] = time.time() + ttl
                    except (ValueError, IndexError):
                        pass
                        
            elif cmd == "DEL" and len(parts) >= 2:
                self.db.pop(parts[1], None)
                self.expiries.pop(parts[1], None)

    def get_all_valid_data(self):
        """Returns the entire database for compaction."""
        return self.db

    # --- LRU Linked List Helpers ---
    def _add_node(self, node):
        node.prev = self.head
        node.next = self.head.next
        self.head.next.prev = node
        self.head.next = node

    def _remove_node(self, node):
        p, n = node.prev, node.next
        p.next, n.prev = n, p

    # --- Core Logic ---
    async def get(self, key: str):
        async with self.lock:
            self.stats["total_commands"] += 1
            
            # 1. Check if key exists in DB
            if key not in self.db:
                return None
            
            # 2. Lazy TTL Check (Crucial for "nil" return on expired keys)
            if key in self.expiries and time.time() > self.expiries[key]:
                # Release lock to call delete safely or handle deletion internally
                # To keep it simple and safe within the same lock:
                self.db.pop(key, None)
                self.expiries.pop(key, None)
                if key in self.cache:
                    node = self.cache.pop(key)
                    self._remove_node(node)
                await self.aof.log_command("DEL", key)
                return None

            # 3. Cache Hit Logic
            if key in self.cache:
                self.stats["cache_hits"] += 1
                self._remove_node(self.cache[key])
                self._add_node(self.cache[key])
                return self.cache[key].value
            
            # 4. Cache Miss (Cold Hit - in DB but not Cache)
            self.stats["cache_misses"] += 1
            value = self.db[key]
            
            # Promote to Hot Cache
            new_node = Node(key, value)
            self.cache[key] = new_node
            self._add_node(new_node)
            
            # Evict from Cache only if over capacity
            if len(self.cache) > self.capacity:
                lru_node = self.tail.prev
                self._remove_node(lru_node)
                del self.cache[lru_node.key]
            
            return value

    async def set(self, key: str, value, ttl: int = None):
        async with self.lock:
            self.stats["total_commands"] += 1
            self.db[key] = str(value)
            
            if ttl:
                self.expiries[key] = time.time() + ttl
            else:
                self.expiries.pop(key, None)

            # Update Hot Cache if present
            if key in self.cache:
                self.cache[key].value = str(value)
                self._remove_node(self.cache[key])
                self._add_node(self.cache[key])
            
            # Log to AOF with TTL if provided
            if ttl:
                await self.aof.log_command("SET", key, value, "EX", ttl)
            else:
                await self.aof.log_command("SET", key, value)
            return True

    async def delete(self, key: str):
        async with self.lock:
            in_db = key in self.db
            self.db.pop(key, None)
            self.expiries.pop(key, None)
            if key in self.cache:
                node = self.cache.pop(key)
                self._remove_node(node)
            
            if in_db:
                await self.aof.log_command("DEL", key)
                return True
            return False

    async def increment(self, key: str):
        # We call get() which already handles Lazy TTL
        val = await self.get(key)
        try:
            new_val = int(val or 0) + 1
            await self.set(key, new_val)
            return new_val
        except ValueError:
            return "ERROR: Value is not an integer"

    async def cleanup_expired_keys(self):
        """Background task to sweep the database for expired TTLs."""
        while True:
            await asyncio.sleep(5)
            async with self.lock:
                now = time.time()
                keys_to_del = [k for k, exp in self.expiries.items() if now > exp]
                
                for key in keys_to_del:
                    self.db.pop(key, None)
                    self.expiries.pop(key, None)
                    if key in self.cache:
                        node = self.cache.pop(key)
                        self._remove_node(node)
                    await self.aof.log_command("DEL", key)
                    print(f"[TTL] Expired and removed from file: {key}")

    def get_info(self):
        return (
            f"keys_in_db: {len(self.db)}\n"
            f"keys_in_hot_cache: {len(self.cache)}\n"
            f"hits: {self.stats['cache_hits']}\n"
            f"misses: {self.stats['cache_misses']}"
        )