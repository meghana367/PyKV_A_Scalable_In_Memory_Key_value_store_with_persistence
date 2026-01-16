import os
import aiofiles

class AOFLogger:
    def __init__(self, filepath="persistence/appendonly.aof"):
        self.filepath = filepath
        # Ensure the persistence directory exists
        os.makedirs(os.path.dirname(self.filepath), exist_ok=True)

    async def log_command(self, *args):
        """
        Accepts multiple arguments (e.g., "SET", "key", "val"), 
        joins them into a string, and appends to the log file.
        """
        command_str = " ".join(map(str, args))
        
        async with aiofiles.open(self.filepath, mode='a') as f:
            await f.write(f"{command_str}\n")
            # Ensure the data is physically written to disk
            await f.flush()

    async def trigger_compaction(self, data_store):
        """
        Rewrites the AOF file using the 'db' dictionary (all data).
        This keeps the file small while ensuring no data is lost,
        regardless of the LRU cache capacity.
        """
        temp_filepath = f"{self.filepath}.tmp"
        
        # Pull the full dataset (db) from the store, not just the LRU cache
        current_data = data_store.get_all_valid_data() 

        try:
            # Write only the final 'SET' state of every key in the database
            async with aiofiles.open(temp_filepath, mode='w') as f:
                for key, value in current_data.items():
                    await f.write(f"SET {key} {value}\n")
                await f.flush()
            
            # Atomic swap to replace the old log with the new compacted one
            os.replace(temp_filepath, self.filepath)
            print(f"[AOF] Compaction successful. Optimized {len(current_data)} keys.")

        except Exception as e:
            print(f"[AOF] Compaction failed: {e}")
            if os.path.exists(temp_filepath):
                os.remove(temp_filepath)

    def read_logs(self):
        """
        Synchronous read used during server startup to rebuild the 
        full database state in memory.
        """
        if not os.path.exists(self.filepath):
            print(f"[AOF] No existing log file found at {self.filepath}")
            return []
        
        try:
            with open(self.filepath, "r") as f:
                lines = [line.strip() for line in f.readlines() if line.strip()]
                print(f"[AOF] Recovery: Loaded {len(lines)} commands from disk.")
                return lines
        except Exception as e:
            print(f"[AOF] Critical: Error reading logs: {e}")
            return []