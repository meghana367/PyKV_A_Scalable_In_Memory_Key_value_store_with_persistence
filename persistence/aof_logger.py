import os
import aiofiles
import time

class AOFLogger:
    def __init__(self, filepath="persistence/appendonly.aof"):
        self.filepath = filepath
        # Ensure the persistence directory exists
        os.makedirs(os.path.dirname(self.filepath), exist_ok=True)

    async def log_command(self, *args):
        """
        Accepts multiple arguments, joins them into a string, 
        and appends to the log file asynchronously.
        """
        command_str = " ".join(map(str, args))
        
        async with aiofiles.open(self.filepath, mode='a') as f:
            await f.write(f"{command_str}\n")
            # Ensure the data is physically pushed to the disk
            await f.flush()

    async def trigger_compaction(self, data_store):
        """
        Rewrites the AOF file using the current valid database state.
        Only saves keys that haven't expired and preserves remaining TTL.
        """
        temp_filepath = f"{self.filepath}.tmp"
        
        # Access the full database and the expiries dictionary from store.py
        current_data = data_store.db
        expiries = data_store.expiries
        now = time.time()

        try:
            async with aiofiles.open(temp_filepath, mode='w') as f:
                for key, value in current_data.items():
                    # Check if the key has an associated expiry
                    if key in expiries:
                        remaining_ttl = int(expiries[key] - now)
                        
                        # Only write to the new log if it hasn't expired yet
                        if remaining_ttl > 0:
                            await f.write(f"SET {key} {value} EX {remaining_ttl}\n")
                    else:
                        # No TTL associated, write standard SET command
                        await f.write(f"SET {key} {value}\n")
                
                await f.flush()
            
            # Atomic swap: replaces the bloated log with the clean one
            os.replace(temp_filepath, self.filepath)
            print(f"[AOF] Compaction successful. Optimized storage for {len(current_data)} keys.")

        except Exception as e:
            print(f"[AOF] Compaction failed: {e}")
            if os.path.exists(temp_filepath):
                os.remove(temp_filepath)

    def read_logs(self):
        """
        Synchronous read used during server startup to rebuild memory state.
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
            print(f"[AOF] Critical error reading logs: {e}")
            return []