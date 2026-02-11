import sys
import os
import asyncio
import datetime
import time

# Ensure project root is in the path for core and persistence imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.store import LRUCache

async def compaction_housekeeper(follower_store, interval=30):
    """
    Background Task: Periodically cleans the follower_appendonly.aof file.
    """
    while True:
        await asyncio.sleep(interval)
        now = datetime.datetime.now().strftime("%H:%M:%S")
        print(f"[Follower Housekeeper {now}] Triggering Compaction...")
        try:
            # Compaction will now skip expired keys because the follower 
            # now tracks expiries locally.
            await follower_store.aof.trigger_compaction(follower_store)
        except Exception as e:
            print(f"[Follower] Compaction error: {e}")

async def run_follower(leader_ip='127.0.0.1', leader_port=8889):
    # 1. Initialize Follower Store
    follower_store = LRUCache(capacity=5) 
    follower_store.aof.filepath = "persistence/follower_appendonly.aof"
    
    # Start the TTL cleanup task on the follower so it can delete keys locally
    asyncio.create_task(follower_store.cleanup_expired_keys())
    
    # 2. Start independent compaction for the follower's log
    asyncio.create_task(compaction_housekeeper(follower_store, interval=30))
    
    print(f"[Follower] Connecting to Leader at {leader_ip}:{leader_port}...")
    
    try:
        reader, writer = await asyncio.open_connection(leader_ip, leader_port)
        
        # Handshake
        writer.write(b"REPLICATE\n")
        await writer.drain()
        
        response = await reader.read(1024)
        if response.decode().strip() == "ACK_REPLICATION":
            print("[Follower] Replication Link Established. Mirroring all Leader data.")

        while True:
            data = await reader.read(1024)
            if not data:
                print("[Follower] Leader disconnected.")
                break
            
            command_str = data.decode().strip()
            # Handle potential multiple commands in one buffer
            for line in command_str.split('\n'):
                if not line: continue
                
                parts = line.split()
                cmd = parts[0].upper()
                
                # --- SYNC SET with TTL Support ---
                if cmd == "SET" and len(parts) >= 3:
                    key = parts[1]
                    ttl = None
                    parts_upper = [p.upper() for p in parts]
                    
                    if "EX" in parts_upper:
                        try:
                            ex_idx = parts_upper.index("EX")
                            ttl = int(parts[ex_idx + 1])
                            value = " ".join(parts[2:ex_idx])
                        except (ValueError, IndexError):
                            value = " ".join(parts[2:])
                    else:
                        value = " ".join(parts[2:])

                    await follower_store.set(key, value, ttl)
                    print(f"[Follower] Synced SET: {key} (TTL: {ttl if ttl else 'None'})")
                
                # --- SYNC DEL ---
                elif cmd == "DEL" and len(parts) == 2:
                    await follower_store.delete(parts[1])
                    print(f"[Follower] Synced DEL: {parts[1]}")
                
                # --- SYNC INCR ---
                elif cmd == "INCR" and len(parts) == 2:
                    await follower_store.increment(parts[1])
                    print(f"[Follower] Synced INCR: {parts[1]}")

    except Exception as e:
        print(f"[Follower] Connection Error: {e}")
    finally:
        writer.close()
        await writer.wait_closed()

if __name__ == "__main__":
    try:
        asyncio.run(run_follower(leader_port=8889))
    except KeyboardInterrupt:
        print("\n[Follower] Shutting down.")