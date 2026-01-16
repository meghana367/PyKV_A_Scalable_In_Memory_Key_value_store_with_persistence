import sys
import os
import asyncio
import datetime

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
            await follower_store.aof.trigger_compaction(follower_store)
        except Exception as e:
            print(f"[Follower] Compaction error: {e}")

async def run_follower(leader_ip='127.0.0.1', leader_port=8889):
    # 1. Initialize Follower Store (Capacity handles the 'Hot' data list)
    # The 'db' dictionary inside will store ALL replicated keys.
    follower_store = LRUCache(capacity=5) 
    follower_store.aof.filepath = "persistence/follower_appendonly.aof"
    
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
                
                # Apply updates to the follower's DB and LRU cache
                if cmd == "SET" and len(parts) >= 3:
                    # In this new architecture, 'set' updates the DB and moves key to 'Hot'
                    await follower_store.set(parts[1], parts[2])
                    print(f"[Follower] Synced SET: {parts[1]}")
                    
                elif cmd == "DEL" and len(parts) == 2:
                    await follower_store.delete(parts[1])
                    print(f"[Follower] Synced DEL: {parts[1]}")
                
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