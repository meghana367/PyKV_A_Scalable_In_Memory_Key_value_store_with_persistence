import sys
import os
import asyncio
import datetime

# Ensure project root is in the path for core and persistence imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.store import LRUCache

# Initialize the store
store = LRUCache(capacity=10)

# List to keep track of active follower connections
connected_followers = []

async def broadcast_to_followers(command_str):
    """
    Sends write commands to all connected followers for replication.
    """
    if not connected_followers:
        return

    msg = f"{command_str}\n".encode()
    for writer in connected_followers[:]:  # Iterate over a copy
        try:
            writer.write(msg)
            await writer.drain()
        except Exception:
            print(f"[Replication] Follower disconnected.")
            connected_followers.remove(writer)

async def handle_client(reader, writer):
    """
    Handles both User Clients and Replication Followers.
    """
    address = writer.get_extra_info('peername')
    print(f"[Server] New connection from {address}")

    try:
        while True:
            data = await reader.read(1024)
            if not data:
                break

            message = data.decode().strip()
            parts = message.split()
            if not parts:
                continue

            command = parts[0].upper()
            response = "ERROR: Unknown Command"

            # --- REPLICATION HANDSHAKE ---
            if command == "REPLICATE":
                print(f"[Replication] Node at {address} is now a Follower.")
                connected_followers.append(writer)
                writer.write(b"ACK_REPLICATION\n")
                await writer.drain()
                return  # Stay connected but stop processing as a standard client

            # --- STANDARD COMMANDS ---
            # 1. SET <key> <value> [EX <seconds>]
            if command == "SET" and len(parts) >= 3:
                key = parts[1]
                ttl = None
                parts_upper = [p.upper() for p in parts]
                if "EX" in parts_upper:
                    try:
                        ex_index = parts_upper.index("EX")
                        ttl = int(parts[ex_index + 1])
                        value = " ".join(parts[2:ex_index])
                    except (ValueError, IndexError):
                        value = " ".join(parts[2:])
                else:
                    value = " ".join(parts[2:])

                await store.set(key, value, ttl)
                await broadcast_to_followers(message) # Broadcast write
                response = "OK"

            # 2. GET <key>
            elif command == "GET" and len(parts) == 2:
                key = parts[1]
                val = await store.get(key)
                response = val if val is not None else "(nil)"

            # 3. INCR <key>
            elif command == "INCR" and len(parts) == 2:
                key = parts[1]
                result = await store.increment(key)
                await broadcast_to_followers(message) # Broadcast write
                response = str(result)

            # 4. INFO
            elif command == "INFO":
                response = store.get_info()

            # 5. DEL <key>
            elif command == "DEL" and len(parts) == 2:
                key = parts[1]
                success = await store.delete(key)
                if success:
                    await broadcast_to_followers(message) # Broadcast write
                response = "OK" if success else "(nil)"

            # Send response to client
            writer.write(f"{response}\n".encode())
            await writer.drain()

    except Exception as e:
        print(f"[Server] Error handling {address}: {e}")
    finally:
        if writer not in connected_followers:
            print(f"[Server] Closing connection from {address}")
            writer.close()
            await writer.wait_closed()

async def compaction_housekeeper(interval=30):
    """
    Background Task: Automatically triggers AOF compaction.
    """
    while True:
        await asyncio.sleep(interval)
        now = datetime.datetime.now().strftime("%H:%M:%S")
        print(f"[{now}] Housekeeper: Triggering AOF Compaction...")
        try:
            await store.aof.trigger_compaction(store)
        except Exception as e:
            print(f"[Server] Compaction error: {e}")

async def main():
    # Use port 8889 as per your recent update
    server = await asyncio.start_server(handle_client, '127.0.0.1', 8889)
    addr = server.sockets[0].getsockname()
    print(f"[Server] PyKV LEADER ACTIVE on {addr}")

    # Start background tasks
    #cleanup_task = asyncio.create_task(store.cleanup_expired_keys())
    compaction_task = asyncio.create_task(compaction_housekeeper(interval=30))
    
    async with server:
        try:
            await server.serve_forever()
        finally:
            #cleanup_task.cancel()
            compaction_task.cancel()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[Server] Shutting down...")