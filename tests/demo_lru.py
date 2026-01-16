import socket
import time

def send_cmd(s, cmd):
    print(f"Sending: {cmd}")
    s.sendall(cmd.encode())
    response = s.recv(1024).decode().strip()
    print(f"Response: {response}")
    return response

def run_demo():
    host, port = '127.0.0.1', 8888
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            
            print("\n--- Phase 1: Filling Cache to Capacity (10 keys) ---")
            for i in range(1, 11):
                send_cmd(s, f"SET key{i} value{i}")
            
            print("\n--- Phase 2: Demonstrating MRU (Most Recently Used) ---")
            print("Accessing key1 to move it to the front...")
            send_cmd(s, "GET key1")
            
            print("\n--- Phase 3: Triggering Eviction ---")
            print("Adding key11. Since key1 was recently used, key2 should be evicted!")
            send_cmd(s, "SET key11 value11")
            
            print("\n--- Phase 4: Verifying Results ---")
            k1 = send_cmd(s, "GET key1")
            k2 = send_cmd(s, "GET key2")
            
            if k1 != "(nil)" and k2 == "(nil)":
                print("\nSUCCESS: LRU logic worked! key2 was evicted, key1 was preserved.")
            else:
                print("\nCHECK: Verify the logic in core/store.py.")

    except Exception as e:
        print(f"Error: {e}. Is the server running?")

if __name__ == "__main__":
    run_demo()