import socket
#import sys

def run_client(host='127.0.0.1', port=8889):
    """
    Connects to the PyKV server and allows interactive command entry.
    """
    print(f"--- PyKV Client ---")
    print(f"Connecting to {host}:{port}...")
    print("Commands: SET <key> <value>, GET <key>, DEL <key>, EXIT")
    print("Tip: Use Ctrl+C or type 'EXIT' to quit safely.")

    try:
        # Create a persistent connection
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            
            while True:
                try:
                    # Take user input
                    cmd = input("pykv> ").strip()
                    
                    if not cmd:
                        continue
                    
                    if cmd.upper() == "EXIT":
                        print("Exiting...")
                        break

                    # Send command to server
                    s.sendall(cmd.encode())

                    # Receive response
                    data = s.recv(1024)
                    if not data:
                        print("[Server] Connection closed by server.")
                        break
                        
                    print(data.decode().strip())

                except EOFError:
                    # Handles Ctrl+D
                    print("\nExiting...")
                    break
                except KeyboardInterrupt:
                    # This catches the Ctrl+C and prevents the Traceback
                    print("\n[Client] Disconnecting safely...")
                    break

    except ConnectionRefusedError:
        print("[Error] Could not connect to server. Is server/main.py running?")
    except Exception as e:
        print(f"[Error] {e}")

if __name__ == "__main__":
    run_client()