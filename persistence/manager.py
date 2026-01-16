import json
import os

class PersistenceManager:
    def __init__(self, filepath="persistence/storage.json"):
        self.filepath = filepath

    def save_snapshot(self, data):
        """
        Implementation of a full memory dump to disk.
        """
        try:
            with open(self.filepath, 'w') as f:
                json.dump(data, f)
            print(f"[Persistence] Snapshot saved to {self.filepath}")
        except Exception as e:
            print(f"[Error] Failed to save snapshot: {e}")

    def load_snapshot(self):
        """
        Implementation of data recovery on startup.
        """
        if not os.path.exists(self.filepath):
            return {}
        
        try:
            with open(self.filepath, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"[Error] Failed to load data: {e}")
            return {}