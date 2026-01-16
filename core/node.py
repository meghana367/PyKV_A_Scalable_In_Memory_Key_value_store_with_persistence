import time

class Node:
    def __init__(self, key=None, value=None, ttl=None):
        self.key = key
        self.value = value
        # expiry is a unix timestamp (current time + ttl seconds)
        self.expiry = (time.time() + ttl) if ttl else None
        self.prev = None
        self.next = None

    def is_expired(self):
        """Check if the current time has passed the expiry timestamp."""
        if self.expiry is None:
            return False
        return time.time() > self.expiry