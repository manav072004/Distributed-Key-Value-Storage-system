# import hashlib
# import bisect

# import hashlib
# import bisect
# import threading

# class ConsistentHashing:
#     def __init__(self, replicas=100):
#         self.replicas = replicas
#         self.ring = {}
#         self.sorted_keys = []
#         self.lock = threading.Lock()

#     def _hash(self, key):
#         return int(hashlib.sha256(key.encode()).hexdigest(), 16)

#     def add_node(self, node):
#         with self.lock:
#             for i in range(self.replicas):
#                 virtual_key = f"{node}-{i}"
#                 hash_val = self._hash(virtual_key)
#                 self.ring[hash_val] = node
#                 bisect.insort(self.sorted_keys, hash_val)

#     def remove_node(self, node):
#         with self.lock:
#             for i in range(self.replicas):
#                 virtual_key = f"{node}-{i}"
#                 hash_val = self._hash(virtual_key)
#                 if hash_val in self.ring:
#                     self.ring.pop(hash_val)
#                     self.sorted_keys.remove(hash_val)

#     def get_node(self, key):
#         with self.lock:
#             if not self.ring:
#                 return None
            
#             hash_val = self._hash(key)
#             idx = bisect.bisect(self.sorted_keys, hash_val) % len(self.sorted_keys)
#             return self.ring[self.sorted_keys[idx]]

# ch = ConsistentHashing(replicas=3)
# ch.add_node("node1")
# ch.add_node("node13")
# print(ch.get_node("abcdef"))
# print(ch.get_node("2"))
# print(ch.get_node("helloasdfasdf"))
# print(ch.get_node("34545"))

# ch.add_node("pp")
# print(ch.get_node("abcdef"))
# print(ch.get_node("2"))
# print(ch.get_node("helloasdfasdf"))
# print(ch.get_node("34545"))
import hashlib
import bisect
import threading

class ConsistentHashing:
    def __init__(self, replicas=100):
        self.replicas = replicas
        self.ring = {}
        self.sorted_keys = []
        self.lock = threading.Lock()

    def _hash(self, key):
        return int(hashlib.sha256(key.encode()).hexdigest(), 16)

    def add_node(self, node):
        with self.lock:
            for i in range(self.replicas):
                virtual_key = f"{node}-{i}"
                hash_val = self._hash(virtual_key)
                if hash_val not in self.ring:
                    self.ring[hash_val] = node
                    bisect.insort(self.sorted_keys, hash_val)

    def remove_node(self, node):
        with self.lock:
            for i in range(self.replicas):
                virtual_key = f"{node}-{i}"
                hash_val = self._hash(virtual_key)
                if hash_val in self.ring:
                    self.ring.pop(hash_val)
                    self.sorted_keys.remove(hash_val)

    def get_node(self, key):
        with self.lock:
            if not self.ring:
                return None
            
            hash_val = self._hash(key)
            idx = bisect.bisect(self.sorted_keys, hash_val) % len(self.sorted_keys)
            return self.ring[self.sorted_keys[idx]]

ch = ConsistentHashing(100)
first = []
second = []
ch.add_node("Shard1")
for i in range(1000):
    print(ch.get_node(f"key{i}"))
    first.append(ch.get_node(f"key{i}"))
for i in range(2, 23):
    ch.add_node(f"Shard{i}")
for i in range(1000):
    print(ch.get_node(f"key{i}"))
    second.append(ch.get_node(f"key{i}"))
first = set(first)
second = set(second)
print(first)
print(second)