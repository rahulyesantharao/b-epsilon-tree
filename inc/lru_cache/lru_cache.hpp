#ifndef LRUCache_H
#define LRUCache_H

#include <cstdint>
#include <unordered_map>

class LRUNode {
 public:
  uint32_t id, pos;
  LRUNode *prev, *next;
  LRUNode(uint32_t _id, uint32_t _pos)
      : id(_id), pos(_pos), prev(nullptr), next(nullptr) {}
};

class LRULinkedList {
  LRUNode *head, *rear;

 public:
  LRULinkedList();
  ~LRULinkedList();

  LRUNode *AddNodeToHead(uint32_t id, uint32_t pos);
  void MoveNodeToHead(LRUNode *node);
  void RemoveRearNode();
  LRUNode *GetRearNode();
};

class LRUCache {
  int cap, size;
  LRULinkedList *node_list;
  std::unordered_map<uint32_t, LRUNode *> node_hash;

 public:
  LRUCache(int _cap);
  ~LRUCache();

  uint32_t Get(uint32_t id);
  uint32_t Put(uint32_t id, uint32_t *evicted_id);

  std::unordered_map<uint32_t, LRUNode *>::iterator GetBegin();
  std::unordered_map<uint32_t, LRUNode *>::iterator GetEnd();
};

#endif  // LRUCache_H
