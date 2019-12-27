#include <lru_cache/lru_cache.hpp>

///////////////////////////////////////////////////////////////
// LRULinkedList implementation
///////////////////////////////////////////////////////////////

LRULinkedList::LRULinkedList() {
  head = new LRUNode(-1, -1);
  rear = new LRUNode(-1, -1);
  head->next = rear;
  rear->prev = head;
}

LRULinkedList::~LRULinkedList() {
  delete head;
  delete rear;
}

LRUNode *LRULinkedList::AddNodeToHead(uint32_t id, uint32_t pos) {
  LRUNode *node = new LRUNode(id, pos);
  MoveNodeToHead(node);
  return node;
}

void LRULinkedList::MoveNodeToHead(LRUNode *node) {
  // remove from current position
  if (node->prev) node->prev->next = node->next;
  if (node->next) node->next->prev = node->prev;

  // add to head
  node->next = head->next;
  node->prev = head;
  head->next->prev = node;
  head->next = node;
}

void LRULinkedList::RemoveRearNode() {
  if (rear->prev == head) return;  // empty

  LRUNode *temp = rear->prev;
  rear->prev->prev->next = rear;
  rear->prev = rear->prev->prev;
  delete temp;
}

LRUNode *LRULinkedList::GetRearNode() {
  if (rear->prev == head) return nullptr;
  return rear->prev;
}

///////////////////////////////////////////////////////////////
// LRUCache implementation
///////////////////////////////////////////////////////////////

LRUCache::LRUCache(int _cap) : cap(_cap), size(0) {
  node_list = new LRULinkedList();
}

LRUCache::~LRUCache() {
  for (std::unordered_map<uint32_t, LRUNode *>::iterator it = node_hash.begin();
       it != node_hash.end(); ++it) {
    delete it->second;
  }
  delete node_list;
}

uint32_t LRUCache::Get(uint32_t id) {
  // fprintf(stderr, "get called: %u\n", id);
  std::unordered_map<uint32_t, LRUNode *>::iterator it = node_hash.find(id);
  if (it == node_hash.end()) return cap + 1;
  node_list->MoveNodeToHead(it->second);
  return it->second->pos;
}

uint32_t LRUCache::Put(uint32_t id, uint32_t *evicted_id) {
  // fprintf(stderr, "put called: %u\n", id);
  uint32_t pos = Get(id);
  if (pos >= cap) {     // need to put the block
    if (size == cap) {  // need to evict
      // fprintf(stderr, "put doing eviction\n");
      LRUNode *to_evict = node_list->GetRearNode();
      pos = to_evict->pos;  // take the block position of the evicted block
      if (evicted_id) *evicted_id = to_evict->id;
      node_hash.erase(to_evict->id);
      node_list->RemoveRearNode();
      --size;
    } else {  // get the next open block
      // fprintf(stderr, "put just inserting\n");
      *evicted_id = 0;  // id is never 0
      pos = size;
    }
    // add block to list
    LRUNode *node = node_list->AddNodeToHead(id, pos);
    ++size;
    node_hash[id] = node;
  }
  return pos;
}

std::unordered_map<uint32_t, LRUNode *>::iterator LRUCache::GetBegin() {
  return node_hash.begin();
}

std::unordered_map<uint32_t, LRUNode *>::iterator LRUCache::GetEnd() {
  return node_hash.end();
}
