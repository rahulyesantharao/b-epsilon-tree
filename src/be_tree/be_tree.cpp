#include <algorithm>
#include <cassert>
#include <cstdarg>
#include <iostream>
#include <utility>
#include <vector>

#include <be_tree/be_tree.hpp>

#include <map>
#include <set>

#ifdef NDEBUG
void DebugPrint(std::string name, std::string more = "") {}
#else
void DebugPrint(std::string name, std::string more = "") {
  static std::map<std::string, int> counts;

  counts[name]++;
  std::cerr << " - " << counts[name] << ": ";
  std::cerr << name;
  if (more.length() > 0) std::cerr << " | " << more << " | ";
  std::cerr << std::endl;
}
#endif

static void rtassert(bool cond, const char *const format...) {
  va_list args;
  va_start(args, format);
  if (!cond) {
    fprintf(stderr, format, args);
    exit(1);
  }
}

void PrintUpsert(BeUpsert const &ups) {
  std::cerr << "key:" << ups.key << std::endl;
  std::cerr << "param: " << ups.parameter << std::endl;
  std::cerr << "ts: " << ups.timestamp << std::endl;
  std::cerr << "type: ";
  std::string type;
  switch (ups.type) {
    case INSERT:
      type = "insert";
      break;
    case DELETE:
      type = "delete";
      break;
    case UPDATE:
      type = "update";
      break;
    default:
      type = "invalid";
      break;
  }
  std::cerr << type << std::endl;
}

bool SortBeUpsert(BeUpsert const &lhs, BeUpsert const &rhs) {
  return lhs.timestamp > rhs.timestamp;
}

// noop because we work directly off the Block
int BeNode::Serialize(Block *disk_store, int pos) { return 0; }

///////////////////////////////////////////////////////////////
// BeNode implementation
///////////////////////////////////////////////////////////////
BeNode::BeNode(BlockManager *_bmanager, uint32_t _id)
    : bmanager(_bmanager),
      id(_id),
      parent(nullptr),
      is_leaf(nullptr),
      buffer(nullptr),
      pivots(nullptr),
      data(nullptr) {
  Open();
}

void BeNode::Deserialize(const Block &disk_store) {
  parent = (uint32_t *)(disk_store.block_buf);
  is_leaf = parent + 1;
  data = (struct BeData *)(disk_store.block_buf + 2 * sizeof(uint32_t));
  buffer = (struct BeBuffer *)(disk_store.block_buf + 2 * sizeof(uint32_t));
  pivots = (struct BePivots *)(disk_store.block_buf + 2 * sizeof(uint32_t) +
                               sizeof(struct BeBuffer));
}

void BeNode::Open() {
  // make sure the current block is open
  Deserialize(bmanager->internal_mem[bmanager->OpenBlock(id)]);
}

// TODO: binary search
int BeNode::IndexOfKey(uint32_t key) {
  assert(!*is_leaf);
  Open();

  for (int i = 0; i <= pivots->size; ++i) {
    if ((i == pivots->size || key < pivots->pivots[i]) &&
        (i == 0 || key >= pivots->pivots[i - 1]))
      return i;
  }
}

std::set<uint32_t> seen_keys;
void CheckKeys() {
  for (uint32_t i = 1u; i <= 20000u; i++) {
    if (!seen_keys.count(i)) {
      std::cerr << i << " not seen\n";
      break;
    }
  }
}

bool BeNode::UpsertLeaf(struct BeUpsert upsert[], int &num) {
  assert(*is_leaf);
  while (num > 0) {
    num--;
    // find the index of the key
    int index = -1;
    for (int j = 0; j < data->size; ++j) {
      if (data->keys[j] == upsert[num].key) {
        index = j;
        break;
      }
    }

#ifndef NDEBUG
    seen_keys.insert((uint32_t)upsert[num].key);
#endif
    // deal with upsert
    switch (upsert[num].type) {
      case INSERT:
        rtassert(index < 0, "inserting an existing key: %u\n", upsert[num].key);
        data->keys[data->size] = upsert[num].key;
        data->values[data->size] = upsert[num].parameter;
        data->size++;
        if (data->size == NUM_DATA_PAIRS) {
          return true;
        }
        break;
      case UPDATE:
        rtassert(index >= 0, "updating a nonexistent key: %u\n",
                 upsert[num].key);
        data->values[index] = upsert[num].parameter;
        break;
      case DELETE:
        rtassert(index >= 0, "deleting a nonexistent key: %u\n",
                 upsert[num].key);
        for (int j = index + 1; j < data->size; ++j) {
          data->keys[j - 1] = data->keys[j];
          data->values[j - 1] = data->values[j];
        }
        --data->size;
        break;
      default:
        rtassert(false, "invalid upsert type: %u\n", upsert[num].type);
    }
  }
  return false;
}

uint32_t BeNode::SplitLeaf(uint32_t &new_id) {
  assert(*is_leaf);
  Open();

  new_id = bmanager->CreateBlock();
  BeNode new_sibling(bmanager, new_id);
  *new_sibling.parent = *parent;
  *new_sibling.is_leaf = *is_leaf;

  DebugPrint("SplitLeaf",
             std::to_string(*parent) + "<-" + std::to_string(new_id));

  // Sort the data in place in the node
  std::vector<std::pair<uint32_t, uint32_t> > pairs(data->size);
  for (int i = 0; i < data->size; i++) {
    pairs[i].first = data->keys[i];
    pairs[i].second = data->values[i];
  }
  std::sort(pairs.begin(), pairs.end());
  for (int i = 0; i < data->size; i++) {
    data->keys[i] = pairs[i].first;
    data->values[i] = pairs[i].second;
  }

  // Move the data pairs over
  for (int i = data->size / 2; i < data->size; ++i) {
    new_sibling.data->keys[i - data->size / 2] = data->keys[i];
    new_sibling.data->values[i - data->size / 2] = data->values[i];
    new_sibling.data->size++;
  }
  // Update the size of the old (left) node
  data->size -= new_sibling.data->size;

  return new_sibling.data->keys[0];  // the upper half of the split
}

void BeNode::PrintInternal() {
  Open();
  assert(!*is_leaf);
  std::cerr << std::endl;
  std::cerr << "Node " << id << std::endl;
  std::cerr << "# Pivots: " << pivots->size << std::endl;
  for (int i = 0; i < pivots->size; i++) {
    std::cerr << pivots->pivots[i] << " ";
  }
  std::cerr << std::endl;
  for (int i = 0; i <= pivots->size; i++) {
    std::cerr << pivots->pointers[i] << " ";
  }
  std::cerr << std::endl;
}

uint32_t BeNode::SplitInternal(uint32_t &new_id) {
  Open();
  assert(!*is_leaf);
  assert(pivots->size == NUM_PIVOTS);

  // create a new block
  new_id = bmanager->CreateBlock();
  BeNode new_node(bmanager, new_id);
  *new_node.is_leaf = *is_leaf;
  *new_node.parent = *parent;

  DebugPrint("SplitInternal",
             std::to_string(*parent) + "<-" + std::to_string(new_id));

  // move pivots/pointers over to the new node
  BeNode moving_node(bmanager, new_id);
  int start_index = (pivots->size + 1) / 2;
  for (int i = start_index; i <= pivots->size; ++i) {
    Open();
    new_node.Open();

    // move the pivots over
    if (i < pivots->size) {  // there is one more pointer than pivot
      new_node.pivots->pivots[i - start_index] = pivots->pivots[i];
      new_node.pivots->size++;
    }
    new_node.pivots->pointers[i - start_index] = pivots->pointers[i];

    // change their parent pointers
    moving_node.SetId(pivots->pointers[i]);
    *moving_node.parent = new_node.id;
  }

  // reset size of old (left) node (drop the middle pivot entirely)
  pivots->size -= (new_node.pivots->size + 1);
  uint32_t split_key =
      pivots->pivots[pivots->size];  // the middle pivot is the split key

  // move regular upserts over to new node
  for (int i = 0; i < buffer->size - buffer->flush_size; i++) {
    if (buffer->buffer[i].key >= split_key) {
      new_node.buffer->buffer[new_node.buffer->size] = buffer->buffer[i];
      new_node.buffer->size++;
      buffer->buffer[i].type = INVALID;
    }
  }
  // move flush buffer
  if (buffer->buffer[buffer->size - buffer->flush_size].key >= split_key) {
    for (int i = buffer->size - buffer->flush_size; i < buffer->size; i++) {
      new_node.buffer->buffer[new_node.buffer->size] = buffer->buffer[i];
      new_node.buffer->size++;
      buffer->buffer[i].type = INVALID;
    }
    new_node.buffer->flush_size = buffer->flush_size;
    buffer->size -= buffer->flush_size;
    buffer->flush_size = 0;
  }
  // tighten current node buffer
  uint32_t new_node_size = 0;
  for (int i = 0; i < buffer->size; i++) {
    if (buffer->buffer[i].type != INVALID) {
      buffer->buffer[new_node_size] = buffer->buffer[i];
      new_node_size++;
    }
  }
  buffer->size = new_node_size;

  return split_key;  // the upper half of the split
}

void BeNode::FullFlushSetup() {
  Open();
  assert(buffer->flush_size == 0);
  assert(!*is_leaf);

  int buf_size = (int)buffer->size;
  int pivot_size = (int)pivots->size;

  uint32_t child_split_key, child_split_id;

  int nums[NUM_CHILDREN];
  memset(nums, 0, sizeof(nums));

  // count number of messages for each child
  for (int i = 0; i < buf_size; ++i) {
    ++nums[IndexOfKey(buffer->buffer[i].key)];
  }

  // find child with maximum number of messages
  // TODO_SOMEDAY: could eagerly maintain this at the cost of disk space
  int to_flush = 0;
  for (int i = 1; i < pivot_size + 1; ++i) {
    if (nums[i] > nums[to_flush]) to_flush = i;
  }

  // reorder upserts
  int flush_pos = buffer->size - 1;
  int cur_pos = buffer->size - 1;
  while (cur_pos >= 0) {
    // find the next flushable upsert
    if (IndexOfKey(buffer->buffer[cur_pos].key) == to_flush) {
      // swap flush_pos, cur_pos
      BeUpsert tmp = buffer->buffer[flush_pos];
      buffer->buffer[flush_pos] = buffer->buffer[cur_pos];
      buffer->buffer[cur_pos] = tmp;
      flush_pos--;
    }
    cur_pos--;
  }

  assert(buffer->size - 1 - flush_pos == nums[to_flush]);

  // update flush size
  buffer->flush_size = nums[to_flush];

  // sort the flush items
  std::sort(&buffer->buffer[buffer->size - buffer->flush_size],
            &buffer->buffer[buffer->size], &SortBeUpsert);
}

void BeNode::SetId(uint32_t new_id) {
  id = new_id;
  Open();
}

FlushResult BeNode::FlushOneLeaf(BeNode &child_node, uint32_t &split_key,
                                 uint32_t &new_id) {
  Open();
  child_node.Open();

  assert(!*is_leaf);
  assert(*child_node.is_leaf);
  assert(*child_node.parent == id);

  BeUpsert *to_flush = buffer->buffer + (buffer->size - buffer->flush_size);

  int num_to_flush = std::min(buffer->flush_size, LEAF_FLUSH_THRESHOLD);
  DebugPrint("Leaf Flush Size", std::to_string(num_to_flush));
  // we can handle all of the updates with at most a single split
  if (child_node.UpsertLeaf(to_flush, num_to_flush)) {
    // need to split
    split_key = child_node.SplitLeaf(new_id);

    // update child_node id if necessary
    uint32_t flush_key = to_flush[0].key;
    if (flush_key >= split_key) {
      child_node.SetId(new_id);
    }

    // flush the remainder
    child_node.UpsertLeaf(to_flush, num_to_flush);

    // update sizes and return
    buffer->size -= std::min(buffer->flush_size, LEAF_FLUSH_THRESHOLD);
    buffer->flush_size = 0;
    return SPLIT;
  }

  // update sizes and return
  buffer->size -= std::min(buffer->flush_size, LEAF_FLUSH_THRESHOLD);
  buffer->flush_size = 0;
  return NO_SPLIT;
}

FlushResult BeNode::FlushOneInternal(BeNode &child_node) {
  Open();
  child_node.Open();

  assert(!*is_leaf);
  assert(!*child_node.is_leaf);
  assert(*child_node.parent == id);

  int num_empty_in_child = NUM_UPSERTS - child_node.buffer->size;

  int flush_num;
  if (num_empty_in_child >= buffer->flush_size) {
    // flush everything down
    flush_num = buffer->flush_size;
  } else if (num_empty_in_child >= FLUSH_THRESHOLD) {
    // flush down as much as possible
    flush_num = FLUSH_THRESHOLD;
  } else {
    SetId(child_node.id);
    return ENSURE_SPACE;
  }

  DebugPrint("Internal Flush Size", std::to_string(flush_num));
  // move the upserts down
  memcpy(child_node.buffer->buffer + child_node.buffer->size,
         buffer->buffer + (buffer->size - buffer->flush_size),
         flush_num * sizeof(BeUpsert));
  // update sizes
  buffer->size -= flush_num;
  buffer->flush_size = 0;
  child_node.buffer->size += flush_num;

  return NO_SPLIT;
}

FlushResult BeNode::FlushOneLevel(uint32_t &split_key, uint32_t &new_id) {
  Open();
  uint32_t child_id = pivots->pointers[IndexOfKey(
      buffer->buffer[buffer->size - buffer->flush_size].key)];
  BeNode child_node(bmanager, child_id);

  if (*child_node.is_leaf)
    return FlushOneLeaf(child_node, split_key, new_id);
  else
    return FlushOneInternal(child_node);
}

bool BeNode::AddPivot(uint32_t split_key, uint32_t new_id) {
  Open();

  assert(!*is_leaf);
  assert(new_id > 0);

  int pos = IndexOfKey(split_key);
  for (int j = pivots->size - 1; j >= pos; --j) {
    pivots->pointers[j + 2] = pivots->pointers[j + 1];
    pivots->pivots[j + 1] = pivots->pivots[j];
  }
  pivots->pivots[pos] = split_key;
  pivots->pointers[pos + 1] = new_id;
  pivots->size = pivots->size + 1;

  return pivots->size == NUM_PIVOTS;
}

uint32_t BeNode::Query(uint32_t key) {
  uint32_t orig_id = id;
  uint32_t ret = KEY_NOT_FOUND;

  uint32_t latest_timestamp = 0;
  bool found = false;
  while (true) {
    Open();
    if (*is_leaf) {
      for (int i = 0; i < data->size; ++i) {
        if (data->keys[i] == key) ret = data->values[i];
      }
      break;
    } else {
      for (int i = 0; i < buffer->size; i++) {
        if (buffer->buffer[i].key == key &&
            buffer->buffer[i].timestamp >= latest_timestamp) {
          latest_timestamp = buffer->buffer[i].timestamp;
          if (buffer->buffer[i].type == DELETE) {
            ret = KEY_NOT_FOUND;
          } else {
            ret = buffer->buffer[i].parameter;
          }
          found = true;
        }
      }
      if (found) break;
    }

    uint32_t next_id = pivots->pointers[IndexOfKey(key)];
    assert(next_id > 0);
    id = next_id;
  }

  id = orig_id;

  if (ret == KEY_NOT_FOUND) printf("key %u not found!\n", key);
  return ret;
}

static uint32_t all_timestamp = 0;

void BeNode::Upsert(uint32_t key, UpsertFunction type, uint32_t val) {
  Open();
  assert(buffer->size < NUM_UPSERTS);  // needs it to not be full

  // add to upsert buffer
  buffer->buffer[buffer->size++] = {
      .key = key, .type = type, .parameter = val, .timestamp = ++all_timestamp};
}

///////////////////////////////////////////////////////////////
// BeTree implementation
///////////////////////////////////////////////////////////////
// TODO: make the initial root node a leaf node
BeTree::BeTree(std::string _name) : name(_name) {
  bmanager = new BlockManager(_name);

  uint32_t root_id = bmanager->CreateBlock();
  uint32_t leaf1_id = bmanager->CreateBlock();
  uint32_t leaf2_id = bmanager->CreateBlock();

  BeNode r1(bmanager, root_id);
  BeNode c1(bmanager, leaf1_id);
  BeNode c2(bmanager, leaf2_id);

  // root setup
  *r1.is_leaf = 0;
  *r1.parent = 0;
  r1.pivots->size = 1;
  r1.pivots->pivots[0] = 500000000;
  r1.pivots->pointers[0] = leaf1_id;
  r1.pivots->pointers[1] = leaf2_id;

  // leaf setup
  *c1.is_leaf = 1;
  *c2.is_leaf = 1;
  // parent setup
  *c1.parent = root_id;
  *c2.parent = root_id;

  // instantiate root
  root = new BeNode(bmanager, root_id);
}

BeTree::~BeTree() {
  delete root;
  delete bmanager;
}

void BeTree::CreateNewRoot(uint32_t split_key, uint32_t new_id) {
  // create a new block for the new root
  uint32_t root_id = bmanager->CreateBlock();

  // set parent pointers
  *root->parent = root_id;
  BeNode new_child(bmanager, new_id);
  *new_child.parent = root_id;

  // setup new root
  uint32_t orig_root_id = root->GetId();
  root->SetId(root_id);

  *root->is_leaf = 0;
  *root->parent = 0;
  root->pivots->size = 1;
  root->pivots->pivots[0] = split_key;
  root->pivots->pointers[0] = orig_root_id;
  root->pivots->pointers[1] = new_id;

  DebugPrint("CreateNewRoot", std::to_string(root_id) + "<-(" +
                                  std::to_string(orig_root_id) + ", " +
                                  std::to_string(new_id) + ")");
}

void BeTree::FullFlush() {
  uint32_t orig_root = root->id;

  BeNode node(root->bmanager, root->id);
  BeNode child_node(root->bmanager, root->id);

  FlushResult flush_res;
  uint32_t split_key, new_id;
  // step 1: flush down as much as possible
  do {
    node.FullFlushSetup();
    flush_res = node.FlushOneLevel(split_key, new_id);
  } while (flush_res == ENSURE_SPACE);

  // step 2: bubble back up, pull up splits and push down remaining upserts
  while (true) {
    // if the previous node split, deal with it
    if (flush_res == SPLIT) {
      if (node.AddPivot(split_key, new_id)) {
        // if the pivots are full, split node and repeat if needed
        split_key = node.SplitInternal(new_id);
        if (node.buffer->flush_size == 0) node.SetId(new_id);
      } else {
        flush_res = NO_SPLIT;
      }
    }

    // if there are any outstanding upserts, push them down
    if (node.buffer->flush_size > 0) {
      FlushResult debug_res;
      debug_res = node.FlushOneLevel(split_key, new_id);
      assert(debug_res == NO_SPLIT);
    }

    // update node for next iteration
    if (*node.parent == 0) {  // got to root, done! TODO: can stop earlier
      if (flush_res == SPLIT) CreateNewRoot(split_key, new_id);
      break;
    }
    node.SetId(*node.parent);
  }
}

uint32_t BeTree::Query(uint32_t key) { return root->Query(key); }

void BeTree::Upsert(uint32_t key, UpsertFunction type, uint32_t parameter) {
  if (root->buffer->size == NUM_UPSERTS) FullFlush();
  root->Upsert(key, type, parameter);
}

void BeTree::Update(uint32_t key, uint32_t val) { Upsert(key, UPDATE, val); }

void BeTree::Delete(uint32_t key) { Upsert(key, DELETE, 0); }

void BeTree::Insert(uint32_t key, uint32_t val) { Upsert(key, INSERT, val); }

