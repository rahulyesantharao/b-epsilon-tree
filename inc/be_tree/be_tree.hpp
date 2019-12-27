#ifndef BeTree_H
#define BeTree_H

#include <block_manager/block_manager.hpp>
#include <cstring>
#include <serializable/serializable.hpp>

// not used, just for reference
#define EPSILON 0.5

// Node: | is_leaf | parent | data |
const int DATA_SIZE = BLOCK_SIZE - 2 * sizeof(uint32_t);
// Leaf Node Data: | # entries | entries |
const int LEAF_SIZE = DATA_SIZE;
// Internal Node Data:
// | # upserts | # flush | buffer (regular | flush) | # pivots | pivots |
//  Pivot: Block size 4096B = 1024 keys. :sqrt = 32 keys => 128 bytes
const int PIVOT_SIZE = 128;  // 15 pivots, 16 pointers, 1 size
const int BUFFER_SIZE = DATA_SIZE - PIVOT_SIZE;
const int NUM_CHILDREN = PIVOT_SIZE / (2 * sizeof(uint32_t));

// Size Analysis for cost amortization
// Alternatives: make unit size 16 bytes, instead of 8, to match upsert; use
// larger block size and have BBST in buffers instead of lists
const uint32_t FLUSH_THRESHOLD = 11;
const uint32_t LEAF_FLUSH_THRESHOLD = 255;

// Upsert Interface
enum UpsertFunction : uint32_t { INSERT, DELETE, UPDATE, INVALID };
struct BeUpsert {
  uint32_t key;
  UpsertFunction type;
  uint32_t parameter;
  uint32_t timestamp;
};
bool SortBeUpsert(BeUpsert const &lhs, BeUpsert const &rhs);

// Debug Functions
void PrintUpsert(BeUpsert const &ups);
void CheckKeys();

// Size Calculations
const int NUM_DATA_PAIRS =
    ((LEAF_SIZE - sizeof(uint32_t)) / sizeof(uint32_t)) / 2;
const int NUM_UPSERTS =
    (BUFFER_SIZE - 2 * sizeof(uint32_t)) / sizeof(struct BeUpsert);
const int NUM_PIVOTS = ((PIVOT_SIZE - sizeof(uint32_t)) / sizeof(uint32_t)) / 2;

// Constants
const uint32_t KEY_NOT_FOUND = 4294967295;

enum FlushResult { SPLIT, NO_SPLIT, ENSURE_SPACE };

struct BeBuffer {
  uint32_t size;
  uint32_t flush_size;
  struct BeUpsert buffer[NUM_UPSERTS];
};
int SerializeBeBuffer(Block *disk_store, int pos, struct BeBuffer *buffer);

struct BePivots {
  uint32_t size;
  uint32_t pivots[NUM_PIVOTS];
  uint32_t pointers[NUM_PIVOTS + 1];
};
int SerializeBePivots(Block *disk_store, int pos, struct BePivots *pivots);

struct BeData {
  uint32_t size;
  uint32_t keys[NUM_DATA_PAIRS];
  uint32_t values[NUM_DATA_PAIRS];
};

class BeNode;  // forward declaration
class BeTree {
  // The underlying name of the folder where the tree is stored.
  std::string name;

  // The root of the BeTree. Dynamically allocated.
  BeNode *root;
  // The BlockManager for this tree. Dynamically allocated.
  BlockManager *bmanager;

  /* Creates a new root for the tree. The parameters are the key on which
   * the previous root split, and the id of the (right) split node.
   *
   * Side Effects: Changes the id of [root].
   * Return: None.
   */
  void CreateNewRoot(uint32_t split_key, uint32_t new_id);

  /* Performs a full flush from the root of the tree.
   *
   * Side Effects: Can potentially effect the entire tree as it flushes
   * upserts down. Return: None.
   */
  void FullFlush();

  /* Adds the specified upsert to the root node, flushing (lazily) if necessary.
   */
  void Upsert(uint32_t key, UpsertFunction type, uint32_t parameter);

 public:
  BeTree(std::string _name);
  ~BeTree();

  /* Insert the [key]/[val] pair into the tree.
   *
   * Throws an error if the key is already in the tree.
   */
  void Insert(uint32_t key, uint32_t val);

  /* Update the [key] with the new [val] in the tree.
   *
   * Throws an error if the key is not already in the tree.
   */
  void Update(uint32_t key, uint32_t val);

  /* Delete the [key] from the tree.
   *
   * Throws an error if the key is not in the tree.
   */
  void Delete(uint32_t key);

  /* Queries for the key in the tree, returns a sentinel value if it is not
   * found.
   */
  uint32_t Query(uint32_t key);
};

class BeNode : public Serializable {
  // Used to load the Node from memory
  BlockManager *bmanager;
  uint32_t id;

  // Node data, loaded from file
  uint32_t *parent;   // id of the parent block
  uint32_t *is_leaf;  // whether or not the block is a leaf
  struct BeBuffer *buffer;
  struct BePivots *pivots;
  struct BeData *data;

  /* Returns the index into [pivots->pointers] of the [key].
   */
  int IndexOfKey(uint32_t key);

  /* Ensures that the current [Node] is "open" (the underlying [Block] is
   * loaded in memory).
   */
  void Open();

  /* Applies up to [num] upserts to the leaf node, backwards in [upsert]
   *
   * Side Effects: [num] always represents the number of upserts left that have
   *               not been applied.
   * Return: whether or not the leaf is full (and needs to be split)
   */
  bool UpsertLeaf(struct BeUpsert upsert[], int &num);

  /* Splits the leaf node in half
   *
   * Side Effects:
   *  - Creates a new node and distributes keys evenly between the two nodes
   *  - Sorts the keys
   * Return:
   *  - Returns the key of the split (lower bound of upper node)
   *  - Puts the id of the new node in [new_id]
   */
  uint32_t SplitLeaf(uint32_t &new_id);

  /* Splits the internal node in half
   *
   * Side Effects:
   *  - Creates a new node
   *  - Distributes the pivots/pointers evenly between the two nodes, dropping
   *    the middle pivot
   *  - Distributes the upsert buffer correctly, including the flush buffer
   * Return:
   *  - Returns the key of the split (lower bound of upper node)
   *  - Puts the id of the new node in [new_id]
   */
  uint32_t SplitInternal(uint32_t &new_id);

  /* Finds the child with the maximum number of outstanding upserts and reorders
   * the [buffer] in the node so that these upserts are in the flush region.
   *
   * Side Effects:
   *  - Sets [buffer->flush_size] to the number of flushable elements.
   * Return: None
   */
  void FullFlushSetup();

  /* Flushes from an internal node to its child leaf, [child_node]. Splits if
   * necessary.
   *
   * Side Effects:
   *  - Updates the leaf [child_node] with upserts
   *  - Can split the leaf, if necessary.
   * Return:
   *  - Sets [split_key] to the lower bound of the upper leaf, if split
   *  - Sets [new_id] to the id of the new node, if split
   *  - Returns [SPLIT] or [NO_SPLIT]
   */
  FlushResult FlushOneLeaf(BeNode &child_node, uint32_t &split_key,
                           uint32_t &new_id);

  /* Tries to flush from an internal node to its internal node child,
   * [child_node]. Uses clever cutoffs to ensure the amortization of the disk
   * access against the number of items flushed.
   *
   * Side Effects:
   *  - Flushes to [child_node] and updates it, if it can.
   *  - *** unexpected behavior ***: If it can't flush, sets [node] id to the
   *    child id.
   * Return:
   *  - [ENSURE_SPACE] or [NO_SPLIT]
   */
  FlushResult FlushOneInternal(BeNode &child_node);

  /* Tries to flush the current node. Calls into [FlushOneLeaf] or
   * [FlushOneInternal], depending on whether the current node is a leaf or not
   * (respectively).
   *
   * Side Effects: See constituent functions.
   * Return: See constituent functions.
   */
  FlushResult FlushOneLevel(uint32_t &split_key, uint32_t &new_id);

  /* Adds the given pivot to the current node. Assumes that there is space! This
   * is a valid assumption because every usage must be followed by a check as to
   * whether the node is full; if it is, this must be dealt with eagerly.
   *
   * Side Effects: Adds a new pivot/pointer pair.
   * Return: Whether the current node's pivots are full.
   */
  bool AddPivot(uint32_t split_key, uint32_t new_id);

  /* For debugging purposes: prints an internal node.
   */
  void PrintInternal();

  friend class BeTree;

 public:
  BeNode(BlockManager *_bmanager, uint32_t _id);

  /* Return this node's id.
   */
  uint32_t GetId() { return id; }

  /* Set this node's id and update the underlying data.
   */
  void SetId(uint32_t new_id);

  /* Insert a [BeUpsert] with the given values into this node.
   *
   * Assumes that the node is an internal node, that there is space in its
   * upsert buffer, and that the key goes under this node.
   */
  void Upsert(uint32_t key, UpsertFunction type, uint32_t parameter);

  /* Queries for the key in the tree rooted at the node, returns a sentinel
   * value if it is not found.
   */
  uint32_t Query(uint32_t key);

  /* Serializes the node to the given [pos] in [disk_store].
   *
   * Currently a no-op, because the data is loaded directly off disk.
   */
  int Serialize(Block *disk_store, int pos);

  /* Deserializes the [BeNode] from the given [Block]: [disk_store].
   */
  void Deserialize(const Block &disk_store);
};

#endif  // BeTree_H
