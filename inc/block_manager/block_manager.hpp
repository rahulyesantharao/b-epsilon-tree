#ifndef BLOCK_MANAGER_H
#define BLOCK_MANAGER_H

#include <cstdint>
#include <lru_cache/lru_cache.hpp>
#include <string>

#define BLOCK_SIZE 4096
#define BLOCKS_IN_MEMORY 16
#define MEMORY_SIZE (BLOCK_SIZE * BLOCKS_IN_MEMORY)

class Block {
 public:
  unsigned char block_buf[BLOCK_SIZE];
};

class BlockManager {
  int num_reads, num_writes;
  std::string name;
  uint32_t cur_num_blocks;
  LRUCache *open_blocks;

  void WriteBlock(uint32_t id, int pos);
  void ReadBlock(uint32_t id, int pos);
  std::string BlockFilename(uint32_t id);

 public:
  BlockManager(std::string _name);
  ~BlockManager();
  uint32_t CreateBlock();
  void DeleteBlock(uint32_t id);
  uint32_t OpenBlock(uint32_t id);

  Block *internal_mem;
};

#endif  // BLOCK_MANAGER_H
