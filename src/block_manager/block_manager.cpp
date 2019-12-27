#include <block_manager/block_manager.hpp>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <unordered_map>

///////////////////////////////////////////////////////////////
// BlockManager implementation
///////////////////////////////////////////////////////////////

// Constructor
BlockManager::BlockManager(std::string _name)
    : name(_name), cur_num_blocks(0), num_reads(0), num_writes(0) {
  internal_mem = new Block[BLOCKS_IN_MEMORY];
  open_blocks = new LRUCache(BLOCKS_IN_MEMORY);
}

// Destructor
BlockManager::~BlockManager() {
  // write back blocks
  uint32_t pos;
  std::unordered_map<uint32_t, LRUNode*>::iterator it = open_blocks->GetBegin();
  std::unordered_map<uint32_t, LRUNode*>::iterator eit = open_blocks->GetEnd();
  for (; it != eit; ++it) {
    pos = open_blocks->Get(it->second->id);
    // printf("write back: pos %d to id %d \n", pos, it->second->id);
    WriteBlock(it->second->id, pos);
  }
  delete[] internal_mem;
  delete open_blocks;
  printf("num block reads: %d\nnum block writes: %d\n", num_reads, num_writes);
}

std::string BlockManager::BlockFilename(uint32_t id) {
  return "./build/app/" + name + "/" + std::to_string(id);
}

// Create Block: Returns block ID
uint32_t BlockManager::CreateBlock() {
  uint32_t id = ++cur_num_blocks;
  std::string filename = BlockFilename(id);
  std::ofstream fout(filename);
  fout.flush();
  fout.close();
  return id;
}

// Delete Block
void BlockManager::DeleteBlock(uint32_t id) {
  std::string filename = BlockFilename(id);
  if (remove(filename.c_str()) != 0) {
    std::string error_msg = "Deleting Block " + std::to_string(id) + " failed!";
    perror(error_msg.c_str());
    exit(1);
  }
}

// Open Block: Returns pos in internal_mem
uint32_t BlockManager::OpenBlock(uint32_t id) {
  // fprintf(stderr, "open_block called: %u\n", id);
  uint32_t pos = open_blocks->Get(id);
  // fprintf(stderr, "pos from get: %u\n", pos);
  if (pos < BLOCKS_IN_MEMORY) return pos;  // already open

  // get a position in internal memory
  uint32_t evicted_id;
  pos = open_blocks->Put(id, &evicted_id);

  // write back old block and read new block from disk to memory
  if (evicted_id > 0) {
    // printf("evicted: %u\n", evicted_id);
    WriteBlock(evicted_id, pos);
  }
  memset(internal_mem[pos].block_buf, 0, sizeof(internal_mem[pos].block_buf));
  ReadBlock(id, pos);

  // return position
  return pos;
}

// TODO: Consider keeping fstream open so that we don't have to open/close twice
// Write Block: Writes the block id back to disk
void BlockManager::WriteBlock(uint32_t id, int pos) {
  // uint32_t pos = open_blocks->get(id);
  if (pos >= BLOCKS_IN_MEMORY) return;  // id is not open
  std::string filename = BlockFilename(id);
  std::ofstream fout(filename, std::ios::out | std::ios::binary);
  fout.write((char*)internal_mem[pos].block_buf, BLOCK_SIZE);
  fout.flush();
  fout.close();
  num_writes++;
}

// Read Block: Reads the block id from disk
void BlockManager::ReadBlock(uint32_t id, int pos) {
  std::string filename = BlockFilename(id);
  std::ifstream fin(filename, std::ios::in | std::ios::binary);
  fin.read((char*)internal_mem[pos].block_buf, BLOCK_SIZE);
  fin.close();
  num_reads++;
}
