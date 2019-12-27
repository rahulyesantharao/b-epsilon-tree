#ifndef SERIALIZABLE_H
#define SERIALIZABLE_H

#include <block_manager/block_manager.hpp>

class Serializable {
 public:
  virtual int Serialize(Block *disk_store, int pos) = 0;
  virtual void Deserialize(const Block &disk_store) = 0;
};

#endif
