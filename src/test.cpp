#include <cassert>
#include <fstream>
#include <iostream>

#include <be_tree/be_tree.hpp>

int main() {
  std::cout << "Startup!" << std::endl;
  BeTree tree("tree");
  uint32_t size = 100000u;
  uint32_t test = 1;

  switch (test) {
    case 0:
      for (uint32_t i = 1u; i <= size; i++) {
        tree.Insert(i, i);
        uint32_t q = tree.Query(i);
        assert(q == i);
      }
      for (uint32_t i = 1u; i <= size; i++) {
        uint32_t q = tree.Query(i);
        assert(q == i);
      }
      break;

    case 1:
      for (int i = size; i >= 1u; i--) {
        tree.Insert(i, size - i);
        assert(tree.Query(i) == size - i);
      }
      for (int i = 1u; i <= size; i++) {
        assert(tree.Query(i) == size - i);
      }
      break;
  }
}
