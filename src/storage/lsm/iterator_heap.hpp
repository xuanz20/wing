#pragma once

#include <queue>

#include "storage/lsm/format.hpp"
#include "storage/lsm/iterator.hpp"
#include "storage/lsm/sst.hpp"

namespace wing {

namespace lsm {

template <typename T>
class IteratorHeap final : public Iterator {
 public:
  IteratorHeap() = default;

  void Push(T* it) { 
    if (it->Valid()) {
      heap_.push(it);
    }
  }

  void Build() { 
  }

  bool Valid() override { 
    return !heap_.empty() && heap_.top()->Valid();
  }

  Slice key() const override { 
    if (heap_.empty() || !heap_.top()->Valid()) return Slice();
    return heap_.top()->key();
  }

  Slice value() const override { 
    if (heap_.empty() || !heap_.top()->Valid()) return Slice();
    return heap_.top()->value();
  }

  void Next() override {
    if (heap_.empty() || !heap_.top()->Valid()) return;
    auto s = heap_.top();
    heap_.pop();
    s->Next();
    heap_.push(s);
  }

 private:
  struct cmp {
    bool operator()(T* a, T* b) {
      if (!a->Valid()) return true;
      if (!b->Valid()) return false;
      return ParsedKey(a->key()
      ) > ParsedKey(b->key());
    }
  };
  std::priority_queue<T*, std::vector<T*>, cmp> heap_;
};

// make sure the SSTable iterator is valid
template <>
inline void IteratorHeap<SSTableIterator>::Push(SSTableIterator* it) {
   it->SeekToFirst();
   heap_.push(it);
}

}  // namespace lsm

}  // namespace wing
