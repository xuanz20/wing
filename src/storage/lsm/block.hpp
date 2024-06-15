#pragma once

#include "storage/lsm/format.hpp"
#include "storage/lsm/iterator.hpp"
#include "storage/lsm/options.hpp"

namespace wing {

namespace lsm {

class BlockBuilder {
 public:
  BlockBuilder(size_t block_size, FileWriter* file)
    : block_size_(block_size), file_(file) {}

  /**
   * It appends key and value to the end of the block
   *
   * If it appends successfully, return true.
   * Otherwise, return false, and you need to append it to a new block.
   */
  bool Append(ParsedKey key, Slice value);

  /**
   * It writes all the offsets to the end of the block
   *
   * It is called when the block is full,
   * or there is no more key value pairs.
   * It writes all the offsets to the end of the block.
   * */
  void Finish();

  /* The size of the block (including key, value and the offsets)*/
  size_t size() const { return current_size_; }

  /* The number of key-value pairs. */
  size_t count() const { return offsets_.size(); }

  void Clear() {
    current_size_ = offset_ = 0;
    offsets_.clear();
  }

 private:
  /* The maximum size of a block */
  size_t block_size_{0};
  /* The current used size of the block. */
  size_t current_size_{0};
  /* The current offset of the key value region */
  offset_t offset_{0};
  /* The writer. */
  FileWriter* file_{nullptr};

  /* The offsets of the records in the block. */
  std::vector<offset_t> offsets_;
};

class BlockIterator final : public Iterator {
 public:
  BlockIterator() = default;

  /* data is a pointer to the beginning of the block. */
  BlockIterator(const char* data, BlockHandle handle) : 
    data_(data), 
    handle_(handle), 
    current_offset_(0), 
    index_(0),
    current_key_(Slice()),
    current_value_(Slice()) {
    // DB_ERR("Not implemented!");
  }

  /* Move the the beginning */
  void SeekToFirst();

  /* Find the first record >= (user_key, seq) */
  void Seek(Slice user_key, seq_t seq);

  Slice key() const override;

  Slice value() const override;

  void Next() override;

  bool Valid() override;

 // private:
  const char* data_{nullptr};
  BlockHandle handle_;
  size_t current_offset_;
  offset_t index_;
  Slice current_key_;
  Slice current_value_;
};

}  // namespace lsm

}  // namespace wing
