#include "storage/lsm/block.hpp"
#include <iostream>

namespace wing {

namespace lsm {

bool BlockBuilder::Append(ParsedKey key, Slice value) {
  // Calculate the size needed to store the key and value
  size_t key_size = key.size();
  size_t value_size = value.size();
  size_t entry_size = sizeof(offset_t) * 3 + key_size + value_size;

  // Check if adding this entry would exceed the block size
  if (current_size_ + entry_size > block_size_) {
    return false;
  }

  // Append the key to the block
  offset_t key_length = static_cast<offset_t>(key_size);
  file_->Append(reinterpret_cast<const char*>(&key_length), sizeof(offset_t));
  // file_->Append(reinterpret_cast<const char*>(&key), key_size);
  file_->Append(key.user_key_.data(), key.user_key_.size());
  file_->Append(reinterpret_cast<const char*>(&key.seq_), sizeof(key.seq_));
  file_->Append(reinterpret_cast<const char*>(&key.type_), sizeof(key.type_));

  // Append the value to the block
  offset_t value_length = static_cast<offset_t>(value_size);
  file_->Append(reinterpret_cast<const char*>(&value_length), sizeof(offset_t));
  file_->Append(value.data(), value_size);

  // Store the offset for this entry
  offsets_.push_back(offset_);

  // Update the current size and offset
  current_size_ += entry_size;
  offset_ += sizeof(offset_t) * 2 + key_size + value_size;
  last_key_ = key;

  return true;
}

void BlockBuilder::Finish() { 
  // If there are no offsets, there is nothing to finish
  if (offsets_.empty()) {
    return;
  }

  // Write each offset to the block
  for (const auto& offset : offsets_) {
    file_->Append(reinterpret_cast<const char*>(&offset), sizeof(offset));
  }
}

/* Find key0 == user_key && largest seq0 <= seq */
/* If not found, return the sentinel (invalid) */
void BlockIterator::Seek(Slice user_key, seq_t seq) {
  // linear search
  /*
  ParsedKey target_key(user_key, seq, RecordType::Value);
  SeekToFirst();
  while (Valid() && !(ParsedKey(current_key_) >= target_key)) {
    Next();
  }

  if (handle_.count_ == 0) {
    current_key_ = Slice();
    current_value_ = Slice();
    return;
  }
  */

  ParsedKey target_key(user_key, seq, RecordType::Value);

  const char* offset_array_start = data_ + handle_.size_ - handle_.count_ * sizeof(offset_t);
  const offset_t* offsets = reinterpret_cast<const offset_t*>(offset_array_start);

  size_t left = 0;
  size_t right = handle_.count_;
  while (left < right) {
    size_t mid = (left + right) / 2;
    const char* key_ptr = data_ + offsets[mid];
    offset_t key_length = *reinterpret_cast<const offset_t*>(key_ptr);
    Slice mid_key(key_ptr + sizeof(offset_t), key_length);
    // seq_t mid_seq = *reinterpret_cast<const seq_t*>(key_ptr + sizeof(offset_t) + key_length);

    ParsedKey mid_parsed_key(mid_key);
    if (mid_parsed_key < target_key) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }

  index_ = left;
  if (index_ < handle_.count_) {
    current_offset_ = offsets[index_];
    offset_t key_length = *reinterpret_cast<const offset_t*>(data_ + current_offset_);
    current_key_ = Slice(data_ + current_offset_ + sizeof(offset_t), key_length);
    offset_t value_length = *reinterpret_cast<const offset_t*>(data_ + current_offset_ + sizeof(offset_t) + key_length);
    current_value_ = Slice(data_ + current_offset_ + sizeof(offset_t) * 2 + key_length, value_length);
  } else {
    current_key_ = Slice();
    current_value_ = Slice();
  }
}

void BlockIterator::SeekToFirst() {
  current_offset_ = 0;
  index_ = 0;
  if (handle_.count_ > 0) {
    offset_t key_length = *reinterpret_cast<const offset_t*>(data_);
    current_key_ = Slice(data_ + sizeof(offset_t), key_length);
    offset_t value_length = *reinterpret_cast<const offset_t*>(data_ + sizeof(offset_t) + key_length);
    current_value_ = Slice(data_ + sizeof(offset_t) * 2 + key_length, value_length);
  } else {
    current_key_ = Slice();
    current_value_ = Slice();
  }
}

Slice BlockIterator::key() const {
  return current_key_;
}

Slice BlockIterator::value() const {
  return current_value_;
}

void BlockIterator::Next() {
  if (!Valid()) return;
  index_ ++;
  current_offset_ += sizeof(offset_t) * 2 + current_key_.size() + current_value_.size();
  if (index_ == handle_.count_) {
    current_key_ = Slice();
    current_value_ = Slice();
    return;
  }
  offset_t key_length = *reinterpret_cast<const offset_t*>(data_ + current_offset_);
  current_key_ = Slice(data_ + current_offset_ + sizeof(offset_t), key_length);
  offset_t value_length = *reinterpret_cast<const offset_t*>(data_ + current_offset_ + sizeof(offset_t) + current_key_.size());
  current_value_ = Slice(data_ + current_offset_ + sizeof(offset_t) * 2 + current_key_.size(), value_length);
}

bool BlockIterator::Valid() { 
  return handle_.count_ > 0 && index_ < handle_.count_;
}

}  // namespace lsm

}  // namespace wing
