#include "storage/lsm/sst.hpp"
#include <iostream>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <fstream>

#include "common/bloomfilter.hpp"

namespace wing {

namespace lsm {

SSTable::SSTable(SSTInfo sst_info, size_t block_size, bool use_direct_io)
  : sst_info_(std::move(sst_info)), block_size_(block_size) {
  // DB_ERR("Not implemented!");
  file_ = std::make_unique<ReadFile>(sst_info_.filename_, use_direct_io);

  FileReader reader_(file_.get(), 1 << 30, 0);
  reader_.Seek(sst_info_.index_offset_);
  size_t index_count = reader_.ReadValue<size_t>();
  for (size_t i = 0; i < index_count; ++i) {
    size_t key_length = reader_.ReadValue<size_t>();
    IndexValue index_value;
    index_value.key_ = reader_.ReadString(key_length);
    index_value.block_.offset_ = reader_.ReadValue<offset_t>();
    index_value.block_.size_ = reader_.ReadValue<offset_t>();
    index_value.block_.count_ = reader_.ReadValue<offset_t>();
    index_.push_back(index_value);
  }

  size_t bloom_filter_length = reader_.ReadValue<size_t>();
  bloom_filter_ = reader_.ReadString(bloom_filter_length);

  size_t smallest_key_length = reader_.ReadValue<size_t>();
  smallest_key_ = reader_.ReadString(smallest_key_length);
  size_t largest_key_length = reader_.ReadValue<size_t>();
  largest_key_ = reader_.ReadString(largest_key_length);
}

SSTable::~SSTable() {
  if (remove_tag_) {
    file_.reset();
    std::filesystem::remove(sst_info_.filename_);
  }
}

// find key0 == key && seq0 <= seq
GetResult SSTable::Get(Slice key, uint64_t seq, std::string* value) {
  if (!utils::BloomFilter::Find(key, bloom_filter_)) {
    return GetResult::kNotFound;
  }
  for (const auto& index_value : index_) {
    if (index_value.key_.user_key() < key) {
      continue;
    }
    BlockHandle block_handle = index_value.block_;
    AlignedBuffer buffer(block_handle.size_, 4096);
    file_->Read(buffer.data(), block_handle.size_, block_handle.offset_);
    BlockIterator it(buffer.data(), block_handle);
    it.Seek(key, seq); // Find key0 == key && largest seq0 <= seq
    if (it.Valid() && ParsedKey(it.key()).user_key_ == key) { // 
      // if (ParsedKey(it.key()).seq_ > seq) return GetResult::kNotFound;
      if (ParsedKey(it.key()).type_ == RecordType::Value) {
        *value = std::string(it.value());
        return GetResult::kFound;
      } else {
        return GetResult::kDelete;
      }
    }
  }
  return GetResult::kNotFound;
}

SSTableIterator SSTable::Seek(Slice key, uint64_t seq) {
  DB_ERR("Not implemented!");
}

SSTableIterator SSTable::Begin() { DB_ERR("Not implemented!"); }

void SSTableIterator::Seek(Slice key, uint64_t seq) {
  DB_ERR("Not implemented!");
}

void SSTableIterator::SeekToFirst() { DB_ERR("Not implemented!"); }

bool SSTableIterator::Valid() { DB_ERR("Not implemented!"); }

Slice SSTableIterator::key() const { DB_ERR("Not implemented!"); }

Slice SSTableIterator::value() const { DB_ERR("Not implemented!"); }

void SSTableIterator::Next() { DB_ERR("Not implemented!"); }

void SSTableBuilder::Append(ParsedKey key, Slice value) {
  // DB_ERR("Not implemented!");

  if (!block_builder_.Append(key, value)) {
    block_builder_.Finish();
    IndexValue index_value;
    index_value.key_ = block_builder_.GetLastKey();
    index_value.block_ = { (offset_t)current_block_offset_, (offset_t)block_builder_.size(), (offset_t)block_builder_.count() };
    index_data_.push_back(index_value);
    current_block_offset_ += block_builder_.size();
    block_builder_.Clear();
    block_builder_.Append(key, value);
  }

  size_t key_hash = utils::BloomFilter::BloomHash(key.user_key_);
  key_hashes_.push_back(key_hash);
  if (smallest_key_.size() == 0 || key < smallest_key_) {
    smallest_key_ = key;
  }
  if (largest_key_.size() == 0 || key > largest_key_) {
    largest_key_ = key;
  }
  ++count_;
}

void SSTableBuilder::Finish() { 
  // DB_ERR("Not implemented!"); 

  block_builder_.Finish();
  IndexValue index_value;
  index_value.key_ = block_builder_.GetLastKey();
  index_value.block_ = { (offset_t)current_block_offset_, (offset_t)block_builder_.size(), (offset_t)block_builder_.count() };
  index_data_.push_back(index_value);
  current_block_offset_ += block_builder_.size();
  block_builder_.Clear();

  index_offset_ = current_block_offset_;
  writer_->AppendValue<size_t>(index_data_.size());
  current_block_offset_ += sizeof(size_t);
  for (const auto& index_value : index_data_) {
    writer_->AppendValue<size_t>(index_value.key_.size());
    writer_->AppendString(index_value.key_.GetSlice());
    writer_->AppendValue<offset_t>(index_value.block_.offset_);
    writer_->AppendValue<offset_t>(index_value.block_.size_);
    writer_->AppendValue<offset_t>(index_value.block_.count_);
    current_block_offset_ += sizeof(size_t) + index_value.key_.size() + sizeof(offset_t) * 3;
  }

  bloom_filter_offset_ = current_block_offset_;
  std::string bloom_bits;
  utils::BloomFilter::Create(key_hashes_.size(), bloom_bits_per_key_, bloom_bits);
  for (const auto& hash : key_hashes_) {
    utils::BloomFilter::Add(hash, bloom_bits);
  }
  writer_->AppendValue<size_t>(bloom_bits.size());
  writer_->AppendString(bloom_bits);

  writer_->AppendValue<size_t>(smallest_key_.size());
  writer_->AppendString(smallest_key_.GetSlice());
  writer_->AppendValue<size_t>(largest_key_.size());
  writer_->AppendString(largest_key_.GetSlice());

  writer_->Flush();
}

}  // namespace lsm

}  // namespace wing
