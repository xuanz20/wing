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

  SSTableIterator it = Seek(key, seq);
  if (it.Valid() && ParsedKey(it.key()).user_key_ == key) {
    if (ParsedKey(it.key()).type_ == RecordType::Value) {
      *value = std::string(it.value());
      return GetResult::kFound;
    } else {
      return GetResult::kDelete;
    }
  }
  return GetResult::kNotFound;

  /*
  for (const auto& index_value : index_) {
    if (index_value.key_.user_key() < key) {
      continue;
    }
    BlockHandle block_handle = index_value.block_;
    AlignedBuffer buffer(block_handle.size_, 4096);
    file_->Read(buffer.data(), block_handle.size_, block_handle.offset_);
    BlockIterator it(buffer.data(), block_handle);
    it.Seek(key, seq); // Find key0 == key && largest seq0 <= seq
    if (it.Valid() && ParsedKey(it.key()).user_key_ == key) {
      if (ParsedKey(it.key()).type_ == RecordType::Value) {
        *value = std::string(it.value());
        return GetResult::kFound;
      } else {
        return GetResult::kDelete;
      }
    }
  }
  return GetResult::kNotFound;
  */
}

SSTableIterator SSTable::Seek(Slice key, uint64_t seq) {
  SSTableIterator it = Begin();
  it.Seek(key, seq);
  return it;
}

SSTableIterator SSTable::Begin() { 
  SSTableIterator it(this);
  it.SeekToFirst();
  return it;
}

void SSTableIterator::Seek(Slice key, uint64_t seq) {
  /*
  for (block_id_ = 0; block_id_ < sst_->index_.size(); ++block_id_) {
    IndexValue block_index = sst_->index_[block_id_];
    InternalKey block_key = block_index.key_;
    if (
      block_key.user_key() < key ||
      (block_key.user_key() == key && block_key.seq() > seq)
    ) {
      continue;
    }
    const auto& block_handle = sst_->index_[block_id_].block_;
    // AlignedBuffer buffer(block_handle.size_, 4096);
    buf_ = AlignedBuffer(block_handle.size_, 4096);
    sst_->file_->Read(buf_.data(), block_handle.size_, block_handle.offset_);
    block_it_ = BlockIterator(buf_.data(), block_handle);
    block_it_.Seek(key, seq);
    return;
  }
  */

  ParsedKey target_key(key, seq, RecordType::Value);
  size_t left = 0;
  size_t right = sst_->index_.size();
  while (left < right) {
    size_t mid = (left + right) / 2;
    ParsedKey mid_parsed_key = sst_->index_[mid].key_;
    if (mid_parsed_key < target_key) left = mid + 1;
    else right = mid;
  }

  block_id_ = left;
  if (block_id_ < sst_->index_.size()) {
    const auto& block_handle = sst_->index_[block_id_].block_;
    // AlignedBuffer buffer(block_handle.size_, 4096);
    buf_ = AlignedBuffer(block_handle.size_, 4096);
    sst_->file_->Read(buf_.data(), block_handle.size_, block_handle.offset_);
    block_it_ = BlockIterator(buf_.data(), block_handle);
    block_it_.Seek(key, seq);
  }
  return;
}

void SSTableIterator::SeekToFirst() {
  block_id_ = 0;
  if (sst_->index_.size() == 0) {
    block_it_ = BlockIterator();
    buf_ = AlignedBuffer();
    return;
  }
  const auto& block_handle = sst_->index_[block_id_].block_;
  buf_ = AlignedBuffer(block_handle.size_, 4096);
  sst_->file_->Read(buf_.data(), block_handle.size_, block_handle.offset_);
  block_it_ = BlockIterator(buf_.data(), block_handle);
  block_it_.SeekToFirst();
}

bool SSTableIterator::Valid() { // TODO
  return block_it_.Valid();
}

Slice SSTableIterator::key() const {
  return block_it_.key();
}

Slice SSTableIterator::value() const {
  return block_it_.value();
}

void SSTableIterator::Next() {
  block_it_.Next();
  if (!block_it_.Valid() && block_id_ < sst_->index_.size() - 1) {
    // move to next block
    ++block_id_;
    const auto& block_handle = sst_->index_[block_id_].block_;
    buf_ = AlignedBuffer(block_handle.size_, 4096);
    sst_->file_->Read(buf_.data(), block_handle.size_, block_handle.offset_);
    block_it_ = BlockIterator(buf_.data(), block_handle);
    block_it_.SeekToFirst();
  }
}

void SSTableBuilder::Append(ParsedKey key, Slice value) {
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
  if (smallest_key_.user_key().size() == 0 || key < smallest_key_) {
    smallest_key_ = key;
  }
  if (largest_key_.user_key().size() == 0 || key > largest_key_) {
    largest_key_ = key;
  }
  ++count_;
}

void SSTableBuilder::Finish() { 
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
