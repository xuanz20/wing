#pragma once

#include "storage/lsm/sst.hpp"
#include <iostream>

namespace wing {

namespace lsm {

class CompactionJob {
 public:
  CompactionJob(FileNameGenerator* gen, size_t block_size, size_t sst_size,
      size_t write_buffer_size, size_t bloom_bits_per_key, bool use_direct_io)
    : file_gen_(gen),
      block_size_(block_size),
      sst_size_(sst_size),
      write_buffer_size_(write_buffer_size),
      bloom_bits_per_key_(bloom_bits_per_key),
      use_direct_io_(use_direct_io) {}

  /**
   * It receives an iterator and returns a list of SSTable
   */
  template <typename IterT>
  std::vector<SSTInfo> Run(IterT&& it) {
    std::vector<SSTInfo> sst_info_list;

    auto filename = file_gen_->Generate();
    SSTableBuilder builder(
      std::make_unique<FileWriter>(
        std::make_unique<SeqWriteFile>(
          filename.first, use_direct_io_),
        1 << 20),
      block_size_, bloom_bits_per_key_
    );

    std::string last_user_key;
    bool first = true;

    while (it.Valid()) {
      ParsedKey current_key(it.key());
      Slice current_value = it.value();

      if (first) {
        first = false;
        last_user_key = current_key.user_key_;
        builder.Append(current_key, current_value);
        it.Next();
        continue;
      }


      if (!first && current_key.user_key_ == last_user_key) {
        it.Next();
        continue;
      }

      // new user key
      last_user_key = current_key.user_key_;

      size_t entry_size = sizeof(offset_t) * 3 + current_key.size() + current_value.size();
      if (builder.size() + entry_size <= sst_size_) {
        builder.Append(current_key, current_value);
      } else {
        builder.Finish();
        SSTInfo info;
        info.count_ = builder.count();
        info.filename_ = filename.first;
        info.index_offset_ = builder.GetIndexOffset();
        info.bloom_filter_offset_ = builder.GetBloomFilterOffset();
        info.size_ = builder.size();
        info.sst_id_ = filename.second;
        sst_info_list.push_back(info);

        // create a new SSTable builder
        filename = file_gen_->Generate();
        
        builder = SSTableBuilder{
          std::make_unique<FileWriter>(
            std::make_unique<SeqWriteFile>(
              filename.first, use_direct_io_),
            1 << 20),
          block_size_, bloom_bits_per_key_
        };

        builder.Append(current_key, current_value);
      }
      it.Next();
    }

    if (builder.count() > 0) {
      builder.Finish();
      SSTInfo info;
      info.count_ = builder.count();
      info.filename_ = filename.first;
      info.index_offset_ = builder.GetIndexOffset();
      info.bloom_filter_offset_ = builder.GetBloomFilterOffset();
      info.size_ = builder.size();
      info.sst_id_ = filename.second;
      sst_info_list.push_back(info);   
    }

    return sst_info_list;
  }

 private:
  /* Generate new SSTable file name */
  FileNameGenerator* file_gen_;
  /* The target block size */
  size_t block_size_;
  /* The target SSTable size */
  size_t sst_size_;
  /* The size of write buffer in FileWriter */
  size_t write_buffer_size_;
  /* The number of bits per key in bloom filter */
  size_t bloom_bits_per_key_;
  /* Use O_DIRECT or not */
  bool use_direct_io_;
};

}  // namespace lsm

}  // namespace wing
