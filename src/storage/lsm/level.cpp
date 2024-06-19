#include "storage/lsm/level.hpp"

#include <iostream>

namespace wing {

namespace lsm {

GetResult SortedRun::Get(Slice key, uint64_t seq, std::string* value) {
  ParsedKey target_key(key, seq, RecordType::Value);

  /*
  SortedRunIterator it = Seek(key, seq);
  if (it.Valid() && ParsedKey(it.key()).user_key_ == key) {
    if (ParsedKey(it.key()).type_ == RecordType::Value) {
      *value = std::string(it.value());
      return GetResult::kFound;
    } else {
      return GetResult::kDelete;
    }
  }
  return GetResult::kNotFound;
  */

  size_t left = 0, right = ssts_.size();
  while (left < right) {
    size_t mid = (left + right) / 2;
    // auto res = ssts_[mid]->Get(key, seq, value);
    auto mid_parsed_key = ssts_[mid]->GetLargestKey();
    if (mid_parsed_key < target_key) left = mid + 1;
    else right = mid;
  }

  if (left < ssts_.size()) {
    return ssts_[left]->Get(key, seq, value);
  }
  return GetResult::kNotFound;

  /*
  for (int i = ssts_.size() - 1; i >= 0; --i) {
    auto res = ssts_[i]->Get(key, seq, value);
    if (res != GetResult::kNotFound) return res;
  }
  return GetResult::kNotFound;
  */
}

SortedRunIterator SortedRun::Seek(Slice key, uint64_t seq) {
  SortedRunIterator it = Begin();
  it.Seek(key, seq);
  return it;
}

SortedRunIterator SortedRun::Begin() { 
  SortedRunIterator it(this, SSTableIterator(), 0);
  it.SeekToFirst();
  return it;
}

SortedRun::~SortedRun() {
  if (remove_tag_) {
    for (auto sst : ssts_) {
      sst->SetRemoveTag(true);
    }
  }
}

void SortedRunIterator::Seek(Slice key, uint64_t seq) {

  /*
  for (sst_id_ = 0; sst_id_ < run_->ssts_.size(); ++sst_id_) {
    ParsedKey largest_key = run_->ssts_[sst_id_]->GetLargestKey();
    if (largest_key < ParsedKey(key, seq, RecordType::Value)) {
      continue;
    }
    sst_it_ = run_->ssts_[sst_id_].get()->Begin();
    sst_it_.Seek(key, seq);
    return;
  }
  */

  ParsedKey target_key(key, seq, RecordType::Value);
  size_t left = 0;
  size_t right = run_->ssts_.size();
  while (left < right) {
    size_t mid = (left + right) / 2;
    ParsedKey mid_parsed_key = run_->ssts_[mid]->GetLargestKey();
    if (mid_parsed_key < target_key) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }

  sst_id_ = left;
  if (sst_id_ < run_->ssts_.size()) {
    sst_it_ = run_->ssts_[sst_id_].get()->Begin();
    sst_it_.Seek(key, seq);
  }
  return;
}

void SortedRunIterator::SeekToFirst() { 
  if (run_->ssts_.size() == 0) {
    sst_it_ = SSTableIterator();
    sst_id_ = 0;
  }
  SSTable* first_sst = run_->ssts_[0].get();
  sst_it_ = first_sst->Begin();
  sst_id_ = 0;
}

bool SortedRunIterator::Valid() {
  return sst_it_.Valid();
}

Slice SortedRunIterator::key() const {
  return sst_it_.key();
}

Slice SortedRunIterator::value() const {
  return sst_it_.value();
}

void SortedRunIterator::Next() { 
  sst_it_.Next();
  if (!sst_it_.Valid() && sst_id_ < run_->ssts_.size() - 1) {
    // move to next SSTable
    ++sst_id_;
    sst_it_ = run_->ssts_[sst_id_]->Begin();
    sst_it_.SeekToFirst();
  }
}

GetResult Level::Get(Slice key, uint64_t seq, std::string* value) {

  for (int i = runs_.size() - 1; i >= 0; --i) {
    auto res = runs_[i]->Get(key, seq, value);
    if (res != GetResult::kNotFound) {
      return res;
    }
  }
  return GetResult::kNotFound;
}

void Level::Append(std::vector<std::shared_ptr<SortedRun>> runs) {
  for (auto& run : runs) {
    size_ += run->size();
  }
  runs_.insert(runs_.end(), std::make_move_iterator(runs.begin()),
      std::make_move_iterator(runs.end()));
}

void Level::Append(std::shared_ptr<SortedRun> run) {
  size_ += run->size();
  runs_.push_back(std::move(run));
}

}  // namespace lsm

}  // namespace wing
