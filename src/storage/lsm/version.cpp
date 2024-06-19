#include "storage/lsm/version.hpp"

namespace wing {

namespace lsm {

bool Version::Get(std::string_view user_key, seq_t seq, std::string* value) {
  for (int i = 0; i < levels_.size(); ++i) {
    auto res = levels_[i].Get(user_key, seq, value);
    if (res != GetResult::kNotFound) {
      if (res == GetResult::kFound) return true;
      return false;
    }
  }
  return false;
}

void Version::Append(
    uint32_t level_id, std::vector<std::shared_ptr<SortedRun>> sorted_runs) {
  while (levels_.size() <= level_id) {
    levels_.push_back(Level(levels_.size()));
  }
  levels_[level_id].Append(std::move(sorted_runs));
}
void Version::Append(uint32_t level_id, std::shared_ptr<SortedRun> sorted_run) {
  while (levels_.size() <= level_id) {
    levels_.push_back(Level(levels_.size()));
  }
  levels_[level_id].Append(std::move(sorted_run));
}

bool SuperVersion::Get(
    std::string_view user_key, seq_t seq, std::string* value) {
  auto res = mt_->Get(user_key, seq, value);
  if (res == GetResult::kFound) return true;
  if (res == GetResult::kDelete) return false;

  for (int i = imms_->size() - 1; i >= 0; --i) {
    auto res = (*imms_)[i]->Get(user_key, seq, value);
    if (res == GetResult::kFound) return true;
    if (res == GetResult::kDelete) return false;
  }

  /*
  size_t left = imms_->size(), right = 0;
  while (left > right) {
    size_t mid = (left + right) / 2;
    auto res = (*imms_)[mid]->GetTable().upper_bound(user_key);
    if (res == (*imms_)[mid]->GetTable().end()) right = mid + 1;
    else left = mid;
    // auto res = (*imms_)[mid]->Get(user_key, seq, value);
    // if (res == GetResult::kNotFound) right = mid + 1;
    // else left = mid;

  }

  if (left < imms_->size()) {
    auto res = (*imms_)[left]->Get(user_key, seq, value);
    if (res == GetResult::kFound) return true;
    if (res == GetResult::kDelete) return false;
  }
  */
  return version_->Get(user_key, seq, value);
}

std::string SuperVersion::ToString() const {
  std::string ret;
  ret += fmt::format("Memtable: size {}, ", mt_->size());
  ret += fmt::format("Immutable Memtable: size {}, ", imms_->size());
  ret += fmt::format("Tree: [ ");
  for (auto& level : version_->GetLevels()) {
    size_t num_sst = 0;
    for (auto& run : level.GetRuns()) {
      num_sst += run->SSTCount();
    }
    ret += fmt::format("{}, ", num_sst);
  }
  ret += "]";
  return ret;
}

void SuperVersionIterator::SeekToFirst() { 
  it_ = IteratorHeap<Iterator>();
  for (int i = 0; i < mt_its_.size(); ++i) {
    mt_its_[i].SeekToFirst();
    it_.Push(&mt_its_[i]);
  }
  for (int i = 0; i < sst_its_.size(); ++i) {
    sst_its_[i].SeekToFirst();
    it_.Push(&sst_its_[i]);
  }
}

void SuperVersionIterator::Seek(Slice key, seq_t seq) {
  it_ = IteratorHeap<Iterator>();
  // SeekToFirst();
  for (int i = 0; i < mt_its_.size(); ++i) {
    mt_its_[i].Seek(key, seq);
    if (mt_its_[i].Valid()) it_.Push(&mt_its_[i]);
  }
  for (int i = 0; i < sst_its_.size(); ++i) {
    sst_its_[i].Seek(key, seq);
    if (sst_its_[i].Valid()) it_.Push(&sst_its_[i]);
  }
}

bool SuperVersionIterator::Valid() { 
  return it_.Valid();
}

Slice SuperVersionIterator::key() const { 
  return it_.key();
}

Slice SuperVersionIterator::value() const { 
  return it_.value();
}

void SuperVersionIterator::Next() { 
  return it_.Next();
}

}  // namespace lsm

}  // namespace wing
