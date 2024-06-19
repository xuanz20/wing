#include "storage/lsm/compaction_pick.hpp"
#include <iostream>

namespace wing {

namespace lsm {

template <typename T>
struct cmp {
  bool operator()(T a, T b) {
    return a->GetSSTInfo().size_ < b->GetSSTInfo().size_;
  }
};


std::unique_ptr<Compaction> LeveledCompactionPicker::Get(Version* version) {
  for (size_t i = 0; i < version->GetLevels().size(); ++i) {
    auto& level = version->GetLevels()[i];

    if (i == 0 && level.GetRuns().size() > level0_compaction_trigger_) {
      auto ssts = std::vector<std::shared_ptr<SSTable>>();
      
      for (auto& run : level.GetRuns())
        ssts.insert(ssts.end(), run->GetSSTs().begin(), run->GetSSTs().end());
      std::shared_ptr<SortedRun> target_sorted_run = nullptr;
      if (version->GetLevels().size() > 1)
        target_sorted_run = version->GetLevels()[i + 1].GetRuns()[0];

      return std::make_unique<Compaction>(
        ssts,
        level.GetRuns(),
        i,
        i + 1,
        target_sorted_run,
        false
      );
    }

    if (i > 0 && level.size() > base_level_size_ * pow(ratio_, i)) { 
      // only need to check the total size of sorted runs

      std::shared_ptr<SortedRun> target_sorted_run = nullptr;
      if (version->GetLevels().size() > i + 1)
        target_sorted_run = version->GetLevels()[i + 1].GetRuns()[0];

      std::vector<std::shared_ptr<SSTable>> ssts;

      // use heap to sort the SSTable with min size
      std::priority_queue<std::shared_ptr<SSTable>, std::vector<std::shared_ptr<SSTable>>, cmp<std::shared_ptr<SSTable>>> sst_heap;

      for (auto& sst : level.GetRuns()[0]->GetSSTs())
        sst_heap.push(sst);

      if (sst_heap.size() > 0)
        ssts.push_back(sst_heap.top());

      return std::make_unique<Compaction>(
        std::move(ssts),
        level.GetRuns(),
        i,
        i + 1,
        target_sorted_run,
        target_sorted_run == nullptr && ssts.size() == 1
      );
    }
  }
  return nullptr;
}

std::unique_ptr<Compaction> TieredCompactionPicker::Get(Version* version) {
  DB_ERR("Not implemented!");
}

std::unique_ptr<Compaction> FluidCompactionPicker::Get(Version* version) {
  DB_ERR("Not implemented!");
}

std::unique_ptr<Compaction> LazyLevelingCompactionPicker::Get(
    Version* version) {
  DB_ERR("Not implemented!");
}

}  // namespace lsm

}  // namespace wing
