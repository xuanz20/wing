#pragma once
#include "execution/executor.hpp"

namespace wing {

class PtVecUpdater {
 public:
  PtVecUpdater(std::unique_ptr<VecExecutor> input) : input_(std::move(input)) {}

  void Execute(const std::string& bloom_filter, BitVector& valid_bits);

 private:
  std::unique_ptr<VecExecutor> input_;
};

}  // namespace wing