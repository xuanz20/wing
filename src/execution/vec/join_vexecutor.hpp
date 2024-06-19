#pragma once

#include "execution/executor.hpp"
#include "execution/vec/expr_vexecutor.hpp"
#include "plan/plan.hpp"
#include "type/tuple_batch.hpp"

namespace wing {

class JoinVecExecutor : public VecExecutor {
public:
    JoinVecExecutor(
      const ExecOptions& options, 
      const std::unique_ptr<Expr>& expr,
      const OutputSchema& input_schema,
      std::unique_ptr<VecExecutor> left_child,
      std::unique_ptr<VecExecutor> right_child
    ) : VecExecutor(options),
        pred_(JoinExprVecExecutor::Create(expr.get(), input_schema)),
        left_child_(std::move(left_child)), 
        right_child_(std::move(right_child)) {}

    void Init() override {
        left_child_->Init();
        right_child_->Init();
    }

protected:
    TupleBatch InternalNext() override {
      auto right_child = std::make_unique<VecExecutor>(right_child_.get());
      auto left_ret = left_child_->Next();
      if (left_ret.size() == 0) {
        return {};
      }


      if (pred_) {
        while (true) {
          auto right_ret = right_child_->Next();
          if (right_ret.size() == 0) {
            break;
          } else {
            pred_.Evaluate(left_ret.GetCols(), right_ret.GetCols(), left_ret.size(), pred_result_);
            for (uint32_t i = 0; i < left_ret.size(); i++)
              if (pred_result_.Get(i).ReadInt() == 0) {
                left_ret.SetValid(i, false);
              }
          }
        }
      }
      right_child_ = std::move(right_child);
      return left_ret;
    }

    virtual size_t GetTotalOutputSize() const override {
      return left_child_->GetTotalOutputSize() + stat_output_size_;
    }

 private:
  std::unique_ptr<VecExecutor> left_child_;
  std::unique_ptr<VecExecutor> right_child_;
  JoinExprVecExecutor pred_;
  Vector pred_result_;

  friend class JoinExprVecExecutor;
};

class JoinExprVecExecutor {
 public:
  void Init();

  void Evaluate(
    std::span<Vector> left, 
    std::span<Vector> right, 
    size_t count, 
    Vector& result
  ) 
  {

  }

  static JoinExprVecExecutor Create(
    const Expr* expr, const OutputSchema& input_schema
  );

  operator bool() const { return IsValid(); }

  bool IsValid() const { return bool(func_); }


 private:
  std::function<int(std::span<Vector>, std::span<Vector>, size_t, Vector&)> func_;
  std::unique_ptr<VecExecutor> ch_;
  std::unique_ptr<VecExecutor> ch2_;
  std::vector<Vector> imm_;
};



} // namespace wing
