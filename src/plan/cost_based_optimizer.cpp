#include <queue>

#include "plan/optimizer.hpp"
#include "plan/predicate_transfer/pt_graph.hpp"
#include "rules/convert_to_hash_join.hpp"

namespace wing {

std::unique_ptr<PlanNode> Apply(std::unique_ptr<PlanNode> plan,
    const std::vector<std::unique_ptr<OptRule>>& rules, const DB& db) {
  for (auto& a : rules) {
    if (a->Match(plan.get())) {
      plan = a->Transform(std::move(plan));
      break;
    }
  }
  if (plan->ch2_ != nullptr) {
    plan->ch_ = Apply(std::move(plan->ch_), rules, db);
    plan->ch2_ = Apply(std::move(plan->ch2_), rules, db);
  } else if (plan->ch_ != nullptr) {
    plan->ch_ = Apply(std::move(plan->ch_), rules, db);
  }
  return plan;
}

size_t GetTableNum(const PlanNode* plan) {
  /* We don't want to consider values clause in cost based optimizer. */
  if (plan->type_ == PlanType::Print) {
    return 10000;
  }

  if (plan->type_ == PlanType::SeqScan) {
    return 1;
  }

  size_t ret = 0;
  if (plan->ch2_ != nullptr) {
    ret += GetTableNum(plan->ch_.get());
    ret += GetTableNum(plan->ch2_.get());
  } else if (plan->ch_ != nullptr) {
    ret += GetTableNum(plan->ch_.get());
  }
  return ret;
}

bool CheckIsAllJoin(const PlanNode* plan) {
  if (plan->type_ == PlanType::Print || plan->type_ == PlanType::SeqScan ||
      plan->type_ == PlanType::RangeScan) {
    return true;
  }
  if (plan->type_ != PlanType::Join) {
    return false;
  }
  return CheckIsAllJoin(plan->ch_.get()) && CheckIsAllJoin(plan->ch2_.get());
}

bool CheckHasStat(const PlanNode* plan, const DB& db) {
  if (plan->type_ == PlanType::Print) {
    return false;
  }
  if (plan->type_ == PlanType::SeqScan) {
    auto stat =
        db.GetTableStat(static_cast<const SeqScanPlanNode*>(plan)->table_name_);
    return stat != nullptr;
  }
  if (plan->type_ == PlanType::RangeScan) {
    auto stat = db.GetTableStat(
        static_cast<const RangeScanPlanNode*>(plan)->table_name_);
    return stat != nullptr;
  }
  if (plan->type_ != PlanType::Join) {
    return false;
  }
  return CheckHasStat(plan->ch_.get(), db) &&
         CheckHasStat(plan->ch2_.get(), db);
}

/**
 * Check whether we can use cost based optimizer.
 * For simplicity, we only use cost based optimizer when:
 * (1) The root plan node is Project, and there is only one Project.
 * (2) The other plan nodes can only be Join or SeqScan or RangeScan.
 * (3) The number of tables is <= 20.
 * (4) All tables have statistics or true cardinality is provided.
 */
bool CheckCondition(const PlanNode* plan, const DB& db) {
  if (GetTableNum(plan) > 20)
    return false;
  if (plan->type_ != PlanType::Project && plan->type_ != PlanType::Aggregate)
    return false;
  if (!CheckIsAllJoin(plan->ch_.get()))
    return false;
  return db.GetOptions().optimizer_options.true_cardinality_hints ||
         CheckHasStat(plan->ch_.get(), db);
}

std::unique_ptr<PlanNode> CostBasedOptimizer::Optimize(
    std::unique_ptr<PlanNode> plan, DB& db) {
  if (CheckCondition(plan.get(), db) &&
      db.GetOptions().optimizer_options.enable_cost_based) {
    // std::vector<std::unique_ptr<OptRule>> R;
    // R.push_back(std::make_unique<ConvertToHashJoinRule>());
    // plan = Apply(std::move(plan), R, db);
    // TODO...

  } else {
    std::vector<std::unique_ptr<OptRule>> R;
    R.push_back(std::make_unique<ConvertToHashJoinRule>());
    plan = Apply(std::move(plan), R, db);
  }
  if (db.GetOptions().exec_options.enable_predicate_transfer) {
    if (plan->type_ != PlanType::Insert && plan->type_ != PlanType::Delete &&
        plan->type_ != PlanType::Update) {
      auto pt_plan = std::make_unique<PredicateTransferPlanNode>();
      pt_plan->graph_ = std::make_shared<PtGraph>(plan.get());
      pt_plan->output_schema_ = plan->output_schema_;
      pt_plan->table_bitset_ = plan->table_bitset_;
      pt_plan->ch_ = std::move(plan);
      plan = std::move(pt_plan);
    }
  }
  return plan;
}

}  // namespace wing
