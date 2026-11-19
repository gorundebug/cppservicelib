#pragma once

#include <cstdint>
#include <stdexcept>
#include <utility>

#include <servicelib/dataquery/endpoint.hpp>
#include <userver/storages/postgres/cluster.hpp>
#include <userver/storages/postgres/portal.hpp>
#include <userver/storages/postgres/transaction.hpp>

namespace servicelib::dataquery::postgres {
namespace detail {
inline void checkContext(const MessageContext& context) {
  if (context.cancelled()) throw OperationCancelledError("Query cancelled");
  userver::engine::current_task::CancellationPoint();
}

// Keep rollback ahead of EndRequest, including cancellation and mapper failure.
// Transaction's native destructor returns the connection to its owning pool.
class Rollback final {
 public:
  explicit Rollback(userver::storages::postgres::Transaction& transaction)
      : transaction_(transaction) {}
  Rollback(const Rollback&) = delete;
  Rollback& operator=(const Rollback&) = delete;
  ~Rollback() {
    if (committed_) return;
    userver::engine::TaskCancellationBlocker blocker;
    try { transaction_.Rollback(); } catch (const std::exception&) {}
  }
  void committed() noexcept { committed_ = true; }
 private:
  userver::storages::postgres::Transaction& transaction_;
  bool committed_{false};
};
}  // namespace detail

// buildQuery(context, state, input) returns a callable taking Transaction&.
// It uses native MakePortal(query, typedArgs...) so no parameter types are
// erased, SQL is not interpolated, and application query construction stays code.
class Rows final {
 public:
  Rows(userver::storages::postgres::ClusterPtr cluster, std::uint32_t fetchSize)
      : cluster_(std::move(cluster)), fetchSize_(fetchSize) {
    if (!cluster_) throw std::invalid_argument("Query PostgreSQL cluster is missing");
    if (!fetchSize_) throw std::invalid_argument("Query fetch size must be positive");
    if (!userver::storages::postgres::Portal::IsSupportedByDriver()) {
      throw std::invalid_argument("Query Rows requires native userver PostgreSQL portal support");
    }
  }

  template <typename State, typename T, typename Handler, typename R>
  void execute(MessageContext context, State& state, const T& input,
               Handler& handler, Collector<R>& out) {
    detail::checkContext(context);
    auto query = handler.buildQuery(context, state, input);
    // Rows describes the result shape, not whether the SQL mutates data:
    // DML RETURNING uses the same bounded portal path as SELECT.
    auto transaction = cluster_->Begin(userver::storages::postgres::Transaction::RW);
    detail::Rollback rollback(transaction);
    {
      auto portal = query(transaction);
      while (!portal.Done()) {
        detail::checkContext(context);
        const auto rows = portal.Fetch(fetchSize_);
        for (const auto& row : rows) {
          detail::checkContext(context);
          handler.handleRow(context, state, row, out);
        }
      }
    }
    detail::checkContext(context);
    transaction.Commit();
    rollback.committed();
  }
 private:
  userver::storages::postgres::ClusterPtr cluster_;
  std::uint32_t fetchSize_;
};

// The callable returned by buildQuery uses native Transaction::Execute and
// returns ResultSet. Commit precedes mapping; mapper failure never retries SQL.
class Execute final {
 public:
  explicit Execute(userver::storages::postgres::ClusterPtr cluster)
      : cluster_(std::move(cluster)) {
    if (!cluster_) throw std::invalid_argument("Query PostgreSQL cluster is missing");
  }

  template <typename State, typename T, typename Handler, typename R>
  void execute(MessageContext context, State& state, const T& input,
               Handler& handler, Collector<R>& out) {
    detail::checkContext(context);
    auto query = handler.buildQuery(context, state, input);
    auto result = [&] {
      auto transaction = cluster_->Begin(userver::storages::postgres::Transaction::RW);
      detail::Rollback rollback(transaction);
      auto result = query(transaction);
      detail::checkContext(context);
      transaction.Commit();
      rollback.committed();
      return result;
    }();
    handler.handleResult(context, state, result, out);
  }
 private:
  userver::storages::postgres::ClusterPtr cluster_;
};
}  // namespace servicelib::dataquery::postgres
