// Compile-only coverage of native query building, typed parameters and mapping.
// Does not open a connection or replace PostgreSQL runtime acceptance.
#include <servicelib/dataquery/postgres/operations.hpp>
#include <userver/storages/postgres/io/integral_types.hpp>

namespace {
using namespace servicelib;
namespace pg = userver::storages::postgres;

struct Handler {
  using State = int;
  dataquery::BeginRequest<State> beginRequest(
      MessageContext context, QueryEndpointStream<int, int, std::exception_ptr>&) {
    return {context, 0, {}};
  }
  void endRequest(MessageContext context, QueryEndpointStream<int, int, std::exception_ptr>&,
                  State*, std::exception_ptr error, dataquery::Collector<int>&,
                  dataquery::Collector<std::exception_ptr>& errors) {
    if (error) errors.out(context, error);
  }
};

struct RowsHandler : Handler {
  auto buildQuery(MessageContext, State&, const int& id) {
    return [id](pg::Transaction& transaction) {
      return transaction.MakePortal("SELECT $1::integer", id);
    };
  }
  void handleRow(MessageContext context, State&, const pg::Row& row,
                 dataquery::Collector<int>& out) {
    out.out(context, row[0].As<int>());
  }
};

struct ExecuteHandler : Handler {
  auto buildQuery(MessageContext, State&, const int& id) {
    return [id](pg::Transaction& transaction) {
      return transaction.Execute("DELETE FROM query_fixture WHERE id = $1", id);
    };
  }
  void handleResult(MessageContext context, State&, const pg::ResultSet& result,
                    dataquery::Collector<int>& out) {
    out.out(context, static_cast<int>(result.RowsAffected()));
  }
};

struct ReturningHandler : RowsHandler {
  auto buildQuery(MessageContext, State&, const int& id) {
    return [id](pg::Transaction& transaction) {
      return transaction.MakePortal(
          "INSERT INTO query_fixture (id) VALUES ($1) RETURNING id", id);
    };
  }
};

[[maybe_unused]] void instantiate(QueryEndpointStream<int, int, std::exception_ptr>& stream,
                                  pg::ClusterPtr cluster, MessageContext context) {
  RowsHandler handler;
  dataquery::Endpoint<int, int, std::exception_ptr, RowsHandler&, dataquery::postgres::Rows>
      rows(stream, handler, dataquery::postgres::Rows(cluster, 32));
  rows.consume(context, Payload<int>::make(1));
  dataquery::Endpoint<int, int, std::exception_ptr, ReturningHandler, dataquery::postgres::Rows>
      returning(stream, ReturningHandler{}, dataquery::postgres::Rows(cluster, 32));
  returning.consume(context, Payload<int>::make(2));
  dataquery::Endpoint<int, int, std::exception_ptr, ExecuteHandler, dataquery::postgres::Execute>
      execute(stream, ExecuteHandler{}, dataquery::postgres::Execute(cluster));
  execute.consume(context, Payload<int>::make(1));
}
}  // namespace
