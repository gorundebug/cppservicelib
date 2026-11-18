#include <nats/nats.h>
#include <sstream>
#include <servicelib/runtime/telemetry/userver/metrics.hpp>
#include "publication_backend.hpp"
#include <fstream>
#include <sstream>
#include <atomic>
#include <userver/utest/utest.hpp>
#include <userver/engine/task/current_task.hpp>
#include <userver/engine/task/single_threaded_task_processors_pool.hpp>
#include <userver/engine/task/cancel.hpp>
#include <userver/engine/sleep.hpp>
#include <userver/formats/yaml.hpp>
#include <servicelib/datasource/nats/userver.hpp>
#include <servicelib/datasink/nats/userver.hpp>
using namespace std::chrono_literals;
namespace {
using namespace servicelib;
UTEST(Nats, CommonConnectionValidation) {
  std::ifstream file(std::string{SERVICELIB_TEST_SOURCE_DIR}+"/tests/testdata/nats-connection-validation.json");
  std::stringstream text; text<<file.rdbuf();
  const auto fixtures=userver::formats::yaml::FromString(text.str());
  EXPECT_EQ(fixtures.GetSize(),53);
  for (const auto& fixture:fixtures) {
    auto cfg=fixture["config"].As<config::NATSDataConnectorConfig>();
    auto actual=nats::InvalidConnectionFields(cfg,true);
    auto expected=fixture["invalidFields"].As<std::vector<std::string>>();
    std::sort(actual.begin(),actual.end()); std::sort(expected.begin(),expected.end());
    EXPECT_EQ(actual,expected)<<fixture["name"].As<std::string>();
  }
}
UTEST(Nats, NativeCoreAndJetStream) {
  const auto* server=std::getenv("SERVICELIB_NATS_TEST_URL");
  if (!server) GTEST_SKIP()<<"Set SERVICELIB_NATS_TEST_URL for native broker acceptance";
  auto& app=userver::engine::current_task::GetTaskProcessor();
  auto blockingPool=userver::engine::SingleThreadedTaskProcessorsPool::MakeForTests(1);
  auto& blocking=blockingPool.At(0);
  ASSERT_NE(&app,&blocking);
  for (auto kind:{api::DataConnectorType::kNATS,api::DataConnectorType::kJetStream}) {
    config::NATSDataConnectorConfig c; c.servers={server}; c.type=kind;
    config::NATSEndpointConfig e; e.enabled=true;
    e.subject=kind==api::DataConnectorType::kNATS ? "userver.core" : "userver.js";
    if(kind==api::DataConnectorType::kJetStream) {e.streamName="USERVER_NATIVE";e.durableName="reader";e.createConsumer=true;e.createStream=true;}
    std::atomic<int> messages{0},results{0},errors{0};
    nats::Client source(app,blocking,c,e);
    auto sinkConfig=e;sinkConfig.durableName.clear();sinkConfig.createConsumer=false;
    nats::Client sink(app,blocking,c,sinkConfig);
    Context owner;
    source.start(owner,true,[&](std::exception_ptr){++errors;});
    source.subscribe([&](nats::ConsumerMessage message) {
      EXPECT_EQ(&userver::engine::current_task::GetTaskProcessor(),&app);
      EXPECT_EQ(message.data(),std::string("\0\xffpayload",9));
      const auto headers=message.headers();
      const auto range=headers.equal_range("X-Repeated");
      std::vector<std::string> values;
      for(auto it=range.first;it!=range.second;++it) values.push_back(it->second);
      EXPECT_EQ(values,(std::vector<std::string>{"first","second"}));
      auto context=nats::ContextFromHeaders(MessageContext{},message.headers(),true);
      EXPECT_EQ(context.streamId(),"event-1");
      if(message.jetStream()) message.doubleAck();
      else EXPECT_THROW(message.ack(),std::logic_error);
      ++messages;
    });
    sink.start(owner,false,[&](std::exception_ptr){++errors;});
    sink.publish(MessageContext{},std::string("\0\xffpayload",9),{{"X-Stream-ID","event-1"},{"X-Repeated","first"},{"X-Repeated","second"}},[&](nats::DeliveryResult result){
      EXPECT_EQ(&userver::engine::current_task::GetTaskProcessor(),&app);
      EXPECT_TRUE(result.accepted); EXPECT_FALSE(result.error);
      EXPECT_EQ(result.ack.has_value(),kind==api::DataConnectorType::kJetStream);
      ++results;
    });
    const auto deadline=std::chrono::steady_clock::now()+5s;
    while ((messages<1 || results<1) && std::chrono::steady_clock::now()<deadline) userver::engine::SleepFor(5ms);
    source.stopAdmission(owner); source.waitForCallbacks(owner); source.stop(owner); sink.stop(owner);
    EXPECT_EQ(messages,1);EXPECT_EQ(results,1);EXPECT_EQ(errors,0);
  }
}
}

#include <servicelib/runtime/testlog/testlog.hpp>
#include <servicelib/runtime/testmetrics/testmetrics.hpp>
#include "test_sink_endpoint_stream.hpp"
namespace {
struct NativeConfig : config::IConfig {
  config::NATSDataConnectorConfig connector;
  config::NATSEndpointConfig source, sink;
  std::vector<const config::ServiceConfig*> GetServices() const override {return {};}
  std::vector<config::StreamConfigRef> GetStreams() const override {return {};}
  std::vector<config::DataConnectorConfigRef> GetDataConnectors() const override {return {connector};}
  std::vector<config::EndpointConfigRef> GetEndpoints() const override {return {source,sink};}
  std::vector<const config::PoolConfig*> GetPools() const override {return {};}
  std::vector<const config::LinkConfig*> GetLinks() const override {return {};}
  std::vector<const config::ModuleConfig*> GetModules() const override {return {};}
  std::vector<const config::TypeConfig*> GetTypes() const override {return {};}
};
struct NativeEnvironment : IRuntimeEnvironment {
  NativeConfig config;
  testlog::TestLog logger;
  testmetrics::TestMetrics metrics;
  pool::ITaskPool* getTaskPool(const std::string&) override {return nullptr;}
  pool::IPriorityTaskPool* getPriorityTaskPool(const std::string&) override {return nullptr;}
  std::shared_ptr<const config::RuntimeConfig> getRuntimeConfigSnapshot() const override {return std::make_shared<const config::RuntimeConfig>(config);}
  std::shared_ptr<const config::ServiceConfig> getServiceConfigSnapshot() const override {return std::make_shared<const config::ServiceConfig>();}
  tracing::Tracing* getTracing() override {return nullptr;}
  log::Logger& getLogger() override {return logger;}
  metrics::Metrics& getMetrics() override {return metrics;}
};
struct Outcome {bool accepted;};
struct NativeInput {
  std::function<void(MessageContext,Payload<Outcome>)> result;
  std::function<void(MessageContext,Payload<std::string>)> output;
  int getEndpointId() const {return 1;}
  std::size_t getConfigId() const {return 0;}
  const NativeInput* getResultStream() const {return this;}
  void setResultConsumer(decltype(result) fn) {result=std::move(fn);}
  void consume(MessageContext ctx,Payload<std::string> value) {output(ctx,std::move(value));}
  void consumeError(MessageContext,Payload<std::exception_ptr>) {ADD_FAILURE()<<"unexpected source error";}
};
struct NativeSourceHandler {
  using State=int;
  std::atomic<int>* ended;
  bool acknowledge{true};
  void onError(MessageContext ctx,auto& stream,std::exception_ptr error) {stream.collectError(ctx,error);}
  BeginResult<State> beginRequest(MessageContext ctx,auto&) {return {ctx,0};}
  void consumeMessage(MessageContext ctx,auto& stream,State&,const nats::ConsumerMessage& message,auto result) {
    if (message.jetStream() && acknowledge) {
      jsMsgMetaData* metadata{};
      ASSERT_EQ(natsMsg_GetMetaData(&metadata, message.native()), NATS_OK);
      const auto attempts = metadata->NumDelivered;
      jsMsgMetaData_Destroy(metadata);
      if (attempts == 1) { message.nak(); result.done(); return; }
    }
    result.setResultCallback("outcome",[message,result,acknowledge=acknowledge](MessageContext,auto&,State&,const Outcome& value) mutable {
      EXPECT_TRUE(value.accepted);
      if (message.jetStream() && acknowledge) message.doubleAck();
      result.done(); return true;
    });
    stream.collect(ctx,std::string{message.data()});
  }
  std::string getMessageId(MessageContext,auto&,State&,const Outcome&) {return "outcome";}
  void endRequest(MessageContext,auto&,std::exception_ptr error,State&) noexcept {EXPECT_FALSE(error);++*ended;}
};
struct NativeSinkHandler {
  using State=int;
  void onError(MessageContext,auto&,std::exception_ptr) {ADD_FAILURE()<<"unexpected native sink error";}
  std::string getStreamId(MessageContext ctx,const std::string&) {return std::string{ctx.streamId()};}
  BeginResult<State> beginRequest(MessageContext ctx,auto&) {return {ctx,0};}
  void consumeMessage(MessageContext,auto&,State&,const std::string& value,datasink::nats::SinkMessage<Outcome>& msg) {
    msg.value=value;
    msg.send([](const nats::DeliveryResult& delivery) {return Outcome{delivery.accepted && !delivery.error};});
  }
  void endRequest(MessageContext,auto&,std::exception_ptr error,State&) noexcept {EXPECT_FALSE(error);}
};
UTEST(Nats, TypedSourceSinkResultLifecycle) {
  const auto* server=std::getenv("SERVICELIB_NATS_TEST_URL");
  if(!server) GTEST_SKIP();
  auto& app=userver::engine::current_task::GetTaskProcessor();
  auto blockingPool=userver::engine::SingleThreadedTaskProcessorsPool::MakeForTests(1);
  auto& blocking=blockingPool.At(0);
  ASSERT_NE(&app,&blocking);
  for(bool acknowledge:{true,false}) for(auto kind:{api::DataConnectorType::kNATS,api::DataConnectorType::kJetStream}) {
    NativeEnvironment env;
    auto& c=env.config.connector; c.id=1;c.type=kind;c.servers={server};c.name="native";
    auto& e=env.config.source; e.id=1;e.type=kind;e.idDataConnector=1;e.name="source";e.enabled=true;
    e.subject=kind==api::DataConnectorType::kNATS ? "typed.core.in" : (acknowledge ? "typed.js.in" : "typed.noack.in");
    if(kind==api::DataConnectorType::kJetStream) {e.streamName=acknowledge ? "USERVER_TYPED_IN" : "USERVER_NOACK_IN";e.durableName="reader";e.createStream=true;e.createConsumer=true;}
    const auto suffix=std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    e.subject += "."+suffix;
    if(kind==api::DataConnectorType::kJetStream) e.streamName += suffix;
    auto& out=env.config.sink;out=e;out.id=2;out.name="sink";out.subject=e.subject+".out";out.durableName.clear();out.createConsumer=false;
    if(kind==api::DataConnectorType::kJetStream) out.streamName=acknowledge ? "USERVER_TYPED_OUT" : "USERVER_NOACK_OUT";
    if(kind==api::DataConnectorType::kJetStream) out.streamName+=suffix;
    nats::Client inputClient(app,blocking,c,e), outputClient(app,blocking,c,out);
    NativeInput input;
    TestSinkEndpointStream<std::string,Outcome> sinkStream{env,2,[&](MessageContext ctx,Payload<Outcome> value){input.result(ctx,std::move(value));},[](auto,auto){ADD_FAILURE()<<"unexpected typed error";}};
    datasink::nats::Endpoint<std::string,Outcome,NativeSinkHandler> sink{sinkStream,outputClient,{}};
    input.output=[&](auto ctx,auto value){sink.consume(ctx,std::move(value));};
    std::atomic<int> ended{0};
    auto source=datasource::nats::Endpoint<std::string,Outcome,NativeSourceHandler>::make(env,input,inputClient,NativeSourceHandler{&ended,acknowledge});
    Context owner;
    sink.start(owner);source->start(owner);
    auto injection=e;injection.durableName.clear();injection.createConsumer=false;
    nats::Client publisher(app,blocking,c,injection);publisher.start(owner,false,[](auto){ADD_FAILURE();});
    auto delivery=publisher.publishSync(MessageContext{},"event",{}); EXPECT_TRUE(delivery.accepted);
    const auto deadline=std::chrono::steady_clock::now()+5s;
    const int expectedDeliveries = kind==api::DataConnectorType::kJetStream && acknowledge ? 2 : 1;
    while(ended<expectedDeliveries && std::chrono::steady_clock::now()<deadline) userver::engine::SleepFor(5ms);
    source->stop(owner);sink.stop(owner);publisher.stop(owner);
    EXPECT_EQ(env.metrics.counter("broker_endpoint.source_observations_total", {{"protocol", kind==api::DataConnectorType::kJetStream ? "jetstream" : "nats"}, {"connector", "native"}, {"endpoint", "source"}, {"observation", "received"}}).count(), expectedDeliveries);
    if (kind==api::DataConnectorType::kJetStream) {
      EXPECT_EQ(env.metrics.counter("broker_endpoint.source_observations_total", {{"protocol", "jetstream"}, {"connector", "native"}, {"endpoint", "source"}, {"observation", "redelivered"}}).count(), acknowledge ? 1 : 0);
    }
    EXPECT_EQ(ended,expectedDeliveries);
    EXPECT_EQ(env.metrics.counter("broker_endpoint.publication_outcomes_total", {{"protocol", kind==api::DataConnectorType::kJetStream ? "jetstream" : "nats"}, {"connector", "native"}, {"endpoint", "sink"}, {"outcome", kind==api::DataConnectorType::kJetStream ? "acknowledged" : "client-accepted"}}).count(), 1);
    if(kind==api::DataConnectorType::kJetStream && !acknowledge) {
      const auto pending=userver::engine::AsyncNoTracing(blocking,[&] {
        natsConnection* nc{}; if(natsConnection_ConnectTo(&nc,server)!=NATS_OK) throw std::runtime_error("inspect connect");
        jsCtx* js{}; natsConnection_JetStream(&js,nc,nullptr);
        jsConsumerInfo* info{};
        const auto status=js_GetConsumerInfo(&info,js,e.streamName.c_str(),e.durableName.c_str(),nullptr,nullptr);
        const auto count=info ? info->NumAckPending : 0;
        jsConsumerInfo_Destroy(info);jsCtx_Destroy(js);natsConnection_Destroy(nc);
        if(status!=NATS_OK) throw std::runtime_error("inspect durable");
        return count;
      }).Get();
      EXPECT_EQ(pending,1)<<"Done and shutdown must not ACK";
    }
  }
}
}

namespace {
UTEST(Nats, CallbackDeadlineAndPublisherCapacity) {
  const auto* server=std::getenv("SERVICELIB_NATS_TEST_URL");
  if(!server) GTEST_SKIP()<<"Set SERVICELIB_NATS_TEST_URL for native acceptance";
  auto& app=userver::engine::current_task::GetTaskProcessor();
  auto blockingPool=userver::engine::SingleThreadedTaskProcessorsPool::MakeForTests(1);
  auto& blocking=blockingPool.At(0);
  ASSERT_NE(&app,&blocking);
  config::NATSDataConnectorConfig config;config.servers={server};config.operationTimeoutMs=2000;
  config::NATSEndpointConfig endpoint;endpoint.enabled=true;endpoint.subject="userver.capacity";
  {
    nats::Client client(app,blocking,config,endpoint);
    client.start(Context{},false,[](auto){ADD_FAILURE()<<"unexpected transport error";});
    std::atomic<bool> release{false};
    std::atomic<int> results{0};
    for(std::size_t i=0;i<nats::Client::kMaxPending;++i) {
      client.publish(MessageContext{},"capacity",{},[&](auto result){
        while(!release) { userver::engine::SleepFor(1ms); }
        EXPECT_TRUE(result.accepted);
        ++results;
      });
    }
    EXPECT_THROW(client.publish(MessageContext{},"overflow",{},[](auto){}),std::runtime_error);
    release=true;
    client.stop(Context{}.withDeadline(std::chrono::steady_clock::now()+2s));
    EXPECT_EQ(results,nats::Client::kMaxPending);
  }
  {
    nats::Client client(app,blocking,config,endpoint);
    client.start(Context{},false,[](auto){ADD_FAILURE()<<"unexpected transport error";});
    userver::engine::SingleUseEvent entered;
    std::atomic<bool> completed{false};
    client.publish(MessageContext{},"slow-callback",{},[&](auto){
      entered.Send();
      userver::engine::InterruptibleSleepFor(2s);
      completed=!userver::engine::current_task::ShouldCancel();
    });
    entered.WaitNonCancellable();
    const auto start=std::chrono::steady_clock::now();
    client.stop(Context{}.withDeadline(start+100ms));
    EXPECT_LT(std::chrono::steady_clock::now()-start,700ms);
    EXPECT_FALSE(completed)<<"Expired stop must cancel cooperative callbacks";
  }
}
}

namespace {
UTEST(Nats, TransportFailureDuringStop) {
  const auto* server=std::getenv("SERVICELIB_NATS_FAULT_URL");
  const auto* subject=std::getenv("SERVICELIB_NATS_FAULT_SUBJECT");
  const auto* stream=std::getenv("SERVICELIB_NATS_FAULT_STREAM");
  if(!server || !subject || !stream) GTEST_SKIP()<<"Use tests/nats_stop_fault.py";
  auto& app=userver::engine::current_task::GetTaskProcessor();
  auto blockingPool=userver::engine::SingleThreadedTaskProcessorsPool::MakeForTests(1);
  auto& blocking=blockingPool.At(0);
  ASSERT_NE(&app,&blocking);
  config::NATSDataConnectorConfig cfg;
  cfg.servers={server};cfg.type=api::DataConnectorType::kJetStream;
  cfg.connectTimeoutMs=200;cfg.operationTimeoutMs=2000;
  config::NATSEndpointConfig endpoint;
  endpoint.enabled=true;endpoint.subject=subject;endpoint.streamName=stream;endpoint.createStream=true;
  nats::Client client(app,blocking,cfg,endpoint);
  std::atomic<int> results{0}, accepted{0}, errors{0};
  client.start(Context{},false,[](auto){});
  client.publish(MessageContext{},"receipt-lost",{},[&](const nats::DeliveryResult& result){
    EXPECT_EQ(&userver::engine::current_task::GetTaskProcessor(),&app);
    ++results;if(result.accepted)++accepted;if(result.error)++errors;
  });
  // The proxy withholds the PubAck or disconnects after forwarding this send.
  // Let the native transport observe the fault without blocking this worker.
  for(int i=0;i<10;++i)userver::engine::SleepFor(5ms);
  const auto start=std::chrono::steady_clock::now();
  client.stop(Context{}.withDeadline(start+100ms));
  EXPECT_LT(std::chrono::steady_clock::now()-start,700ms);
  EXPECT_EQ(results,1);EXPECT_EQ(accepted,0);EXPECT_EQ(errors,1);
  EXPECT_THROW(client.publish(MessageContext{},"after-stop",{},[](auto){}),std::runtime_error);
  client.stop(Context{}); // idempotent after the failed transport is closed.
}
}

namespace {
UTEST(Nats, SourceTransportFailureDuringStop) {
  const auto* server=std::getenv("SERVICELIB_NATS_FAULT_URL");
  const auto* subject=std::getenv("SERVICELIB_NATS_FAULT_SUBJECT");
  const auto* stream=std::getenv("SERVICELIB_NATS_FAULT_STREAM");
  if(!server || !subject || !stream) GTEST_SKIP()<<"Use tests/nats_stop_fault.py";
  auto& app=userver::engine::current_task::GetTaskProcessor();
  auto blockingPool=userver::engine::SingleThreadedTaskProcessorsPool::MakeForTests(1);
  auto& blocking=blockingPool.At(0);
  ASSERT_NE(&app,&blocking);
  config::NATSDataConnectorConfig cfg;cfg.servers={server};
  cfg.type=api::DataConnectorType::kJetStream;cfg.connectTimeoutMs=200;cfg.operationTimeoutMs=2000;
  config::NATSEndpointConfig endpoint;endpoint.enabled=true;endpoint.subject=subject;
  endpoint.streamName=stream;endpoint.durableName="reader";endpoint.createStream=true;endpoint.createConsumer=true;
  nats::Client source(app,blocking,cfg,endpoint);
  source.start(Context{},true,[](auto){});
  source.subscribe([](auto){ADD_FAILURE()<<"Empty test stream cannot deliver a message";});
  for(int i=0;i<10;++i)userver::engine::SleepFor(5ms);
  const auto start=std::chrono::steady_clock::now();
  const auto deadline=Context{}.withDeadline(start+100ms);
  try { source.stopAdmission(deadline); } catch(...) {}
  source.waitForCallbacks(deadline);
  source.stop(deadline);
  EXPECT_LT(std::chrono::steady_clock::now()-start,700ms);
}
}

#include "nats_security_test.hpp"

UTEST(Nats, PublicationBoundaries) {
  const auto* server = std::getenv("SERVICELIB_NATS_TEST_URL");
  if (!server) GTEST_SKIP();
  auto& app = userver::engine::current_task::GetTaskProcessor();
  auto pool = userver::engine::SingleThreadedTaskProcessorsPool::MakeForTests(1);
  config::NATSDataConnectorConfig cfg; cfg.servers = {server};
  config::NATSEndpointConfig ep; ep.enabled = true; ep.subject = "publication.boundary";
  for (bool jetstream : {false, true}) {
  cfg.type = jetstream ? api::DataConnectorType::kJetStream : api::DataConnectorType::kNATS;
  const auto suffix = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
  ep.subject = "publication.boundary." + suffix;
  ep.streamName = jetstream ? "USERVER_PUBLICATION_" + suffix : "";
  ep.createStream = jetstream;
  const std::string protocol = jetstream ? "jetstream" : "nats";
  for (int mode = 0; mode != 5; ++mode) {
    PublicationBackend backend; backend.disabled = mode == 2; backend.throwing = mode == 3;
    auto metrics = std::make_shared<detail::PublicationMetrics>(backend, protocol, "boundary", "output");
    std::weak_ptr<detail::PublicationMetrics> weak = metrics;
    nats::Client client(app, pool.At(0), cfg, ep);
    std::atomic<int> callbacks{0}, outputs{0}, errors{0};
    std::atomic<bool> entered{false}, release{false};
    client.start(Context{}, false, [&](auto){ ++errors; });
    {
      datasink::nats::SinkMessage<int> sink(client, MessageContext{}, {},
        [&](MessageContext, int) { ++outputs; if (mode == 1) throw std::runtime_error("output"); }, metrics);
      sink.value = "payload";
      sink.send([&](const auto& result) {
        EXPECT_TRUE(result.accepted); ++callbacks;
        EXPECT_EQ(backend.attempts.load(), mode == 2 ? 0 : 1);
        entered = true;
        if (mode == 4) while (!release) userver::engine::InterruptibleSleepFor(1ms);
        if (mode == 0) throw std::runtime_error("callback");
        return 1;
      });
    }
    metrics.reset();
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (!entered && std::chrono::steady_clock::now() < deadline) userver::engine::SleepFor(1ms);
    EXPECT_TRUE(entered);
    if (mode == 4) { EXPECT_FALSE(weak.expired()); }
    release = true;
    client.stop(Context{}.withDeadline(std::chrono::steady_clock::now()+2s));
    EXPECT_TRUE(weak.expired());
    EXPECT_EQ(callbacks, 1); EXPECT_EQ(outputs, mode == 0 ? 0 : 1);
    EXPECT_EQ(backend.attempts.load(), mode == 2 ? 0 : 1);
    EXPECT_EQ(backend.count(protocol, jetstream ? "acknowledged" : "client-accepted"), mode == 2 || mode == 3 ? 0 : 1);
    EXPECT_EQ(backend.count(protocol, "error"), 0);
  }
  if (jetstream) {
    userver::engine::AsyncNoTracing(pool.At(0), [&] {
      natsConnection* connection{}; jsCtx* js{};
      ASSERT_EQ(natsConnection_ConnectTo(&connection, server), NATS_OK);
      ASSERT_EQ(natsConnection_JetStream(&js, connection, nullptr), NATS_OK);
      EXPECT_EQ(js_DeleteStream(js, ep.streamName.c_str(), nullptr, nullptr), NATS_OK);
      jsCtx_Destroy(js); natsConnection_Destroy(connection);
    }).Get();
  }
  }
}

UTEST(Nats, PublicationLateCompletionAfterCancellation) {
  const auto* server = std::getenv("SERVICELIB_NATS_FAULT_URL");
  if (!server) GTEST_SKIP();
  auto& app = userver::engine::current_task::GetTaskProcessor();
  auto pool = userver::engine::SingleThreadedTaskProcessorsPool::MakeForTests(1);
  config::NATSDataConnectorConfig cfg; cfg.servers = {server}; cfg.type = api::DataConnectorType::kJetStream;
  cfg.operationTimeoutMs = 3000;
  config::NATSEndpointConfig ep; ep.enabled = true;
  ep.subject = std::getenv("SERVICELIB_NATS_FAULT_SUBJECT");
  ep.streamName = std::getenv("SERVICELIB_NATS_FAULT_STREAM"); ep.createStream = true;
  PublicationBackend backend;
  auto metrics = std::make_shared<detail::PublicationMetrics>(backend, "jetstream", "boundary", "output");
  std::weak_ptr<detail::PublicationMetrics> weak = metrics;
  nats::Client client(app, pool.At(0), cfg, ep);
  std::atomic<int> callbacks{0}, outputs{0};
  client.start(Context{}, false, [](auto){});
  std::stop_source cancellation;
  {
    datasink::nats::SinkMessage<int> sink(client, MessageContext{}.withStopToken(cancellation.get_token()), {},
        [&](MessageContext, int) { ++outputs; }, metrics);
    sink.value = "late";
    sink.send([&](const auto& result) {
      EXPECT_TRUE(result.accepted); EXPECT_TRUE(result.ack.has_value()); EXPECT_FALSE(result.error);
      EXPECT_EQ(backend.count("jetstream", "acknowledged"), 1);
      ++callbacks; return 1;
    });
    // publish() returned after the native handoff; proxy holds its PubAck.
    cancellation.request_stop();
  }
  metrics.reset();
  EXPECT_FALSE(weak.expired()); EXPECT_EQ(backend.attempts.load(), 0);
  { std::ofstream marker(std::getenv("SERVICELIB_NATS_RELEASE_MARKER")); marker << "release"; }
  client.stop(Context{}.withDeadline(std::chrono::steady_clock::now()+2s));
  EXPECT_EQ(callbacks, 1); EXPECT_EQ(outputs, 1);
  EXPECT_EQ(backend.attempts.load(), 1);
  EXPECT_EQ(backend.count("jetstream", "error"), 0);
  EXPECT_TRUE(weak.expired());
}
UTEST(Nats, PublicationExporterFamily) {
  userver::utils::statistics::Storage storage;
  telemetry::userver_adapter::UserverMetrics backend(storage);
  const std::vector<std::pair<std::string, std::vector<std::string>>> profiles{
    {"nats", {"client-accepted", "flushed", "error"}},
    {"jetstream", {"acknowledged", "error"}},
    {"pulsar", {"not-sent", "accepted", "rejected", "unknown"}},

  };
  std::vector<std::shared_ptr<detail::PublicationMetrics>> owners;
  for (const auto& [protocol, outcomes] : profiles) {
    auto publication = std::make_shared<detail::PublicationMetrics>(backend, protocol, "boundary", "output");
    for (const auto& outcome : outcomes) publication->observe(outcome);
    owners.push_back(std::move(publication));
  }
  const auto text = userver::utils::statistics::ToPrometheusFormat(storage);
  EXPECT_EQ(text.find("broker_endpoint.publication_outcomes_total"), std::string::npos);
  for (const auto& [protocol, outcomes] : profiles) for (const auto& outcome : outcomes) {
    std::istringstream lines(text); std::string line; int matches = 0;
    while (std::getline(lines, line)) {
      if (line.starts_with("broker_endpoint_publication_outcomes_total{") &&
          line.find("protocol=\"" + protocol + "\"") != std::string::npos &&
          line.find("outcome=\"" + outcome + "\"") != std::string::npos) {
        EXPECT_NE(line.find("connector=\"boundary\""), std::string::npos);
        EXPECT_NE(line.find("endpoint=\"output\""), std::string::npos);
        EXPECT_EQ(line.substr(line.rfind('}') + 1), " 1"); ++matches;
      }
    }
    EXPECT_EQ(matches, 1) << protocol << "/" << outcome << "\n" << text;
  }
}

UTEST(Nats, SourceExporterFamily) {
  userver::utils::statistics::Storage storage;
  telemetry::userver_adapter::UserverMetrics backend(storage);
  const std::vector<std::pair<std::string, std::vector<std::string>>> profiles{
    {"kafka", {"received"}}, {"nats", {"received"}}, {"jetstream", {"received", "redelivered"}},
    {"pulsar", {"received", "redelivered"}},
  };
  std::vector<std::shared_ptr<detail::SourceMetrics>> owners;
  for (const auto& [protocol, outcomes] : profiles) {
    auto publication = std::make_shared<detail::SourceMetrics>(backend, protocol, "boundary", "output");
    for (const auto& outcome : outcomes) publication->observe(outcome);
    owners.push_back(std::move(publication));
  }
  const auto text = userver::utils::statistics::ToPrometheusFormat(storage);
  EXPECT_EQ(text.find("broker_endpoint.source_observations_total"), std::string::npos);
  for (const auto& [protocol, outcomes] : profiles) for (const auto& outcome : outcomes) {
    std::istringstream lines(text); std::string line; int matches = 0;
    while (std::getline(lines, line)) {
      if (line.starts_with("broker_endpoint_source_observations_total{") &&
          line.find("protocol=\"" + protocol + "\"") != std::string::npos &&
          line.find("observation=\"" + outcome + "\"") != std::string::npos) {
        EXPECT_NE(line.find("connector=\"boundary\""), std::string::npos);
        EXPECT_NE(line.find("endpoint=\"output\""), std::string::npos);
        EXPECT_EQ(line.substr(line.rfind('}') + 1), " 1"); ++matches;
      }
    }
    EXPECT_EQ(matches, 1) << protocol << "/" << outcome << "\n" << text;
  }
}

UTEST(Nats, SourceBackendFailureAndOwnership) {
  for (int mode = 0; mode != 4; ++mode) {
    PublicationBackend backend; backend.disabled = mode == 1; backend.throwing = mode == 2; backend.failBinding = mode == 3;
    auto owner = std::make_shared<detail::SourceMetrics>(backend, "pulsar", "boundary", "output");
    std::weak_ptr<detail::SourceMetrics> weak = owner;
    auto completion = [retained = owner] { retained->observe("received"); };
    owner.reset(); EXPECT_FALSE(weak.expired());
    EXPECT_NO_THROW(completion());
    EXPECT_EQ(backend.attempts.load(), mode == 1 || mode == 3 ? 0 : 1);
    EXPECT_EQ(backend.recorded.counter("broker_endpoint.source_observations_total", {{"protocol", "pulsar"}, {"connector", "boundary"}, {"endpoint", "output"}, {"observation", "received"}}).count(), mode == 0 ? 1 : 0);
  }
}

#include "nats_contention_test.hpp"

#include "nats_async_pull_probe.hpp"

#include "nats_async_pull_production.hpp"
