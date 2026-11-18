#pragma once
namespace {
UTEST(Nats, AsyncPullProductionRearmAndCancel) {
  const char* server=std::getenv("SERVICELIB_NATS_TEST_URL"); if(!server) GTEST_SKIP();
  auto& app=userver::engine::current_task::GetTaskProcessor();
  auto pool=userver::engine::SingleThreadedTaskProcessorsPool::MakeForTests(1); auto& blocking=pool.At(0);
  for(bool cancelledStop:{false,true}) {
  const auto suffix=std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
  config::NATSDataConnectorConfig cfg;cfg.servers={server};cfg.type=api::DataConnectorType::kJetStream;cfg.operationTimeoutMs=200;
  config::NATSEndpointConfig ep;ep.enabled=true;ep.subject="rearm."+suffix;ep.streamName="REARM_"+suffix;ep.durableName="reader";ep.createStream=true;ep.createConsumer=true;
  nats::Client source(app,blocking,cfg,ep);
  auto out=ep;out.durableName.clear();out.createConsumer=false;nats::Client publisher(app,blocking,cfg,out);
  int delivered=0,errors=0,live=0; userver::engine::SingleUseEvent entered;
  source.start(Context{},true,[&](auto){++errors;}); publisher.start(Context{},false,[&](auto){++errors;});
  source.subscribe([&](nats::ConsumerMessage message){
    EXPECT_EQ(&userver::engine::current_task::GetTaskProcessor(),&app);++delivered;
    if(message.data()=="hold") {++live;struct Release { int& count; ~Release(){--count;} } release{live};entered.Send();userver::engine::InterruptibleSleepFor(5s);EXPECT_TRUE(userver::engine::current_task::ShouldCancel());return;}
    message.doubleAck();
  });
  // An idle window expires and rearms, then backlog exercises delivery racing
  // subscribe return across many windows, rather than only publish-after-arm.
  userver::engine::SleepFor(450ms);
  for(int i=0;i<32;++i) EXPECT_TRUE(publisher.publishSync(MessageContext{},"record",{}).accepted);
  const auto until=std::chrono::steady_clock::now()+5s;
  while(delivered<32 && std::chrono::steady_clock::now()<until)userver::engine::SleepFor(5ms);
  EXPECT_EQ(delivered,32);
  EXPECT_TRUE(publisher.publishSync(MessageContext{},"hold",{}).accepted);
  EXPECT_EQ(entered.WaitUntil(userver::engine::Deadline::FromDuration(2s)),userver::engine::FutureStatus::kReady);
  EXPECT_TRUE(publisher.publishSync(MessageContext{},"successor",{}).accepted);
  const auto begin=std::chrono::steady_clock::now();auto stop=Context{}.bounded(50ms);
  std::stop_source stopCancellation;stopCancellation.request_stop();
  if(cancelledStop)stop=Context{}.withStopToken(stopCancellation.get_token());
  source.stopAdmission(stop);source.stop(stop); EXPECT_EQ(live,0);EXPECT_EQ(delivered,33);
  EXPECT_LT(std::chrono::steady_clock::now()-begin,700ms);publisher.stop(Context{}.bounded(1s));EXPECT_EQ(errors,0);
  userver::engine::AsyncNoTracing(blocking,[&]{
    natsConnection* nc{};ASSERT_EQ(natsConnection_ConnectTo(&nc,server),NATS_OK);jsCtx* js{};ASSERT_EQ(natsConnection_JetStream(&js,nc,nullptr),NATS_OK);
    jsConsumerInfo* ci{};ASSERT_EQ(js_GetConsumerInfo(&ci,js,ep.streamName.c_str(),"reader",nullptr,nullptr),NATS_OK);
    EXPECT_EQ(ci->NumAckPending,1);EXPECT_EQ(ci->NumPending,1);jsConsumerInfo_Destroy(ci);
    EXPECT_EQ(js_DeleteStream(js,ep.streamName.c_str(),nullptr,nullptr),NATS_OK);jsCtx_Destroy(js);natsConnection_Destroy(nc);
  }).Get();
  }
}
UTEST(Nats, AsyncPullProductionOversize) {
  const char* server=std::getenv("SERVICELIB_NATS_LARGE_TEST_URL");if(!server)GTEST_SKIP()<<"Use isolated max_payload=16MB broker";
  auto& app=userver::engine::current_task::GetTaskProcessor();auto pool=userver::engine::SingleThreadedTaskProcessorsPool::MakeForTests(1);auto& blocking=pool.At(0);
  auto suffix=std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
  config::NATSDataConnectorConfig cfg;cfg.servers={server};cfg.type=api::DataConnectorType::kJetStream;cfg.operationTimeoutMs=300;
  config::NATSEndpointConfig ep;ep.enabled=true;ep.subject="oversize."+suffix;ep.streamName="OVERSIZE_"+suffix;ep.durableName="reader";ep.createStream=true;ep.createConsumer=true;
  nats::Client source(app,blocking,cfg,ep);int errors=0,delivered=0;
  source.start(Context{},true,[&](auto){++errors;});
  // Bypass SDK publisher's intentionally bounded admission only in the test peer.
  userver::engine::AsyncNoTracing(blocking,[&]{
    natsConnection* nc{};ASSERT_EQ(natsConnection_ConnectTo(&nc,server),NATS_OK);jsCtx* js{};ASSERT_EQ(natsConnection_JetStream(&js,nc,nullptr),NATS_OK);
    std::string large(nats::Client::kMaxPendingBytes+1,'x');jsPubAck* ack{};
    ASSERT_EQ(js_Publish(&ack,js,ep.subject.c_str(),large.data(),large.size(),nullptr,nullptr),NATS_OK);jsPubAck_Destroy(ack);jsCtx_Destroy(js);natsConnection_Destroy(nc);
  }).Get();
  source.subscribe([&](auto){++delivered;});
  const auto end=std::chrono::steady_clock::now()+3s;while(!errors && std::chrono::steady_clock::now()<end)userver::engine::SleepFor(5ms);
  userver::engine::SleepFor(500ms);EXPECT_EQ(errors,1);EXPECT_EQ(delivered,0);source.stop(Context{}.bounded(1s));
  userver::engine::AsyncNoTracing(blocking,[&]{natsConnection* nc{};ASSERT_EQ(natsConnection_ConnectTo(&nc,server),NATS_OK);jsCtx* js{};ASSERT_EQ(natsConnection_JetStream(&js,nc,nullptr),NATS_OK);jsConsumerInfo* ci{};ASSERT_EQ(js_GetConsumerInfo(&ci,js,ep.streamName.c_str(),"reader",nullptr,nullptr),NATS_OK);EXPECT_EQ(ci->NumAckPending,0);EXPECT_EQ(ci->NumPending,1);jsConsumerInfo_Destroy(ci);EXPECT_EQ(js_DeleteStream(js,ep.streamName.c_str(),nullptr,nullptr),NATS_OK);jsCtx_Destroy(js);natsConnection_Destroy(nc);}).Get();
}
}

UTEST(Nats, AsyncPullProductionReconnect) {
  const char* server=std::getenv("SERVICELIB_NATS_TEST_URL");const char* proxy=std::getenv("SERVICELIB_NATS_RECONNECT_URL");if(!server||!proxy)GTEST_SKIP();
  auto& app=userver::engine::current_task::GetTaskProcessor();auto pool=userver::engine::SingleThreadedTaskProcessorsPool::MakeForTests(1);auto& blocking=pool.At(0);
  auto suffix=std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
  config::NATSDataConnectorConfig cfg;cfg.servers={proxy};cfg.type=api::DataConnectorType::kJetStream;cfg.operationTimeoutMs=250;
  config::NATSEndpointConfig ep;ep.enabled=true;ep.subject="reconnect."+suffix;ep.streamName="RECONNECT_"+suffix;ep.durableName="reader";ep.createStream=true;ep.createConsumer=true;
  nats::Client source(app,blocking,cfg,ep);int delivered=0;source.start(Context{},true,[](auto){});
  source.subscribe([&](nats::ConsumerMessage message){EXPECT_EQ(&userver::engine::current_task::GetTaskProcessor(),&app);message.doubleAck();++delivered;});
  cfg.servers={server};auto out=ep;out.durableName.clear();out.createConsumer=false;nats::Client publisher(app,blocking,cfg,out);publisher.start(Context{},false,[](auto){ADD_FAILURE();});
  EXPECT_TRUE(publisher.publishSync(MessageContext{},"recovered",{}).accepted);
  const auto end=std::chrono::steady_clock::now()+10s;while(!delivered && std::chrono::steady_clock::now()<end)userver::engine::SleepFor(5ms);
  EXPECT_EQ(delivered,1);source.stop(Context{}.bounded(1s));publisher.stop(Context{}.bounded(1s));
  userver::engine::AsyncNoTracing(blocking,[&]{natsConnection* nc{};ASSERT_EQ(natsConnection_ConnectTo(&nc,server),NATS_OK);jsCtx* js{};ASSERT_EQ(natsConnection_JetStream(&js,nc,nullptr),NATS_OK);EXPECT_EQ(js_DeleteStream(js,ep.streamName.c_str(),nullptr,nullptr),NATS_OK);jsCtx_Destroy(js);natsConnection_Destroy(nc);}).Get();
}

UTEST(Nats, AsyncPullProductionOwnerCancellation) {
  const char* server=std::getenv("SERVICELIB_NATS_TEST_URL");if(!server)GTEST_SKIP();
  auto& app=userver::engine::current_task::GetTaskProcessor();auto pool=userver::engine::SingleThreadedTaskProcessorsPool::MakeForTests(1);auto& blocking=pool.At(0);
  for(bool token:{true,false}) {
    auto suffix=std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    config::NATSDataConnectorConfig cfg;cfg.servers={server};cfg.type=api::DataConnectorType::kJetStream;cfg.operationTimeoutMs=2000;
    config::NATSEndpointConfig ep;ep.enabled=true;ep.subject="cancel."+suffix;ep.streamName="CANCEL_"+suffix;ep.durableName="reader";ep.createStream=true;ep.createConsumer=true;
    std::stop_source cancellation;auto owner=token ? Context{}.withStopToken(cancellation.get_token()) : Context{}.bounded(150ms);
    nats::Client source(app,blocking,cfg,ep);int delivered=0;source.start(owner,true,[](auto){ADD_FAILURE();});source.subscribe([&](auto){++delivered;});
    cancellation.request_stop();userver::engine::SleepFor(200ms);
    EXPECT_TRUE(source.waitForCallbacks(Context{}.bounded(200ms)))<<"Owner cancellation must retire an idle native pull without waiting for its 2s expiry";
    auto out=ep;out.durableName.clear();out.createConsumer=false;nats::Client publisher(app,blocking,cfg,out);publisher.start(Context{},false,[](auto){ADD_FAILURE();});
    EXPECT_TRUE(publisher.publishSync(MessageContext{},"late",{}).accepted);userver::engine::SleepFor(30ms);EXPECT_EQ(delivered,0);
    source.stop(Context{}.bounded(100ms));publisher.stop(Context{}.bounded(1s));
    userver::engine::AsyncNoTracing(blocking,[&]{natsConnection* nc{};ASSERT_EQ(natsConnection_ConnectTo(&nc,server),NATS_OK);jsCtx* js{};ASSERT_EQ(natsConnection_JetStream(&js,nc,nullptr),NATS_OK);jsConsumerInfo* ci{};ASSERT_EQ(js_GetConsumerInfo(&ci,js,ep.streamName.c_str(),"reader",nullptr,nullptr),NATS_OK);EXPECT_EQ(ci->NumAckPending,0);EXPECT_EQ(ci->NumPending,1);jsConsumerInfo_Destroy(ci);EXPECT_EQ(js_DeleteStream(js,ep.streamName.c_str(),nullptr,nullptr),NATS_OK);jsCtx_Destroy(js);natsConnection_Destroy(nc);}).Get();
  }
}
