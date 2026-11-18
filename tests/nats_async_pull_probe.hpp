#pragma once
#include <userver/engine/single_use_event.hpp>
namespace {
struct AsyncPullWindow {
  userver::engine::SingleUseEvent ready, complete;
  std::atomic<natsMsg*> message{nullptr};
  std::atomic<int> calls{0};
  std::atomic<bool> nativeComplete{false};
  std::atomic<bool> wrongNativeAffinity{false};
  std::atomic<natsStatus> status{NATS_OK};
  natsSubscription* subscription{};
  ~AsyncPullWindow() { if (subscription && !nativeComplete) std::terminate(); natsMsg_Destroy(message.load()); natsSubscription_Destroy(subscription); }
  static void Receive(natsConnection*, natsSubscription*, natsMsg* msg, void* closure) noexcept {
    auto& self=*static_cast<AsyncPullWindow*>(closure);
    if(userver::engine::current_task::IsTaskProcessorThread()) self.wrongNativeAffinity=true;
    ++self.calls;
    if (self.message.exchange(msg)) std::terminate(); // A second message violates the native window.
    self.ready.Send();
  }
  static void Complete(natsConnection*, natsSubscription*, natsStatus status, void* closure) noexcept {
    auto& self=*static_cast<AsyncPullWindow*>(closure);
    if(userver::engine::current_task::IsTaskProcessorThread()) self.wrongNativeAffinity=true;
    self.status=status; self.nativeComplete=true; self.complete.Send();
  }
};
UTEST(Nats, AsyncPullBoundedSuitabilityProbe) {
  const auto* server=std::getenv("SERVICELIB_NATS_TEST_URL"); if (!server) GTEST_SKIP();
  using Clock=std::chrono::steady_clock;
  const auto ms=[](auto d){return std::chrono::duration<double,std::milli>(d).count();};
  auto& app=userver::engine::current_task::GetTaskProcessor();
  auto pool=userver::engine::SingleThreadedTaskProcessorsPool::MakeForTests(1);
  auto& blocking=pool.At(0); ASSERT_NE(&app,&blocking);
  const auto check=[](natsStatus s){if(s!=NATS_OK)throw std::runtime_error(natsStatus_GetText(s));};
  const auto wait=[](auto& event){return event.WaitUntil(userver::engine::Deadline::FromDuration(5s))==userver::engine::FutureStatus::kReady;};
  const auto suffix=std::to_string(Clock::now().time_since_epoch().count());
  const auto stream="ASYNC_PROBE_"+suffix; const auto subject="asyncprobe."+suffix;
  natsConnection* nc{}; jsCtx* js{};
  userver::engine::AsyncNoTracing(blocking,[&]{
    check(natsConnection_ConnectTo(&nc,server)); jsOptions options; check(jsOptions_Init(&options)); options.Wait=2000;
    check(natsConnection_JetStream(&js,nc,&options));
    jsStreamConfig sc; check(jsStreamConfig_Init(&sc)); sc.Name=stream.c_str();
    auto wildcard=subject+".*"; const char* subjects[]={wildcard.c_str()}; sc.Subjects=subjects; sc.SubjectsLen=1; sc.MaxMsgSize=512;
    jsStreamInfo* info{}; check(js_AddStream(&info,js,&sc,nullptr,nullptr)); jsStreamInfo_Destroy(info);
    for(int i=0;i<7;++i){
      jsConsumerConfig cc; check(jsConsumerConfig_Init(&cc)); auto name="reader"+std::to_string(i); auto filter=subject+"."+std::to_string(i);
      cc.Durable=name.c_str(); cc.FilterSubject=filter.c_str(); cc.AckPolicy=js_AckExplicit; cc.MaxAckPending=1;
      jsConsumerInfo* ci{}; check(js_AddConsumer(&ci,js,stream.c_str(),&cc,nullptr,nullptr)); jsConsumerInfo_Destroy(ci);
    }
  }).Get();
  auto arm=[&](int index){
    auto state=std::make_shared<AsyncPullWindow>();
    userver::engine::AsyncNoTracing(blocking,[&,state,index]{
      jsOptions opts; check(jsOptions_Init(&opts)); opts.Wait=2000;
      auto& pull=opts.PullSubscribeAsync; pull.MaxMessages=1; pull.MaxBytes=4096; pull.FetchSize=1; pull.KeepAhead=0;
      pull.Timeout=30000; pull.CompleteHandler=AsyncPullWindow::Complete; pull.CompleteHandlerClosure=state.get();
      jsSubOptions sub; check(jsSubOptions_Init(&sub)); auto durable="reader"+std::to_string(index); auto filter=subject+"."+std::to_string(index);
      sub.Stream=stream.c_str(); sub.Consumer=durable.c_str(); sub.ManualAck=true;
      check(js_PullSubscribeAsync(&state->subscription,js,filter.c_str(),durable.c_str(),AsyncPullWindow::Receive,state.get(),&opts,&sub,nullptr));
    }).Get(); return state;
  };
  std::vector<std::shared_ptr<AsyncPullWindow>> idle;
  for(int i=1;i<7;++i) idle.push_back(arm(i));
  std::atomic<bool> done{false}; int ticks=0; double maxGap=0;
  auto heartbeat=userver::engine::AsyncNoTracing(app,[&]{auto last=Clock::now();while(!done){userver::engine::SleepFor(5ms);auto now=Clock::now();maxGap=std::max(maxGap,ms(now-last));last=now;++ticks;}});
  config::NATSDataConnectorConfig cfg; cfg.type=api::DataConnectorType::kJetStream; cfg.servers={server}; cfg.operationTimeoutMs=2000;
  config::NATSEndpointConfig ep; ep.enabled=true; ep.subject=subject+".0"; ep.streamName=stream;
  nats::Client publisher(app,blocking,cfg,ep); int errors=0;
  publisher.start(Context{},false,[&](auto){++errors;});
  auto publish=[&]{
    auto completion=std::make_shared<userver::engine::SingleUseEvent>();
    publisher.publish(MessageContext{},"payload",{},[completion,&app](nats::DeliveryResult result){
      EXPECT_EQ(&userver::engine::current_task::GetTaskProcessor(),&app); EXPECT_TRUE(result.accepted); EXPECT_TRUE(result.ack); EXPECT_FALSE(result.error); completion->Send();
    }); return completion;
  };
  userver::engine::SleepFor(250ms); // Six native pulls remain idle; no blocking-worker fetch tasks.
  for(int sample=0;sample<6;++sample){
    auto begin=Clock::now(); auto active=arm(0); auto armed=Clock::now(); auto receipt=publish();
    EXPECT_TRUE(wait(*receipt)); auto published=Clock::now(); EXPECT_TRUE(wait(active->ready));
    EXPECT_EQ(&userver::engine::current_task::GetTaskProcessor(),&app);
    userver::engine::AsyncNoTracing(blocking,[&]{check(natsMsg_AckSync(active->message.load(),nullptr,nullptr));}).Get();
    EXPECT_TRUE(wait(active->complete)); EXPECT_EQ(active->calls,1); EXPECT_FALSE(active->wrongNativeAffinity); EXPECT_EQ(active->status,NATS_MAX_DELIVERED_MSGS);
    std::cout<<"ASYNC_PULL sample="<<sample<<" rearm_ms="<<ms(armed-begin)<<" puback_ms="<<ms(published-armed)<<" roundtrip_ack_ms="<<ms(Clock::now()-armed)<<std::endl;
  }
  // Saturate the broker while one application callback is held. No ACK and no
  // rearm: later records must remain in the native consumer, not application tasks.
  auto held=arm(0); auto receipt=publish(); EXPECT_TRUE(wait(*receipt));
  userver::engine::SingleUseEvent entered, neverRelease;
  std::atomic<int> live{0};
  auto pipeline=userver::engine::AsyncNoTracing(app,[held,&entered,&neverRelease,&live,&app]{
    if(held->ready.WaitUntil(userver::engine::Deadline::FromDuration(5s))!=userver::engine::FutureStatus::kReady) return;
    EXPECT_EQ(&userver::engine::current_task::GetTaskProcessor(),&app);
    ++live; entered.Send(); const auto status=neverRelease.WaitUntil(userver::engine::Deadline::FromDuration(5s));
    EXPECT_NE(status,userver::engine::FutureStatus::kReady); EXPECT_TRUE(userver::engine::current_task::ShouldCancel()); --live;
  });
  EXPECT_TRUE(wait(entered));
  for(int i=0;i<8;++i){auto extra=publish();EXPECT_TRUE(wait(*extra));}
  userver::engine::SleepFor(100ms);
  EXPECT_TRUE(wait(held->complete)); EXPECT_EQ(held->calls,1); EXPECT_EQ(live,1);
  userver::engine::AsyncNoTracing(blocking,[&]{
    jsConsumerInfo* ci{};check(js_GetConsumerInfo(&ci,js,stream.c_str(),"reader0",nullptr,nullptr));
    EXPECT_EQ(ci->NumAckPending,1); EXPECT_EQ(ci->NumPending,8); jsConsumerInfo_Destroy(ci);
  }).Get();
  auto stopBegin=Clock::now(); pipeline.RequestCancel(); pipeline.Get(); EXPECT_EQ(live,0);
  // Idle subscriptions still have pending native pulls. Completion fences their
  // closure lifetime; cancellation never frees a closure on Unsubscribe return.
  for(auto& state:idle) check(natsSubscription_Unsubscribe(state->subscription));
  for(auto& state:idle){EXPECT_TRUE(wait(state->complete)); EXPECT_EQ(state->calls,0); EXPECT_FALSE(state->wrongNativeAffinity);}
  EXPECT_FALSE(held->wrongNativeAffinity);
  held.reset(); idle.clear();
  publisher.stop(Context{}.bounded(5s));
  userver::engine::AsyncNoTracing(blocking,[&]{check(js_DeleteStream(js,stream.c_str(),nullptr,nullptr));jsCtx_Destroy(js);natsConnection_Destroy(nc);}).Get();
  done=true; heartbeat.Get(); EXPECT_EQ(errors,0); EXPECT_GT(ticks,20);
  std::cout<<"ASYNC_PULL shutdown_ms="<<ms(Clock::now()-stopBegin)<<" heartbeat_max_gap_ms="<<maxGap<<" ticks="<<ticks<<" held=1 broker_pending=8 errors="<<errors<<std::endl;
}
}
