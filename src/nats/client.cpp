#include <servicelib/runtime/nats/client.hpp>
#include <servicelib/runtime/detail/kafka_context.hpp>
#include <atomic>
#include <chrono>
#include <deque>
#include <mutex>
#include <unordered_map>
#include <userver/concurrent/background_task_storage.hpp>
#include <userver/engine/async.hpp>
#include <userver/engine/sleep.hpp>
#include <userver/engine/single_use_event.hpp>
#include <userver/engine/task/cancel.hpp>
#include <userver/formats/json.hpp>

namespace servicelib::nats {
namespace {
void Check(natsStatus s) {
  // Do not include server URLs, payloads or authentication material in errors.
  if (s != NATS_OK) throw std::runtime_error(std::string{"NATS: "}+natsStatus_GetText(s));
}
std::exception_ptr Error(natsStatus s) {
  return std::make_exception_ptr(std::runtime_error(std::string{"NATS: "}+natsStatus_GetText(s)));
}
std::string String(const char* s) { return s ? s : ""; }
using Msg = std::unique_ptr<natsMsg, decltype(&natsMsg_Destroy)>;
}
ConsumerMessage::ConsumerMessage(std::shared_ptr<natsMsg> message, bool jetstream,
    userver::engine::TaskProcessor& blocking, std::int64_t timeout)
    : message_(std::move(message)), jetstream_(jetstream), blocking_(&blocking), timeout_(timeout) {}
std::string_view ConsumerMessage::data() const { return {natsMsg_GetData(native()), static_cast<std::size_t>(natsMsg_GetDataLength(native()))}; }
std::string_view ConsumerMessage::subject() const { return natsMsg_GetSubject(native()); }
std::string_view ConsumerMessage::reply() const { const auto* r=natsMsg_GetReply(native()); return r ? r : ""; }
Headers ConsumerMessage::headers() const {
  Headers result;
  const char** keys{}; int count{};
  const auto status=natsMsgHeader_Keys(native(), &keys, &count);
  if (status == NATS_NOT_FOUND) return result;
  Check(status);
  for (int i=0; i<count; ++i) {
    const char** values{}; int size{};
    if (natsMsgHeader_Values(native(), keys[i], &values, &size)==NATS_OK) {
      for (int j=0; j<size; ++j) result.emplace(keys[i], String(values[j]));
      free(values);
    }
  }
  free(keys);
  return result;
}
void ConsumerMessage::requireJetStream() const { if (!jetstream_) throw std::logic_error("Core NATS has no ACK"); }
void ConsumerMessage::ack() const {
  requireJetStream();
  userver::engine::AsyncNoTracing(*blocking_, [message=message_] {
    const auto status = natsMsg_Ack(message.get(),nullptr);
    Check(status);
  }).Get();
}
void ConsumerMessage::doubleAck() const {
  requireJetStream();
  userver::engine::AsyncNoTracing(*blocking_, [message=message_, timeout=timeout_] {
    jsOptions options; Check(jsOptions_Init(&options)); options.Wait=timeout;
    const auto status = natsMsg_AckSync(message.get(),&options,nullptr);
    Check(status);
  }).Get();
}
void ConsumerMessage::nak() const {
  requireJetStream();
  userver::engine::AsyncNoTracing(*blocking_, [message=message_] {
    const auto status = natsMsg_Nak(message.get(),nullptr);
    Check(status);
  }).Get();
}
void ConsumerMessage::inProgress() const {
  requireJetStream();
  userver::engine::AsyncNoTracing(*blocking_, [message=message_] {
    const auto status = natsMsg_InProgress(message.get(),nullptr);
    Check(status);
  }).Get();
}
void ConsumerMessage::term() const {
  requireJetStream();
  userver::engine::AsyncNoTracing(*blocking_, [message=message_] {
    const auto status = natsMsg_Term(message.get(),nullptr);
    Check(status);
  }).Get();
}
MessageContext ContextFromHeaders(MessageContext context, const Headers& headers, bool tracingEnabled) {
  detail::KafkaHeaders canonical;
  for (const auto& [original,value] : headers) {
    auto key=original;
    std::transform(key.begin(),key.end(),key.begin(),[](unsigned char c){return std::tolower(c);});
    canonical.emplace(std::move(key), value);
  }
  return detail::ContextFromKafkaHeaders(std::move(context), canonical, tracingEnabled);
}
void InjectContext(const MessageContext& context, Headers& headers, bool tracingEnabled) {
  detail::KafkaHeaders injected;
  detail::InjectKafkaContext(context,injected,tracingEnabled);
  for (const auto& [key,value] : injected) {
    headers.erase(key);
    headers.emplace(key,value);
  }
}
struct Client::Impl {
  detail::SourceMetrics::Slot sourceMetrics;
  userver::engine::TaskProcessor& application;
  userver::engine::TaskProcessor& blocking;
  config::NATSDataConnectorConfig connector;
  config::NATSEndpointConfig endpoint;
  natsConnection* connection{};
  jsCtx* js{};
  natsSubscription* subscription{};
  ErrorHandler errorHandler;
  MessageHandler messageHandler;
  Context owner;
  bool pull{};
  std::atomic<bool> admission{false};
  std::atomic<bool> stopping{false};
  std::atomic<bool> errorPending{false};
  userver::engine::SingleUseEvent closed;
  std::mutex mutex;
  std::deque<std::shared_ptr<natsMsg>> messages;
  std::size_t queuedBytes{};
  bool worker{};
  std::unordered_map<natsMsg*,DeliveryHandler> publishes;
  std::atomic<std::size_t> deliveries{0};
  std::atomic<std::size_t> publishBytes{0};
  userver::concurrent::BackgroundTaskStorage tasks;
  userver::engine::TaskWithResult<void> pullTask;
  Impl(userver::engine::TaskProcessor& app, userver::engine::TaskProcessor& io,
       config::NATSDataConnectorConfig c, config::NATSEndpointConfig e)
      : application(app),blocking(io),connector(std::move(c)),endpoint(std::move(e)),tasks(app) {}
  bool jetstream() const { return connector.type==api::DataConnectorType::kJetStream; }
  // One native async window per endpoint lane. The native completion callback
  // fences both message callbacks and the closure; application retirement fences
  // rearming. No worker waits for an idle broker pull.
  struct PullWindow {
    userver::engine::SingleUseEvent complete;
    std::shared_ptr<natsMsg> message;
    natsSubscription* subscription{};
    natsStatus status{NATS_OK};
    ~PullWindow() { natsSubscription_Destroy(subscription); }
    static void OnMessage(natsConnection*, natsSubscription*, natsMsg* msg, void* closure) noexcept {
      auto& self=*static_cast<PullWindow*>(closure);
      self.message.reset(msg,natsMsg_Destroy);
    }
    static void OnComplete(natsConnection*, natsSubscription*, natsStatus status, void* closure) noexcept {
      auto& self=*static_cast<PullWindow*>(closure);
      self.status=status;
      self.complete.Send();
    }
  };
  std::shared_ptr<PullWindow> pullWindow; // protected by mutex, including unsubscribe/destroy
  void cancelPull() noexcept {
    std::lock_guard lock(mutex);
    if (pullWindow) (void)natsSubscription_Unsubscribe(pullWindow->subscription);
  }
  std::shared_ptr<PullWindow> armPull() {
    auto window=std::make_shared<PullWindow>();
    // Native setup may block, but does not wait for a record. Preserve task
    // ownership through setup even when cancellation races the returned handle.
    userver::engine::TaskCancellationBlocker blocker;
    userver::engine::AsyncNoTracing(blocking,[&,window] {
      jsOptions options; Check(jsOptions_Init(&options)); options.Wait=connector.operationTimeoutMs;
      auto& fetch=options.PullSubscribeAsync;
      fetch.MaxMessages=1; fetch.FetchSize=1; fetch.KeepAhead=0;
      fetch.MaxBytes=kMaxPendingBytes; fetch.Timeout=connector.operationTimeoutMs;
      fetch.CompleteHandler=PullWindow::OnComplete; fetch.CompleteHandlerClosure=window.get();
      jsSubOptions sub; Check(jsSubOptions_Init(&sub));
      sub.Stream=endpoint.streamName.c_str(); sub.Consumer=endpoint.durableName.c_str(); sub.ManualAck=true;
      Check(js_PullSubscribeAsync(&window->subscription,js,endpoint.subject.c_str(),endpoint.durableName.c_str(),PullWindow::OnMessage,window.get(),&options,&sub,nullptr));
    }).Get();
    { std::lock_guard lock(mutex); pullWindow=window;
      if (!admission || owner.cancelled() || userver::engine::current_task::ShouldCancel())
        (void)natsSubscription_Unsubscribe(window->subscription);
    }
    return window;
  }
  void runPull(std::shared_ptr<PullWindow> window) {
    const std::function<void()> cancel=[this]{cancelPull();};
    std::stop_callback ownerCancellation(owner.stopToken(),cancel);
    std::vector<std::unique_ptr<std::stop_callback<std::function<void()>>>> external;
    for (const auto& token:owner.externalStopTokens())
      external.push_back(std::make_unique<std::stop_callback<std::function<void()>>>(token,cancel));
    for (;;) {
      auto deadline=userver::engine::Deadline{};
      if (owner.deadline()) deadline=userver::engine::Deadline::FromTimePoint(*owner.deadline());
      if (window->complete.WaitUntil(deadline)!=userver::engine::FutureStatus::kReady) {
        cancelPull();
        window->complete.WaitNonCancellable(); // native completion, never broker receipt
      }
      { std::lock_guard lock(mutex); pullWindow.reset(); }
      if (!admission || owner.cancelled() || userver::engine::current_task::ShouldCancel()) break;
      if (window->message) {
        try {
          ConsumerMessage delivery{std::move(window->message),true,blocking,connector.operationTimeoutMs};
          detail::SourceMetrics::Observe(sourceMetrics,"received");
          jsMsgMetaData* metadata{};
          if (natsMsg_GetMetaData(&metadata,delivery.native())==NATS_OK) {
            if (metadata->NumDelivered>1) detail::SourceMetrics::Observe(sourceMetrics,"redelivered");
            jsMsgMetaData_Destroy(metadata);
          }
          messageHandler(std::move(delivery));
        } catch (...) { if (admission) report(std::current_exception()); }
      } else if (window->status!=NATS_TIMEOUT && window->status!=NATS_OK) {
        report(Error(window->status));
        // Terminal errors (including a record exceeding the byte window) must
        // not become a hot retry loop or silently skip an unacknowledged record.
        if (window->status!=NATS_NO_RESPONDERS && window->status!=NATS_CONNECTION_DISCONNECTED && window->status!=NATS_MISSED_HEARTBEAT) break;
        userver::engine::InterruptibleSleepFor(std::chrono::milliseconds(100));
      }
      window.reset();
      if (!admission || owner.cancelled() || userver::engine::current_task::ShouldCancel()) break;
      // Reconnect/setup can fail while the official connection is reconnecting.
      // Retry only that observable transport state; all other setup errors park.
      while (admission && !owner.cancelled() && !userver::engine::current_task::ShouldCancel()) {
        try { window=armPull(); break; }
        catch (...) {
          report(std::current_exception());
          if (!natsConnection_IsReconnecting(connection)) return;
          userver::engine::InterruptibleSleepFor(std::chrono::milliseconds(100));
        }
      }
      if (!window) break;
    }
  }

  void report(std::exception_ptr error) noexcept {
    if (stopping || errorPending.exchange(true)) return;
    try {
      tasks.CriticalAsyncDetach("nats-error",[this,error] {
        struct Reset { std::atomic<bool>& flag; ~Reset(){flag=false;} } reset{errorPending};
        if (errorHandler) errorHandler(error);
      });
    } catch (...) { errorPending=false; }
  }
  void deliver(DeliveryHandler callback, DeliveryResult result) noexcept {
    try {
      tasks.CriticalAsyncDetach("nats-delivery",[this,callback=std::move(callback),result=std::move(result)]() mutable {
        struct Release { std::atomic<std::size_t>& count; ~Release(){--count;} } release{deliveries};
        try { callback(std::move(result)); } catch (...) { report(std::current_exception()); }
      });
    } catch (...) { --deliveries; }
  }
  static void OnAck(jsCtx*,natsMsg* msg,jsPubAck* ack,jsPubAckErr* error,void* closure) noexcept {
    auto& self=*static_cast<Impl*>(closure);
    Msg owned{msg,natsMsg_Destroy};
    DeliveryHandler callback;
    { std::lock_guard lock(self.mutex); auto it=self.publishes.find(msg);
      if (it==self.publishes.end()) return;
      callback=std::move(it->second); self.publishes.erase(it); }
    DeliveryResult result;
    if (ack) { result.accepted=true; result.ack=PublishAck{String(ack->Stream),ack->Sequence,ack->Duplicate}; }
    else result.error=Error(error ? error->Err : NATS_ERR);
    self.deliver(std::move(callback),std::move(result));
  }
  void enqueue(std::shared_ptr<natsMsg> msg) noexcept {
    if (!admission) return;
    bool launch=false;
    { std::lock_guard lock(mutex);
      const auto size=static_cast<std::size_t>(natsMsg_GetDataLength(msg.get()));
      if (messages.size()>=kMaxPending || queuedBytes+size>kMaxPendingBytes) {
        report(Error(NATS_SLOW_CONSUMER)); return;
      }
      queuedBytes+=size; messages.push_back(std::move(msg));
      if (!worker) { worker=true; launch=true; }
    }
    if (launch) tasks.CriticalAsyncDetach("nats-source",[this] {
      for (;;) {
        std::shared_ptr<natsMsg> msg;
        { std::lock_guard lock(mutex);
          if (messages.empty()) {worker=false; return;}
          msg=std::move(messages.front()); messages.pop_front();
          queuedBytes-=natsMsg_GetDataLength(msg.get());
        }
        try {
          ConsumerMessage delivery{std::move(msg),jetstream(),blocking,connector.operationTimeoutMs};
          detail::SourceMetrics::Observe(sourceMetrics, "received");
          messageHandler(std::move(delivery));
        }
        catch (...) {report(std::current_exception());}
      }
    });
  }
  static void OnMessage(natsConnection*,natsSubscription*,natsMsg* msg,void* closure) noexcept {
    static_cast<Impl*>(closure)->enqueue(std::shared_ptr<natsMsg>{msg,natsMsg_Destroy});
  }
  std::int64_t startupRemaining(std::int64_t maximum) const {
    if (owner.cancelled() || userver::engine::current_task::ShouldCancel())
      throw std::runtime_error("NATS startup cancelled");
    if (owner.deadline()) maximum=std::min(maximum,
        std::chrono::duration_cast<std::chrono::milliseconds>(*owner.deadline()-std::chrono::steady_clock::now()).count());
    return std::max<std::int64_t>(1,maximum);
  }
  jsOptions startupOptions() const {
    jsOptions options; Check(jsOptions_Init(&options));
    options.Wait=startupRemaining(connector.operationTimeoutMs);
    return options;
  }
  void ensureStream() {
    jsStreamInfo* info{}; jsErrCode code{};
    auto options=startupOptions();
    auto status=js_GetStreamInfo(&info,js,endpoint.streamName.c_str(),&options,&code);
    if (status!=NATS_OK) {
      if (code!=JSStreamNotFoundErr || !endpoint.createStream) Check(status);
      jsStreamConfig cfg; Check(jsStreamConfig_Init(&cfg));
      cfg.Name=endpoint.streamName.c_str(); const char* subject=endpoint.subject.c_str();
      cfg.Subjects=&subject; cfg.SubjectsLen=1; cfg.Storage=js_FileStorage;
      options=startupOptions();
      Check(js_AddStream(&info,js,&cfg,&options,&code));
    }
    jsStreamInfo_Destroy(info);
  }
  void ensureConsumer() {
    jsConsumerInfo* info{}; jsErrCode code{};
    auto options=startupOptions();
    auto status=js_GetConsumerInfo(&info,js,endpoint.streamName.c_str(),endpoint.durableName.c_str(),&options,&code);
    if (status!=NATS_OK) {
      if (code!=JSConsumerNotFoundErr || !endpoint.createConsumer) Check(status);
      // nats.c AddConsumer is create-or-update. Use the native management
      // request with explicit create action to preserve existing configuration.
      userver::formats::json::ValueBuilder body;
      body["stream_name"]=endpoint.streamName; body["action"]="create";
      body["config"]["durable_name"]=endpoint.durableName;
      body["config"]["name"]=endpoint.durableName;
      body["config"]["ack_policy"]="explicit";
      body["config"]["deliver_policy"]="all";
      body["config"]["filter_subject"]=endpoint.subject;
      body["config"]["max_ack_pending"]=static_cast<int>(kMaxPending);
      const auto payload=userver::formats::json::ToString(body.ExtractValue());
      const auto subject="$JS.API.CONSUMER.CREATE."+endpoint.streamName+"."+endpoint.durableName;
      natsMsg* response{};
      Check(natsConnection_Request(&response,connection,subject.c_str(),payload.data(),static_cast<int>(payload.size()),startupRemaining(connector.operationTimeoutMs)));
      Msg owned{response,natsMsg_Destroy};
      const auto answer=userver::formats::json::FromString(std::string{natsMsg_GetData(response),static_cast<std::size_t>(natsMsg_GetDataLength(response))});
      if (answer.HasMember("error")) throw std::runtime_error("NATS create-only consumer failed");
      options=startupOptions();
      Check(js_GetConsumerInfo(&info,js,endpoint.streamName.c_str(),endpoint.durableName.c_str(),&options,&code));
    }
    std::unique_ptr<jsConsumerInfo,decltype(&jsConsumerInfo_Destroy)> owned{info,jsConsumerInfo_Destroy};
    if (info->Config->AckPolicy!=js_AckExplicit || String(info->Config->FilterSubject)!=endpoint.subject || info->Config->FilterSubjectsLen>0)
      throw std::invalid_argument("JetStream existing consumer must match subject and explicit ACK policy");
    pull=String(info->Config->DeliverSubject).empty();
  }
};
void Client::setSourceMetrics(std::shared_ptr<detail::SourceMetrics> metrics) {
  impl_->sourceMetrics.store(std::move(metrics));
}
Client::Client(userver::engine::TaskProcessor& app,userver::engine::TaskProcessor& blocking,
               config::NATSDataConnectorConfig connector,config::NATSEndpointConfig endpoint)
    :impl_(std::make_unique<Impl>(app,blocking,std::move(connector),std::move(endpoint))) {}
Client::~Client() {
  if (impl_->connection) {
    try { stop(Context{}); } catch (...) { std::terminate(); }
  }
}
const std::string& Client::subject() const {return impl_->endpoint.subject;}
void Client::start(Context context,bool source,ErrorHandler onError) {
  auto& p=*impl_; ValidateEndpoint(p.connector,p.endpoint,source);
  if (context.cancelled() || userver::engine::current_task::ShouldCancel()) throw std::runtime_error("NATS startup cancelled");
  p.owner=context; p.errorHandler=std::move(onError);
  if (!p.endpoint.enabled) return;
  try {
  userver::engine::AsyncNoTracing(p.blocking,[&p,source] {
    natsOptions* raw{}; Check(natsOptions_Create(&raw));
    std::unique_ptr<natsOptions,decltype(&natsOptions_Destroy)> options{raw,natsOptions_Destroy};
    std::vector<const char*> servers;
    for (const auto& server:p.connector.servers) servers.push_back(server.c_str());
    Check(natsOptions_SetServers(raw,servers.data(),static_cast<int>(servers.size())));
    Check(natsOptions_SetTimeout(raw,p.startupRemaining(p.connector.connectTimeoutMs)));
    Check(natsOptions_SetMaxReconnect(raw,-1));
    Check(natsOptions_SetReconnectBufSize(raw,kMaxPendingBytes));
    Check(natsOptions_SetErrorHandler(raw,[](natsConnection*,natsSubscription*,natsStatus status,void* closure){static_cast<Impl*>(closure)->report(Error(status));},&p));
    Check(natsOptions_SetClosedCB(raw,[](natsConnection*,void* closure){static_cast<Impl*>(closure)->closed.Send();},&p));
    if (!p.connector.username.empty()) Check(natsOptions_SetUserInfo(raw,p.connector.username.c_str(),p.connector.password.c_str()));
    if (!p.connector.token.empty()) Check(natsOptions_SetToken(raw,p.connector.token.c_str()));
    if (!p.connector.credentialsFile.empty()) Check(natsOptions_SetUserCredentialsFromFiles(raw,p.connector.credentialsFile.c_str(),nullptr));
    const bool tls=p.connector.tlsEnabled || p.connector.servers.front().starts_with("tls://");
    if (tls) Check(natsOptions_SetSecure(raw,true));
    if (!p.connector.tlsServerName.empty()) Check(natsOptions_SetExpectedHostname(raw,p.connector.tlsServerName.c_str()));
    if (!p.connector.tlsCaFile.empty()) Check(natsOptions_LoadCATrustedCertificates(raw,p.connector.tlsCaFile.c_str()));
    if (!p.connector.tlsCertFile.empty()) Check(natsOptions_LoadCertificatesChain(raw,p.connector.tlsCertFile.c_str(),p.connector.tlsKeyFile.c_str()));
    Check(natsConnection_Connect(&p.connection,raw));
    (void)p.startupRemaining(p.connector.operationTimeoutMs);
    if (p.jetstream()) {
      jsOptions options; Check(jsOptions_Init(&options)); options.Wait=p.connector.operationTimeoutMs;
      options.PublishAsync.MaxPending=kMaxPending; options.PublishAsync.StallWait=1;
      options.PublishAsync.AckHandler=Impl::OnAck; options.PublishAsync.AckHandlerClosure=&p;
      Check(natsConnection_JetStream(&p.js,p.connection,&options));
      p.ensureStream(); if (source) p.ensureConsumer();
    }
  }).Get();
  // A native connect may finish after context/task cancellation. Never turn
  // that completion into admission; close while task-owned state is still alive.
  (void)p.startupRemaining(p.connector.operationTimeoutMs);
  p.admission=true;
  } catch (...) {
    const auto error=std::current_exception();
    stop(context.withDeadline(std::chrono::steady_clock::now()));
    std::rethrow_exception(error);
  }
}
void Client::subscribe(MessageHandler handler) {
  auto& p=*impl_; p.messageHandler=std::move(handler); if (!p.admission) return;
  if (p.pull) {
    auto window=p.armPull();
    try {
      p.pullTask=userver::engine::CriticalAsyncNoTracing(p.application,[&p,window]() mutable {p.runPull(std::move(window));});
    } catch (...) {
      p.cancelPull(); window->complete.WaitNonCancellable();
      { std::lock_guard lock(p.mutex); p.pullWindow.reset(); }
      throw;
    }
    return;
  }
  userver::engine::AsyncNoTracing(p.blocking,[&p] {
    if (p.jetstream()) {
      jsSubOptions options; Check(jsSubOptions_Init(&options));
      options.Stream=p.endpoint.streamName.c_str(); options.Consumer=p.endpoint.durableName.c_str(); options.ManualAck=true;
      Check(js_Subscribe(&p.subscription,p.js,p.endpoint.subject.c_str(),Impl::OnMessage,&p,nullptr,&options,nullptr));
    } else if (p.endpoint.queueGroup.empty()) Check(natsConnection_Subscribe(&p.subscription,p.connection,p.endpoint.subject.c_str(),Impl::OnMessage,&p));
    else Check(natsConnection_QueueSubscribe(&p.subscription,p.connection,p.endpoint.subject.c_str(),p.endpoint.queueGroup.c_str(),Impl::OnMessage,&p));
    Check(natsSubscription_SetPendingLimits(p.subscription,kMaxPending,kMaxPendingBytes));
    Check(natsConnection_FlushTimeout(p.connection,p.connector.operationTimeoutMs));
  }).Get();

}
namespace {
Msg MakeMessage(const std::string& subject,const std::string& value,const Headers& headers) {
  natsMsg* raw{}; Check(natsMsg_Create(&raw,subject.c_str(),nullptr,value.data(),static_cast<int>(value.size())));
  Msg result{raw,natsMsg_Destroy};
  for (const auto& [key,value]:headers) Check(natsMsgHeader_Add(raw,key.c_str(),value.c_str()));
  return result;
}
}
void Client::publish(MessageContext context,std::string value,Headers headers,DeliveryHandler callback) {
  auto& p=*impl_;
  if (!p.admission || context.cancelled()) throw std::runtime_error("NATS publish stopped or cancelled");
  if (value.size()>kMaxPendingBytes) throw std::runtime_error("NATS message exceeds local byte bound");
  if (p.deliveries.fetch_add(1)>=kMaxPending) {--p.deliveries; throw std::runtime_error("NATS publish admission full");}
  const auto bytes=value.size();
  if(p.publishBytes.fetch_add(bytes)+bytes>kMaxPendingBytes) {
    p.publishBytes.fetch_sub(bytes);--p.deliveries;
    throw std::runtime_error("NATS publish byte admission full");
  }
  auto original=std::move(callback);
  callback=[&p,bytes,original=std::move(original)](DeliveryResult result) mutable {
    struct Release { std::atomic<std::size_t>& count; std::size_t bytes; ~Release(){count.fetch_sub(bytes);} } release{p.publishBytes,bytes};
    original(std::move(result));
  };
  Msg msg{nullptr,natsMsg_Destroy};
  try { msg=MakeMessage(p.endpoint.subject,value,headers); }
  catch (...) {p.publishBytes.fetch_sub(bytes);--p.deliveries; throw;}
  // Async NATS APIs may wait on native locks or the pending-publication limit.
  // Keep that admission wait off the application task processor; PubAck itself
  // remains callback-driven and is handed back through BackgroundTaskStorage.
  userver::engine::AsyncNoTracing(p.blocking,
      [&p,msg=std::move(msg),callback=std::move(callback)]() mutable {
  if (!p.jetstream()) {
    const auto status=natsConnection_PublishMsg(p.connection,msg.get());
    DeliveryResult result; result.accepted=status==NATS_OK; if (status!=NATS_OK) result.error=Error(status);
    p.deliver(std::move(callback),std::move(result)); return;
  }
  auto* raw=msg.release();
  {std::lock_guard lock(p.mutex); p.publishes.emplace(raw,std::move(callback));}
  const auto key=raw;
  jsPubOptions options; Check(jsPubOptions_Init(&options)); options.MaxWait=p.connector.operationTimeoutMs; options.ExpectStream=p.endpoint.streamName.c_str();
  const auto status=js_PublishMsgAsync(p.js,&raw,&options);
  if (status!=NATS_OK) {
    DeliveryHandler cb;
    {std::lock_guard lock(p.mutex); auto it=p.publishes.find(key); if(it!=p.publishes.end()){cb=std::move(it->second);p.publishes.erase(it);}}
    natsMsg_Destroy(raw);
    if(cb) {DeliveryResult result;result.error=Error(status);p.deliver(std::move(cb),std::move(result));}
  }
  }).Get();
}
DeliveryResult Client::publishSync(MessageContext context,std::string value,Headers headers) {
  auto& p=*impl_;
  if (!p.admission || context.cancelled()) throw std::runtime_error("NATS publish stopped or cancelled");
  return userver::engine::AsyncNoTracing(p.blocking,[&p,value=std::move(value),headers=std::move(headers)] {
    DeliveryResult result;
    try {
      auto msg=MakeMessage(p.endpoint.subject,value,headers);
      if (p.jetstream()) {
        jsPubAck* ack{}; jsPubOptions options; Check(jsPubOptions_Init(&options)); options.MaxWait=p.connector.operationTimeoutMs; options.ExpectStream=p.endpoint.streamName.c_str();
        Check(js_PublishMsg(&ack,p.js,msg.get(),&options,nullptr));
        result.ack=PublishAck{String(ack->Stream),ack->Sequence,ack->Duplicate}; jsPubAck_Destroy(ack);
      } else {Check(natsConnection_PublishMsg(p.connection,msg.get()));Check(natsConnection_FlushTimeout(p.connection,p.connector.operationTimeoutMs));result.flushed=true;}
      result.accepted=true;
    } catch (...) {result.error=std::current_exception();}
    return result;
  }).Get();
}
void Client::stopAdmission(Context context) {
  auto& p=*impl_;
  if (p.pull) { p.admission=false; p.cancelPull(); return; }
  auto deadline=std::chrono::steady_clock::now()+std::chrono::milliseconds(p.connector.operationTimeoutMs);
  if(context.deadline()) deadline=std::min(deadline,*context.deadline());
  if(context.cancelled()) deadline=std::chrono::steady_clock::now();
  if (p.subscription) userver::engine::AsyncNoTracing(p.blocking,[&p,deadline] {
    const auto remaining=[deadline] {
      return std::max<std::int64_t>(1,std::chrono::duration_cast<std::chrono::milliseconds>(deadline-std::chrono::steady_clock::now()).count());
    };
    // Subscription drain shares the caller's stop budget; it must not add a
    // fresh operationTimeout before the bounded connection shutdown can begin.
    Check(natsSubscription_DrainTimeout(p.subscription,remaining()));
    Check(natsSubscription_WaitForDrainCompletion(p.subscription,remaining()));
  }).Get();
  p.admission=false;
}
bool Client::waitForCallbacks(Context context) {
  auto& p=*impl_;
  auto deadline=std::chrono::steady_clock::now()+std::chrono::milliseconds(p.connector.operationTimeoutMs);
  if(context.deadline()) deadline=std::min(deadline,*context.deadline());
  while(p.tasks.ActiveTasksApprox()!=0 || (p.pullTask.IsValid() && !p.pullTask.IsFinished())) {
    if(context.cancelled() || std::chrono::steady_clock::now()>=deadline) return false;
    userver::engine::SleepFor(std::chrono::milliseconds(1));
  }
  return true;
}
void Client::stop(Context context) {
  auto& p=*impl_; userver::engine::TaskCancellationBlocker cancelBlocker;
  p.admission=false;
  p.cancelPull();
  if (!p.connection) return;
  auto deadline=std::chrono::steady_clock::now()+std::chrono::milliseconds(p.connector.operationTimeoutMs);
  if(context.deadline()) deadline=std::min(deadline,*context.deadline());
  if(context.cancelled()) deadline=std::chrono::steady_clock::now();
  // Join the single pull lane before closing its connection. In particular a
  // queued/running native rearm must not race connection destruction. The lane
  // owns completion fences; cancellation cannot abandon an active closure.
  if (p.pullTask.IsValid()) {
    p.pullTask.WaitUntil(userver::engine::Deadline::FromTimePoint(deadline));
    if (!p.pullTask.IsFinished()) p.pullTask.RequestCancel();
    p.pullTask.Wait();
    p.pullTask={};
  }
  // Delivery callbacks retain the result/error routes until this drain ends.
  while(p.deliveries && std::chrono::steady_clock::now()<deadline) userver::engine::SleepFor(std::chrono::milliseconds(1));
  p.stopping=true;
  const auto remaining=std::max<std::int64_t>(1,std::chrono::duration_cast<std::chrono::milliseconds>(deadline-std::chrono::steady_clock::now()).count());
  const auto status=userver::engine::AsyncNoTracing(p.blocking,[&p,remaining] {
    return natsConnection_DrainTimeout(p.connection,remaining);
  }).Get();
  if(status!=NATS_OK || p.closed.WaitUntil(userver::engine::Deadline::FromTimePoint(deadline))!=userver::engine::FutureStatus::kReady) {
    natsConnection_Close(p.connection);
    p.closed.WaitNonCancellable();
  }
  // Close completes native delivery callbacks before the callback store closes.
  std::vector<DeliveryHandler> pending;
  {std::lock_guard lock(p.mutex);for(auto& [_,cb]:p.publishes) pending.push_back(std::move(cb));p.publishes.clear();}
  for(auto& cb:pending){DeliveryResult result;result.error=Error(NATS_CONNECTION_CLOSED);p.deliver(std::move(cb),std::move(result));}
  // Give admitted callbacks the remaining graceful-stop budget, then request
  // cooperative cancellation using the stock userver task storage.
  if (waitForCallbacks(context.withDeadline(deadline))) p.tasks.WaitAndDisposeSlow();
  else p.tasks.CancelAndWait();
  natsSubscription_Destroy(p.subscription); p.subscription=nullptr;
  jsCtx_Destroy(p.js);p.js=nullptr;
  natsConnection_Destroy(p.connection);p.connection=nullptr;
}
}  // namespace servicelib::nats
