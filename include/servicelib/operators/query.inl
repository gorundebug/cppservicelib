template <typename R, typename E, typename QueryFunction, typename C = StreamConsumer<R>>
class Query final : public TransformStream<R, C>,
                             public virtual StreamConsumer<_Tp>,
                             public QueryEndpointStream<_Tp, R, E> {
  template <typename, typename>
  friend class StreamExecutionEnvironment;
  template <typename, typename, typename>
  friend class Stream;
  friend class StreamBuilderContext;

  StreamFunction<QueryFunction, Query> f_;
  int endpointId_{0};
  ErrorStream<E> errorStream_;

 public:
  using topology_value_type = R;
  using TransformStream<R, C>::consume;

  void consume(MessageContext ctx, Payload<_Tp> payload) override {
    const auto snapshot = this->getEnv()->getRuntimeConfigSnapshot();
    if (snapshot) {
      const auto endpoint = snapshot->GetEndpointConfigByID(endpointId_);
      if (endpoint) {
        const auto* query = endpoint->template As<servicelib::config::QueryEndpointConfig>();
        if (query && !query->enabled) return;
      }
    }
    tracing::ActiveSpan activeSpan;
    if (this->getStreamTracer() && tracing::SamplingEnabled(ctx)) {
      activeSpan = tracing::StartStreamSpan(ctx, *this, "stream.query");
    }
    f_(std::move(ctx), payload.get());
  }

  // Go: SinkStreamWithResult[T,R,E].errorConsumer — MakeErrorStream[E](id, env),
  // same config id as this Query (see constructor).
  ErrorStream<E>& getErrorStream() noexcept { return errorStream_; }

  void produceError(MessageContext ctx, Payload<E> payload) {
    errorStream_.emit(std::move(ctx), std::move(payload));
  }

  void consumeResult(MessageContext ctx, Payload<R> payload) {
    if (this->hasConsumer()) {
      this->context().template consume<R>(std::move(ctx), *this,
                                          *this->consumer(),
                                          std::move(payload));
    }
  }

  void consumeResult(MessageContext ctx, const R& value) {
    consumeResult(std::move(ctx), Payload<R>::make(value));
  }

  void consumeResult(MessageContext ctx, R&& value) {
    consumeResult(std::move(ctx), Payload<R>::make(std::move(value)));
  }

  [[nodiscard]] IServiceEnvironment& environment() const override {
    return *this->getEnv();
  }
  [[nodiscard]] int endpointId() const noexcept override { return endpointId_; }
  [[nodiscard]] size_t streamConfigId() const noexcept override {
    return this->getConfigId();
  }
  void collectResult(MessageContext ctx, Payload<R> payload) override {
    consumeResult(std::move(ctx), std::move(payload));
  }
  void collectError(MessageContext ctx, Payload<E> payload) override {
    produceError(std::move(ctx), std::move(payload));
  }

  [[nodiscard]] int getEndpointId() const noexcept { return endpointId_; }
  size_t getId() const noexcept override {
    return TransformStream<R, C>::getId();
  }
  const std::string& getName() const noexcept override {
    return TransformStream<R, C>::getName();
  }

 private:
  // Go-aligned: config id + env; result serde (R) is freshly resolved
  // (Go: SinkStreamWithResult embeds ConsumedStream[R] via
  // runtime.MakeSerde[R](env)). The _Tp serde parameter is just propagated
  // as-is — Go has no serde slot for the sink's consumed input type.
  template <typename Ctx>
  Query(const servicelib::config::QueryStreamConfig& cfg,
                 serde::StreamSerde<_Tp>* serde, IRuntimeEnvironment* env,
                 StreamFunction<QueryFunction, Ctx>&& f)
      : f_(std::move(f), *this), endpointId_(cfg.idEndpoint) {
    this->setConfigIdentity(cfg);
    StreamConsumer<_Tp>::serde_ = serde;
    TransformStream<R, C>::resolveDefaultSerde();
    this->setEnv(env);
    errorStream_.configure(static_cast<size_t>(cfg.id), cfg.name + "Error", env);
  }

  template <typename T, typename Ctx>
  Query(const servicelib::config::QueryStreamConfig& cfg,
                 serde::StreamSerde<_Tp>* serde, IRuntimeEnvironment* env,
                 StreamFunction<QueryFunction, Ctx>&& f, unique_ptr<T> consumer)
      : TransformStream<R, T>(std::move(consumer)),
        f_(std::move(f), *this),
        endpointId_(cfg.idEndpoint) {
    this->setConfigIdentity(cfg);
    StreamConsumer<_Tp>::serde_ = serde;
    TransformStream<R, C>::resolveDefaultSerde();
    this->setEnv(env);
    errorStream_.configure(static_cast<size_t>(cfg.id), cfg.name + "Error", env);
  }

  template <typename Ctx>
  static unique_ptr<Query> make(
      const servicelib::config::QueryStreamConfig& cfg,
      serde::StreamSerde<_Tp>* serde, IRuntimeEnvironment* env,
      StreamFunction<QueryFunction, Ctx>&& f) {
    return unique_ptr<Query>(
        new Query(cfg, serde, env, std::move(f)));
  }

  template <typename T, typename Ctx>
  static unique_ptr<Query<R, E, QueryFunction, T>> make(
      const servicelib::config::QueryStreamConfig& cfg,
      serde::StreamSerde<_Tp>* serde, IRuntimeEnvironment* env,
      StreamFunction<QueryFunction, Ctx>&& f, unique_ptr<T> consumer) {
    return unique_ptr<Query<R, E, QueryFunction, T>>(
        new Query<R, E, QueryFunction, T>(cfg, serde, env, std::move(f),
                                               std::move(consumer)));
  }

  const std::string_view& getType() const override {
    return StreamBuilderContext::getType<decltype(*this)>();
  }
  std::string getCode() const override {
    return f_.isInternalType() ? f_.getFunctionCode() : std::string{};
  }
  bool hasConsumer() const noexcept override {
    return TransformStream<R, C>::hasConsumer();
  }
  const StreamBase& getConsumer() const override {
    return TransformStream<R, C>::getConsumer();
  }
  StreamBase& getConsumer() override {
    return TransformStream<R, C>::getConsumer();
  }
  const StreamBase& getBase() const noexcept override {
    return TransformStream<R, C>::getBase();
  }
  StreamBase& getBase() noexcept override {
    return TransformStream<R, C>::getBase();
  }

  size_t buildTopology(StreamBuilderContext& ctx, size_t id,
                       StreamBuilderContext::TIdsList* splitConsumerIds,
                       bool skip) override {
    ctx.buildTopology(*this, id);
    if (splitConsumerIds) splitConsumerIds->emplace_back(id);
    const auto nextId = this->buildTopologyCommon(ctx, id, nullptr, skip);
    errorStream_.prepareTopologyId(this->getId());
    errorStream_.prepareConsumerCaller();
    return nextId;
  }
  void verifyTopology(StreamVerifyContext& ctx) const override {
    TransformStream<R, C>::verifyTopology(ctx);
  }
  void printTopology(TopologyPrinter& tp,
                     std::unordered_set<size_t>& visited) const override {
    TransformStream<R, C>::printTopology(tp, visited);
    if (errorStream_.getErrorConsumer()) {
      tp.printLink(tp.makeNode(*this), tp.makeNode(errorStream_));
      errorStream_.printErrorTopology(tp, visited);
    }
  }

  template <typename Ctx>
  static auto build(Query& stream,
                    StreamFunction<QueryFunction, Ctx>&& f) {
    servicelib::config::QueryStreamConfig cfg;
    cfg.id = static_cast<int>(stream.getConfigId());
    cfg.name = stream.getName();
    cfg.idEndpoint = stream.endpointId_;
    auto result = make(cfg, stream.getSerde(), stream.getEnv(), std::move(f));
    result->copySettings(stream);
    return result;
  }

  template <typename T, typename Ctx>
  static auto build(Query& stream, unique_ptr<T> consumer,
                    StreamFunction<QueryFunction, Ctx>&& f) {
    servicelib::config::QueryStreamConfig cfg;
    cfg.id = static_cast<int>(stream.getConfigId());
    cfg.name = stream.getName();
    cfg.idEndpoint = stream.endpointId_;
    auto result = make(cfg, stream.getSerde(), stream.getEnv(), std::move(f),
                       std::move(consumer));
    result->copySettings(stream);
    return result;
  }
};
