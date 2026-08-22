// Part of class Stream<_Tp, _Cp, _Context> — included by stream.hpp.
// Go analog: operators/multijoin.go.
// Link/storage primitives shared with binary Join are declared in join.inl.

template <typename _TTp, typename _JTp, JoinStrategyEnum _JoinStrategy,
          typename _CCp>
class MultiJoinBuilder;

template <typename _TTp, typename _JTp, JoinStrategyEnum _JoinStrategy,
          typename _CCp = StreamConsumer<_JTp>>
class MultiJoin : public TransformStream<_JTp, _CCp> {
  template <typename, typename>
  friend class StreamExecutionEnvironment;

 protected:
  MultiJoin() = default;
  ~MultiJoin() override = default;

  template <typename T>
  explicit MultiJoin(unique_ptr<T> consumer)
      : TransformStream<_JTp, T>(std::move(consumer)) {}

  const std::string_view& getType() const override {
    return StreamBuilderContext::getType<decltype(*this)>();
  }

  template <typename JoinFunction, typename Ctx>
  static auto build(MultiJoin& stream,
                    StreamFunction<JoinFunction, Ctx>&& function) {
    auto result = MultiJoinBuilder<_TTp, _JTp, _JoinStrategy, _CCp>::build(
        std::move(function));
    result->copySettings(stream);
    return result;
  }

  template <typename T, typename JoinFunction, typename Ctx>
  static auto build(MultiJoin& stream, unique_ptr<T> consumer,
                    StreamFunction<JoinFunction, Ctx>&& function) {
    auto result = MultiJoinBuilder<_TTp, _JTp, _JoinStrategy, T>::build(
        std::move(consumer), std::move(function));
    result->copySettings(stream);
    return result;
  }
};

// Concrete multi-input join implementation.

template <typename _TTp, typename _JTp, typename _JoinFunction,
          JoinStrategyEnum _JoinStrategy, typename _CCp = StreamConsumer<_JTp>>
class MultiJoinImpl final : public MultiJoin<_TTp, _JTp, _JoinStrategy, _CCp>,
                            public virtual StreamConsumer<_Tp> {
  template <typename, typename>
  friend class StreamExecutionEnvironment;
  template <typename, typename, typename>
  friend class Stream;
  template <typename, typename, JoinStrategyEnum, typename>
  friend class MultiBuilder;
  template <typename, typename, size_t>
  friend class JoinLinkImpl;
  friend class StreamBuilderContext;

  StreamFunction<_JoinFunction, MultiJoinImpl> f_;

 public:
  using topology_value_type = _JTp;
  using MultiJoin<_TTp, _JTp, _JoinStrategy, _CCp>::consume;

  void consume(MessageContext ctx,
               [[maybe_unused]] Payload<_Tp> payload) override {
    [[maybe_unused]] auto activeSpan =
        tracing::StartStreamSpan(ctx, *this, "stream.join");
    if (this->hasConsumer()) {
    }
  }

  size_t getId() const noexcept override {
    return MultiJoin<_TTp, _JTp, _JoinStrategy, _CCp>::getId();
  }

  const std::string& getName() const noexcept override {
    return MultiJoin<_TTp, _JTp, _JoinStrategy, _CCp>::getName();
  }

 protected:
  template <typename T, typename Ctx = MultiJoinImpl>
  MultiJoinImpl(StreamFunction<_JoinFunction, Ctx>&& f, unique_ptr<T> consumer)
      : MultiJoin<_TTp, _JTp, _JoinStrategy, T>(std::move(consumer)),
        f_(std::move(f), *this) {}

  template <typename Ctx = MultiJoinImpl>
  explicit MultiJoinImpl(StreamFunction<_JoinFunction, Ctx>&& f)
      : MultiJoin<_TTp, _JTp, _JoinStrategy, _CCp>(), f_(std::move(f), *this) {}

  // Go-aligned: config id + env; output serde (_JTp) is freshly resolved
  // (Go: runtime.MakeSerde[R](env)). The _Tp serde parameter is just
  // propagated as-is — Go has no serde slot for the input type of a
  // type-changing operator.
  template <typename Ctx = MultiJoinImpl>
  MultiJoinImpl(const servicelib::config::MultiJoinStreamConfig& cfg,
                serde::StreamSerde<_Tp>* serde, IRuntimeEnvironment* env,
                StreamFunction<_JoinFunction, Ctx>&& f)
      : MultiJoin<_TTp, _JTp, _JoinStrategy, _CCp>(), f_(std::move(f), *this) {
    this->setConfigIdentity(cfg);
    StreamConsumer<_Tp>::serde_ = serde;
    MultiJoin<_TTp, _JTp, _JoinStrategy, _CCp>::resolveDefaultSerde();
    this->env_ = env;
  }

  template <typename T, typename F = _JoinFunction,
            typename Ctx = MultiJoinImpl>
  static unique_ptr<MultiJoinImpl<_TTp, _JTp, F, _JoinStrategy, T>> make(
      StreamFunction<F, Ctx>&& f, unique_ptr<T> consumer) {
    return unique_ptr<MultiJoinImpl<_TTp, _JTp, F, _JoinStrategy, T>>(
        new MultiJoinImpl<_TTp, _JTp, F, _JoinStrategy, T>(
            std::move(f), std::move(consumer)));
  }

  template <typename F = _JoinFunction, typename Ctx = MultiJoinImpl>
  static unique_ptr<MultiJoinImpl<_TTp, _JTp, F, _JoinStrategy>> make(
      StreamFunction<F, Ctx>&& f) {
    return unique_ptr<MultiJoinImpl<_TTp, _JTp, F, _JoinStrategy>>(
        new MultiJoinImpl<_TTp, _JTp, F, _JoinStrategy>(std::move(f)));
  }

  template <typename F = _JoinFunction, typename Ctx = MultiJoinImpl>
  static unique_ptr<MultiJoinImpl<_TTp, _JTp, F, _JoinStrategy>> make(
      const servicelib::config::MultiJoinStreamConfig& cfg,
      serde::StreamSerde<_Tp>* serde, IRuntimeEnvironment* env,
      StreamFunction<F, Ctx>&& f) {
    return unique_ptr<MultiJoinImpl<_TTp, _JTp, F, _JoinStrategy>>(
        new MultiJoinImpl<_TTp, _JTp, F, _JoinStrategy>(cfg, serde, env,
                                                        std::move(f)));
  }

  template <typename _PTp, size_t N = 0>
  void consumeRight([[maybe_unused]] MessageContext ctx,
                    [[maybe_unused]] Payload<_PTp> payload) {}

  size_t buildTopology(StreamBuilderContext& ctx, size_t id,
                       StreamBuilderContext::TIdsList* splitConsumerIds,
                       bool skip) override {
    ctx.buildTopology(*this, id);
    if (splitConsumerIds != nullptr) {
      splitConsumerIds->emplace_back(id);
    }
    return this->buildTopologyCommon(ctx, id, nullptr, skip);
  }

  std::string getCode() const override {
    if (f_.isInternalType()) {
      return f_.getFunctionCode();
    }
    return std::string();
  }

  const StreamBase& getConsumer() const override {
    return MultiJoin<_TTp, _JTp, _JoinStrategy, _CCp>::getConsumer();
  }

  StreamBase& getConsumer() override {
    return MultiJoin<_TTp, _JTp, _JoinStrategy, _CCp>::getConsumer();
  }

  bool hasConsumer() const noexcept override {
    return MultiJoin<_TTp, _JTp, _JoinStrategy, _CCp>::hasConsumer();
  }

  const StreamBase& getBase() const noexcept override {
    return MultiJoin<_TTp, _JTp, _JoinStrategy, _CCp>::getBase();
  }

  StreamBase& getBase() noexcept override {
    return MultiJoin<_TTp, _JTp, _JoinStrategy, _CCp>::getBase();
  }

  void verifyTopology(StreamVerifyContext& ctx) const override {
    MultiJoin<_TTp, _JTp, _JoinStrategy, _CCp>::verifyTopology(ctx);
  }

  void printTopology(TopologyPrinter& tp,
                     std::unordered_set<size_t>& visited) const override {
    MultiJoin<_TTp, _JTp, _JoinStrategy, _CCp>::printTopology(tp, visited);
  }

  const std::string_view& getType() const override {
    if (f_.isInternalType()) {
      return MultiJoin<_TTp, _JTp, _JoinStrategy, _CCp>::getType();
    }
    return StreamBuilderContext::getType<decltype(*this)>();
  }

  static auto build(MultiJoinImpl& stream) {
    auto r = make(std::move(stream.f_));
    r->copySettings(stream);
    static_cast<StreamConsumer<_Tp>&>(*r).copyConsumerSettings(
        static_cast<const StreamConsumer<_Tp>&>(stream));
    return r;
  }

  template <typename T>
  static auto build(MultiJoinImpl& stream, unique_ptr<T> consumer) {
    auto r = make(std::move(stream.f_), std::move(consumer));
    r->copySettings(stream);
    static_cast<StreamConsumer<_Tp>&>(*r).copyConsumerSettings(
        static_cast<const StreamConsumer<_Tp>&>(stream));
    return r;
  }

  template <typename F, typename Ctx>
  static auto build(StreamFunction<F, Ctx>&& f) {
    return make(std::move(f));
  }

  template <typename T, typename F, typename Ctx>
  static auto build(unique_ptr<T> consumer, StreamFunction<F, Ctx>&& f) {
    return make(std::move(f), std::move(consumer));
  }
};

template <typename _TTp, typename _JTp, JoinStrategyEnum _JoinStrategy,
          typename _CCp>
class MultiJoinBuilder final {
  template <typename, typename, JoinStrategyEnum, typename>
  friend class MultiJoin;

 protected:
  template <typename _JoinFunction, typename Ctx>
  static auto build(StreamFunction<_JoinFunction, Ctx>&& f) {
    return MultiJoinImpl<_TTp, _JTp, _JoinFunction, _JoinStrategy, _CCp>::build(
        std::move(f));
  }

  template <typename _JoinFunction, typename T, typename Ctx>
  static auto build(unique_ptr<T> consumer,
                    StreamFunction<_JoinFunction, Ctx>&& f) {
    return MultiJoinImpl<_TTp, _JTp, _JoinFunction, _JoinStrategy, T>::build(
        std::move(consumer), std::move(f));
  }
};
