(function () {
'use strict';

// ── Parameter database ────────────────────────────────────────────────────
var DB = {

/* PRODUCER */
'acks': {
  scope:'producer', def:'1',
  desc:'Number of acknowledgements the leader must receive from ISR before considering a ProduceRequest complete. The single most important producer durability knob.',
  values:[
    {val:'0', when:'Fire-and-forget metrics/logs. Zero durability — broker does not confirm receipt. Maximum throughput.'},
    {val:'1', when:'Non-critical streams. Leader ACKs after writing to its own log. Data lost if leader crashes before replication.'},
    {val:'all or -1', when:'Financial data, payments, audit. Leader waits for ALL min.insync.replicas copies. Always pair with min.insync.replicas=2 and RF=3.'}
  ],
  related:['min.insync.replicas','enable.idempotence','retries']
},
'batch.size': {
  scope:'producer', def:'16384 (16 KB)',
  desc:'Maximum bytes accumulated per partition before the Sender thread flushes a batch. Upper bound only — sender fires sooner when linger.ms expires. Larger batches = fewer requests + better compression ratio.',
  values:[
    {val:'16384', when:'Default. Small messages at low throughput.'},
    {val:'65536 (64 KB)', when:'Production starting point. Good throughput/memory balance.'},
    {val:'131072–524288', when:'High-throughput pipelines with large messages. Maximises compression, reduces broker request overhead.'}
  ],
  related:['linger.ms','buffer.memory','compression.type']
},
'linger.ms': {
  scope:'producer', def:'0',
  desc:'Time the Sender waits for more records before sending an incomplete batch. linger.ms=0 means send immediately — tiny batches, high broker CPU. Even 5–20 ms dramatically increases batch fill rate with imperceptible latency impact.',
  values:[
    {val:'0', when:'Latency-critical interactive use cases only. Results in many micro-batches.'},
    {val:'5–20', when:'Standard production. Imperceptible to users; typically 5–50× improvement in batch size.'},
    {val:'100–500', when:'Bulk ETL/ingestion where end-to-end SLO > 1 second. Maximum batching efficiency.'}
  ],
  related:['batch.size','buffer.memory','compression.type']
},
'buffer.memory': {
  scope:'producer', def:'33554432 (32 MB)',
  desc:'Total bytes the producer buffers for records waiting to be sent. When full, send() blocks for max.block.ms then throws TimeoutException. Shock-absorber during broker slowdowns. Monitor bufferpool-wait-ratio — if >0 regularly, buffer is undersized.',
  values:[
    {val:'33554432 (32 MB)', when:'Default. Often undersized for throughput >10 MB/s.'},
    {val:'67108864 (64 MB)', when:'Production minimum for most workloads.'},
    {val:'134217728+ (128 MB+)', when:'High-throughput producers (>100 MB/s) or when broker slowdowns are expected.'}
  ],
  warn:'Each producer instance reserves this RAM. 10 producers × 128 MB = 1.25 GB. Account for this in container limits.',
  related:['max.block.ms','batch.size','linger.ms']
},
'max.block.ms': {
  scope:'producer', def:'60000 (60 s)',
  desc:'Maximum time send() blocks when buffer.memory is full or metadata unavailable. After this, a TimeoutException is thrown. Acts as a backpressure valve — turns buffer-full into a visible exception rather than an infinite thread block.',
  values:[
    {val:'60000', when:'Default. Very long — application thread can be frozen 60 s with no signal.'},
    {val:'5000–10000', when:'Recommended. Fail fast so the application can implement circuit-breaking.'},
    {val:'1000', when:'Latency-sensitive services. Must handle TimeoutException explicitly.'}
  ],
  related:['buffer.memory','delivery.timeout.ms']
},
'compression.type': {
  scope:'producer / broker / topic', def:'none (producer) | producer (broker)',
  desc:'Compression codec for message batches. On the producer: compresses before sending (saves network + disk). On the broker: if set to a specific codec DIFFERENT from the producer, the broker decompresses and recompresses every batch — high CPU. "producer" means accept whatever the producer sent, no recompression.',
  values:[
    {val:'none', when:'Lowest CPU. Highest network + disk cost. Only for tiny messages where ratio is negligible.'},
    {val:'lz4', when:'Best all-round. Fast compress/decompress, good ratio. Recommended for most production systems.'},
    {val:'zstd', when:'Best compression ratio (20–40% better than lz4). Slightly higher CPU. Ideal for large messages or long-retention topics.'},
    {val:'producer (broker side)', when:'Accept whatever producer sends. Zero broker recompression CPU. Set this on the broker to avoid mismatch spikes.'}
  ],
  related:['batch.size','linger.ms','num.io.threads']
},
'enable.idempotence': {
  scope:'producer', def:'true (Kafka 3.0+)',
  desc:'Assigns the producer a unique Producer ID (PID) and sequence number per partition. Broker deduplicates retried records using PID + sequence. Guarantees exactly-once at the produce level — retries never create duplicates even across broker failovers.',
  values:[
    {val:'true', when:'Always. Zero performance cost. Prerequisite for transactions. Implicitly enforces acks=all, retries=MAX_INT, max.in.flight≤5.'},
    {val:'false', when:'Legacy only (<Kafka 0.11). Never set false on modern Kafka.'}
  ],
  related:['acks','retries','max.in.flight.requests.per.connection','transactional.id']
},
'max.in.flight.requests.per.connection': {
  scope:'producer', def:'5',
  desc:'Maximum unacknowledged send requests in-flight to a single broker at once. Higher = more pipelining = more throughput. With enable.idempotence=true MUST be ≤5 — Kafka\'s dedup uses a window of 5 sequence numbers per partition.',
  values:[
    {val:'1', when:'Strict ordering without idempotence. One request at a time.'},
    {val:'5', when:'Default. Recommended with idempotence. Best throughput + ordering.'},
    {val:'>5', when:'Only without idempotence when ordering is irrelevant. Rarely needed.'}
  ],
  related:['enable.idempotence','retries','acks']
},
'retries': {
  scope:'producer', def:'Integer.MAX_VALUE (Kafka 2.1+)',
  desc:'How many times the producer retries a failed send. With enable.idempotence=true retries are safe (deduped). The actual retry window is governed by delivery.timeout.ms — the producer keeps retrying within that budget regardless of this count.',
  values:[
    {val:'0', when:'At-most-once semantics. A failed send is permanently lost. Only for metrics/logs where loss is acceptable.'},
    {val:'Integer.MAX_VALUE', when:'Standard production. Keeps retrying within delivery.timeout.ms. Safe with idempotence.'}
  ],
  related:['retry.backoff.ms','delivery.timeout.ms','enable.idempotence']
},
'retry.backoff.ms': {
  scope:'producer', def:'100',
  desc:'Wait time before retrying a failed request. Prevents hammering a temporarily unavailable broker. Acts as a base delay between retry attempts.',
  values:[
    {val:'100', when:'Default. Fine for most cases.'},
    {val:'500–1000', when:'Broker under heavy load or network instability. Gives broker breathing room.'}
  ],
  related:['retries','delivery.timeout.ms']
},
'delivery.timeout.ms': {
  scope:'producer', def:'120000 (2 min)',
  desc:'Total time budget from send() call to callback (success or failure), encompassing retries, linger time, and in-flight time. If the record is not delivered within this window the callback fires with TimeoutException. This is the "give up" timer.',
  values:[
    {val:'120000', when:'Default. Fine for most systems.'},
    {val:'30000', when:'Payment systems. Fail fast so upstream can retry at application level.'},
    {val:'300000+', when:'Batch/ETL where broker outages of several minutes are acceptable.'}
  ],
  warn:'Must be > request.timeout.ms + linger.ms, otherwise it expires before the first attempt completes.',
  related:['retries','max.block.ms']
},
'transactional.id': {
  scope:'producer', def:'(not set)',
  desc:'A stable, unique string identifying this producer for transactional purposes across restarts. Enables the Transaction Coordinator to fence zombie producers — new instance with same ID fences the old one and aborts its open transactions. Prerequisite for end-to-end EOS.',
  values:[
    {val:'(not set)', when:'No transactions. Producer cannot use beginTransaction()/commitTransaction().'},
    {val:'unique-stable-string', when:'Use a stable ID per logical producer partition, e.g., "payment-producer-p7". Must survive restarts — use pod name or static config, NEVER a random UUID.'}
  ],
  warn:'Using a random UUID as transactional.id means old zombie producers are never fenced and can corrupt the transaction log.',
  related:['enable.idempotence','transaction.timeout.ms']
},
'transaction.timeout.ms': {
  scope:'producer', def:'60000 (60 s)',
  desc:'Maximum time a transaction can stay open (between beginTransaction() and commit/abort) before the broker Transaction Coordinator auto-aborts it. The producer is NOT immediately notified — it finds out only on the next produce() or commitTransaction() call (ProducerFencedException). Aborted records are invisible to read_committed consumers.',
  values:[
    {val:'60000', when:'Default. Works if all work inside the transaction completes well within 60 s.'},
    {val:'10000–30000', when:'Recommended. Fail fast, forces short transaction boundaries, surfaces slow external calls immediately.'},
    {val:'>60000', when:'Only if legitimately slow operations must be inside the transaction. Never put external API calls inside a transaction window.'}
  ],
  warn:'A leading cause of silent data loss: an external API call inside the transaction window exceeds this timeout. The broker aborts; if the resulting ProducerFencedException is swallowed, the record is permanently lost.',
  related:['transactional.id','transaction.max.timeout.ms','isolation.level']
},

/* CONSUMER */
'enable.auto.commit': {
  scope:'consumer', def:'true',
  desc:'Whether the consumer automatically commits offsets on a timer (every auto.commit.interval.ms). With true, offsets are committed regardless of processing success — a crash between commit and processing completion causes silent data loss. With false, the application commits manually only after confirmed processing.',
  values:[
    {val:'true', when:'At-most-once semantics. Analytics/logging where occasional loss is fine. Simpler code.'},
    {val:'false', when:'RECOMMENDED for all production transactional systems. Commit only after confirmed DB write. Enables at-least-once or exactly-once.'}
  ],
  warn:'Most common cause of "consumer lag=0 but records not processed" — auto-commit fired before processing completed.',
  related:['auto.commit.interval.ms','isolation.level']
},
'auto.commit.interval.ms': {
  scope:'consumer', def:'5000 (5 s)',
  desc:'How frequently auto-commit fires when enable.auto.commit=true. Shorter = smaller replay window on crash but more commit overhead.',
  values:[
    {val:'5000', when:'Default. Tolerable for most analytics consumers.'},
    {val:'1000', when:'Reduce replay window at cost of slightly more commit traffic.'}
  ],
  related:['enable.auto.commit']
},
'isolation.level': {
  scope:'consumer', def:'read_uncommitted',
  desc:'Controls visibility of transactional records. read_uncommitted sees all records including those in open or aborted transactions. read_committed sees only records from committed transactions — aborted records are filtered by the broker.',
  values:[
    {val:'read_uncommitted', when:'Default. Use when producers do not use transactions. No overhead.'},
    {val:'read_committed', when:'REQUIRED when consuming from a transactional producer topic. Without this, aborted transaction records are processed, violating EOS guarantees.'}
  ],
  related:['transactional.id','transaction.timeout.ms','enable.auto.commit']
},
'max.poll.records': {
  scope:'consumer', def:'500',
  desc:'Maximum records returned per poll() call. Key lever for staying within max.poll.interval.ms. Formula: max.poll.records × avg_processing_time_per_record < max.poll.interval.ms × 0.8.',
  values:[
    {val:'500', when:'Default. Fine if processing is fast (<1 ms/record).'},
    {val:'50–200', when:'Slow processing (external DB writes, API calls). Reduces per-batch work.'},
    {val:'1000–5000', when:'Lightweight processing (filter, aggregate). Increases fetch efficiency.'}
  ],
  related:['max.poll.interval.ms','fetch.min.bytes','fetch.max.wait.ms']
},
'max.poll.interval.ms': {
  scope:'consumer', def:'300000 (5 min)',
  desc:'Maximum time between consecutive poll() calls before the Kafka CLIENT voluntarily sends LeaveGroup, triggering a rebalance. This is client-side detection of "alive but stuck processing." Unlike session.timeout.ms (detects dead JVM), this fires when the poll loop is blocked by slow processing. The heartbeat thread continues even when poll() is delayed.',
  values:[
    {val:'300000 (5 min)', when:'Default. Sufficient for most consumers.'},
    {val:'600000–1800000', when:'Slow processing (ML inference, large batch DB). Set to 2× worst-case processing time for a full batch.'},
    {val:'30000–60000', when:'Low-latency consumers. Detect stuck consumers quickly.'}
  ],
  warn:'Breaching this causes self-eviction (LeaveGroup). The in-progress batch may be reprocessed. Consumers must be idempotent.',
  related:['session.timeout.ms','heartbeat.interval.ms','max.poll.records']
},
'session.timeout.ms': {
  scope:'consumer', def:'45000 (45 s) in Kafka 3.x',
  desc:'Maximum time the GROUP COORDINATOR (broker-side) waits for a heartbeat before declaring a consumer dead and triggering a rebalance. Fires when the JVM freezes (GC pause, OOM), network drops, or process dies. The heartbeat runs on a background thread separate from the poll loop.',
  values:[
    {val:'45000', when:'Good default. Balances fast failure detection with tolerance for GC pauses.'},
    {val:'10000–20000', when:'Low-latency systems. Risk: GC pauses >session.timeout trigger false rebalances.'},
    {val:'60000–120000', when:'Large-heap JVMs or unreliable networks. Reduces false rebalances.'}
  ],
  warn:'Must be within [group.min.session.timeout.ms, group.max.session.timeout.ms] on the broker. Must be at least 3× heartbeat.interval.ms.',
  related:['heartbeat.interval.ms','max.poll.interval.ms','group.instance.id']
},
'heartbeat.interval.ms': {
  scope:'consumer', def:'3000 (3 s)',
  desc:'How often the background heartbeat thread sends a keepalive to the Group Coordinator. Carries no data. Runs independently of the poll() thread (Kafka client 2.x+) — slow record processing does NOT delay heartbeats. Must be < 1/3 of session.timeout.ms.',
  values:[
    {val:'3000', when:'Default. Fine for most cases.'},
    {val:'session.timeout.ms / 3', when:'Always keep this ratio. e.g., if session.timeout.ms=45000 then heartbeat.interval.ms=15000.'},
    {val:'1000', when:'Very low session.timeout.ms. More network overhead per consumer.'}
  ],
  related:['session.timeout.ms','max.poll.interval.ms']
},
'fetch.min.bytes': {
  scope:'consumer', def:'1',
  desc:'Minimum data the broker must accumulate before responding to a FetchRequest. With 1 (default), the broker responds immediately if even 1 byte is available. Increasing this reduces fetch round-trips and broker CPU at the cost of slightly higher latency.',
  values:[
    {val:'1', when:'Default. Minimum latency. Responds immediately with available data.'},
    {val:'1048576 (1 MB)', when:'High-throughput batch consumers. Dramatically reduces fetch request count.'},
    {val:'52428800 (50 MB)', when:'Bulk ETL. Maximum batching. Always pair with fetch.max.wait.ms to bound wait time.'}
  ],
  related:['fetch.max.wait.ms','max.partition.fetch.bytes','max.poll.records']
},
'fetch.max.wait.ms': {
  scope:'consumer', def:'500',
  desc:'Maximum time the broker waits for fetch.min.bytes to accumulate before sending whatever it has. Acts as a latency cap — even on a quiet topic the consumer sees data within this window.',
  values:[
    {val:'500', when:'Default. Good balance.'},
    {val:'1000–5000', when:'Batch consumers with large fetch.min.bytes. Accept higher latency for better batching.'},
    {val:'100', when:'Low-latency consumers needing near-real-time data.'}
  ],
  related:['fetch.min.bytes','max.poll.records']
},
'max.partition.fetch.bytes': {
  scope:'consumer', def:'1048576 (1 MB)',
  desc:'Maximum data the broker returns per partition per fetch request. If messages are large (e.g., 500 KB each), this limits you to ~2 messages per partition per fetch. Increase for large-message topics.',
  values:[
    {val:'1048576 (1 MB)', when:'Default. Fine for messages <100 KB.'},
    {val:'5242880 (5 MB)', when:'Messages of 500 KB–2 MB. Reduces fetch round-trips.'}
  ],
  warn:'Must be at least as large as the largest message size. Otherwise the consumer can never fetch an oversized message.',
  related:['fetch.min.bytes','message.max.bytes','max.poll.records']
},
'auto.offset.reset': {
  scope:'consumer', def:'latest',
  desc:'What to do when there is no committed offset for a group, or the committed offset is outside the retention window. Controls where a NEW consumer group starts reading.',
  values:[
    {val:'latest', when:'Default. New consumers start from the tip — only read messages produced after first start. Misses historical data.'},
    {val:'earliest', when:'New consumers read from the very beginning (as far back as retention allows). Use to process all historical records.'},
    {val:'none', when:'Throw exception if no committed offset found. Detects misconfigured consumers instead of silently skipping data.'}
  ],
  related:['enable.auto.commit','log.retention.ms']
},
'group.id': {
  scope:'consumer', def:'(must be set)',
  desc:'Identifies the consumer group. Consumers sharing the same group.id split partition assignments and track offsets together (horizontal scale-out). Different groups are fully independent — each receives all messages.',
  values:[
    {val:'unique-per-application', when:'Each logical consumer application needs its own group.id, e.g., "payments-processor", "analytics-consumer".'},
    {val:'shared across instances', when:'Only when multiple service instances are horizontally scaling the SAME logical consumer.'}
  ],
  related:['group.instance.id','partition.assignment.strategy']
},
'group.instance.id': {
  scope:'consumer', def:'(not set — dynamic membership)',
  desc:'Enables static group membership. When set, the broker treats this consumer as a persistent member. If it disconnects and reconnects within session.timeout.ms using the same ID, no rebalance is triggered — it resumes with its previous partition assignments. Eliminates rebalances during K8s rolling deploys.',
  values:[
    {val:'(not set)', when:'Default. Every restart triggers a rebalance (leave + rejoin).'},
    {val:'stable-unique-string', when:'K8s deployments, stateful stream processing. Use pod name: "consumer-0", "consumer-1". Each instance must have a unique value.'}
  ],
  warn:'If a consumer with group.instance.id crashes and never returns, its partitions are NOT reassigned until session.timeout.ms expires. Design for this failure mode.',
  related:['session.timeout.ms','partition.assignment.strategy']
},
'partition.assignment.strategy': {
  scope:'consumer', def:'RangeAssignor',
  desc:'Algorithm for distributing partitions during a rebalance. Eager strategies (Range, RoundRobin) revoke ALL partitions from ALL consumers before reassigning — full processing stop. CooperativeStickyAssignor is incremental — only partitions that must move are revoked; others keep processing.',
  values:[
    {val:'RangeAssignor', when:'Default. Contiguous partition ranges per topic per consumer. Eager — full revoke on rebalance. Can cause uneven distribution across topics.'},
    {val:'RoundRobinAssignor', when:'Even distribution. Still eager. Better than Range for multiple topics.'},
    {val:'StickyAssignor', when:'Minimises partition moves. Still eager on first generation. Migration step from Range to Cooperative.'},
    {val:'CooperativeStickyAssignor', when:'RECOMMENDED. Incremental rebalance — only moves necessary partitions. Consumers keep processing throughout. Minimises lag spikes.'}
  ],
  related:['group.instance.id','session.timeout.ms','max.poll.interval.ms']
},

/* BROKER */
'num.io.threads': {
  scope:'broker', def:'8',
  desc:'IO (request handler) threads that dequeue from RequestQueue and do the actual work — write to log, read from log, manage replication state. These perform the bulk of broker CPU work. Monitor RequestHandlerAvgIdlePercent — below 30% means these are saturated.',
  values:[
    {val:'8', when:'Default. Fine for moderate throughput (<200 MB/s per broker).'},
    {val:'12–16', when:'High-throughput brokers (200–600 MB/s). Standard production.'},
    {val:'24–32', when:'Very high throughput or many small requests. Only increase if handler idle% is the bottleneck, not disk or network.'}
  ],
  related:['num.network.threads','queued.max.requests']
},
'num.network.threads': {
  scope:'broker', def:'3',
  desc:'NetworkProcessor threads. Each manages a set of client TCP connections: reads request bytes from socket, places in RequestQueue, picks responses and writes back. Monitor NetworkProcessorAvgIdlePercent — below 30% means saturated.',
  values:[
    {val:'3', when:'Default. Adequate for low client connection counts.'},
    {val:'5–8', when:'Production clusters with many concurrent producers/consumers.'},
    {val:'10+', when:'Very high connection counts (thousands of clients) or extreme request rates.'}
  ],
  related:['num.io.threads','queued.max.requests']
},
'num.replica.fetchers': {
  scope:'broker', def:'1',
  desc:'Threads per broker that pull data from partition leaders on remote brokers (replication). More threads = faster ISR recovery after a broker restart, but also higher inter-broker network load.',
  values:[
    {val:'1', when:'Default. Sufficient for low-throughput clusters.'},
    {val:'4–8', when:'High-throughput clusters. Reduces ISR rebuild time after broker restart.'}
  ],
  related:['replica.lag.time.max.ms','min.insync.replicas']
},
'queued.max.requests': {
  scope:'broker', def:'500',
  desc:'Maximum requests waiting in the RequestQueue before new incoming requests are blocked. If this fills, network threads stop reading from sockets (TCP backpressure). Monitor RequestQueueSize — approaching this value means IO threads are the bottleneck.',
  values:[
    {val:'500', when:'Default. Usually sufficient.'},
    {val:'1000–2000', when:'Burst-heavy traffic. Reduces producer timeout errors during spikes.'}
  ],
  related:['num.io.threads','num.network.threads']
},
'replica.lag.time.max.ms': {
  scope:'broker', def:'30000 (30 s)',
  desc:'If a follower has not sent a fetch request to the leader within this window, the leader evicts it from the ISR. Fires even if the follower is alive but temporarily slow (GC pause, disk saturation, network blip). The follower automatically rejoins ISR once it catches up.',
  values:[
    {val:'30000', when:'Default. Good balance between fast detection and tolerance for slowdowns.'},
    {val:'10000–15000', when:'Faster failure detection. Risk: brief GC pauses trigger false ISR evictions.'},
    {val:'60000', when:'Tolerant of slow followers (large heaps, slow disks). Reduces false evictions.'}
  ],
  related:['min.insync.replicas','num.replica.fetchers','unclean.leader.election.enable']
},
'min.insync.replicas': {
  scope:'broker / topic', def:'1',
  desc:'When acks=all, the minimum number of replicas (including leader) that must be in ISR and acknowledge a write. If ISR shrinks below this value, the leader rejects writes with NotEnoughReplicasException. This is the critical durability gate — refuses writes when too few replicas are available.',
  values:[
    {val:'1', when:'Default — only leader must ACK. Zero protection against leader failure. Never use for critical data.'},
    {val:'2', when:'PRODUCTION STANDARD with RF=3. Survives 1 broker failure with zero data loss. If ISR drops to 1, writes are blocked — availability sacrificed for durability.'},
    {val:'3', when:'Maximum durability with RF=3. ANY broker failure blocks writes. Only for highest-criticality regulatory data.'}
  ],
  warn:'min.insync.replicas=2 with RF=3 means losing 2 brokers simultaneously makes the topic unwritable. This is by design — durability over availability.',
  related:['acks','unclean.leader.election.enable','replica.lag.time.max.ms']
},
'unclean.leader.election.enable': {
  scope:'broker / topic', def:'false',
  desc:'Whether a replica NOT in the ISR can be elected leader when all ISR members are unavailable. true = availability over durability (accepts data loss). false = durability over availability (partition stays OFFLINE until an ISR member recovers).',
  values:[
    {val:'false', when:'ALWAYS for financial, payment, and regulatory data. Zero data loss tolerance.'},
    {val:'true', when:'Analytics, logging, metrics topics where losing a few messages is acceptable and availability matters more.'}
  ],
  warn:'With unclean.leader.election.enable=true during a network partition, a stale replica can become leader, accept writes, and when the partition heals those writes are truncated and LOST.',
  related:['min.insync.replicas','replica.lag.time.max.ms']
},
'auto.create.topics.enable': {
  scope:'broker', def:'true',
  desc:'Whether brokers auto-create a topic when a producer or consumer references a non-existent one. A common source of production misconfigurations — a typo creates a new empty topic with default settings (often RF=1) instead of failing loudly.',
  values:[
    {val:'true', when:'Development only. Convenient for testing.'},
    {val:'false', when:'ALL production clusters. Forces explicit topic creation with correct RF, partitions, and retention. Typos fail loudly.'}
  ],
  related:['default.replication.factor','num.partitions']
},
'auto.leader.rebalance.enable': {
  scope:'broker', def:'true',
  desc:'Whether the controller periodically triggers preferred leader elections to restore even leader distribution. After a broker restart its leaders may stay elsewhere — this setting eventually restores them to prevent one broker being overloaded.',
  values:[
    {val:'true', when:'Production default. Keeps leaders evenly distributed.'},
    {val:'false', when:'Temporarily during major incidents to stop auto-elections adding noise. Re-enable once stable.'}
  ],
  related:['leader.imbalance.per.broker.percentage','leader.imbalance.check.interval.seconds']
},
'leader.imbalance.per.broker.percentage': {
  scope:'broker', def:'10',
  desc:'Percentage threshold of leader imbalance per broker before the controller triggers a preferred leader election. If a broker has >10% more leaders than its fair share, rebalancing fires.',
  values:[
    {val:'10', when:'Default. Up to 10% imbalance tolerated.'},
    {val:'5', when:'More aggressive balancing. Keeps load very even.'},
    {val:'20', when:'Less frequent elections. Tolerates more imbalance on clusters with frequent restarts.'}
  ],
  related:['auto.leader.rebalance.enable','leader.imbalance.check.interval.seconds']
},
'leader.imbalance.check.interval.seconds': {
  scope:'broker', def:'300 (5 min)',
  desc:'How often the active controller checks for leader imbalance and triggers preferred elections if needed.',
  values:[
    {val:'300', when:'Default.'},
    {val:'60', when:'Faster re-balancing after rolling restarts. Reduces the window where one broker carries excess leaders.'}
  ],
  related:['auto.leader.rebalance.enable','leader.imbalance.per.broker.percentage']
},
'log.retention.ms': {
  scope:'broker / topic', def:'604800000 (7 days)',
  desc:'How long Kafka retains log segments (cleanup.policy=delete). A segment is not deleted until ALL its messages are older than this threshold. Supersedes log.retention.hours when both are set.',
  values:[
    {val:'604800000 (7 days)', when:'Standard production default. Enough for consumers to recover from multi-day outages.'},
    {val:'86400000 (1 day)', when:'High-volume analytics topics sinking to a data warehouse. Kafka is transit, not long-term storage.'},
    {val:'2592000000 (30 days)', when:'Audit/compliance topics. Keep hot in Kafka; archive older data to S3 via S3 Sink connector.'},
    {val:'-1', when:'Infinite retention. NEVER use without also setting log.retention.bytes or disk will fill.'}
  ],
  related:['log.retention.bytes','cleanup.policy','log.segment.bytes']
},
'log.retention.bytes': {
  scope:'broker / topic', def:'-1 (unlimited)',
  desc:'Maximum total size of log segments retained PER PARTITION before older segments are deleted. -1 means no size limit (time-based only). When both log.retention.ms and log.retention.bytes are set, Kafka deletes segments violating EITHER.',
  values:[
    {val:'-1', when:'Default. Only time-based retention.'},
    {val:'10737418240 (10 GB)', when:'Safety cap per partition to prevent runaway disk fill on high-volume topics.'}
  ],
  related:['log.retention.ms','cleanup.policy']
},
'log.segment.bytes': {
  scope:'broker / topic', def:'1073741824 (1 GB)',
  desc:'Maximum size of a single log segment file before Kafka rolls to a new one. Smaller = more files, more granular retention cleanup. Larger = fewer files but older data trapped in same segment as newer data until the whole segment expires.',
  values:[
    {val:'1073741824 (1 GB)', when:'Default. Good balance for most topics.'},
    {val:'536870912 (512 MB)', when:'Short-retention topics (<1 day). More granular cleanup.'},
    {val:'2147483648 (2 GB)', when:'Long-retention high-throughput topics. Fewer open file handles.'}
  ],
  related:['log.retention.ms','log.retention.bytes']
},
'cleanup.policy': {
  scope:'broker / topic', def:'delete',
  desc:'How Kafka handles log data over time. "delete" removes old segments by time/size. "compact" retains only the latest record per key — like a key-value store of current state. "delete,compact" does both: compact old data then delete after a time threshold.',
  values:[
    {val:'delete', when:'Event streams, logs, metrics. Records are immutable facts; old ones expire.'},
    {val:'compact', when:'CDC topics, config stores, reference data. Consumers need current state per key even after restart.'},
    {val:'delete,compact', when:'Long-lived CDC topics. Compact keeps latest state per key; delete removes old compacted segments.'}
  ],
  related:['log.retention.ms','log.retention.bytes','delete.retention.ms']
},
'broker.rack': {
  scope:'broker', def:'(not set)',
  desc:'Rack/AZ label for this broker. When set on all brokers, Kafka\'s rack-aware assignment algorithm spreads replicas across different racks/AZs so no single AZ failure can take down all replicas of a partition. Critical for multi-AZ HA.',
  values:[
    {val:'(not set)', when:'Single-AZ or single-DC deployments.'},
    {val:'us-east-1a / 1b / 1c', when:'AWS multi-AZ. All 3 AZ labels required for perfect 1-replica-per-AZ with RF=3.'},
    {val:'rack-1 / rack-2 / rack-3', when:'On-prem multi-rack deployment.'}
  ],
  related:['default.replication.factor','min.insync.replicas']
},
'controlled.shutdown.enable': {
  scope:'broker', def:'true',
  desc:'When a broker stops, it first migrates its partition leaders to other brokers before shutting down. Prevents URP alerts and leader election storms during planned maintenance. Without this, stopping a broker causes simultaneous leader elections for all its partitions.',
  values:[
    {val:'true', when:'Always. Rolling restarts, OS patching, and planned maintenance all rely on this.'},
    {val:'false', when:'Legacy. Never disable. Only force-kill (kill -9) as last resort when broker is stuck.'}
  ],
  related:['num.replica.fetchers','replica.lag.time.max.ms']
},
'default.replication.factor': {
  scope:'broker', def:'1',
  desc:'Default replication factor for auto-created topics. RF=1 means no redundancy — a single broker failure permanently loses data. This cluster default should always be overridden to 3 for production.',
  values:[
    {val:'1', when:'Development only. Never production.'},
    {val:'3', when:'PRODUCTION STANDARD. Tolerates 1 broker failure with zero data loss (with min.insync.replicas=2).'},
    {val:'5', when:'Mission-critical regulatory topics. Tolerates 2 simultaneous broker failures. 5× storage cost.'}
  ],
  related:['min.insync.replicas','broker.rack']
},
'inter.broker.protocol.version': {
  scope:'broker', def:'current Kafka version',
  desc:'Protocol version for inter-broker communication. During a rolling Kafka upgrade, keep this at the OLD version until ALL brokers are on the new binary. Only then bump it. This is the point of no return in a Kafka upgrade — bumping is irreversible without wiping broker state.',
  values:[
    {val:'old-version (e.g. 3.6)', when:'Phase 1 of upgrade. All brokers running new binary, protocol stays old. Safe rollback: just swap binary back.'},
    {val:'new-version (e.g. 3.7)', when:'Phase 2 only after all brokers stable on new binary for 48 h. IRREVERSIBLE.'}
  ],
  warn:'Never bump inter.broker.protocol.version at the same time as the binary upgrade. These are separate steps specifically to maintain rollback ability.',
  related:['log.message.format.version']
},
'log.message.format.version': {
  scope:'broker', def:'current Kafka version',
  desc:'Message format version written to disk. Like inter.broker.protocol.version, keep at OLD version during phase 1 of a Kafka upgrade and only bump in phase 2 after all brokers are stable on the new binary. Bumping this is IRREVERSIBLE.',
  values:[
    {val:'old-version', when:'Phase 1 of Kafka upgrade. Keeps rollback possible.'},
    {val:'new-version', when:'Phase 2, after cluster is stable. Enables new message format features.'}
  ],
  warn:'Once bumped, you cannot downgrade without wiping broker state.',
  related:['inter.broker.protocol.version']
},
'message.max.bytes': {
  scope:'broker', def:'1048588 (~1 MB)',
  desc:'Maximum size of a single compressed message batch the broker accepts. Producers sending larger records get MessageTooLargeException. Must be coordinated with consumer\'s max.partition.fetch.bytes.',
  values:[
    {val:'1048588 (~1 MB)', when:'Default. Fine for most workloads.'},
    {val:'5242880 (5 MB)', when:'Applications with large payloads (images, large JSON). Increase broker AND consumer setting together.'}
  ],
  warn:'Increasing message.max.bytes significantly increases broker buffer memory. Consider splitting large payloads — Kafka is optimised for many small records.',
  related:['max.partition.fetch.bytes','batch.size']
},
'num.partitions': {
  scope:'broker', def:'1',
  desc:'Default partition count for auto-created topics. Single-partition topics have no parallelism and are a throughput bottleneck. Override per topic based on throughput formula and consumer parallelism.',
  values:[
    {val:'1', when:'Default. Change this — single partition topics are a common production mistake.'},
    {val:'12', when:'Reasonable cluster default for a 6-broker cluster. 2 partitions per broker, allows 12 parallel consumers.'}
  ],
  related:['default.replication.factor']
},
'zookeeper.session.timeout.ms': {
  scope:'broker (ZK mode)', def:'18000 (18 s)',
  desc:'ZooKeeper session timeout for the broker. If no heartbeat within this window ZK considers the session expired, removes the broker from /brokers/ids, and if it was the controller triggers a new election. Long GC pauses on the broker can cause ZK session expiry.',
  values:[
    {val:'18000', when:'Default. Fine for well-tuned JVMs.'},
    {val:'30000–60000', when:'Large-heap brokers with frequent GC pauses. Prevents false controller elections.'}
  ],
  related:['replica.lag.time.max.ms']
},
'transaction.max.timeout.ms': {
  scope:'broker', def:'900000 (15 min)',
  desc:'Maximum value a producer may request for transaction.timeout.ms. If a producer requests a longer timeout the broker rejects InitProducerId. Acts as a cluster-wide cap to prevent runaway open transactions.',
  values:[
    {val:'900000', when:'Default. 15-minute cap for transaction duration.'},
    {val:'60000', when:'Enforce short transaction windows cluster-wide. Forces developers to keep transaction boundaries tight.'}
  ],
  related:['transaction.timeout.ms','transactional.id']
},
'offsets.topic.replication.factor': {
  scope:'broker', def:'3',
  desc:'Replication factor for the internal __consumer_offsets topic. If this is less than the number of brokers, reducing it could lead to offset data loss on broker failure. Should always match your standard production RF.',
  values:[
    {val:'3', when:'Production standard. Matches default.replication.factor.'},
    {val:'1', when:'Development only. Never production — offset loss = consumer starts over.'}
  ],
  related:['default.replication.factor','min.insync.replicas']
},

/* KRAFT */
'process.roles': {
  scope:'broker (KRaft)', def:'(not set — ZK mode)',
  desc:'Defines whether this node is a broker, a controller, or both in KRaft mode. Controllers store cluster metadata in __cluster_metadata via Raft consensus. Brokers handle produce/consume. Combined mode is supported but not recommended for production.',
  values:[
    {val:'broker', when:'Dedicated broker node. Handles producer/consumer traffic only.'},
    {val:'controller', when:'Dedicated controller node. Participates in Raft quorum. Does NOT handle produce/consume. Recommended for production.'},
    {val:'broker,controller', when:'Combined mode. Development/single-node only. Controller overload can impact broker throughput.'}
  ],
  related:['controller.quorum.voters','node.id','controller.listener.names']
},
'controller.quorum.voters': {
  scope:'broker (KRaft)', def:'(must be set in KRaft mode)',
  desc:'Comma-separated list of KRaft controller nodes in nodeId@host:port format. All brokers and controllers must have the same value. This is how brokers know where to register and how controllers find their peers.',
  values:[
    {val:'1@c1:9093,2@c2:9093,3@c3:9093', when:'3-node quorum (production minimum). Tolerates 1 controller failure.'},
    {val:'1@c1:9093,...,5@c5:9093', when:'5-node quorum. Tolerates 2 simultaneous controller failures.'}
  ],
  related:['process.roles','node.id']
},
'node.id': {
  scope:'broker (KRaft)', def:'(must be set — replaces broker.id)',
  desc:'Unique numeric identifier for this Kafka node in KRaft mode. Replaces broker.id. Must be unique across ALL nodes in the cluster (both brokers and controllers). Must match the IDs listed in controller.quorum.voters for controller nodes.',
  values:[
    {val:'1–N (unique per node)', when:'Assign sequentially: controllers 1,2,3; brokers 4,5,6,... to avoid overlap.'}
  ],
  related:['process.roles','controller.quorum.voters']
},
'controller.listener.names': {
  scope:'broker (KRaft)', def:'CONTROLLER',
  desc:'Comma-separated list of listener names used for controller-to-controller and broker-to-controller communication in KRaft mode. Must be listed in listeners config.',
  values:[
    {val:'CONTROLLER', when:'Standard single controller listener.'},
    {val:'CONTROLLER,CONTROLLER_SSL', when:'When controller communication needs TLS-encrypted channel separate from broker traffic.'}
  ],
  related:['process.roles','controller.quorum.voters']
},

/* CONNECT */
'offset.flush.interval.ms': {
  scope:'connect', def:'60000 (60 s)',
  desc:'How frequently Kafka Connect flushes source connector offsets to the connect-offsets internal topic. Lower value = smaller at-least-once replay window if a connector restarts. Higher value = less write overhead.',
  values:[
    {val:'60000', when:'Default. At most 60 s of records replayed on connector restart.'},
    {val:'10000', when:'Connectors with expensive-to-replay sources (large Debezium tables). Smaller replay window.'}
  ],
  related:['errors.tolerance','tasks.max']
},
'errors.tolerance': {
  scope:'connect', def:'none',
  desc:'How a Kafka Connect connector handles processing errors. "none" fails immediately on first error — connector enters FAILED state. "all" logs and continues — bad records are sent to the DLQ if configured.',
  values:[
    {val:'none', when:'Strict. First error stops the connector. Use when all data must be processed and errors are unexpected.'},
    {val:'all', when:'Tolerant. Bad records are DLQ\'d. Use when occasional malformed records must not halt the pipeline.'}
  ],
  related:['errors.deadletterqueue.topic.name','offset.flush.interval.ms']
},
'errors.deadletterqueue.topic.name': {
  scope:'connect', def:'(not set)',
  desc:'When errors.tolerance=all, the Kafka topic where failed records are sent. Failed records include error context in headers (exception class, message, stack trace) for investigation and selective replay.',
  values:[
    {val:'(not set)', when:'Failed records are logged then silently dropped. No replay possible.'},
    {val:'connector-name.dlq', when:'Failed records preserved in a dedicated topic. Enable errors.deadletterqueue.context.headers.enable=true to include error details in headers.'}
  ],
  related:['errors.tolerance']
},
'tasks.max': {
  scope:'connect', def:'1',
  desc:'Maximum number of tasks created for a connector. Tasks are the parallelism unit in Kafka Connect — each handles a subset of work (e.g., subset of DB tables for Debezium, subset of Kafka partitions for a sink). More tasks = more throughput, more resource use.',
  values:[
    {val:'1', when:'Default. Sequential processing. Fine for low-volume connectors.'},
    {val:'4–8', when:'High-throughput connectors. For sink connectors, set equal to source partition count for maximum parallelism.'}
  ],
  related:['offset.flush.interval.ms']
},

/* TOPIC-LEVEL (per-topic overrides) */
'delete.retention.ms': {
  scope:'topic (compact)', def:'86400000 (24 h)',
  desc:'For compacted topics: how long tombstone records (null-value records that signal key deletion) are retained before being removed. Consumers that are down longer than this may miss the tombstone and never learn a key was deleted.',
  values:[
    {val:'86400000 (1 day)', when:'Default. Consumers that are offline for >1 day may miss deletions.'},
    {val:'604800000 (7 days)', when:'When consumers may be down for several days. Ensures they always see deletion tombstones.'}
  ],
  related:['cleanup.policy','min.compaction.lag.ms']
},
'min.compaction.lag.ms': {
  scope:'topic (compact)', def:'0',
  desc:'Minimum time a message must remain uncompacted. Gives consumers time to consume a record before compaction may remove the older version. Also useful to preserve recent history for debugging.',
  values:[
    {val:'0', when:'Default. Compaction can happen immediately.'},
    {val:'3600000 (1 h)', when:'Ensure consumers always have at least 1 hour of recent records available before compaction removes superseded versions.'}
  ],
  related:['cleanup.policy','delete.retention.ms']
},
'message.timestamp.type': {
  scope:'topic', def:'CreateTime',
  desc:'Whether the message timestamp reflects when the producer created the record (CreateTime) or when the broker appended it (LogAppendTime). Affects time-based retention (log.retention.ms uses this timestamp) and time-based consumer seeks.',
  values:[
    {val:'CreateTime', when:'Default. Timestamp is set by the producer. Preserves event time semantics. Can be spoofed by a misbehaving producer.'},
    {val:'LogAppendTime', when:'Broker overwrites timestamp on append. Trustworthy ingestion time. Use for auditing or when producers cannot be trusted to set accurate timestamps.'}
  ],
  related:['log.retention.ms']
}

}; // end DB

// ── CSS ────────────────────────────────────────────────────────────────────
var styleEl = document.createElement('style');
styleEl.textContent = [
'#kp-ov{display:none;position:fixed;inset:0;background:rgba(0,0,0,.72);z-index:9999;',
'align-items:center;justify-content:center;padding:16px;',
'backdrop-filter:blur(4px);-webkit-backdrop-filter:blur(4px)}',
'#kp-box{background:#1a1d27;border:1px solid #2e3350;border-radius:14px;',
'padding:28px 32px 24px;max-width:660px;width:100%;max-height:82vh;',
'overflow-y:auto;box-shadow:0 24px 80px rgba(0,0,0,.6);position:relative;',
'font-family:"Segoe UI",system-ui,sans-serif}',
'#kp-box h2{font-size:18px;font-family:SFMono-Regular,Consolas,monospace;',
'color:#e2e8f0;margin:0 36px 6px 0;word-break:break-all}',
'#kp-x{position:absolute;top:18px;right:18px;background:#22263a;',
'border:1px solid #2e3350;color:#7b859c;border-radius:6px;',
'width:28px;height:28px;cursor:pointer;font-size:15px;',
'display:flex;align-items:center;justify-content:center;border:none;line-height:1}',
'#kp-x:hover{background:#2e3350;color:#e2e8f0}',
'.kp-bgs{display:flex;gap:8px;margin-bottom:14px;flex-wrap:wrap}',
'.kp-bg{font-size:11px;font-weight:600;padding:3px 10px;border-radius:20px}',
'.kp-bg-producer{background:rgba(91,141,246,.18);color:#5b8df6}',
'.kp-bg-consumer{background:rgba(62,207,142,.18);color:#3ecf8e}',
'.kp-bg-broker{background:rgba(124,92,191,.18);color:#a47fe0}',
'.kp-bg-topic{background:rgba(56,189,193,.18);color:#38bdc1}',
'.kp-bg-connect{background:rgba(245,166,35,.18);color:#f5a623}',
'.kp-bg-default{background:#22263a;color:#7b859c;border:1px solid #2e3350}',
'#kp-desc{color:#c8d0e0;font-size:13.5px;line-height:1.7;margin-bottom:18px}',
'.kp-sl{font-size:10px;font-weight:700;letter-spacing:1.2px;',
'text-transform:uppercase;color:#7b859c;margin-bottom:10px}',
'#kp-vt{width:100%;border-collapse:collapse;margin-bottom:18px;font-size:13px}',
'#kp-vt th{background:#22263a;color:#7b859c;font-weight:600;font-size:11px;',
'letter-spacing:.8px;text-transform:uppercase;padding:8px 12px;',
'text-align:left;border-bottom:1px solid #2e3350}',
'#kp-vt td{padding:9px 12px;border-bottom:1px solid #1e2234;',
'vertical-align:top;color:#c8d0e0;line-height:1.5}',
'#kp-vt tr:last-child td{border-bottom:none}',
'#kp-vt td:first-child{font-family:monospace;font-size:12px;color:#3ecf8e;',
'white-space:nowrap;font-weight:600;min-width:120px}',
'#kp-vt tr:hover td{background:#1e2234}',
'.kp-warn{background:rgba(224,82,82,.1);border:1px solid rgba(224,82,82,.25);',
'border-left:3px solid #e05252;border-radius:6px;padding:10px 14px;',
'color:#f0a0a0;font-size:13px;line-height:1.5;margin-bottom:16px}',
'#kp-rl{display:flex;gap:8px;flex-wrap:wrap}',
'.kp-rc{background:#22263a;border:1px solid #2e3350;color:#5b8df6;',
'font-size:12px;font-family:monospace;padding:4px 10px;border-radius:6px;',
'cursor:pointer;transition:background .15s,border-color .15s}',
'.kp-rc:hover{background:rgba(91,141,246,.15);border-color:#5b8df6}',
'.kp-rc.kp-nodoc{color:#7b859c;cursor:default}',
'.kp-rc.kp-nodoc:hover{background:#22263a;border-color:#2e3350}',
'code.kp-p{border-bottom:1px dashed #5b8df6;cursor:help;transition:background .15s}',
'code.kp-p:hover{background:rgba(91,141,246,.14);border-radius:3px}'
].join('');
document.head.appendChild(styleEl);

// ── build modal DOM ────────────────────────────────────────────────────────
var ov = document.createElement('div');
ov.id = 'kp-ov';
ov.setAttribute('role','dialog');
ov.setAttribute('aria-modal','true');
ov.innerHTML =
  '<div id="kp-box">' +
  '<button id="kp-x" title="Close (Esc)">✕</button>' +
  '<h2 id="kp-t"></h2>' +
  '<div class="kp-bgs" id="kp-bg"></div>' +
  '<div id="kp-desc"></div>' +
  '<div id="kp-ww"></div>' +
  '<div class="kp-sl">Values &amp; when to use</div>' +
  '<table id="kp-vt"><thead><tr><th>Value</th><th>Use when</th></tr></thead>' +
  '<tbody id="kp-vb"></tbody></table>' +
  '<div class="kp-sl" id="kp-rlab" style="display:none">Related parameters</div>' +
  '<div id="kp-rl"></div>' +
  '</div>';
document.body.appendChild(ov);

// ── helpers ────────────────────────────────────────────────────────────────
function esc(s){
  return String(s)
    .replace(/&/g,'&amp;').replace(/</g,'&lt;')
    .replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// ── show / hide ────────────────────────────────────────────────────────────
function show(name){
  var p = DB[name]; if(!p) return;
  document.getElementById('kp-t').textContent = name;

  var bgEl = document.getElementById('kp-bg');
  bgEl.innerHTML = '';
  (p.scope || '').split('/').forEach(function(s){
    var b = document.createElement('span');
    var key = s.trim().split(' ')[0].split('(')[0].trim();
    b.className = 'kp-bg kp-bg-' + key;
    b.textContent = s.trim();
    bgEl.appendChild(b);
  });
  if(p.def){
    var d = document.createElement('span');
    d.className = 'kp-bg kp-bg-default';
    d.textContent = 'default: ' + p.def;
    bgEl.appendChild(d);
  }

  document.getElementById('kp-desc').textContent = p.desc || '';

  document.getElementById('kp-ww').innerHTML = p.warn
    ? '<div class="kp-warn">⚠ ' + esc(p.warn) + '</div>' : '';

  var tb = document.getElementById('kp-vb');
  tb.innerHTML = '';
  (p.values || []).forEach(function(v){
    var tr = document.createElement('tr');
    tr.innerHTML = '<td>' + esc(v.val) + '</td><td>' + esc(v.when) + '</td>';
    tb.appendChild(tr);
  });

  var rl = document.getElementById('kp-rl');
  var rlab = document.getElementById('kp-rlab');
  rl.innerHTML = '';
  if(p.related && p.related.length){
    rlab.style.display = 'block';
    p.related.forEach(function(r){
      var c = document.createElement('span');
      c.className = DB[r] ? 'kp-rc' : 'kp-rc kp-nodoc';
      c.textContent = r;
      if(DB[r]) c.addEventListener('click', function(){ show(r); });
      rl.appendChild(c);
    });
  } else { rlab.style.display = 'none'; }

  ov.style.display = 'flex';
  document.getElementById('kp-box').scrollTop = 0;
}

function hide(){ ov.style.display = 'none'; }

ov.addEventListener('click', function(e){ if(e.target === ov) hide(); });
document.getElementById('kp-x').addEventListener('click', hide);
document.addEventListener('keydown', function(e){ if(e.key==='Escape') hide(); });

// ── annotate <code> elements ───────────────────────────────────────────────
function annotate(){
  document.querySelectorAll('code').forEach(function(el){
    if(el.dataset.kpDone) return;
    var name = el.textContent.trim();
    if(!DB[name]) return;
    el.dataset.kpDone = '1';
    el.classList.add('kp-p');
    el.title = name + ' — click for details';
    el.addEventListener('click', function(e){ e.stopPropagation(); show(name); });
  });
}

if(document.readyState === 'loading'){
  document.addEventListener('DOMContentLoaded', annotate);
} else { annotate(); }

})();
