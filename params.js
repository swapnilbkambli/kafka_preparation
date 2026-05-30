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

,

/* BROKER - NETWORK */
'advertised.listeners': {
  scope:'broker', def:'(must match listeners)',
  desc:'The listener addresses the broker advertises to clients (producers, consumers) and other brokers. Clients receive this address from bootstrap; all subsequent connections go here. Format: LISTENER_NAME://hostname:port. The hostname MUST be reachable from client networks — the most common Kafka connectivity misconfiguration is advertising an internal IP unreachable from external clients.',
  values:[
    {val:'PLAINTEXT://broker1.internal:9092', when:'Internal-only cluster. All clients on the same network as brokers.'},
    {val:'PLAINTEXT://broker1.internal:9092,SSL://broker1.public:9093', when:'Dual listener: internal plaintext + external TLS. Clients choose the appropriate listener.'},
    {val:'INTERNAL://...,EXTERNAL://...', when:'Separate listeners for inter-broker traffic vs. client traffic.'}
  ],
  warn:'The most common cause of "can connect at bootstrap but fail on all subsequent produce/consume requests" — the advertised hostname is wrong or unreachable from the client network.',
  related:['listeners','bootstrap.servers','controller.listener.names']
},
'listeners': {
  scope:'broker', def:'PLAINTEXT://:9092',
  desc:'Comma-separated list of URIs the broker binds to and listens on. Format: LISTENER_NAME://host:port. Listener names must match advertised.listeners. You can define multiple listeners for different protocols (PLAINTEXT, SSL, SASL) or different traffic roles (inter-broker, client, controller).',
  values:[
    {val:'PLAINTEXT://:9092', when:'Default. Single plaintext listener on all interfaces.'},
    {val:'PLAINTEXT://:9092,SSL://:9093', when:'Dual: plaintext for internal, TLS for external clients.'},
    {val:'INTERNAL://:9092,EXTERNAL://:9093,CONTROLLER://:9094', when:'Production KRaft: separate listeners for inter-broker, client, and controller traffic.'}
  ],
  related:['advertised.listeners','controller.listener.names']
},
'socket.send.buffer.bytes': {
  scope:'broker', def:'102400 (100 KB)',
  desc:'TCP socket send buffer size (SO_SNDBUF) for all network connections. -1 means use OS default. The OS kernel uses this to buffer outgoing data before it is acknowledged by the remote end. Larger values help on high-latency networks or high-throughput clusters.',
  values:[
    {val:'102400 (100 KB)', when:'Default. Adequate for low-latency LAN.'},
    {val:'-1', when:'Use OS default (auto-tuned via tcp_wmem). Often the best choice — kernel adjusts automatically.'},
    {val:'1048576 (1 MB)', when:'Cross-datacenter links or high-latency WAN. Reduces stalls when ACKs are delayed.'}
  ],
  related:['socket.receive.buffer.bytes','num.network.threads']
},
'socket.receive.buffer.bytes': {
  scope:'broker', def:'102400 (100 KB)',
  desc:'TCP socket receive buffer size (SO_RCVBUF) for all network connections. -1 uses OS default. Larger values reduce stalling when producers burst data faster than the broker can process it.',
  values:[
    {val:'102400 (100 KB)', when:'Default.'},
    {val:'-1', when:'Let the OS auto-tune via net.core.rmem_max. Recommended on tuned Linux hosts.'},
    {val:'1048576 (1 MB)', when:'High-throughput brokers receiving >500 MB/s. Reduces TCP receive window stalls.'}
  ],
  related:['socket.send.buffer.bytes','num.network.threads']
},

/* BROKER - STORAGE */
'log.dirs': {
  scope:'broker', def:'/tmp/kafka-logs',
  desc:'Comma-separated list of directories where Kafka stores partition log data. Multiple directories = JBOD (Just a Bunch Of Disks) — Kafka distributes partitions across all dirs by free space, multiplying disk throughput without RAID. Each directory should map to a separate physical disk or NVMe for maximum I/O parallelism.',
  values:[
    {val:'/tmp/kafka-logs', when:'Default — development only. Single directory, no redundancy.'},
    {val:'/data1/kafka,/data2/kafka,/data3/kafka', when:'Production JBOD with 3 dedicated data disks.'},
    {val:'/nvme0/kafka,/nvme1/kafka', when:'Dual NVMe JBOD. Maximise random IOPS on high-throughput clusters.'}
  ],
  warn:'NEVER place log.dirs on the OS root partition or any shared filesystem. Disk full on the root partition kills the OS. Disk full on Kafka data crashes the broker with DiskAccessException.',
  related:['log.retention.ms','log.retention.bytes','num.recovery.threads.per.data.dir']
},
'log.flush.interval.messages': {
  scope:'broker', def:'Long.MAX_VALUE (effectively disabled)',
  desc:'Number of messages written before forcing an fsync to disk. Kafka deliberately disables this — durability comes from RF=3 + min.insync.replicas=2 (replication across multiple brokers), NOT from fsync. Forcing frequent fsyncs destroys throughput because fsync blocks the entire write path.',
  values:[
    {val:'Long.MAX_VALUE', when:'Default and ALWAYS recommended. Let OS page cache flush on its own schedule.'},
    {val:'1', when:'NEVER. One fsync per message. Throughput collapses to ~1000 msg/s. Only if a regulator explicitly mandates it AND you have benchmarked the impact.'}
  ],
  warn:'Kafka durability is achieved through replication, not fsync. Setting this low is a common misunderstanding that destroys performance without meaningful additional safety.',
  related:['log.flush.interval.ms','min.insync.replicas','replica.lag.time.max.ms']
},
'log.flush.interval.ms': {
  scope:'broker', def:'Long.MAX_VALUE (effectively disabled)',
  desc:'Maximum time between forced fsyncs. Like log.flush.interval.messages, disabled by default. Kafka trusts OS page cache and replication for durability.',
  values:[
    {val:'Long.MAX_VALUE', when:'Default and recommended. Rely on OS and replication.'},
    {val:'1000–5000', when:'Only if explicit regulatory requirement for periodic fsync AND performance impact is accepted.'}
  ],
  related:['log.flush.interval.messages','min.insync.replicas']
},
'log.cleaner.threads': {
  scope:'broker', def:'1',
  desc:'Number of background threads dedicated to log compaction. These scan compact topics, identify duplicate keys, and remove older records per key. More threads = faster compaction, but higher disk and CPU overhead. Monitor log-cleaner-io-time-total metric.',
  values:[
    {val:'1', when:'Default. Sufficient for a few compacted topics.'},
    {val:'2–4', when:'Clusters with many high-throughput compacted topics (CDC, config stores). Watch cleaner-io-time metric.'}
  ],
  related:['log.cleaner.min.cleanable.ratio','cleanup.policy','min.compaction.lag.ms']
},
'log.cleaner.min.cleanable.ratio': {
  scope:'broker', def:'0.5',
  desc:'Minimum ratio of dirty (uncompacted) bytes to total log bytes before the cleaner selects a partition for compaction. 0.5 = at least 50% of the log must be dirty before compaction fires. Lower = more aggressive compaction (more CPU). Higher = less frequent (more disk but less CPU).',
  values:[
    {val:'0.5', when:'Default. Balanced. Compaction fires when >50% of log is dirty.'},
    {val:'0.1–0.3', when:'Aggressive compaction. Topics where consumers need very fresh state.'},
    {val:'0.7–0.9', when:'Reduce cleaner overhead on large-volume compacted topics where some staleness is acceptable.'}
  ],
  related:['log.cleaner.threads','cleanup.policy','delete.retention.ms']
},
'log.roll.hours': {
  scope:'broker', def:'168 (7 days)',
  desc:'Maximum time before Kafka rolls a log segment to a new file, even if it has not reached log.segment.bytes. Ensures segments eventually roll on low-traffic partitions (which allows retention policies to delete them). Without this, a quiet partition could sit on a single segment forever.',
  values:[
    {val:'168 (7 days)', when:'Default. Low-traffic partitions roll at most weekly.'},
    {val:'24', when:'Daily roll. Predictable segment boundaries; easier to reason about retention windows.'},
    {val:'1', when:'Hourly roll. Very granular retention. Creates many small files — avoid unless necessary.'}
  ],
  related:['log.segment.bytes','log.retention.ms']
},
'num.recovery.threads.per.data.dir': {
  scope:'broker', def:'1',
  desc:'Threads used per log.dirs directory to load and recover log segments at broker startup after an unclean shutdown. Higher values parallelise partition recovery and dramatically reduce startup time on large brokers. After a clean shutdown, this setting has no effect — recovery is near-instant.',
  values:[
    {val:'1', when:'Default. Sequential recovery. A broker with 1000 partitions may take 10+ minutes to start after a crash.'},
    {val:'4–8', when:'Production. Parallelises recovery across all partitions in a directory. Critical for large brokers.'}
  ],
  related:['log.dirs','controlled.shutdown.enable']
},
'replica.fetch.max.bytes': {
  scope:'broker', def:'1048576 (1 MB)',
  desc:'Maximum bytes a follower fetches per partition per replication fetch request. Must be at least as large as message.max.bytes — if a message is larger than this limit, the follower can never replicate it and will fall out of ISR permanently.',
  values:[
    {val:'1048576 (1 MB)', when:'Default. Matches default message.max.bytes.'},
    {val:'5242880 (5 MB)', when:'When message.max.bytes is increased. Always keep these in sync.'}
  ],
  warn:'replica.fetch.max.bytes must be >= message.max.bytes. A mismatch causes followers to permanently fail to replicate large messages, causing irreversible ISR shrinkage.',
  related:['message.max.bytes','num.replica.fetchers','replica.lag.time.max.ms']
},
'log.local.retention.ms': {
  scope:'broker / topic', def:'-2 (inherits log.retention.ms)',
  desc:'For topics with tiered storage enabled: how long data is retained in local broker disk before being eligible for deletion (after upload to remote storage like S3 or GCS). -2 inherits log.retention.ms (no local reduction). Set shorter than log.retention.ms to keep only recent hot data locally while cold data remains accessible via remote storage.',
  values:[
    {val:'-2', when:'Default. Local retention same as total retention. Tiered storage does not reduce local disk.'},
    {val:'86400000 (1 day)', when:'Keep only 24 h locally. Older data on remote storage. Drastically reduces local disk for long-retention topics.'}
  ],
  related:['log.retention.ms','log.retention.bytes']
},

/* BROKER - ADMIN */
'offsets.retention.minutes': {
  scope:'broker', def:'10080 (7 days)',
  desc:'How long committed consumer group offsets are retained after a group goes inactive (no active consumers). After this window, the group\'s offsets are deleted. If a consumer is offline longer than this, it loses its saved position and falls back to auto.offset.reset on next start.',
  values:[
    {val:'10080 (7 days)', when:'Default. Groups idle >7 days lose their offsets.'},
    {val:'20160 (14 days)', when:'When consumers may be offline for extended periods (e.g., weekly batch jobs).'},
    {val:'1440 (1 day)', when:'Active streaming clusters. Short-lived consumer groups. Saves __consumer_offsets space.'}
  ],
  related:['auto.offset.reset','enable.auto.commit','offsets.topic.replication.factor']
},
'controlled.shutdown.max.retries': {
  scope:'broker', def:'3',
  desc:'Number of times the broker retries a graceful shutdown if the first attempt fails (e.g., followers not yet caught up for leader migration). After exhausting retries, the broker shuts down forcibly.',
  values:[
    {val:'3', when:'Default.'},
    {val:'5–10', when:'Large clusters where leader migration may need more time. Increases the clean-shutdown window.'}
  ],
  related:['controlled.shutdown.enable','controlled.shutdown.retry.backoff.ms']
},
'controlled.shutdown.retry.backoff.ms': {
  scope:'broker', def:'5000 (5 s)',
  desc:'Wait time between controlled shutdown retry attempts. Gives followers time to catch up after a failed migration before retrying.',
  values:[
    {val:'5000', when:'Default.'},
    {val:'10000', when:'High-latency clusters or when follower catch-up is known to be slow.'}
  ],
  related:['controlled.shutdown.enable','controlled.shutdown.max.retries']
},
'queued.max.request.bytes': {
  scope:'broker', def:'-1 (unlimited)',
  desc:'Maximum total byte size of all requests waiting in the RequestQueue. A byte-size companion to queued.max.requests (which limits by count). -1 = no byte cap. Useful to prevent memory exhaustion from a small number of very large produce requests filling the queue.',
  values:[
    {val:'-1', when:'Default. No byte-size cap. Rely on queued.max.requests count limit.'},
    {val:'1073741824 (1 GB)', when:'Cap total queue memory to prevent OOM when large produce batches back up.'}
  ],
  related:['queued.max.requests','num.io.threads']
},
'group.max.session.timeout.ms': {
  scope:'broker', def:'1800000 (30 min)',
  desc:'Broker-side upper bound on what session.timeout.ms value a consumer may request. If a consumer tries to join with session.timeout.ms exceeding this value, the broker rejects the JoinGroup. Acts as a cluster-wide cap preventing consumers from setting excessively long session timeouts that mask dead consumers.',
  values:[
    {val:'1800000 (30 min)', when:'Default. Consumers can request up to 30-minute sessions.'},
    {val:'300000 (5 min)', when:'Stricter: ensures dead consumers are detected within 5 minutes maximum.'}
  ],
  related:['session.timeout.ms','heartbeat.interval.ms']
},

/* PRODUCER / CONSUMER */
'request.timeout.ms': {
  scope:'producer / consumer', def:'30000 (30 s)',
  desc:'Maximum time the client waits for a response from the broker after sending a request. If no response arrives, the request fails and is retried (if retries > 0). Distinct from delivery.timeout.ms (total send lifecycle) and max.poll.interval.ms (consumer poll loop timing).',
  values:[
    {val:'30000 (30 s)', when:'Default. Appropriate for most stable clusters.'},
    {val:'10000 (10 s)', when:'Fast failure detection in well-connected clusters.'},
    {val:'60000 (60 s)', when:'Slow brokers, heavy load, or cross-region deployments with higher RTT.'}
  ],
  warn:'delivery.timeout.ms must be > request.timeout.ms + linger.ms, otherwise records can time out before the first delivery attempt even completes.',
  related:['delivery.timeout.ms','retries','max.block.ms']
},
'fetch.max.bytes': {
  scope:'consumer', def:'52428800 (50 MB)',
  desc:'Maximum total bytes the broker returns in a single FetchResponse across ALL partitions assigned to this consumer. A global cap on the response size. If a single partition\'s data exceeds max.partition.fetch.bytes it is still returned (to avoid starvation), but this caps the aggregate across all partitions in the response.',
  values:[
    {val:'52428800 (50 MB)', when:'Default.'},
    {val:'104857600 (100 MB)', when:'High-throughput consumers subscribed to many partitions. Reduces fetch round-trips.'},
    {val:'5242880 (5 MB)', when:'Memory-constrained consumers or containers with tight heap limits.'}
  ],
  related:['max.partition.fetch.bytes','fetch.min.bytes','fetch.max.wait.ms']
},
'bootstrap.servers': {
  scope:'producer / consumer / admin', def:'(must be set)',
  desc:'Comma-separated host:port pairs for the initial broker connection. Used ONLY for bootstrapping — the client connects here first to fetch full cluster metadata, then connects directly to partition leaders. You do NOT need to list every broker; 2–3 ensures resiliency if one is down at startup.',
  values:[
    {val:'broker1:9092', when:'Single entry. No resiliency — if that broker is down, client cannot start.'},
    {val:'broker1:9092,broker2:9092,broker3:9092', when:'Production: 3 brokers. Client bootstraps successfully even if 1–2 are down.'}
  ],
  related:['advertised.listeners']
},
'producer_byte_rate': {
  scope:'broker (quota)', def:'Long.MAX_VALUE (unlimited)',
  desc:'Per-client-ID byte rate quota for producers. Set via kafka-configs.sh --entity-type clients. When a producer exceeds this rate, the broker throttles it by delaying responses (produce-throttle-time-avg increases). Protects the cluster from a single runaway producer monopolising bandwidth.',
  values:[
    {val:'Long.MAX_VALUE', when:'Default: no quota.'},
    {val:'10485760 (10 MB/s)', when:'Standard quota for a well-behaved production producer.'},
    {val:'1048576 (1 MB/s)', when:'Restrict a bulk-ingest job to prevent starving other producers.'}
  ],
  related:['request.timeout.ms','buffer.memory']
},

/* CONNECT / MM2 */
'exactly.once.source.support': {
  scope:'connect', def:'disabled',
  desc:'Enables exactly-once semantics for Kafka Connect source connectors. When enabled, source connectors use Kafka transactions to atomically write records and commit source offsets. Requires connector to implement ExactlyOnceSourceTask interface and ACL grants for IDEMPOTENT_WRITE + transaction permissions.',
  values:[
    {val:'disabled', when:'Default. At-least-once for source connectors.'},
    {val:'enabled', when:'Enable for EOS-capable source connectors. Connector must support it explicitly.'},
    {val:'preparing', when:'Transitional state during upgrade to EOS. Grants necessary ACLs without enforcing EOS yet.'}
  ],
  related:['transactional.id','enable.idempotence','offset.flush.interval.ms']
},
'replication.policy.class': {
  scope:'connect (MM2)', def:'DefaultReplicationPolicy',
  desc:'MirrorMaker 2 policy controlling how topic names map between source and target clusters. DefaultReplicationPolicy prefixes topics with the source cluster alias (e.g., "dc-a.payments"). IdentityReplicationPolicy preserves original names — required when DR consumers should use the same topic names without config changes.',
  values:[
    {val:'DefaultReplicationPolicy', when:'Multi-cluster fan-in / fan-out. Topic names become "source-alias.topic-name". Makes data origin explicit.'},
    {val:'IdentityReplicationPolicy', when:'Active-passive DR. Consumers on DR cluster use identical topic names as primary. No consumer config changes on failover.'}
  ],
  related:['sync.group.offsets.enabled']
},
'sync.group.offsets.enabled': {
  scope:'connect (MM2)', def:'false',
  desc:'Whether MirrorMaker 2 periodically translates and syncs consumer group committed offsets from the source cluster to the target cluster. When true, consumer groups can resume from approximately the correct position after a DR failover instead of starting from auto.offset.reset.',
  values:[
    {val:'false', when:'Default. No offset sync. DR failover means consumers restart from auto.offset.reset.'},
    {val:'true', when:'DR scenarios. Consumers on the target cluster resume near the correct offset after failover. Pair with sync.group.offsets.interval.seconds.'}
  ],
  related:['sync.group.offsets.interval.seconds','replication.policy.class','auto.offset.reset']
},
'sync.group.offsets.interval.seconds': {
  scope:'connect (MM2)', def:'60',
  desc:'How often MM2 syncs translated consumer group offsets from source to target cluster. More frequent sync = smaller offset gap on failover (less reprocessing), but more write overhead on the target __consumer_offsets topic.',
  values:[
    {val:'60', when:'Default. Up to 60 s of messages may need reprocessing on failover.'},
    {val:'10–30', when:'Low-latency DR. Minimize reprocessing window on failover.'},
    {val:'300', when:'Batch consumers where a few minutes of replay is acceptable.'}
  ],
  related:['sync.group.offsets.enabled','replication.policy.class']
},

/* KRAFT / MIGRATION */
'zookeeper.metadata.migration.enable': {
  scope:'broker', def:'false',
  desc:'Enables the ZooKeeper-to-KRaft metadata migration mode. When set to true on a ZK-mode cluster with a new KRaft controller quorum configured, Kafka enters hybrid mode and begins migrating all metadata from ZooKeeper to the __cluster_metadata Raft log. This is Step 1 of the official ZK→KRaft migration path.',
  values:[
    {val:'false', when:'Default. Cluster operates in pure ZK mode (or pure KRaft if process.roles is set).'},
    {val:'true', when:'During ZK→KRaft migration only. Remove after migration is fully complete.'}
  ],
  warn:'Migration is one-way. Once started and completed, do not revert to false. A partial migration leaves the cluster in an inconsistent state. Follow the official step-by-step migration runbook.',
  related:['process.roles','controller.quorum.voters','zookeeper.session.timeout.ms']
},

/* KSQLDB */
'ksql.service.id': {
  scope:'ksqlDB', def:'default_',
  desc:'Unique identifier for a ksqlDB cluster. Used as a prefix for all ksqlDB-internal Kafka topics (command topic, query topics, etc.). Two ksqlDB clusters sharing the same service.id will corrupt each other\'s command topic. Must be stable — changing it orphans all existing internal topics.',
  values:[
    {val:'default_', when:'Default. Fine only if there is a single ksqlDB cluster per Kafka cluster.'},
    {val:'payments-ksql_', when:'Production: descriptive per-environment ID with trailing underscore for topic prefix clarity.'}
  ],
  warn:'Never share ksql.service.id between two ksqlDB clusters on the same Kafka cluster. They will write to the same command topic and corrupt each other\'s query state.',
  related:['ksql.streams.num.standby.replicas','ksql.advertised.listener']
},
'ksql.streams.num.standby.replicas': {
  scope:'ksqlDB', def:'0',
  desc:'Number of standby replicas for ksqlDB state stores (backed by Kafka Streams / RocksDB). Standbys keep warm copies of state on other ksqlDB nodes. When a node fails, a standby replica allows near-instant failover without rebuilding state from the beginning of Kafka topics.',
  values:[
    {val:'0', when:'Default. Single copy of state. Node failure = full state rebuild from Kafka (can take minutes for large stores).'},
    {val:'1', when:'Production HA. One standby per state store. Failover in seconds. 2× state storage requirement.'},
    {val:'2', when:'Mission-critical pipelines. Tolerates 2 simultaneous node failures. 3× storage.'}
  ],
  related:['ksql.service.id','ksql.advertised.listener']
},
'ksql.advertised.listener': {
  scope:'ksqlDB', def:'http://localhost:8088',
  desc:'The URL this ksqlDB node advertises to other ksqlDB nodes for inter-node communication (pull query routing, state store lookups, standby replication). Must be set to this node\'s externally reachable URL. Without correct configuration, pull queries routed to the wrong node fail.',
  values:[
    {val:'http://localhost:8088', when:'Default — broken in a real multi-node cluster. Single-node development only.'},
    {val:'http://ksql-node1.internal:8088', when:'Production: set to this node\'s reachable hostname/IP.'}
  ],
  related:['ksql.service.id','ksql.streams.num.standby.replicas']
},

/* SECURITY */
'ssl.client.auth': {
  scope:'broker', def:'none',
  desc:'Controls whether the broker requires clients to present a TLS client certificate (mutual TLS / mTLS). "none" = server-only TLS (clients verify broker cert only). "required" = both sides present certificates — strong client authentication without SASL. "requested" = broker asks for cert but does not reject clients without one.',
  values:[
    {val:'none', when:'Default. One-way TLS. Use SASL for client identity. Standard in most deployments.'},
    {val:'required', when:'Mutual TLS (mTLS). Both client and broker present certificates. Common in financial/regulated environments. No SASL needed.'},
    {val:'requested', when:'Transitional during cert rollout. Broker asks for cert but does not block clients without one.'}
  ],
  related:['super.users','bootstrap.servers']
},
'super.users': {
  scope:'broker', def:'(not set)',
  desc:'Semicolon-separated list of principals that bypass ALL ACL checks. Format: User:name or User:CN=...,OU=... for mTLS principals. Super users can read/write any topic, manage ACLs, and alter configs without any explicit ACL grant.',
  values:[
    {val:'(not set)', when:'No super users. All access controlled exclusively via ACLs.'},
    {val:'User:admin;User:kafka-broker', when:'Admin principal + broker inter-node communication principal. Minimum viable production super.users list.'}
  ],
  warn:'Super user access bypasses ALL authorisation. Restrict to the absolute minimum set of service accounts. Audit access logs regularly for super user activity.',
  related:['ssl.client.auth']
},
'consumer.instance.timeout.ms': {
  scope:'REST Proxy', def:'300000 (5 min)',
  desc:'Kafka REST Proxy: how long a consumer instance is kept alive without a /records poll. After this timeout the proxy destroys the instance and commits offsets. Clients must poll before expiry or recreate the consumer. This is REST Proxy-specific — unrelated to Kafka\'s own session.timeout.ms.',
  values:[
    {val:'300000 (5 min)', when:'Default. REST consumers must poll at least every 5 minutes.'},
    {val:'60000', when:'Strict cleanup of idle consumers. Reclaims proxy resources faster.'},
    {val:'600000', when:'Slow consumers or infrequent polling patterns. Prevents unexpected consumer expiry.'}
  ],
  related:['session.timeout.ms','max.poll.interval.ms']
},

/* JMX METRICS */
'RequestChannel.Request': {
  scope:'broker (metric)', def:'N/A — internal object type',
  desc:'The internal Kafka object representing a decoded client request waiting in the RequestQueue. Network threads parse raw TCP bytes into RequestChannel.Request objects and enqueue them. IO threads dequeue these, perform the actual work (disk write, log read, replication state update), and place responses on the response queue. The key observable: RequestQueueSize tells you how many are waiting. LocalTimeMs = IO thread processing time on disk. RemoteTimeMs = time in purgatory waiting for ISR acks.',
  values:[
    {val:'RequestQueueSize', when:'JMX: kafka.network:type=RequestChannel,name=RequestQueueSize. Alert if consistently approaching queued.max.requests.'},
    {val:'LocalTimeMs high (>20 ms)', when:'Disk bottleneck. IO thread spending too long on disk write/read.'},
    {val:'RemoteTimeMs high (>100 ms)', when:'Follower lag. Request parked in purgatory waiting for ISR acks. Fix the slow follower, not the IO threads.'}
  ],
  related:['num.io.threads','num.network.threads','queued.max.requests','RequestHandlerAvgIdlePercent']
},
'RequestHandlerAvgIdlePercent': {
  scope:'broker (metric)', def:'target > 0.3 (30%)',
  desc:'JMX metric: kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent. Fraction of time IO handler threads are idle. Below 30% means IO threads are saturated — requests are queuing faster than threads can process them. This is the primary broker CPU health signal. Causes: high throughput, compression mismatch (broker recompressing), or too few io threads.',
  values:[
    {val:'> 0.3', when:'Healthy. IO threads have spare capacity.'},
    {val:'0.1 – 0.3', when:'Warning. Increase num.io.threads or investigate what is consuming CPU (async-profiler).'},
    {val:'< 0.1', when:'Critical. Requests backing up. Clients will see TimeoutException. Immediate action required.'}
  ],
  warn:'If this drops without a matching increase in bytes in/out, the issue is CPU-intensive requests (broker recompression from codec mismatch, large message parsing). Profile with async-profiler before just raising num.io.threads.',
  related:['num.io.threads','RequestChannel.Request','NetworkProcessorAvgIdlePercent','compression.type']
},
'NetworkProcessorAvgIdlePercent': {
  scope:'broker (metric)', def:'target > 0.3 (30%)',
  desc:'JMX metric: kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent. Fraction of time network processor (NIO Selector) threads are idle. Below 30% means network threads cannot read from sockets fast enough. Distinct from RequestHandlerAvgIdlePercent (IO thread saturation). Fix: increase num.network.threads.',
  values:[
    {val:'> 0.3', when:'Healthy. Network threads keeping up with socket I/O.'},
    {val:'< 0.3', when:'Saturated. Increase num.network.threads. Also check for very high client connection counts.'}
  ],
  related:['num.network.threads','RequestHandlerAvgIdlePercent','RequestChannel.Request']
},
'ActiveControllerCount': {
  scope:'broker (metric)', def:'exactly 1 across cluster',
  desc:'JMX metric: kafka.controller:type=KafkaController,name=ActiveControllerCount. Per-broker metric — each broker reports 0 or 1; only the active controller reports 1. The SUM across all brokers MUST always equal exactly 1. Zero = no controller (leader election in progress or all controllers down). Two+ = split-brain (data corruption risk).',
  values:[
    {val:'sum = 1', when:'Normal cluster operation.'},
    {val:'sum = 0', when:'Controller election in progress (< 30 s normal) OR all ZK/KRaft quorum nodes down. Alert if sustained > 30 s.'},
    {val:'sum > 1', when:'CRITICAL split-brain. Immediate investigation. Alert expression: sum(kafka_controller_active) != 1'}
  ],
  warn:'Alert on sum != 1, not just sum = 0. Two active controllers can silently corrupt metadata.',
  related:['process.roles','controller.quorum.voters','zookeeper.session.timeout.ms']
},
'UnderReplicatedPartitions': {
  scope:'broker (metric)', def:'must be 0',
  desc:'JMX metric: kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions. Number of partitions where ISR count < replication factor. Non-zero means at least one follower is behind or a broker is down. The most important Kafka health alert — any sustained non-zero value means the cluster is degraded and a subsequent broker failure could cause data loss.',
  values:[
    {val:'0', when:'Healthy. All partitions fully replicated.'},
    {val:'> 0 for > 60 s', when:'Alert. Diagnose: broker down, follower falling behind (disk/network saturation), or ISR eviction loop.'},
    {val:'Count = all partitions', when:'Critical. Likely a full broker failure or partition leadership instability.'}
  ],
  warn:'During a rolling restart, URP spikes briefly as each broker stops. Add a time buffer to your alert (e.g., > 0 for 1 min) to avoid false alarms during planned maintenance.',
  related:['replica.lag.time.max.ms','min.insync.replicas','num.replica.fetchers','ReplicaFetcherManager.MaxLag']
},
'bufferpool-wait-ratio': {
  scope:'producer (metric)', def:'target = 0',
  desc:'JMX metric: kafka.producer:type=producer-metrics,name=bufferpool-wait-ratio. Fraction of time the producer Sender thread was blocked waiting for buffer.memory space to be freed. Any value > 0 means the producer is back-pressured — producing faster than the broker can accept. If this is > 0, send() calls are blocking on max.block.ms.',
  values:[
    {val:'0', when:'Healthy. Producer never waits for buffer space.'},
    {val:'0.01 – 0.1', when:'Mild backpressure. Consider increasing buffer.memory or investigating broker throughput.'},
    {val:'> 0.1', when:'Severe backpressure. Producer bottlenecked. Check broker health (URPs, disk, network). Increase buffer.memory or reduce production rate.'}
  ],
  related:['buffer.memory','max.block.ms','batch.size']
},
'records-lag-max': {
  scope:'consumer (metric)', def:'target ≈ 0',
  desc:'JMX metric: kafka.consumer:type=consumer-fetch-manager-metrics,name=records-lag-max. Maximum consumer lag (records behind latest offset) across ALL partitions assigned to this consumer instance. This is per-consumer-instance. For cluster-wide group lag monitoring, use Burrow, kminion, or kafka_consumer_group_lag Prometheus metric.',
  values:[
    {val:'≈ 0', when:'Consumer keeping up in real time.'},
    {val:'> threshold (growing)', when:'Consumer falling behind. Scale out (add instances up to partition count), optimise processing, or investigate broker issues.'},
    {val:'monotonically growing', when:'Consumer cannot keep up at all. Must scale horizontally or reduce max.poll.records to process smaller batches.'}
  ],
  related:['max.poll.records','max.poll.interval.ms','fetch.min.bytes','session.timeout.ms']
},
'ReplicaFetcherManager.MaxLag': {
  scope:'broker (metric)', def:'target ≈ 0',
  desc:'JMX metric: kafka.server:type=ReplicaFetcherManager,name=MaxLag. Maximum offset lag of follower replicas on THIS broker compared to their leaders. High MaxLag means this broker (as a follower) is not keeping up — risk of ISR eviction. Distinct from consumer lag (which is application-side). Common causes: disk I/O saturation, GC pause, network congestion between brokers.',
  values:[
    {val:'0 – few thousand', when:'Healthy. Follower roughly in sync.'},
    {val:'Growing steadily', when:'Follower bottlenecked. Check disk IOPS, GC logs, num.replica.fetchers, network to leader brokers.'},
    {val:'Stuck high > replica.lag.time.max.ms', when:'Follower will be evicted from ISR. If ISR shrinks below min.insync.replicas, writes will be rejected.'}
  ],
  related:['replica.lag.time.max.ms','num.replica.fetchers','UnderReplicatedPartitions']
},

/* EXCEPTIONS */
'ProducerFencedException': {
  scope:'producer', def:'N/A — exception class',
  desc:'Runtime exception thrown when a transactional producer is fenced by a newer instance with the same transactional.id. Happens when: (1) the Transaction Coordinator auto-aborts a transaction because transaction.timeout.ms expired, issuing a new producer epoch; (2) a new producer instance starts with the same transactional.id, fencing the old one. The fenced producer throws this on its next produce() or commitTransaction() call.',
  values:[
    {val:'thrown on commitTransaction()', when:'Transaction was auto-aborted by broker (timeout expired) before the client noticed. Records are permanently gone — they were written as ABORTED transactions.'},
    {val:'thrown on produce()', when:'Another producer instance with same transactional.id started and claimed the epoch.'}
  ],
  warn:'CRITICAL: Never catch and swallow ProducerFencedException. It means a transaction was silently aborted and records are lost. Log at ERROR, alert, and handle at application level — retrying will only start a fresh transaction, it cannot recover the lost records.',
  related:['transactional.id','transaction.timeout.ms','isolation.level']
},
'NotEnoughReplicasException': {
  scope:'producer', def:'N/A — exception class',
  desc:'Exception thrown to producers when acks=all and the ISR count drops below min.insync.replicas. The broker deliberately refuses the write — this is intentional durability protection, not a bug. Common causes: broker down, follower falling behind due to disk/network saturation, or broker in the middle of a restart.',
  values:[
    {val:'thrown on send()', when:'ISR shrank below min.insync.replicas. Producer should retry with backoff — writes will succeed once ISR recovers.'},
    {val:'sustained for minutes', when:'A broker is permanently down or a follower is irrecoverably lagging. Investigate UnderReplicatedPartitions and broker logs.'}
  ],
  warn:'With min.insync.replicas=2 and RF=3, losing 2 brokers simultaneously makes ALL critical topics unwritable. This is by design. Have a runbook for this failure mode.',
  related:['min.insync.replicas','acks','unclean.leader.election.enable','UnderReplicatedPartitions']
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
'.kp-bg-ksqlDB{background:rgba(255,82,164,.18);color:#ff52a4}',
'.kp-bg-REST{background:rgba(255,140,50,.18);color:#ff8c32}',
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
// Sort keys longest-first so 'max.in.flight.requests.per.connection' matches
// before 'max.in.flight.requests' when text has a '=' suffix etc.
var _keys = Object.keys(DB).sort(function(a,b){ return b.length - a.length; });

function _resolve(text){
  // 1. exact match
  if(DB[text]) return text;
  // 2. prefix match: text starts with key followed by =, space, (, <, >, !
  for(var i=0; i<_keys.length; i++){
    var k = _keys[i];
    if(text.length > k.length && text.indexOf(k) === 0){
      var next = text.charAt(k.length);
      if(/[=\s(<>!]/.test(next)) return k;
    }
  }
  return null;
}

function annotate(){
  document.querySelectorAll('code').forEach(function(el){
    if(el.dataset.kpDone) return;
    var matched = _resolve(el.textContent.trim());
    if(!matched) return;
    el.dataset.kpDone = '1';
    el.classList.add('kp-p');
    el.title = matched + ' — click for details';
    (function(name){ el.addEventListener('click', function(e){ e.stopPropagation(); show(name); }); })(matched);
  });
}

if(document.readyState === 'loading'){
  document.addEventListener('DOMContentLoaded', annotate);
} else { annotate(); }

})();
