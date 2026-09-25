package example.monitoring

import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.javadsl.AbstractBehavior
import akka.actor.typed.javadsl.ActorContext
import akka.actor.typed.javadsl.Behaviors
import akka.actor.typed.javadsl.Receive
import akka.actor.typed.javadsl.TimerScheduler
import akka.http.javadsl.Http
import akka.http.javadsl.model.ContentTypes
import akka.http.javadsl.model.HttpEntities
import akka.http.javadsl.model.HttpMethods
import akka.http.javadsl.model.HttpRequest
import akka.http.javadsl.model.HttpResponse
import akka.http.javadsl.ServerBinding
import akka.stream.javadsl.Sink
import akka.stream.javadsl.Source
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetSocketAddress
import java.net.SocketException
import java.net.URI
import java.nio.ByteBuffer
import java.nio.charset.CharacterCodingException
import java.nio.charset.CodingErrorAction
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.atomic.LongAdder
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import java.util.PriorityQueue
import java.util.UUID
import kotlin.system.exitProcess
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import org.slf4j.LoggerFactory

data class WarehouseSettings(
    val warehouseId: String,
    val bindAddress: String = "127.0.0.1",
    val temperaturePort: Int = 3344,
    val humidityPort: Int = 3355,
    val healthPort: Int = 8081,
    val centralEndpoint: URI = URI("http://127.0.0.1:8080/readings"),
    val maxDatagramBytes: Int = 512,
    val capacity: Int = 1024,
    val maxInFlight: Int = 8,
    val attemptTimeout: Duration = Duration.ofSeconds(2),
    val maxAttempts: Int = 3,
    val maxEventAge: Duration = Duration.ofSeconds(30),
    val drainTimeout: Duration = Duration.ofSeconds(10),
) {
    init {
        require(EnvelopeValidation.validIdentifier(warehouseId)) { "WAREHOUSE_ID must be a safe 1-64 character identifier" }
        require(bindAddress.isNotBlank()) { "UDP_BIND_ADDRESS must not be blank" }
        require(temperaturePort in 0..65535 && humidityPort in 0..65535 && healthPort in 0..65535) { "Invalid port" }
        require(temperaturePort == 0 || temperaturePort != humidityPort) { "Sensor ports must differ" }
        require(centralEndpoint.scheme == "http" && !centralEndpoint.host.isNullOrBlank()) { "CENTRAL_ENDPOINT must be an HTTP URI" }
        require(centralEndpoint.rawUserInfo == null && centralEndpoint.rawQuery == null && centralEndpoint.rawFragment == null) { "CENTRAL_ENDPOINT must not contain credentials, query, or fragment" }
        require(centralEndpoint.path == "/readings" && centralEndpoint.port in -1..65535 && centralEndpoint.port != 0) { "CENTRAL_ENDPOINT must target /readings on a valid port" }
        require(maxDatagramBytes in 1..65506) { "Invalid datagram limit" }
        require(capacity in 1..100_000 && maxInFlight in 1..capacity && maxInFlight <= 256) { "Invalid pending/in-flight capacity" }
        require(maxAttempts in 1..10) { "MAX_ATTEMPTS must be 1-10" }
        require(attemptTimeout.toMillis() in 1..60_000) { "Invalid request timeout" }
        require(maxEventAge >= attemptTimeout && maxEventAge <= Duration.ofHours(1)) { "Invalid maximum event age" }
        require(drainTimeout.toMillis() in 1..60_000) { "Invalid shutdown timeout" }
    }

    companion object {
        fun from(config: Config): WarehouseSettings {
            val c = config.getConfig("warehouse")
            require(c.hasPath("id")) { "WAREHOUSE_ID is required" }
            return WarehouseSettings(
                c.getString("id"), c.getString("bind-address"), c.getInt("temperature-port"),
                c.getInt("humidity-port"), c.getInt("health-port"), URI(c.getString("central-endpoint")),
                c.getInt("max-datagram-bytes"), c.getInt("capacity"), c.getInt("max-in-flight"),
                c.getDuration("attempt-timeout"), c.getInt("max-attempts"),
                c.getDuration("max-event-age"), c.getDuration("drain-timeout"),
            )
        }
    }
}

data class SensorReading(val sensorId: String, val value: Double)

sealed interface ParseResult {
    data class Accepted(val reading: SensorReading) : ParseResult
    data class Rejected(val reason: String) : ParseResult
}

object SensorPayloadParser {
    private val decimal = Regex("[+-]?[0-9]+(?:\\.[0-9]+)?")

    fun parse(bytes: ByteArray, length: Int = bytes.size): ParseResult {
        if (length !in 1..bytes.size) return ParseResult.Rejected("empty_or_invalid_length")
        val text = try {
            Charsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes, 0, length)).toString()
                .removeSuffix("\n").removeSuffix("\r")
        } catch (_: CharacterCodingException) {
            return ParseResult.Rejected("invalid_utf8")
        }
        if (text.any { it.isISOControl() }) return ParseResult.Rejected("control_character")
        val fields = text.split(';')
        if (fields.size != 2) return ParseResult.Rejected("invalid_field_count")
        val values = mutableMapOf<String, String>()
        for (field in fields) {
            val parts = field.split('=')
            if (parts.size != 2) return ParseResult.Rejected("invalid_assignment")
            val key = parts[0].trim()
            if (key !in setOf("sensor_id", "value") || values.containsKey(key)) {
                return ParseResult.Rejected("unknown_or_duplicate_field")
            }
            values[key] = parts[1].trim()
        }
        val id = values["sensor_id"] ?: return ParseResult.Rejected("missing_sensor_id")
        if (!EnvelopeValidation.validIdentifier(id)) return ParseResult.Rejected("invalid_sensor_id")
        val rawValue = values["value"] ?: return ParseResult.Rejected("missing_value")
        if (!decimal.matches(rawValue)) return ParseResult.Rejected("invalid_decimal")
        val value = rawValue.toDoubleOrNull()
        if (value == null || !value.isFinite()) return ParseResult.Rejected("non_finite_value")
        return ParseResult.Accepted(SensorReading(id, value))
    }
}

data class PendingReading(val envelope: ReadingEnvelope, val acceptedNanos: Long, val attempt: Int = 0)

class IngressBuffer(private val capacity: Int) {
    private val permits = Semaphore(capacity)
    private val queue = ArrayBlockingQueue<PendingReading>(capacity)
    private val notificationPending = AtomicBoolean()
    private val highWater = AtomicInteger()
    @Volatile var notifyAvailable: () -> Unit = {}

    fun offer(reading: PendingReading): Boolean {
        if (!permits.tryAcquire()) return false
        check(queue.offer(reading)) { "Admission accounting is inconsistent" }
        highWater.accumulateAndGet(retained(), ::maxOf)
        if (notificationPending.compareAndSet(false, true)) notifyAvailable()
        return true
    }

    fun notificationConsumed() { notificationPending.set(false) }
    fun poll(): PendingReading? = queue.poll()
    fun release() { permits.release() }
    fun retained(): Int = capacity - permits.availablePermits()
    fun highWaterMark(): Int = highWater.get()
    fun isEmpty(): Boolean = queue.isEmpty()
}

class WarehouseCounters {
    val received = LongAdder()
    val invalid = LongAdder()
    val admitted = LongAdder()
    val dropped = LongAdder()
    val attempts = LongAdder()
    val delivered = LongAdder()
    val retries = LongAdder()
    val expired = LongAdder()
    val failed = LongAdder()
    val abandoned = LongAdder()
    val inFlight = AtomicInteger()

    fun summary(buffer: IngressBuffer): String =
        "received=${received.sum()} invalid=${invalid.sum()} admitted=${admitted.sum()} " +
            "dropped=${dropped.sum()} retained=${buffer.retained()} highWater=${buffer.highWaterMark()} " +
            "inFlight=${inFlight.get()} attempts=${attempts.sum()} delivered=${delivered.sum()} " +
            "retries=${retries.sum()} expired=${expired.sum()} failed=${failed.sum()} abandoned=${abandoned.sum()}"
}

class UdpReceiver(
    address: String,
    port: Int,
    private val type: MeasurementType,
    private val settings: WarehouseSettings,
    private val ingress: IngressBuffer,
    private val counters: WarehouseCounters,
    private val onFailure: (Exception) -> Unit,
) : AutoCloseable {
    private val closing = AtomicBoolean()
    private val socket = DatagramSocket(null).apply {
        try {
            reuseAddress = false
            bind(InetSocketAddress(address, port))
        } catch (error: Exception) {
            close()
            throw error
        }
    }
    val localPort: Int get() = socket.localPort
    private val thread = Thread(::receive, "udp-${type.name.lowercase()}").apply { isDaemon = true }

    fun start() = thread.start()

    private fun receive() {
        val bytes = ByteArray(settings.maxDatagramBytes + 1)
        val packet = DatagramPacket(bytes, bytes.size)
        try {
            while (!closing.get()) {
                packet.length = bytes.size
                socket.receive(packet)
                counters.received.increment()
                if (packet.length > settings.maxDatagramBytes) {
                    counters.invalid.increment()
                    continue
                }
                when (val result = SensorPayloadParser.parse(bytes, packet.length)) {
                    is ParseResult.Rejected -> counters.invalid.increment()
                    is ParseResult.Accepted -> {
                        val reading = ReadingEnvelope(
                            1, UUID.randomUUID().toString(), settings.warehouseId,
                            result.reading.sensorId, type, result.reading.value, Instant.now().toString(),
                        )
                        if (ingress.offer(PendingReading(reading, System.nanoTime()))) counters.admitted.increment()
                        else counters.dropped.increment()
                    }
                }
            }
        } catch (error: SocketException) {
            if (!closing.get()) onFailure(error)
        } catch (error: Exception) {
            if (!closing.get()) onFailure(error)
        }
    }

    override fun close() {
        closing.set(true)
        socket.close()
        if (thread.isAlive && Thread.currentThread() != thread) thread.join(1000)
    }
}

data class PublishOutcome(val accepted: Boolean, val retryable: Boolean, val status: Int? = null) {
    companion object {
        fun transportFailure() = PublishOutcome(false, true)
        fun fromStatus(status: Int, expected: Int = 204) =
            PublishOutcome(status == expected, status == 429 || status >= 500, status)
    }
}

interface ReadingPublisher {
    fun publish(reading: ReadingEnvelope): CompletionStage<PublishOutcome>
    fun probe(): CompletionStage<PublishOutcome>
}

class HttpReadingPublisher(private val settings: WarehouseSettings, private val system: ActorSystem<*>) : ReadingPublisher {
    override fun publish(reading: ReadingEnvelope): CompletionStage<PublishOutcome> = send(
        HttpRequest.create().withMethod(HttpMethods.POST).withUri("/readings")
            .withEntity(HttpEntities.create(ContentTypes.APPLICATION_JSON, ReadingCodec.encode(reading))),
        204,
    )

    override fun probe(): CompletionStage<PublishOutcome> = send(HttpRequest.GET("/health"), 200)

    private fun send(request: HttpRequest, expected: Int): CompletionStage<PublishOutcome> {
        val endpoint = settings.centralEndpoint
        // One owned connection per attempt makes stream cancellation close timed-out work.
        return Source.single(request)
            .via(Http.get(system).connectionTo(endpoint.host).toPort(if (endpoint.port == -1) 80 else endpoint.port).http())
            .mapAsync(1) { response ->
                response.entity().withSizeLimit(4096).toStrict(settings.attemptTimeout.toMillis(), system)
                    .thenApply { PublishOutcome.fromStatus(response.status().intValue(), expected) }
            }
            .completionTimeout(settings.attemptTimeout)
            .runWith(Sink.head(), system)
            .exceptionally { PublishOutcome.transportFailure() }
    }
}

enum class DestinationState { UNKNOWN, UP, DOWN }

sealed interface ForwarderCommand {
    data object Wake : ForwarderCommand
    data object RetryDue : ForwarderCommand
    data object Probe : ForwarderCommand
    data object DrainExpired : ForwarderCommand
    data class Completed(val id: String, val attempt: Int, val outcome: PublishOutcome) : ForwarderCommand
    data class ProbeCompleted(val outcome: PublishOutcome) : ForwarderCommand
    data class Stop(val completion: CompletableFuture<Unit>) : ForwarderCommand
}

object DeliveryPolicy {
    fun retryDelay(attempt: Int, factor: Double = ThreadLocalRandom.current().nextDouble(0.8, 1.2)): Duration {
        require(attempt in 1..10 && factor in 0.8..1.2)
        return Duration.ofMillis(((250L shl (attempt - 1)) * factor).toLong())
    }
}

class ForwarderActor private constructor(
    context: ActorContext<ForwarderCommand>,
    private val timers: TimerScheduler<ForwarderCommand>,
    private val settings: WarehouseSettings,
    private val ingress: IngressBuffer,
    private val counters: WarehouseCounters,
    private val destination: AtomicReference<DestinationState>,
    private val publisher: ReadingPublisher,
    private val nanoTime: () -> Long,
) : AbstractBehavior<ForwarderCommand>(context) {
    private data class Retry(val reading: PendingReading, val due: Long)
    private val retryQueue = PriorityQueue<Retry>(compareBy { it.due })
    private val inFlight = mutableMapOf<String, PendingReading>()
    private var probeRunning = false
    private var stopping: CompletableFuture<Unit>? = null

    init {
        ingress.notifyAvailable = { context.self.tell(ForwarderCommand.Wake) }
        context.self.tell(ForwarderCommand.Wake)
        timers.startSingleTimer(ForwarderCommand.Probe, Duration.ofMillis(1))
    }

    override fun createReceive(): Receive<ForwarderCommand> = newReceiveBuilder()
        .onMessage(ForwarderCommand.Wake::class.java) {
            ingress.notificationConsumed()
            pump()
            this
        }
        .onMessage(ForwarderCommand.RetryDue::class.java) { pump(); this }
        .onMessage(ForwarderCommand.Completed::class.java) { completed(it); this }
        .onMessage(ForwarderCommand.Probe::class.java) { probe(); this }
        .onMessage(ForwarderCommand.ProbeCompleted::class.java) {
            probeRunning = false
            destination.set(if (it.outcome.accepted) DestinationState.UP else DestinationState.DOWN)
            if (stopping == null) timers.startSingleTimer(ForwarderCommand.Probe, Duration.ofSeconds(2))
            pump()
            this
        }
        .onMessage(ForwarderCommand.Stop::class.java) {
            stopping = it.completion
            timers.cancel(ForwarderCommand.Probe)
            timers.cancel(ForwarderCommand.RetryDue)
            while (retryQueue.isNotEmpty()) {
                retryQueue.remove()
                counters.abandoned.increment()
                ingress.release()
            }
            timers.startSingleTimer(ForwarderCommand.DrainExpired, settings.drainTimeout)
            pump()
            this
        }
        .onMessage(ForwarderCommand.DrainExpired::class.java) {
            counters.abandoned.add(ingress.retained().toLong())
            while (ingress.poll() != null) ingress.release()
            retryQueue.forEach { ingress.release() }
            retryQueue.clear()
            inFlight.forEach { _ -> ingress.release() }
            inFlight.clear()
            counters.inFlight.set(0)
            finish()
            this
        }
        .build()

    private fun expired(reading: PendingReading): Boolean =
        nanoTime() - reading.acceptedNanos >= settings.maxEventAge.toNanos()

    private fun pump() {
        while (inFlight.size + (if (probeRunning) 1 else 0) < settings.maxInFlight) {
            val retry = retryQueue.peek()
            val next = if (retry != null && (retry.due <= nanoTime() || expired(retry.reading))) {
                retryQueue.remove().reading
            } else ingress.poll() ?: break
            if (expired(next)) {
                counters.expired.increment()
                ingress.release()
                continue
            }
            val attempt = next.copy(attempt = next.attempt + 1)
            inFlight[attempt.envelope.eventId] = attempt
            counters.inFlight.set(inFlight.size)
            counters.attempts.increment()
            context.pipeToSelf(publisher.publish(attempt.envelope)) { outcome, error ->
                ForwarderCommand.Completed(
                    attempt.envelope.eventId, attempt.attempt,
                    if (error == null) outcome else PublishOutcome.transportFailure(),
                )
            }
        }
        timers.cancel(ForwarderCommand.RetryDue)
        if (retryQueue.isNotEmpty() && stopping == null && inFlight.size + (if (probeRunning) 1 else 0) < settings.maxInFlight) {
            val delay = (retryQueue.peek().due - nanoTime()).coerceAtLeast(1_000_000)
            timers.startSingleTimer(ForwarderCommand.RetryDue, Duration.ofNanos(delay))
        }
        if (stopping != null && ingress.retained() == 0 && !probeRunning) finish()
    }

    private fun completed(message: ForwarderCommand.Completed) {
        val reading = inFlight[message.id] ?: return
        if (reading.attempt != message.attempt) return
        inFlight.remove(message.id)
        counters.inFlight.set(inFlight.size)
        destination.set(if (message.outcome.accepted) DestinationState.UP else DestinationState.DOWN)
        if (message.outcome.accepted) {
            counters.delivered.increment()
            ingress.release()
        } else if (expired(reading)) {
            counters.expired.increment()
            ingress.release()
        } else if (message.outcome.retryable && reading.attempt < settings.maxAttempts && stopping == null) {
            counters.retries.increment()
            retryQueue.add(Retry(reading, nanoTime() + DeliveryPolicy.retryDelay(reading.attempt).toNanos()))
        } else {
            counters.failed.increment()
            ingress.release()
        }
        pump()
    }

    private fun probe() {
        if (stopping != null) return
        if (probeRunning || inFlight.size >= settings.maxInFlight) {
            timers.startSingleTimer(ForwarderCommand.Probe, Duration.ofSeconds(2))
            return
        }
        probeRunning = true
        context.pipeToSelf(publisher.probe()) { outcome, error ->
            ForwarderCommand.ProbeCompleted(if (error == null) outcome else PublishOutcome.transportFailure())
        }
    }

    private fun finish() {
        timers.cancelAll()
        stopping?.complete(Unit)
        context.system.terminate()
    }

    companion object {
        fun create(
            settings: WarehouseSettings,
            ingress: IngressBuffer,
            counters: WarehouseCounters,
            destination: AtomicReference<DestinationState>,
            publisherFactory: (ActorSystem<*>) -> ReadingPublisher = { HttpReadingPublisher(settings, it) },
            nanoTime: () -> Long = System::nanoTime,
        ): Behavior<ForwarderCommand> = Behaviors.withTimers { timers ->
            Behaviors.setup { context ->
                ForwarderActor(context, timers, settings, ingress, counters, destination, publisherFactory(context.system), nanoTime)
            }
        }
    }
}

class WarehouseService(private val settings: WarehouseSettings, config: Config = ConfigFactory.load()) : AutoCloseable {
    val counters = WarehouseCounters()
    val ingress = IngressBuffer(settings.capacity)
    val destination = AtomicReference(DestinationState.UNKNOWN)
    private val closing = AtomicBoolean()
    private val failed = AtomicBoolean()
    private val ready = AtomicBoolean()
    private val log = LoggerFactory.getLogger("monitoring.warehouse")
    private val status = Executors.newSingleThreadScheduledExecutor { task ->
        Thread(task, "warehouse-status").apply { isDaemon = true }
    }
    private val system = ActorSystem.create(
        ForwarderActor.create(settings, ingress, counters, destination), "warehouse", config,
    )
    private val receivers = mutableListOf<UdpReceiver>()
    private var binding: ServerBinding? = null
    val temperaturePort: Int get() = receivers[0].localPort
    val humidityPort: Int get() = receivers[1].localPort
    val healthPort: Int get() = checkNotNull(binding).localAddress().port

    init {
        try {
            receivers.add(receiver(settings.temperaturePort, MeasurementType.TEMPERATURE))
            receivers.add(receiver(settings.humidityPort, MeasurementType.HUMIDITY))
            binding = Http.get(system).newServerAt(settings.bindAddress, settings.healthPort)
                .bindSync { request ->
                    request.discardEntityBytes(system)
                    if (request.method() == HttpMethods.GET && request.getUri().path() == "/health") {
                        HttpResponse.create().withStatus(if (ready.get() && destination.get() == DestinationState.UP) 200 else 503)
                            .withEntity(HttpEntities.create(ContentTypes.APPLICATION_JSON, healthBody()))
                    } else HttpResponse.create().withStatus(404)
                }.toCompletableFuture().get(10, TimeUnit.SECONDS)
            receivers.forEach { it.start() }
            ready.set(true)
            log.info("LISTENING warehouse={} temperaturePort={} humidityPort={} healthPort={} destination=UNKNOWN", settings.warehouseId, temperaturePort, humidityPort, healthPort)
            status.scheduleWithFixedDelay({
                log.info("STATUS warehouse={} destination={} {}", settings.warehouseId, destination.get(), counters.summary(ingress))
            }, 5, 5, TimeUnit.SECONDS)
        } catch (error: Exception) {
            failed.set(true)
            close()
            throw error
        }
    }

    private fun receiver(port: Int, type: MeasurementType): UdpReceiver =
        UdpReceiver(settings.bindAddress, port, type, settings, ingress, counters) { error ->
            failed.set(true)
            ready.set(false)
            status.execute { log.error("LISTENER_FAILED warehouse={} type={} cause={}", settings.warehouseId, type, error.javaClass.simpleName) }
            Thread({ close() }, "warehouse-failure-cleanup").apply { isDaemon = true }.start()
        }

    private fun healthBody(): String = buildJsonObject {
        put("status", if (!ready.get()) "stopping" else if (destination.get() == DestinationState.UP) "ready" else "degraded")
        put("warehouseId", settings.warehouseId)
        put("destination", destination.get().name)
        put("received", counters.received.sum())
        put("invalid", counters.invalid.sum())
        put("admitted", counters.admitted.sum())
        put("dropped", counters.dropped.sum())
        put("retained", ingress.retained())
        put("highWater", ingress.highWaterMark())
        put("inFlight", counters.inFlight.get())
        put("attempts", counters.attempts.sum())
        put("delivered", counters.delivered.sum())
        put("retries", counters.retries.sum())
        put("expired", counters.expired.sum())
        put("failed", counters.failed.sum())
        put("abandoned", counters.abandoned.sum())
        put("heapUsedBytes", java.lang.management.ManagementFactory.getMemoryMXBean().heapMemoryUsage.used)
        put("threads", java.lang.management.ManagementFactory.getThreadMXBean().threadCount)
    }.toString()

    fun awaitTermination(): Boolean {
        system.whenTerminated.toCompletableFuture().join()
        return closing.get() && !failed.get()
    }

    override fun close() {
        if (!closing.compareAndSet(false, true)) return
        ready.set(false)
        receivers.forEach { it.close() }
        binding?.unbind()
        val drained = CompletableFuture<Unit>()
        system.tell(ForwarderCommand.Stop(drained))
        try {
            drained.get(settings.drainTimeout.toMillis() + 1000, TimeUnit.MILLISECONDS)
        } catch (_: Exception) {
            failed.set(true)
        } finally {
            system.terminate()
            try {
                system.whenTerminated.toCompletableFuture().get(5, TimeUnit.SECONDS)
            } catch (_: Exception) {
                failed.set(true)
            }
            status.execute { log.info("STOPPED warehouse={} {}", settings.warehouseId, counters.summary(ingress)) }
            status.shutdown()
        }
    }
}

fun startWarehouse(args: Array<String>) {
    if (args.contentEquals(arrayOf("--health-check"))) {
        exitProcess(try {
            val port = WarehouseSettings.from(ConfigFactory.load()).healthPort
            val connection = java.net.URI("http://127.0.0.1:$port/health").toURL().openConnection() as java.net.HttpURLConnection
            connection.connectTimeout = 1000
            connection.readTimeout = 1000
            try { if (connection.responseCode == 200) 0 else 1 } finally { connection.disconnect() }
        } catch (_: Exception) { 1 })
    }
    require(args.isEmpty()) { "Only --health-check is supported" }
    val service = try {
        val config = ConfigFactory.load()
        WarehouseService(WarehouseSettings.from(config), config)
    } catch (error: Exception) {
        val reason = if (error is IllegalArgumentException) error.message else error.javaClass.simpleName
        System.err.println("STARTUP_FAILED service=warehouse reason=$reason")
        exitProcess(1)
    }
    Runtime.getRuntime().addShutdownHook(Thread({ service.close() }, "warehouse-shutdown"))
    val normal = try { service.awaitTermination() } finally { service.close() }
    if (!normal) exitProcess(1)
}
