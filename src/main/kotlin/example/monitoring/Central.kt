package example.monitoring

import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.javadsl.Behaviors
import akka.http.javadsl.Http
import akka.http.javadsl.model.ContentTypes
import akka.http.javadsl.model.HttpEntities
import akka.http.javadsl.model.HttpMethods
import akka.http.javadsl.model.HttpRequest
import akka.http.javadsl.model.HttpResponse
import akka.http.javadsl.model.MediaTypes
import akka.http.javadsl.model.StatusCodes
import akka.http.javadsl.ServerBinding
import akka.http.scaladsl.model.EntityStreamSizeException
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import java.time.Duration
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.LongAdder
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.CompletionStage
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.Semaphore
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlin.system.exitProcess
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import org.slf4j.LoggerFactory
import org.slf4j.Logger

data class MonitoringSettings(
    val bindAddress: String = "127.0.0.1",
    val port: Int = 8080,
    val temperatureThreshold: Double = 35.0,
    val humidityThreshold: Double = 50.0,
    val capacity: Int = 64,
    val bodyLimit: Long = 2048,
    val requestTimeout: Duration = Duration.ofSeconds(2),
    val drainTimeout: Duration = Duration.ofSeconds(10),
) {
    init {
        require(bindAddress.isNotBlank() && port in 0..65535) { "Invalid HTTP bind address/port" }
        require(temperatureThreshold.isFinite() && humidityThreshold.isFinite()) { "Thresholds must be finite" }
        require(capacity in 1..100_000 && bodyLimit in 1..65_536) { "Invalid admission/body limit" }
        require(requestTimeout.toMillis() in 1..60_000 && drainTimeout.toMillis() in 1..60_000) { "Invalid timeout" }
    }

    companion object {
        fun from(config: Config): MonitoringSettings {
            val c = config.getConfig("central")
            return MonitoringSettings(
                c.getString("bind-address"), c.getInt("port"), c.getDouble("temperature-threshold"),
                c.getDouble("humidity-threshold"), c.getInt("capacity"), c.getLong("body-limit"),
                c.getDuration("request-timeout"), c.getDuration("drain-timeout"),
            )
        }
    }
}

data class MonitoringDecision(val reading: ReadingEnvelope, val threshold: Double, val alarm: Boolean) {
    val unit: String get() = if (reading.measurementType == MeasurementType.TEMPERATURE) "C" else "percent"

    fun logMessage(): String =
        "${if (alarm) "ALARM" else "NORMAL"} warehouse=${reading.warehouseId} sensor=${reading.sensorId} " +
            "type=${reading.measurementType} value=${reading.value} unit=$unit threshold=$threshold " +
            "eventId=${reading.eventId} receivedAt=${reading.receivedAt}"
}

class ThresholdPolicy(private val temperature: Double = 35.0, private val humidity: Double = 50.0) {
    init { require(temperature.isFinite() && humidity.isFinite()) }

    fun evaluate(reading: ReadingEnvelope): MonitoringDecision {
        require(reading.value.isFinite())
        val threshold = when (reading.measurementType) {
            MeasurementType.TEMPERATURE -> temperature
            MeasurementType.HUMIDITY -> humidity
        }
        return MonitoringDecision(reading, threshold, reading.value > threshold)
    }
}

fun interface AlarmSink {
    fun emit(decision: MonitoringDecision): CompletionStage<Unit>
}

class ConsoleAlarmSink(capacity: Int) : AlarmSink {
    private val alarmLog = LoggerFactory.getLogger("monitoring.alarm")
    private val normalLog = LoggerFactory.getLogger("monitoring.reading")
    private val executor = ThreadPoolExecutor(
        1, 1, 0, TimeUnit.MILLISECONDS, ArrayBlockingQueue(capacity),
        { task -> Thread(task, "console-output").apply { isDaemon = true } },
        ThreadPoolExecutor.AbortPolicy(),
    )

    override fun emit(decision: MonitoringDecision): CompletionStage<Unit> {
        val result = CompletableFuture<Unit>()
        try {
            executor.execute {
                try {
                    if (decision.alarm) alarmLog.warn(decision.logMessage())
                    else normalLog.debug(decision.logMessage())
                    result.complete(Unit)
                } catch (error: Exception) {
                    result.completeExceptionally(error)
                }
            }
        } catch (error: RejectedExecutionException) {
            result.completeExceptionally(error)
        }
        return result
    }

    fun close(timeout: Duration) {
        executor.shutdown()
        if (!executor.awaitTermination(timeout.toMillis().coerceAtLeast(0), TimeUnit.MILLISECONDS)) {
            executor.shutdownNow()
        }
    }
}

class MonitoringCounters {
    val received = LongAdder()
    val invalid = LongAdder()
    val rejected = LongAdder()
    val normal = LongAdder()
    val alarms = LongAdder()
    val failed = LongAdder()
}

sealed interface MonitoringCommand {
    data class Process(val reading: ReadingEnvelope, val result: CompletableFuture<Boolean>) : MonitoringCommand
    data class Finished(val decision: MonitoringDecision, val result: CompletableFuture<Boolean>, val error: Throwable?) : MonitoringCommand
}

object MonitoringActor {
    fun create(policy: ThresholdPolicy, sink: AlarmSink, counters: MonitoringCounters): Behavior<MonitoringCommand> =
        Behaviors.setup { context ->
            Behaviors.receive(MonitoringCommand::class.java)
                .onMessage(MonitoringCommand.Process::class.java) { message ->
                    val decision = policy.evaluate(message.reading)
                    val output = try {
                        sink.emit(decision)
                    } catch (error: Exception) {
                        CompletableFuture.failedFuture(error)
                    }
                    context.pipeToSelf(output) { _, error -> MonitoringCommand.Finished(decision, message.result, error) }
                    Behaviors.same()
                }
                .onMessage(MonitoringCommand.Finished::class.java) { message ->
                    if (message.error == null) {
                        if (message.decision.alarm) counters.alarms.increment() else counters.normal.increment()
                        message.result.complete(true)
                    } else {
                        counters.failed.increment()
                        message.result.complete(false)
                    }
                    Behaviors.same()
                }
                .build()
        }
}

class CentralService(
    private val settings: MonitoringSettings,
    config: Config = ConfigFactory.load(),
    suppliedSink: AlarmSink? = null,
) : AutoCloseable {
    val counters = MonitoringCounters()
    private val closing = AtomicBoolean()
    private val ready = AtomicBoolean()
    private val permits = Semaphore(settings.capacity)
    private val admissionLock = Any()
    private val pending = ConcurrentHashMap.newKeySet<CompletableFuture<HttpResponse>>()
    private val ownedSink = if (suppliedSink == null) ConsoleAlarmSink(settings.capacity) else null
    private val sink = suppliedSink ?: checkNotNull(ownedSink)
    private val runtimeConfig = ConfigFactory.parseMap(
        mapOf(
            "akka.http.server.request-timeout" to "${settings.requestTimeout.toMillis()}ms",
            "akka.http.server.parsing.max-content-length" to settings.bodyLimit,
            "akka.http.server.max-connections" to settings.capacity + 16,
            "akka.http.server.pipelining-limit" to 1,
            "akka.http.server.idle-timeout" to "5s",
        ),
    ).withFallback(config)
    private val system = ActorSystem.create(
        MonitoringActor.create(ThresholdPolicy(settings.temperatureThreshold, settings.humidityThreshold), sink, counters),
        "central-monitoring", runtimeConfig,
    )
    private val statusLog = LoggerFactory.getLogger("monitoring.central")
    private val status = Executors.newSingleThreadScheduledExecutor { task ->
        Thread(task, "central-status").apply { isDaemon = true }
    }
    private var binding: ServerBinding? = null
    val localPort: Int get() = checkNotNull(binding).localAddress().port
    val activeRequests: Int get() = settings.capacity - permits.availablePermits()

    init {
        try {
            binding = Http.get(system).newServerAt(settings.bindAddress, settings.port)
                .bind(::handle).toCompletableFuture().get(10, TimeUnit.SECONDS)
            ready.set(true)
            statusLog.info("READY service=central port={} temperatureThreshold={} humidityThreshold={}", localPort, settings.temperatureThreshold, settings.humidityThreshold)
            status.scheduleWithFixedDelay({ statusLog.info("STATUS {}", healthBody()) }, 5, 5, TimeUnit.SECONDS)
        } catch (error: Exception) {
            close()
            throw error
        }
    }

    private fun handle(request: HttpRequest): CompletionStage<HttpResponse> {
        val path = request.getUri().path()
        if (path == "/health" && request.method() == HttpMethods.GET) {
            request.discardEntityBytes(system)
            return completed(response(if (ready.get()) 200 else 503, healthBody()))
        }
        if (path != "/readings") return reject(request, 404, "not_found")
        if (request.method() != HttpMethods.POST) return reject(request, 405, "method_not_allowed")
        counters.received.increment()
        if (!ready.get()) return reject(request, 503, "stopping")
        if (request.entity().getContentType().mediaType() != MediaTypes.APPLICATION_JSON) {
            counters.invalid.increment()
            return reject(request, 415, "unsupported_media_type")
        }
        val result = CompletableFuture<HttpResponse>()
        synchronized(admissionLock) {
            if (!ready.get()) return reject(request, 503, "stopping")
            if (!permits.tryAcquire()) {
                counters.rejected.increment()
                return reject(request, 429, "capacity_exhausted")
            }
            pending.add(result)
        }
        result.whenComplete { _, _ ->
            pending.remove(result)
            permits.release()
        }
        request.entity().withSizeLimit(settings.bodyLimit).toStrict(settings.requestTimeout.toMillis(), system)
            .whenComplete { entity, error ->
                if (error != null) {
                    counters.invalid.increment()
                    var cause = error
                    while (cause is CompletionException && cause.cause != null) cause = cause.cause!!
                    result.complete(response(if (cause is EntityStreamSizeException) 413 else 400, "{\"error\":\"invalid_body\"}"))
                } else if (!result.isDone) {
                    val reading = try {
                        ReadingCodec.decode(entity.getData().utf8String())
                    } catch (_: IllegalArgumentException) {
                        null
                    }
                    if (reading == null || EnvelopeValidation.error(reading) != null) {
                        counters.invalid.increment()
                        result.complete(response(400, "{\"error\":\"invalid_envelope\"}"))
                    } else {
                        val processed = CompletableFuture<Boolean>()
                        processed.whenComplete { success, failure ->
                            result.complete(
                                if (failure == null && success == true) HttpResponse.create().withStatus(StatusCodes.NO_CONTENT)
                                else response(500, "{\"error\":\"processing_failed\"}"),
                            )
                        }
                        system.tell(MonitoringCommand.Process(reading, processed))
                    }
                }
            }
        return result
    }

    private fun reject(request: HttpRequest, code: Int, reason: String): CompletionStage<HttpResponse> {
        request.discardEntityBytes(system)
        return completed(response(code, buildJsonObject { put("error", reason) }.toString()))
    }

    private fun response(code: Int, body: String): HttpResponse = HttpResponse.create().withStatus(code)
        .withEntity(HttpEntities.create(ContentTypes.APPLICATION_JSON, body))

    private fun completed(response: HttpResponse): CompletionStage<HttpResponse> = CompletableFuture.completedFuture(response)

    private fun healthBody(): String = buildJsonObject {
        put("status", if (ready.get()) "ready" else "stopping")
        put("active", activeRequests)
        put("received", counters.received.sum())
        put("invalid", counters.invalid.sum())
        put("rejected", counters.rejected.sum())
        put("normal", counters.normal.sum())
        put("alarmInvocations", counters.alarms.sum())
        put("failed", counters.failed.sum())
        put("heapUsedBytes", java.lang.management.ManagementFactory.getMemoryMXBean().heapMemoryUsage.used)
        put("threads", java.lang.management.ManagementFactory.getThreadMXBean().threadCount)
    }.toString()

    fun awaitTermination(): Boolean {
        system.whenTerminated.toCompletableFuture().join()
        return closing.get()
    }

    override fun close() {
        synchronized(admissionLock) {
            if (!closing.compareAndSet(false, true)) return
            ready.set(false)
        }
        val deadline = System.nanoTime() + settings.drainTimeout.toNanos()
        fun remaining(): Long = (deadline - System.nanoTime()).coerceAtLeast(0)
        try {
            binding?.terminate(Duration.ofNanos(remaining()))?.toCompletableFuture()
                ?.get(remaining().coerceAtLeast(1), TimeUnit.NANOSECONDS)
        } catch (_: Exception) {
            // Shutdown must continue even when a client or output sink stalls.
        } finally {
            pending.forEach { it.complete(response(503, "{\"error\":\"shutdown_deadline\"}")) }
            ownedSink?.close(Duration.ofNanos(remaining()))
            system.terminate()
            try {
                system.whenTerminated.toCompletableFuture().get(5, TimeUnit.SECONDS)
            } catch (_: Exception) {
                // The outer process test verifies that all owned resources terminate.
            }
            status.execute { statusLog.info("STOPPED {}", healthBody()) }
            status.shutdown()
        }
    }
}

fun startCentral(args: Array<String>) {
    if (args.contentEquals(arrayOf("--health-check"))) {
        exitProcess(try {
            val port = MonitoringSettings.from(ConfigFactory.load()).port
            val connection = java.net.URI("http://127.0.0.1:$port/health").toURL().openConnection() as java.net.HttpURLConnection
            connection.connectTimeout = 1000
            connection.readTimeout = 1000
            try { if (connection.responseCode == 200) 0 else 1 } finally { connection.disconnect() }
        } catch (_: Exception) { 1 })
    }
    require(args.isEmpty()) { "Only --health-check is supported" }
    val service = try {
        val config = ConfigFactory.load()
        CentralService(MonitoringSettings.from(config), config)
    } catch (error: Exception) {
        val reason = if (error is IllegalArgumentException) error.message else error.javaClass.simpleName
        System.err.println("STARTUP_FAILED service=central reason=$reason")
        exitProcess(1)
    }
    Runtime.getRuntime().addShutdownHook(Thread({ service.close() }, "central-shutdown"))
    val normal = try { service.awaitTermination() } finally { service.close() }
    if (!normal) exitProcess(1)
}
