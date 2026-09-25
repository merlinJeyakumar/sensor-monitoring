package example.monitoring

import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetAddress
import java.net.URI
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout

@Timeout(30)
class SensorMonitoringTest {
    @Test fun `parse the two assignment payloads`() {
        assertEquals(ParseResult.Accepted(SensorReading("t1", 30.0)), parse("sensor_id=t1; value=30"))
        assertEquals(ParseResult.Accepted(SensorReading("h1", 40.0)), parse("sensor_id=h1; value=40"))
    }

    @Test fun `reject missing or invalid sensor values`() {
        for (payload in listOf("sensor_id=t1", "sensor_id=t1; value=NaN", "sensor_id=t1; value=bad")) {
            assertInstanceOf(ParseResult.Rejected::class.java, parse(payload), payload)
        }
    }

    @Test fun `alarm only when temperature or humidity exceeds its threshold`() {
        val policy = ThresholdPolicy()
        for ((type, threshold) in listOf(MeasurementType.TEMPERATURE to 35.0, MeasurementType.HUMIDITY to 50.0)) {
            for (value in listOf(threshold - 1, threshold, threshold + 1)) {
                val reading = ReadingEnvelope(1, UUID.randomUUID().toString(), "warehouse-a", "sensor-1", type, value, Instant.now().toString())
                assertEquals(value > threshold, policy.evaluate(reading).alarm, "$type at $value")
            }
        }
    }

    @Test fun `UDP readings reach central over HTTP with normal and alarm outcomes`() {
        val outcomes = LinkedBlockingQueue<MonitoringDecision>()
        CentralService(MonitoringSettings(port = 0), suppliedSink = AlarmSink {
            outcomes.add(it)
            CompletableFuture.completedFuture(Unit)
        }).use { central ->
            WarehouseService(WarehouseSettings(
                "warehouse-a", temperaturePort = 0, humidityPort = 0, healthPort = 0,
                centralEndpoint = URI("http://127.0.0.1:${central.localPort}/readings"),
            )).use { warehouse ->
                send(warehouse.temperaturePort, "sensor_id=t1; value=30")
                send(warehouse.humidityPort, "sensor_id=h1; value=51")
                val received = List(2) {
                    outcomes.poll(5, TimeUnit.SECONDS) ?: throw AssertionError("Reading did not reach central")
                }.associateBy { it.reading.sensorId }
                val normal = received.getValue("t1")
                val alarm = received.getValue("h1")
                assertFalse(normal.alarm)
                assertEquals(MeasurementType.TEMPERATURE, normal.reading.measurementType)
                assertTrue(alarm.alarm)
                assertEquals(MeasurementType.HUMIDITY, alarm.reading.measurementType)
                assertEquals("warehouse-a", alarm.reading.warehouseId)
                assertTrue(alarm.logMessage().startsWith("ALARM warehouse=warehouse-a sensor=h1"))
            }
        }
    }

    private fun parse(payload: String) = SensorPayloadParser.parse(payload.toByteArray(Charsets.UTF_8))

    private fun send(port: Int, payload: String) {
        val bytes = payload.toByteArray(Charsets.UTF_8)
        DatagramSocket().use { it.send(DatagramPacket(bytes, bytes.size, InetAddress.getLoopbackAddress(), port)) }
    }
}
