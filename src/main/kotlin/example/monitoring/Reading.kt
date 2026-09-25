package example.monitoring

import java.time.Instant
import java.util.UUID
import kotlinx.serialization.json.Json
import kotlinx.serialization.Serializable

@Serializable
enum class MeasurementType { TEMPERATURE, HUMIDITY }

@Serializable
data class ReadingEnvelope(
    val schemaVersion: Int,
    val eventId: String,
    val warehouseId: String,
    val sensorId: String,
    val measurementType: MeasurementType,
    val value: Double,
    val receivedAt: String,
)

object ReadingCodec {
    private val json = Json {
        ignoreUnknownKeys = false
        isLenient = false
        allowSpecialFloatingPointValues = false
        encodeDefaults = true
    }

    fun encode(reading: ReadingEnvelope): String = json.encodeToString(reading)
    fun decode(body: String): ReadingEnvelope = json.decodeFromString(body)
}

object EnvelopeValidation {
    private val identifier = Regex("[A-Za-z0-9_.-]{1,64}")

    fun validIdentifier(value: String): Boolean = identifier.matches(value)

    fun error(reading: ReadingEnvelope): String? = when {
        reading.schemaVersion != 1 -> "unsupported_schema_version"
        !validIdentifier(reading.warehouseId) -> "invalid_warehouse_id"
        !validIdentifier(reading.sensorId) -> "invalid_sensor_id"
        !reading.value.isFinite() -> "non_finite_value"
        !validUuid(reading.eventId) -> "invalid_event_id"
        !validTimestamp(reading.receivedAt) -> "invalid_received_at"
        else -> null
    }

    private fun validUuid(value: String): Boolean = try {
        value.length == 36 && UUID.fromString(value).toString().equals(value, ignoreCase = true)
    } catch (_: IllegalArgumentException) {
        false
    }

    private fun validTimestamp(value: String): Boolean = try {
        value.length <= 40 && value.endsWith("Z") && Instant.parse(value).toString().isNotEmpty()
    } catch (_: java.time.format.DateTimeParseException) {
        false
    }
}
