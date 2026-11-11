package dev.brahmkshatriya.echo.extension.tidal

import dev.brahmkshatriya.echo.extension.ExtensionText

import kotlinx.serialization.Serializable
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.ArrayDeque

@Serializable
data class StoredSession(
    val id: String,
    val secret: String,
    val expiresAt: String = "",
    val installId: String
) {
    fun isValid(): Boolean {
        return id.isNotEmpty() && id.length <= 4096 &&
            secret.isNotEmpty() && secret.length <= 4096 &&
            '\u0000' !in id && '\u0000' !in secret &&
            HEX_32_REGEX.matches(installId.trim())
    }

    fun expiresAtEpochMs(): Long? {
        val raw = expiresAt.trim()
        if (raw.isEmpty()) return null
        return runCatching { Instant.parse(raw).toEpochMilli() }.getOrNull()
    }

    companion object {
        val HEX_32_REGEX = Regex("^[a-f0-9]{32}$")
    }
}

@Serializable
data class PendingVerification(
    val state: String,
    val installId: String,
    val until: Long,
    val automatic: Boolean
)

data class ResolvedFlacStream(
    val url: String? = null,
    val dashManifestBase64: String? = null,
    val durationMs: Long = 0L,
    val bitDepth: Int? = null,
    val sampleRate: Int? = null,
    val resolvedAtMs: Long = System.currentTimeMillis()
) {
    val isDash: Boolean get() = !dashManifestBase64.isNullOrBlank() && url.isNullOrBlank()

    fun label(): String = buildString {
        append("Tidal · FLAC")
        if (bitDepth != null && bitDepth > 0) {
            append(" · ").append(bitDepth).append("-bit")
        }
        if (sampleRate != null && sampleRate > 0) {
            val khz = if (sampleRate % 1000 == 0) {
                (sampleRate / 1000).toString()
            } else {
                val v = sampleRate / 1000.0
                if (v == v.toLong().toDouble()) v.toLong().toString() else v.toString()
            }
            append(" · ").append(khz).append(" kHz")
        }
    }

    fun isFresh(nowMs: Long, maxAgeMs: Long = 240_000L): Boolean =
        nowMs - resolvedAtMs in 0..maxAgeMs
}

enum class VerificationStatus {
    COMPLETE,
    OPENED,
    DEFER,
    BUSY,
    RATE_LIMITED,
    AUTH_UNAVAILABLE,
    FAILED
}

class TidalGatewayException(val code: String) : Exception(code)

object TidalDiagnostics {
    private val TIME_FORMAT: DateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss")
    private val URL_SANITIZE = Regex("(?:https?|spotiflac)://\\S+", RegexOption.IGNORE_CASE)

    fun humanWait(ms: Long): String {
        if (ms <= 60_000L) {
            val seconds = maxOf(1L, (ms + 999L) / 1000L)
            return ExtensionText.get("providers.1_d_s_7c37b9", "%1\$d s", seconds)
        }
        val minutes = (ms + 59_999L) / 60_000L
        if (minutes < 60L) {
            return ExtensionText.get("providers.1_d_min_3122a2", "%1\$d min", minutes)
        }
        val hours = minutes / 60L
        val rest = minutes % 60L
        val hLabel = ExtensionText.get("providers.1_d_h_0c18fe", "%1\$d h", hours)
        return if (rest > 0L) {
            ExtensionText.get("providers.1_d_h_2_d_min_14b10f", "%1\$d h %2\$d min", hours, rest)
        } else {
            hLabel
        }
    }

    fun failureCategory(err: Throwable?): String {
        if (err is TidalGatewayException) {
            return when (err.code) {
                "NO_TICKET" -> ExtensionText.get("providers.ticket_not_received_a47522", "Ticket not received")
                "GATEWAY_UNAVAILABLE" -> ExtensionText.get("providers.server_response_unavailable_064370", "Server response unavailable")
                "TIME_BUDGET" -> "timeout"
                else -> ExtensionText.get("providers.unclassified_error_34a2d1", "Unclassified error")
            }
        }
        val className = err?.javaClass?.simpleName?.lowercase().orEmpty()
        val msg = err?.message?.trim()?.lowercase().orEmpty()
        return when {
            "sockettimeout" in className || "interruptedio" in className ||
                "timeout" in msg || "timed out" in msg || "time out" in msg -> "timeout"
            "unknownhost" in className || "unknownhost" in msg ||
                "unable to resolve host" in msg || "dns" in msg -> ExtensionText.get("providers.dns_unreachable_169546", "DNS unreachable")
            "connect" in className || "connect" in msg ||
                "network" in msg || "unreachable" in msg -> ExtensionText.get("providers.connection_unavailable_a05d49", "Connection unavailable")
            "no_ticket" in msg -> ExtensionText.get("providers.ticket_not_received_a47522", "Ticket not received")
            "gateway_unavailable" in msg -> ExtensionText.get("providers.server_response_unavailable_064370", "Server response unavailable")
            else -> ExtensionText.get("providers.unclassified_error_34a2d1", "Unclassified error")
        }
    }

    fun sanitizeMessage(message: String): String =
        message.replace(URL_SANITIZE, "[redatto]").trim()

    fun formatTime(epochMillis: Long): String =
        Instant.ofEpochMilli(epochMillis)
            .atZone(ZoneId.systemDefault())
            .format(TIME_FORMAT)
}

class DiagnosticLogBuffer(private val maxEntries: Int = 40) {
    private val entries = ArrayDeque<String>(maxEntries)

    @Synchronized
    fun append(message: String, nowMs: Long = System.currentTimeMillis()) {
        val clean = TidalDiagnostics.sanitizeMessage(message)
        if (clean.isEmpty()) return
        val line = "${TidalDiagnostics.formatTime(nowMs)} · $clean"
        if (entries.size >= maxEntries) {
            entries.removeFirst()
        }
        entries.addLast(line)
        println("[Tidal Extension] $clean")
    }

    @Synchronized
    fun snapshot(): List<String> = entries.toList().asReversed()

    @Synchronized
    fun clear() {
        entries.clear()
    }
}
