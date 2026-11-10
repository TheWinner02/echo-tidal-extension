package dev.brahmkshatriya.echo.extension.bridge

import kotlinx.serialization.Serializable
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.ArrayDeque

@Serializable
enum class ProviderKey(val wireKey: String) {
    TIDAL("tidal"),
    QOBUZ("qobuz"),
    DEEZER("deezer"),
    AMAZON("amazon");

    companion object {
        fun fromKey(key: String?): ProviderKey? =
            entries.firstOrNull { it.wireKey.equals(key?.trim(), ignoreCase = true) }
    }
}

data class ProviderSpec(
    val providerKey: ProviderKey,
    val key: String,
    val name: String,
    val app: String,
    val storage: String,
    val ticketAlias: String,
    val dlPath: String,
    val experimental: Boolean = false
) {
    companion object {
        val TIDAL = ProviderSpec(
            providerKey = ProviderKey.TIDAL,
            key = "tidal",
            name = "Tidal",
            app = "tidal-web@1.2.5",
            storage = "tidal_session",
            ticketAlias = "tid",
            dlPath = "/dl/tid"
        )
        val QOBUZ = ProviderSpec(
            providerKey = ProviderKey.QOBUZ,
            key = "qobuz",
            name = "Qobuz",
            app = "qobuz-web@1.2.21",
            storage = "qobuz_session",
            ticketAlias = "qbz",
            dlPath = "/dl/qbz"
        )
        val DEEZER = ProviderSpec(
            providerKey = ProviderKey.DEEZER,
            key = "deezer",
            name = "Deezer",
            app = "deezer@1.3.5",
            storage = "deezer_session",
            ticketAlias = "dzr",
            dlPath = "/dl/dzr",
            experimental = true
        )
        val AMAZON = ProviderSpec(
            providerKey = ProviderKey.AMAZON,
            key = "amazon",
            name = "Amazon Music",
            app = "amzn@2.3.12",
            storage = "amazon_session",
            ticketAlias = "amazeamazeamaze",
            dlPath = "/dl/amazeamazeamaze",
            experimental = true
        )

        val ALL: List<ProviderSpec> = listOf(TIDAL, QOBUZ, DEEZER, AMAZON)

        fun byKey(key: String?): ProviderSpec? =
            ALL.firstOrNull { it.key.equals(key?.trim(), ignoreCase = true) }

        fun byName(name: String?): ProviderSpec? =
            ALL.firstOrNull { it.name.equals(name?.trim(), ignoreCase = true) }
    }
}

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
    val provider: String,
    val state: String,
    val installId: String,
    val until: Long,
    val automatic: Boolean
)

@Serializable
data class TrackMatchSpec(
    val id: String,
    val title: String,
    val artist: String = "",
    val artists: List<String> = emptyList(),
    val durationMs: Long = 0L,
    val isrc: String = "",
    val spotifyId: String = "",
    val youtubeId: String = "",
    val preferredProviderKey: String = "",
    val providerTrackId: String = ""
) {
    fun cacheKey(): String {
        val normIsrc = isrc.trim().uppercase()
        return if (normIsrc.length == 12) "isrc:$normIsrc"
        else "meta:${title.trim().lowercase()}|${primaryArtist().lowercase()}|$durationMs|$spotifyId"
    }

    fun primaryArtist(): String =
        artist.trim().ifEmpty { artists.firstOrNull()?.trim().orEmpty() }
}

data class CatalogueHit(
    val id: String,
    val durationMs: Long,
    val isrcMatched: Boolean,
    val differenceMs: Long = 0L,
    val lossless: Boolean = false,
    val title: String = "",
    val artist: String = "",
    val artists: List<String> = emptyList(),
    val albumTitle: String = "",
    val albumId: String = "",
    val coverUrl: String = "",
    val isrc: String = "",
    val isExplicit: Boolean = false,
    val providerKey: String = ""
)

data class ResolvedFlacStream(
    val providerKey: String,
    val providerName: String,
    val url: String? = null,
    val dashManifestBase64: String? = null,
    val durationMs: Long = 0L,
    val isrcMatched: Boolean = false,
    val bitDepth: Int? = null,
    val sampleRate: Int? = null,
    val resolvedAtMs: Long = System.currentTimeMillis()
) {
    val isDash: Boolean get() = !dashManifestBase64.isNullOrBlank() && url.isNullOrBlank()

    fun label(): String = buildString {
        append(providerName)
        append(" · FLAC")
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

class BridgeGatewayException(val code: String) : Exception(code)

object FlacBridgeDiagnostics {
    private val TIME_FORMAT: DateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss")
    private val URL_SANITIZE = Regex("(?:https?|spotiflac)://\\S+", RegexOption.IGNORE_CASE)

    fun humanWait(ms: Long): String {
        if (ms <= 60_000L) {
            val seconds = maxOf(1L, (ms + 999L) / 1000L)
            return "$seconds ${if (seconds == 1L) "secondo" else "secondi"}"
        }
        val minutes = (ms + 59_999L) / 60_000L
        if (minutes < 60L) {
            return "$minutes ${if (minutes == 1L) "minuto" else "minuti"}"
        }
        val hours = minutes / 60L
        val rest = minutes % 60L
        val hLabel = "$hours ${if (hours == 1L) "ora" else "ore"}"
        return if (rest > 0L) {
            "$hLabel e $rest ${if (rest == 1L) "minuto" else "minuti"}"
        } else {
            hLabel
        }
    }

    fun failureCategory(err: Throwable?): String {
        if (err is BridgeGatewayException) {
            return when (err.code) {
                "NO_TICKET" -> "ticket non ricevuto"
                "GATEWAY_UNAVAILABLE" -> "risposta del server non disponibile"
                "TIME_BUDGET" -> "timeout"
                else -> "errore non classificato"
            }
        }
        val className = err?.javaClass?.simpleName?.lowercase().orEmpty()
        val msg = err?.message?.trim()?.lowercase().orEmpty()
        return when {
            "sockettimeout" in className || "interruptedio" in className ||
                "timeout" in msg || "timed out" in msg || "time out" in msg -> "timeout"
            "data saver" in msg -> "risparmio dati attivo"
            "unknownhost" in className || "unknownhost" in msg ||
                "unable to resolve host" in msg || "dns" in msg -> "DNS non raggiungibile"
            "connect" in className || "connect" in msg ||
                "network" in msg || "unreachable" in msg -> "connessione non disponibile"
            "no_ticket" in msg -> "ticket non ricevuto"
            "gateway_unavailable" in msg -> "risposta del server non disponibile"
            else -> "errore non classificato"
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
        val clean = FlacBridgeDiagnostics.sanitizeMessage(message)
        if (clean.isEmpty()) return
        val line = "${FlacBridgeDiagnostics.formatTime(nowMs)} · $clean"
        if (entries.size >= maxEntries) {
            entries.removeFirst()
        }
        entries.addLast(line)
        println("[FLAC Bridge] $clean")
    }

    @Synchronized
    fun snapshot(): List<String> = entries.toList().asReversed()

    @Synchronized
    fun clear() {
        entries.clear()
    }
}
