package dev.brahmkshatriya.echo.extension.bridge

import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.security.SecureRandom
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.concurrent.atomic.AtomicLong
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

object ZarzCrypto {
    private const val BASE64URL_CHARS =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
    private val HEX_REGEX = Regex("^[0-9a-fA-F]+$")
    private val ISO_MILLIS_UTC: DateTimeFormatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC)

    private val nonceCounter = AtomicLong(0L)
    private val secureRandom = SecureRandom()

    fun sha256Hex(input: String): String {
        val digest = MessageDigest.getInstance("SHA-256")
            .digest(input.toByteArray(StandardCharsets.UTF_8))
        return bytesToHex(digest)
    }

    fun hmacSha256Hex(key: String, message: String): String {
        val mac = Mac.getInstance("HmacSHA256")
        val keyBytes = key.toByteArray(StandardCharsets.UTF_8)
        mac.init(SecretKeySpec(keyBytes, "HmacSHA256"))
        val out = mac.doFinal(message.toByteArray(StandardCharsets.UTF_8))
        return bytesToHex(out)
    }

    fun hexToBase64Url(hex: String): String {
        if (!HEX_REGEX.matches(hex) || hex.length % 2 != 0) {
            throw IllegalArgumentException("BAD_MAC")
        }
        val out = StringBuilder((hex.length * 2) / 3 + 4)
        var i = 0
        while (i < hex.length) {
            val a = hex.substring(i, i + 2).toInt(16)
            val hasB = i + 2 < hex.length
            val hasC = i + 4 < hex.length
            val b = if (hasB) hex.substring(i + 2, i + 4).toInt(16) else 0
            val c = if (hasC) hex.substring(i + 4, i + 6).toInt(16) else 0
            out.append(BASE64URL_CHARS[a shr 2])
            out.append(BASE64URL_CHARS[((a and 3) shl 4) or (b shr 4)])
            if (hasB) out.append(BASE64URL_CHARS[((b and 15) shl 2) or (c shr 6)])
            if (hasC) out.append(BASE64URL_CHARS[c and 63])
            i += 6
        }
        return out.toString()
    }

    fun formatIsoTimestamp(nowMs: Long): String =
        ISO_MILLIS_UTC.format(Instant.ofEpochMilli(nowMs))

    fun generateInstallId(nowMs: Long = System.currentTimeMillis()): String =
        sha256Hex("$nowMs:${secureRandom.nextDouble()}:${secureRandom.nextDouble()}").take(32)

    fun generateState(serverNonce: String, installId: String, nowMs: Long = System.currentTimeMillis()): String =
        sha256Hex("$serverNonce:$installId:${secureRandom.nextDouble()}:$nowMs")

    /**
     * Builds ZARZ-HMAC-V1 signed headers identical to FLAC Bridge Multi (main.js).
     * Signs escaped path `/v2<path>` and an empty query line even for GET requests.
     */
    fun signedHeaders(
        provider: ProviderSpec,
        session: StoredSession,
        method: String,
        path: String,
        body: String,
        nowMs: Long = System.currentTimeMillis(),
        explicitNonce: String? = null
    ): Map<String, String> {
        val timestamp = formatIsoTimestamp(nowMs)
        val nonce = explicitNonce ?: sha256Hex(
            "${session.secret}:$nowMs:${nonceCounter.incrementAndGet()}:${secureRandom.nextDouble()}"
        ).take(24)
        val digest = sha256Hex(body)
        val windowNumber = Math.floorDiv(nowMs, 300_000L)
        val rolling = hexToBase64Url(hmacSha256Hex(session.secret, "$windowNumber:${session.id}"))
        val canonical = listOf(
            "ZARZ-HMAC-V1",
            method,
            "/v2$path",
            "",
            digest,
            timestamp,
            nonce,
            session.id,
            provider.app,
            "extension"
        ).joinToString("\n")
        val signature = hexToBase64Url(hmacSha256Hex(rolling, canonical))
        return linkedMapOf(
            "User-Agent" to ZarzSessionManager.MODERN_CHROME_UA,
            "Content-Type" to "application/json",
            "Accept" to "application/json",
            "X-Zarz-Session" to session.id,
            "X-Zarz-Timestamp" to timestamp,
            "X-Zarz-Nonce" to nonce,
            "X-Zarz-Body-SHA256" to digest,
            "X-Zarz-Signature" to signature,
            "X-Zarz-App-Version" to provider.app,
            "X-Zarz-Platform" to "extension"
        )
    }

    /**
     * Parses `grant`, `code`, `state`, and nested `cb` values from `spotiflac://session-grant`
     * query or fragment strings (max recursion depth 2), matching `queryValues` in main.js.
     */
    fun parseCallbackValues(url: String?, depth: Int = 0): Map<String, String> {
        if (depth > 2 || url.isNullOrBlank()) return emptyMap()
        val qIdx = url.indexOf('?')
        val hIdx = url.indexOf('#')
        val first = when {
            qIdx >= 0 -> qIdx
            hIdx >= 0 -> hIdx
            else -> return emptyMap()
        }
        val tail = url.substring(first + 1).replace('#', '&')
        val out = linkedMapOf<String, String>()
        for (part in tail.split('&')) {
            val eq = part.indexOf('=')
            if (eq < 0) continue
            runCatching {
                val k = URLDecoder.decode(part.substring(0, eq), StandardCharsets.UTF_8)
                val v = URLDecoder.decode(part.substring(eq + 1), StandardCharsets.UTF_8)
                if ((k == "grant" || k == "code" || k == "state" || k == "cb") && !out.containsKey(k)) {
                    out[k] = v
                }
            }
        }
        if (!out.containsKey("grant") && !out.containsKey("code") && out.containsKey("cb")) {
            val nested = parseCallbackValues(out["cb"], depth + 1)
            if (nested.containsKey("grant") || nested.containsKey("code")) {
                return nested
            }
        }
        return out
    }

    private fun bytesToHex(bytes: ByteArray): String {
        val chars = CharArray(bytes.size * 2)
        val hexDigits = "0123456789abcdef"
        for (i in bytes.indices) {
            val v = bytes[i].toInt() and 0xFF
            chars[i * 2] = hexDigits[v ushr 4]
            chars[i * 2 + 1] = hexDigits[v and 0x0F]
        }
        return String(chars)
    }
}
