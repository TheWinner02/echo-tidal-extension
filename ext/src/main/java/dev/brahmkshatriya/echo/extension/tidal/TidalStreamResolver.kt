package dev.brahmkshatriya.echo.extension.tidal

import dev.brahmkshatriya.echo.common.helpers.ContinuationCallback.Companion.await
import dev.brahmkshatriya.echo.common.models.Streamable
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import okhttp3.OkHttpClient
import okhttp3.Request
import java.io.ByteArrayInputStream
import java.io.IOException
import java.io.InputStream
import java.io.SequenceInputStream
import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.Collections
import java.util.Locale
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

class TidalStreamResolver(
    baseHttpClient: OkHttpClient,
    private val sessionManager: TidalSessionManager,
    private val clock: () -> Long = System::currentTimeMillis
) {
    private val httpClient = sessionManager.httpClient

    companion object {
        private val MPD_DRM_REGEX = Regex("<ContentProtection\\b|<cenc:pssh\\b|cenc:default_KID", RegexOption.IGNORE_CASE)
        private val MPD_CODECS_FLAC_REGEX = Regex("codecs=\"flac\"", RegexOption.IGNORE_CASE)
        private val MPD_TIMELINE_REGEX = Regex("<SegmentTimeline\\b", RegexOption.IGNORE_CASE)
        private val MPD_SEGMENT_D_REGEX = Regex("<S\\b[^>]*\\bd=\"\\d+\"", RegexOption.IGNORE_CASE)
        private val MPD_REP_REGEX = Regex("<Representation\\b", RegexOption.IGNORE_CASE)
        private val MPD_ADAPT_REGEX = Regex("<AdaptationSet\\b", RegexOption.IGNORE_CASE)
        private val MPD_ANY_CODEC_REGEX = Regex("\\bcodecs\\s*=\\s*\"([^\"]+)\"", RegexOption.IGNORE_CASE)
        private val MPD_INIT_REGEX = Regex("\\binitialization=\"([^\"]+)\"", RegexOption.IGNORE_CASE)
        private val MPD_MEDIA_REGEX = Regex("\\bmedia=\"([^\"]+)\"", RegexOption.IGNORE_CASE)
        private val MPD_START_NUM_REGEX = Regex("\\bstartNumber=\"(\\d+)\"", RegexOption.IGNORE_CASE)
        private val MPD_S_TAG_REGEX = Regex("<S\\b([^>]*)/?>", RegexOption.IGNORE_CASE)
        private val MPD_R_ATTR_REGEX = Regex("\\br=\"(-?\\d+)\"", RegexOption.IGNORE_CASE)

        fun isTrustedTidalUrl(url: String): Boolean {
            val trimmed = url.trim()
            if (!trimmed.startsWith("https://", ignoreCase = true)) return false
            val host = runCatching { java.net.URI(trimmed).host?.lowercase(Locale.ROOT) }.getOrNull() ?: return false
            return host == "audio.tidal.com" ||
                host.endsWith(".audio.tidal.com") ||
                host.endsWith(".sp-cdn.com") ||
                host.endsWith(".tidal.com")
        }
    }

    private val streamCache = ConcurrentHashMap<String, ResolvedFlacStream>()

    fun getCachedStream(trackId: String): ResolvedFlacStream? {
        val cached = streamCache[trackId] ?: return null
        return if (cached.isFresh(clock())) cached else {
            streamCache.remove(trackId)
            null
        }
    }

    fun putCachedStream(trackId: String, stream: ResolvedFlacStream) {
        streamCache[trackId] = stream
        if (streamCache.size > 128) {
            val now = clock()
            streamCache.entries.removeIf { !it.value.isFresh(now) }
        }
    }

    suspend fun resolveStream(
        rawTrackId: String,
        deadlineMs: Long = clock() + 15_000L
    ): ResolvedFlacStream? = withContext(Dispatchers.IO) {
        val trackId = TidalConverter.extractNumericId(rawTrackId)
        if (trackId.isEmpty()) return@withContext null

        getCachedStream(trackId)?.let { return@withContext it }

        val session = sessionManager.activeSession(deadlineMs)
            ?: throw TidalGatewayException("AUTH_REQUIRED")

        val ticket = sessionManager.requestTicket(trackId, deadlineMs)
        val payload = JsonObject(
            mapOf(
                "id" to JsonPrimitive(trackId),
                "quality" to JsonPrimitive("LOSSLESS")
            )
        )
        val resp = sessionManager.signedRequest(
            method = "POST",
            path = TidalSessionManager.TIDAL_DL_PATH,
            payload = payload,
            extraHeaders = mapOf("X-Zarz-Ticket" to ticket),
            deadlineMs = deadlineMs
        )
        val data = resp["data"] as? JsonObject
        val decoded = decodeTidalData(data) ?: return@withContext null

        val rawBits = data?.get("bitDepth")?.jsonPrimitive?.doubleOrNull
        val rawSampleRate = data?.get("sampleRate")?.jsonPrimitive?.doubleOrNull

        val bdInt = rawBits?.toInt()?.takeIf { it.toDouble() == rawBits && it in 8..32 }
        var srDouble = rawSampleRate ?: 0.0
        if (srDouble > 0.0 && srDouble < 1000.0) {
            srDouble = kotlin.math.round(srDouble * 1000.0)
        }
        val srInt = srDouble.toInt().takeIf { it.toDouble() == srDouble && it in 8000..768_000 }

        val resolved = ResolvedFlacStream(
            url = decoded.first,
            dashManifestBase64 = decoded.second,
            durationMs = 0L,
            bitDepth = bdInt,
            sampleRate = srInt,
            resolvedAtMs = clock()
        )
        putCachedStream(trackId, resolved)
        sessionManager.log("Tidal: stream FLAC risolto (${resolved.label()}).")
        resolved
    }

    internal fun decodeTidalData(data: JsonObject?): Pair<String?, String?>? {
        if (data == null) return null
        val presentation = data["assetPresentation"]?.jsonPrimitive?.contentOrNull
        if (presentation?.equals("PREVIEW", ignoreCase = true) == true) return null

        val quality = data["audioQuality"]?.jsonPrimitive?.contentOrNull?.uppercase(Locale.ROOT).orEmpty()
        if (quality.isNotEmpty() && quality != "LOSSLESS" && quality != "HI_RES_LOSSLESS") {
            return null
        }
        val encoded = data["manifest"]?.jsonPrimitive?.contentOrNull?.replace(Regex("\\s+"), "").orEmpty()
        if (encoded.isEmpty() || encoded.length > 1_000_000) return null

        val decoded = runCatching {
            String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8)
        }.getOrNull() ?: return null

        if (decoded.trimStart().startsWith("{")) {
            val bts = runCatching { TidalSessionManager.json.parseToJsonElement(decoded).jsonObject }.getOrNull()
                ?: return null
            val mime = bts["mimeType"]?.jsonPrimitive?.contentOrNull?.lowercase(Locale.ROOT).orEmpty()
            val codec = bts["codecs"]?.jsonPrimitive?.contentOrNull?.lowercase(Locale.ROOT).orEmpty()
            val enc = bts["encryptionType"]?.jsonPrimitive?.contentOrNull?.uppercase(Locale.ROOT).orEmpty()
            if (mime != "audio/flac" || (codec.isNotEmpty() && codec != "flac") || (enc.isNotEmpty() && enc != "NONE")) {
                return null
            }
            val urls = (bts["urls"] as? JsonArray)?.mapNotNull { (it as? JsonPrimitive)?.contentOrNull?.trim() }.orEmpty()
            if (urls.size != 1 || !isTrustedTidalUrl(urls[0])) return null
            return urls[0] to null
        }

        if (!decoded.contains("<MPD") ||
            !MPD_CODECS_FLAC_REGEX.containsMatchIn(decoded) ||
            MPD_DRM_REGEX.containsMatchIn(decoded) ||
            !MPD_TIMELINE_REGEX.containsMatchIn(decoded) ||
            !MPD_SEGMENT_D_REGEX.containsMatchIn(decoded)
        ) {
            return null
        }
        if (MPD_REP_REGEX.findAll(decoded).count() != 1 || MPD_ADAPT_REGEX.findAll(decoded).count() != 1) {
            return null
        }
        for (m in MPD_ANY_CODEC_REGEX.findAll(decoded)) {
            if (m.groupValues[1].trim().lowercase(Locale.ROOT) != "flac") return null
        }
        val initUrl = MPD_INIT_REGEX.find(decoded)?.groupValues?.getOrNull(1)?.replace("&amp;", "&") ?: return null
        val mediaUrl = MPD_MEDIA_REGEX.find(decoded)?.groupValues?.getOrNull(1)?.replace("&amp;", "&") ?: return null
        if (!isTrustedTidalUrl(initUrl) || !isTrustedTidalUrl(mediaUrl) || !mediaUrl.contains("\$Number\$")) {
            return null
        }
        return null to encoded
    }

    fun createDashDownloadInputProvider(dashManifestBase64: String): Streamable.InputProvider {
        val decoded = String(Base64.getDecoder().decode(dashManifestBase64), StandardCharsets.UTF_8)
        val initUrl = MPD_INIT_REGEX.find(decoded)?.groupValues?.getOrNull(1)?.replace("&amp;", "&")
            ?: throw IllegalStateException("Manifest DASH privo di URL init")
        val mediaTemplate = MPD_MEDIA_REGEX.find(decoded)?.groupValues?.getOrNull(1)?.replace("&amp;", "&")
            ?: throw IllegalStateException("Manifest DASH privo di template media")
        val startNum = MPD_START_NUM_REGEX.find(decoded)?.groupValues?.getOrNull(1)?.toIntOrNull() ?: 1
        var segmentCount = 0
        for (sMatch in MPD_S_TAG_REGEX.findAll(decoded)) {
            val repeat = MPD_R_ATTR_REGEX.find(sMatch.groupValues[1])?.groupValues?.getOrNull(1)?.toIntOrNull() ?: 0
            if (repeat < 0 || repeat > 5000) break
            segmentCount += (1 + repeat)
        }
        val urls = buildList {
            add(initUrl)
            for (i in 0..<segmentCount) {
                add(mediaTemplate.replace("\$Number\$", (startNum + i).toString()))
            }
        }

        return Streamable.InputProvider { position, _ ->
            val iter = urls.iterator()
            val enumeration = object : java.util.Enumeration<InputStream> {
                override fun hasMoreElements(): Boolean = iter.hasNext()
                override fun nextElement(): InputStream {
                    val segUrl = iter.next()
                    val req = Request.Builder()
                        .url(segUrl)
                        .header("User-Agent", TidalSessionManager.MODERN_CHROME_UA)
                        .get()
                        .build()
                    val resp = httpClient.newCall(req).execute()
                    if (!resp.isSuccessful) {
                        resp.close()
                        throw IOException("Segmento fMP4 FLAC non scaricabile: HTTP ${resp.code}")
                    }
                    val bytes = resp.body.bytes()
                    return ByteArrayInputStream(bytes)
                }
            }
            val seq = SequenceInputStream(enumeration)
            if (position > 0L) {
                seq.skip(position)
            }
            seq to -1L
        }
    }
}
