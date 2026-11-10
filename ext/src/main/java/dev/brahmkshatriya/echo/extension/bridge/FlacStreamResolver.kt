package dev.brahmkshatriya.echo.extension.bridge

import dev.brahmkshatriya.echo.common.helpers.ContinuationCallback.Companion.await
import dev.brahmkshatriya.echo.common.models.Album
import dev.brahmkshatriya.echo.common.models.Artist
import dev.brahmkshatriya.echo.common.models.Feed
import dev.brahmkshatriya.echo.common.models.Feed.Companion.toFeed
import dev.brahmkshatriya.echo.common.models.Feed.Companion.toFeedData
import dev.brahmkshatriya.echo.common.models.ImageHolder.Companion.toImageHolder
import dev.brahmkshatriya.echo.common.models.Shelf
import dev.brahmkshatriya.echo.common.models.Streamable
import dev.brahmkshatriya.echo.common.models.Tab
import dev.brahmkshatriya.echo.common.models.Track
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.HttpUrl.Companion.toHttpUrlOrNull
import okhttp3.OkHttpClient
import okhttp3.Request
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.io.SequenceInputStream
import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.Collections
import java.util.Locale
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

class FlacStreamResolver(
    baseHttpClient: OkHttpClient,
    private val sessionManager: ZarzSessionManager,
    private val tidalApiBase: String = "https://tidal.com/v1",
    private val qobuzApiBase: String = "https://www.qobuz.com/api.json/0.2",
    private val deezerApiBase: String = "https://api.deezer.com",
    private val songlinkBase: String = "https://song.link",
    private val trustedUrlChecker: (String, String?) -> Boolean = TrackMatcher::isTrustedMediaUrl,
    private val clock: () -> Long = System::currentTimeMillis
) {
    private val httpClient: OkHttpClient = ZarzSessionManager.withBrowserHeaders(baseHttpClient)

    private class StageHolder(var value: String = "sessione")
    companion object {
        private const val TIDAL_PUBLIC_TOKEN = "49YxDN9a2aFV6RTG"
        private const val QOBUZ_PUBLIC_APP_ID = "735532640"
        private const val QOBUZ_GATEWAY_APP_ID = "798273057"

        private val CONTENT_RANGE_ZERO = Regex("^bytes\\s+0-\\d+/(?:\\d+|\\*)", RegexOption.IGNORE_CASE)
        private val SCRIPT_TAG_REGEX = Regex("<script\\b([^>]*)>([\\s\\S]*?)</script\\s*>", RegexOption.IGNORE_CASE)
        private val NEXT_DATA_ID_REGEX = Regex("(?:^|\\s)id\\s*=\\s*[\"']__NEXT_DATA__[\"']", RegexOption.IGNORE_CASE)

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
    }

    private val streamCache = ConcurrentHashMap<String, ResolvedFlacStream>()

    fun getCachedStream(providerKey: String, spec: TrackMatchSpec): ResolvedFlacStream? {
        val key = "$providerKey|${spec.cacheKey()}"
        val cached = streamCache[key] ?: return null
        return if (cached.isFresh(clock())) cached else {
            streamCache.remove(key)
            null
        }
    }

    fun putCachedStream(stream: ResolvedFlacStream, spec: TrackMatchSpec) {
        val key = "${stream.providerKey}|${spec.cacheKey()}"
        streamCache[key] = stream
        // Prune expired entries opportunistically
        if (streamCache.size > 128) {
            val now = clock()
            streamCache.entries.removeIf { !it.value.isFresh(now) }
        }
    }

    suspend fun resolve(
        spec: TrackMatchSpec,
        budgetMs: Long,
        collectAll: Boolean,
        scope: CoroutineScope,
        onlyFirstPreferred: Boolean = false
    ): List<ResolvedFlacStream> {
        if (spec.title.isBlank() && spec.isrc.isBlank() && spec.spotifyId.isBlank() && spec.providerTrackId.isBlank()) {
            return emptyList()
        }
        sessionManager.scheduleRenewalIfDue(scope)

        val deadline = clock() + budgetMs
        val baseOrder = sessionManager.preferenceOrder()
        val hintProvider = ProviderSpec.byKey(spec.preferredProviderKey)
        val fullOrder = if (hintProvider != null && hintProvider in baseOrder) {
            listOf(hintProvider) + baseOrder.filter { it.key != hintProvider.key }
        } else {
            baseOrder
        }
        val order = if (onlyFirstPreferred) fullOrder.take(1) else fullOrder

        // Fast path if streaming and preferred top source is already fresh in 4-min cache
        if (!collectAll) {
            for (p in order) {
                if (sessionManager.getStoredSession(p) == null) continue
                val cached = getCachedStream(p.key, spec)
                if (cached != null) return listOf(cached)
                break
            }
        }

        var anySession = false
        var needsVerification = false
        val results = mutableListOf<ResolvedFlacStream>()

        for (p in order) {
            val stored = sessionManager.getStoredSession(p) ?: continue
            if (!stored.isValid()) continue
            anySession = true

            if (p.key == "qobuz" && clock() < sessionManager.qobuzUnavailableUntil) {
                continue
            }
            if (deadline - clock() < 1400L) break

            val tidalStage = StageHolder("sessione")

            try {
                val session = sessionManager.activeSession(p, deadline) ?: continue
                val resolved = when (p.key) {
                    "tidal" -> resolveTidal(spec, p, session, deadline, tidalStage)
                    "qobuz" -> resolveQobuz(spec, p, session, deadline)
                    "deezer" -> resolveDeezer(spec, p, session, deadline)
                    else -> resolveAmazon(spec, p, session, deadline)
                }
                if (resolved != null) {
                    sessionManager.log("FLAC compatibile: ${p.name}.")
                    putCachedStream(resolved, spec)
                    results.add(resolved)
                    if (!collectAll) return results
                } else {
                    sessionManager.log("${p.name}: nessun FLAC compatibile per questa ricerca.")
                }
            } catch (ce: CancellationException) {
                throw ce
            } catch (err: Throwable) {
                val code = (err as? BridgeGatewayException)?.code.orEmpty()
                when (code) {
                    "VERIFY_REQUIRED" -> {
                        needsVerification = true
                        sessionManager.queueAutomaticForProvider(scope, p)
                        sessionManager.log("${p.name}: serve una nuova verifica.")
                    }
                    "TIME_BUDGET" -> {
                        if (p.key == "tidal") {
                            sessionManager.log("Tidal: tempo esaurito nella fase ${tidalStage.value}.")
                        }
                        break
                    }
                    else -> {
                        if (p.key == "tidal") {
                            sessionManager.log("Tidal: fase ${tidalStage.value}, ${FlacBridgeDiagnostics.failureCategory(err)}.")
                        }
                        sessionManager.log("${p.name}: fonte non disponibile o risposta non utilizzabile.")
                    }
                }
            }
        }

        if (!anySession) {
            if (sessionManager.autoRunning && sessionManager.automaticAllowed()) {
                sessionManager.notice("Attendi la verifica automatica delle sorgenti o completa la pagina aperta.")
            } else {
                sessionManager.notice("Apri le impostazioni di FLAC Bridge Multi e attiva almeno una sorgente.")
            }
        } else if (needsVerification && results.isEmpty()) {
            if (sessionManager.autoRunning && sessionManager.automaticAllowed()) {
                sessionManager.notice("Una sorgente richiede una nuova verifica automatica.")
            } else {
                sessionManager.notice("Una sorgente richiede una nuova verifica nelle impostazioni di FLAC Bridge Multi.")
            }
        }

        return results
    }

    /**
     * Resolves a track strictly against a single user-selected provider (manual server switch
     * in Echo's player Quality/Server bottom sheet), without silently falling back to a
     * different provider.
     */
    suspend fun resolveSingleProvider(
        spec: TrackMatchSpec,
        provider: ProviderSpec,
        budgetMs: Long,
        scope: CoroutineScope
    ): ResolvedFlacStream {
        getCachedStream(provider.key, spec)?.let { cached ->
            sessionManager.log("Server manuale attivo (cache): ${provider.name}.")
            return cached
        }

        sessionManager.scheduleRenewalIfDue(scope)
        sessionManager.log("Scelta manuale server: ${provider.name}.")

        val stored = sessionManager.getStoredSession(provider)
        if (stored == null || !stored.isValid()) {
            val msg = "${provider.name}: sessione non attiva. Attivala nelle impostazioni di FLAC Bridge Multi."
            sessionManager.log(msg)
            sessionManager.notify(msg)
            throw IllegalStateException(msg)
        }

        if (provider.key == "qobuz" && clock() < sessionManager.qobuzUnavailableUntil) {
            val msg = "Qobuz è temporaneamente sospesa dal server (HTTP 503). Riprova tra poco."
            sessionManager.log(msg)
            sessionManager.notify(msg)
            throw IllegalStateException(msg)
        }

        val deadline = clock() + budgetMs
        val tidalStage = StageHolder("sessione")

        try {
            val session = sessionManager.activeSession(provider, deadline)
                ?: throw IllegalStateException("${provider.name}: sessione non disponibile.")
            val specForProvider = if (spec.preferredProviderKey == provider.key) spec else spec.copy(preferredProviderKey = "")
            val resolved = when (provider.key) {
                "tidal" -> resolveTidal(specForProvider, provider, session, deadline, tidalStage)
                "qobuz" -> resolveQobuz(specForProvider, provider, session, deadline)
                "deezer" -> resolveDeezer(specForProvider, provider, session, deadline)
                else -> resolveAmazon(specForProvider, provider, session, deadline)
            }
            if (resolved != null) {
                sessionManager.log("FLAC compatibile (${provider.name}, scelta manuale).")
                putCachedStream(resolved, spec)
                sessionManager.notify("Server attivo: ${resolved.label()}")
                return resolved
            }
        } catch (ce: CancellationException) {
            throw ce
        } catch (err: BridgeGatewayException) {
            when (err.code) {
                "VERIFY_REQUIRED" -> {
                    sessionManager.queueAutomaticForProvider(scope, provider)
                    val msg = "${provider.name}: richiede una nuova verifica nelle impostazioni."
                    sessionManager.log(msg)
                    sessionManager.notify(msg)
                    throw IllegalStateException(msg)
                }
                "TIME_BUDGET" -> {
                    val msg = "${provider.name}: tempo esaurito durante il cambio server."
                    sessionManager.log(msg)
                    sessionManager.notify(msg)
                    throw IllegalStateException(msg)
                }
                else -> {
                    if (provider.key == "tidal") {
                        sessionManager.log("Tidal: fase ${tidalStage.value}, ${FlacBridgeDiagnostics.failureCategory(err)}.")
                    }
                    val msg = "${provider.name}: fonte temporaneamente non disponibile."
                    sessionManager.log(msg)
                    sessionManager.notify(msg)
                    throw IllegalStateException(msg)
                }
            }
        }

        val notFoundMsg = "Brano non disponibile in FLAC su ${provider.name}."
        sessionManager.log(notFoundMsg)
        sessionManager.notify(notFoundMsg)
        throw IllegalStateException(notFoundMsg)
    }

    private suspend fun requestTicket(
        provider: ProviderSpec,
        session: StoredSession,
        alias: String,
        resource: String,
        deadlineMs: Long
    ): String {
        val hash = ZarzCrypto.sha256Hex("$alias:track:${resource.trim().lowercase(Locale.ROOT)}")
        val payload = JsonObject(
            mapOf(
                "capability" to JsonPrimitive("download_ticket"),
                "provider" to JsonPrimitive(alias),
                "resource_hash" to JsonPrimitive(hash)
            )
        )
        val resp = sessionManager.signedRequest(
            provider = provider,
            session = session,
            method = "POST",
            path = "/tickets",
            payload = payload,
            deadlineMs = deadlineMs
        )
        val value = (resp.stringField("ticket_id") ?: resp.stringField("ticket")).orEmpty()
        if (value.isEmpty() || value.length > 4096) {
            throw BridgeGatewayException("NO_TICKET")
        }
        return value
    }

    private suspend fun requestJson(
        url: String,
        headers: Map<String, String> = mapOf("Accept" to "application/json"),
        deadlineMs: Long,
        timeoutMs: Long = 2600L,
        diagnostic: String? = null
    ): JsonObject? {
        val actualTimeout = sessionManager.remainingTimeoutMs(deadlineMs, timeoutMs)
        val callClient = httpClient.newBuilder()
            .callTimeout(actualTimeout, TimeUnit.MILLISECONDS)
            .connectTimeout(actualTimeout, TimeUnit.MILLISECONDS)
            .readTimeout(actualTimeout, TimeUnit.MILLISECONDS)
            .build()
        val reqBuilder = Request.Builder()
            .url(url)
            .header("User-Agent", ZarzSessionManager.MODERN_CHROME_UA)
        headers.forEach { (k, v) -> reqBuilder.header(k, v) }
        return callClient.newCall(reqBuilder.get().build()).await().use { r ->
            if (!r.isSuccessful) {
                if (diagnostic == "tidal") {
                    sessionManager.log("Tidal: catalogo HTTP ${r.code}.")
                }
                return@use null
            }
            val raw = runCatching { r.body.string() }.getOrDefault("")
            runCatching { ZarzSessionManager.json.parseToJsonElement(raw).jsonObject }.getOrElse {
                if (diagnostic == "tidal") {
                    sessionManager.log("Tidal: risposta del catalogo non leggibile.")
                }
                null
            }
        }
    }

    private suspend fun tidalHit(spec: TrackMatchSpec, deadlineMs: Long): CatalogueHit? {
        val directId = if (spec.preferredProviderKey == "tidal") TrackMatcher.validNumericId(spec.providerTrackId) else ""
        if (directId.isNotEmpty()) {
            return CatalogueHit(
                id = directId,
                durationMs = spec.durationMs,
                isrcMatched = spec.isrc.length == 12,
                lossless = true,
                providerKey = "tidal"
            )
        }

        val wantedIsrc = TrackMatcher.cleanIsrc(spec.isrc)
        val url = if (wantedIsrc.isNotEmpty()) {
            (tidalApiBase.trimEnd('/') + "/tracks").toHttpUrl().newBuilder()
                .addQueryParameter("isrc", wantedIsrc)
                .addQueryParameter("countryCode", "US")
                .addQueryParameter("limit", "25")
                .build()
                .toString()
        } else {
            if (spec.title.isBlank() || spec.primaryArtist().isBlank()) return null
            (tidalApiBase.trimEnd('/') + "/search/tracks").toHttpUrl().newBuilder()
                .addQueryParameter("query", "${spec.primaryArtist()} ${spec.title}".take(240))
                .addQueryParameter("countryCode", "US")
                .addQueryParameter("limit", "25")
                .build()
                .toString()
        }
        val headers = mapOf(
            "Accept" to "application/json",
            "x-tidal-token" to TIDAL_PUBLIC_TOKEN
        )
        val jsonObj = try {
            requestJson(url, headers, deadlineMs, 2600L, "tidal")
        } catch (ce: CancellationException) {
            throw ce
        } catch (err: Throwable) {
            if (FlacBridgeDiagnostics.failureCategory(err) != "timeout" || deadlineMs - clock() < 7500L) {
                throw err
            }
            sessionManager.log("Tidal: timeout del catalogo; un nuovo tentativo.")
            requestJson(url, headers, deadlineMs, 3200L, "tidal")
        }
        val items = (jsonObj?.get("items") as? JsonArray)?.take(25).orEmpty()
        val hits = items.mapNotNull { elem ->
            val obj = elem as? JsonObject ?: return@mapNotNull null
            parseTidalCandidate(spec, obj)
        }
        return TrackMatcher.selectBestHit(hits)
    }

    private fun parseTidalCandidate(spec: TrackMatchSpec, obj: JsonObject): CatalogueHit? {
        val streamReady = obj.booleanField("streamReady") != false &&
            obj.booleanField("allowStreaming") != false
        val artists = buildList {
            (obj["artists"] as? JsonArray)?.forEach { a ->
                (a as? JsonObject)?.stringField("name")?.takeIf(String::isNotEmpty)?.let(::add)
            }
            (obj["artist"] as? JsonObject)?.stringField("name")?.takeIf(String::isNotEmpty)?.let {
                if (it !in this) add(it)
            }
        }
        val albumObj = obj["album"] as? JsonObject
        val coverUuid = albumObj?.stringField("cover").orEmpty()
        val coverUrl = if (coverUuid.isNotEmpty()) {
            "https://resources.tidal.com/images/${coverUuid.replace('-', '/')}/640x640.jpg"
        } else ""
        return TrackMatcher.matchCatalogueCandidate(
            wanted = spec,
            rawId = obj.stringField("id"),
            rawTitle = obj.stringField("title"),
            rawVersion = obj.stringField("version"),
            candidateArtists = artists,
            durationSeconds = obj.doubleField("duration"),
            rawIsrc = obj.stringField("isrc"),
            audioQuality = obj.stringField("audioQuality"),
            streamable = streamReady,
            kind = "tidal",
            albumTitle = albumObj?.stringField("title").orEmpty(),
            albumId = albumObj?.stringField("id").orEmpty(),
            coverUrl = coverUrl,
            isExplicit = obj.booleanField("explicit") == true
        )
    }

    private suspend fun resolveTidal(
        spec: TrackMatchSpec,
        provider: ProviderSpec,
        session: StoredSession,
        deadlineMs: Long,
        tidalStage: StageHolder
    ): ResolvedFlacStream? {
        tidalStage.value = "catalogo"
        val hit = tidalHit(spec, deadlineMs) ?: return null
        tidalStage.value = "ticket"
        val ticket = requestTicket(provider, session, provider.ticketAlias, hit.id, deadlineMs)
        tidalStage.value = "stream"
        val payload = JsonObject(
            mapOf(
                "id" to JsonPrimitive(hit.id),
                "quality" to JsonPrimitive("LOSSLESS")
            )
        )
        val resp = sessionManager.signedRequest(
            provider = provider,
            session = session,
            method = "POST",
            path = provider.dlPath,
            payload = payload,
            extraHeaders = mapOf("X-Zarz-Ticket" to ticket),
            deadlineMs = deadlineMs
        )
        val data = resp["data"] as? JsonObject
        val decoded = decodeTidalData(data) ?: return null
        return buildResolvedStream(
            provider = provider,
            hit = hit,
            url = decoded.first,
            dashManifest = decoded.second,
            rawBits = data?.doubleField("bitDepth"),
            rawSampleRate = data?.doubleField("sampleRate")
        )
    }

    internal fun decodeTidalData(data: JsonObject?): Pair<String?, String?>? {
        if (data == null) return null
        if (data.stringField("assetPresentation")?.equals("PREVIEW", ignoreCase = true) == true) {
            return null
        }
        val quality = data.stringField("audioQuality")?.uppercase(Locale.ROOT).orEmpty()
        if (quality.isNotEmpty() && quality != "LOSSLESS" && quality != "HI_RES_LOSSLESS") {
            return null
        }
        val encoded = data.stringField("manifest")?.replace(Regex("\\s+"), "").orEmpty()
        if (encoded.isEmpty() || encoded.length > 1_000_000) return null
        val decoded = runCatching {
            String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8)
        }.getOrNull() ?: return null

        if (decoded.trimStart().startsWith("{")) {
            val bts = runCatching { ZarzSessionManager.json.parseToJsonElement(decoded).jsonObject }.getOrNull()
                ?: return null
            val mime = bts.stringField("mimeType")?.lowercase(Locale.ROOT).orEmpty()
            val codec = bts.stringField("codecs")?.lowercase(Locale.ROOT).orEmpty()
            val enc = bts.stringField("encryptionType")?.uppercase(Locale.ROOT).orEmpty()
            if (mime != "audio/flac" || (codec.isNotEmpty() && codec != "flac") || (enc.isNotEmpty() && enc != "NONE")) {
                return null
            }
            val urls = (bts["urls"] as? JsonArray)?.mapNotNull { (it as? JsonPrimitive)?.contentOrNull?.trim() }.orEmpty()
            if (urls.size != 1 || !trustedUrlChecker("tidal", urls[0])) return null
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
        if (!trustedUrlChecker("tidal", initUrl) || !trustedUrlChecker("tidal", mediaUrl) || !mediaUrl.contains("\$Number\$")) {
            return null
        }
        return null to encoded
    }

    private suspend fun qobuzHit(
        spec: TrackMatchSpec,
        provider: ProviderSpec,
        session: StoredSession,
        deadlineMs: Long
    ): CatalogueHit? {
        val directId = if (spec.preferredProviderKey == "qobuz") TrackMatcher.validNumericId(spec.providerTrackId) else ""
        if (directId.isNotEmpty()) {
            return CatalogueHit(
                id = directId,
                durationMs = spec.durationMs,
                isrcMatched = spec.isrc.length == 12,
                providerKey = "qobuz"
            )
        }
        if (spec.title.isBlank() || spec.primaryArtist().isBlank()) return null
        val queryStr = "${spec.primaryArtist()} ${spec.title}".take(240)

        var items: JsonArray? = runCatching {
            val url = (qobuzApiBase.trimEnd('/') + "/track/search").toHttpUrl().newBuilder()
                .addQueryParameter("query", queryStr)
                .addQueryParameter("limit", "25")
                .addQueryParameter("offset", "0")
                .build()
                .toString()
            val resp = requestJson(
                url = url,
                headers = mapOf("Accept" to "application/json", "X-App-Id" to QOBUZ_PUBLIC_APP_ID),
                deadlineMs = deadlineMs,
                timeoutMs = 2400L
            )
            (resp?.get("tracks") as? JsonObject)?.get("items") as? JsonArray
        }.getOrNull()

        if (items == null) {
            items = runCatching {
                val resp = sessionManager.signedRequest(
                    provider = provider,
                    session = session,
                    method = "GET",
                    path = "/qbz/track/search",
                    query = mapOf(
                        "query" to queryStr,
                        "limit" to "25",
                        "offset" to "0",
                        "app_id" to QOBUZ_GATEWAY_APP_ID
                    ),
                    deadlineMs = deadlineMs
                )
                (resp["tracks"] as? JsonObject)?.get("items") as? JsonArray
            }.getOrNull() ?: return null
        }

        val hits = items.take(25).mapNotNull { elem ->
            val obj = elem as? JsonObject ?: return@mapNotNull null
            parseQobuzCandidate(spec, obj)
        }
        return TrackMatcher.selectBestHit(hits)
    }

    private fun parseQobuzCandidate(spec: TrackMatchSpec, obj: JsonObject): CatalogueHit? {
        val streamable = obj.booleanField("streamable") != false
        val artists = buildList {
            (obj["performer"] as? JsonObject)?.stringField("name")?.takeIf(String::isNotEmpty)?.let(::add)
            (obj["artist"] as? JsonObject)?.stringField("name")?.takeIf(String::isNotEmpty)?.let {
                if (it !in this) add(it)
            }
            ((obj["album"] as? JsonObject)?.get("artist") as? JsonObject)?.stringField("name")
                ?.takeIf(String::isNotEmpty)?.let { if (it !in this) add(it) }
        }
        val albumObj = obj["album"] as? JsonObject
        val imgObj = albumObj?.get("image") as? JsonObject
        val coverUrl = imgObj?.stringField("large") ?: imgObj?.stringField("small") ?: ""
        return TrackMatcher.matchCatalogueCandidate(
            wanted = spec,
            rawId = obj.stringField("id"),
            rawTitle = obj.stringField("title"),
            rawVersion = obj.stringField("version"),
            candidateArtists = artists,
            durationSeconds = obj.doubleField("duration"),
            rawIsrc = obj.stringField("isrc"),
            audioQuality = if (obj.booleanField("hires_streamable") == true) "HI_RES_LOSSLESS" else "LOSSLESS",
            streamable = streamable,
            kind = "qobuz",
            albumTitle = albumObj?.stringField("title").orEmpty(),
            albumId = albumObj?.stringField("id").orEmpty(),
            coverUrl = coverUrl,
            isExplicit = obj.booleanField("parental_warning") == true
        )
    }

    private fun looksPreview(data: JsonObject?, hit: CatalogueHit): Boolean {
        if (data == null) return false
        if (data.booleanField("sample") == true ||
            data.booleanField("preview") == true ||
            data.booleanField("is_preview") == true ||
            data.stringField("assetPresentation")?.equals("PREVIEW", ignoreCase = true) == true
        ) {
            return true
        }
        val seconds = data.doubleField("duration") ?: return false
        val expected = hit.durationMs
        return seconds > 0.0 && seconds < 86_400.0 && expected > 0L && (seconds * 1000.0 + 5000.0) < expected
    }

    private suspend fun resolveQobuz(
        spec: TrackMatchSpec,
        provider: ProviderSpec,
        session: StoredSession,
        deadlineMs: Long
    ): ResolvedFlacStream? {
        val hit = qobuzHit(spec, provider, session, deadlineMs) ?: return null
        val trackUrl = "https://open.qobuz.com/track/${hit.id}"
        val ticket = requestTicket(provider, session, provider.ticketAlias, trackUrl, deadlineMs)
        val payload = JsonObject(
            mapOf(
                "quality" to JsonPrimitive("cd"),
                "upload_to_r2" to JsonPrimitive(false),
                "id" to JsonPrimitive(hit.id),
                "type" to JsonPrimitive("track"),
                "url" to JsonPrimitive(trackUrl)
            )
        )
        val resp = sessionManager.signedRequest(
            provider = provider,
            session = session,
            method = "POST",
            path = provider.dlPath,
            payload = payload,
            extraHeaders = mapOf("X-Zarz-Ticket" to ticket),
            deadlineMs = deadlineMs
        )
        if (resp.booleanField("success") == false || resp["error"] != null || resp["detail"] != null) {
            return null
        }
        val nested = resp["data"] as? JsonObject
        if (looksPreview(resp, hit) || looksPreview(nested, hit)) return null
        val rawUrl = resp.stringField("download_url")
            ?: resp.stringField("url")
            ?: resp.stringField("link")
            ?: nested?.stringField("download_url")
            ?: nested?.stringField("url")
            ?: nested?.stringField("link")
            ?: return null
        val verifiedUrl = verifyPlainFlacUrl("qobuz", rawUrl, deadlineMs)
        if (verifiedUrl.isEmpty()) {
            sessionManager.log("Qobuz: nessun FLAC diretto verificabile.")
            return null
        }
        return buildResolvedStream(
            provider = provider,
            hit = hit,
            url = verifiedUrl,
            dashManifest = null,
            rawBits = resp.doubleField("bit_depth") ?: nested?.doubleField("bit_depth"),
            rawSampleRate = resp.doubleField("sampling_rate") ?: nested?.doubleField("sampling_rate")
        )
    }

    private suspend fun deezerHit(spec: TrackMatchSpec, deadlineMs: Long): CatalogueHit? {
        val directId = if (spec.preferredProviderKey == "deezer") TrackMatcher.validNumericId(spec.providerTrackId) else ""
        if (directId.isNotEmpty()) {
            return CatalogueHit(
                id = directId,
                durationMs = spec.durationMs,
                isrcMatched = spec.isrc.length == 12,
                providerKey = "deezer"
            )
        }
        val wantedIsrc = TrackMatcher.cleanIsrc(spec.isrc)
        return if (wantedIsrc.isNotEmpty()) {
            val url = "${deezerApiBase.trimEnd('/')}/track/isrc:$wantedIsrc"
            val obj = requestJson(url, deadlineMs = deadlineMs, timeoutMs = 2400L) ?: return null
            parseDeezerCandidate(spec, obj)
        } else {
            if (spec.title.isBlank() || spec.primaryArtist().isBlank()) return null
            val url = (deezerApiBase.trimEnd('/') + "/search").toHttpUrl().newBuilder()
                .addQueryParameter("q", "${spec.primaryArtist()} ${spec.title}".take(240))
                .addQueryParameter("limit", "25")
                .build()
                .toString()
            val obj = requestJson(url, deadlineMs = deadlineMs, timeoutMs = 2400L) ?: return null
            val items = (obj["data"] as? JsonArray)?.take(25).orEmpty()
            val hits = items.mapNotNull { elem ->
                (elem as? JsonObject)?.let { parseDeezerCandidate(spec, it) }
            }
            TrackMatcher.selectBestHit(hits)
        }
    }

    private fun parseDeezerCandidate(spec: TrackMatchSpec, obj: JsonObject): CatalogueHit? {
        val readable = obj.booleanField("readable") != false
        val artists = buildList {
            (obj["artist"] as? JsonObject)?.stringField("name")?.takeIf(String::isNotEmpty)?.let(::add)
            (obj["contributors"] as? JsonArray)?.forEach { c ->
                (c as? JsonObject)?.stringField("name")?.takeIf(String::isNotEmpty)?.let {
                    if (it !in this) add(it)
                }
            }
        }
        val albumObj = obj["album"] as? JsonObject
        val coverUrl = albumObj?.stringField("cover_xl")
            ?: albumObj?.stringField("cover_big")
            ?: albumObj?.stringField("cover_medium")
            ?: ""
        return TrackMatcher.matchCatalogueCandidate(
            wanted = spec,
            rawId = obj.stringField("id"),
            rawTitle = obj.stringField("title_short") ?: obj.stringField("title"),
            rawVersion = obj.stringField("title_version"),
            candidateArtists = artists,
            durationSeconds = obj.doubleField("duration"),
            rawIsrc = obj.stringField("isrc"),
            audioQuality = null,
            streamable = readable,
            kind = "deezer",
            albumTitle = albumObj?.stringField("title").orEmpty(),
            albumId = albumObj?.stringField("id").orEmpty(),
            coverUrl = coverUrl,
            isExplicit = obj.booleanField("explicit_lyrics") == true
        )
    }

    private suspend fun resolveDeezer(
        spec: TrackMatchSpec,
        provider: ProviderSpec,
        session: StoredSession,
        deadlineMs: Long
    ): ResolvedFlacStream? {
        val hit = deezerHit(spec, deadlineMs) ?: return null
        val trackUrl = "https://www.deezer.com/track/${hit.id}"
        val ticket = requestTicket(provider, session, provider.ticketAlias, trackUrl, deadlineMs)
        val payload = JsonObject(
            mapOf(
                "id" to JsonPrimitive(hit.id),
                "type" to JsonPrimitive("track"),
                "platform" to JsonPrimitive("deezer"),
                "url" to JsonPrimitive(trackUrl)
            )
        )
        val d = sessionManager.signedRequest(
            provider = provider,
            session = session,
            method = "POST",
            path = provider.dlPath,
            payload = payload,
            extraHeaders = mapOf("X-Zarz-Ticket" to ticket),
            deadlineMs = deadlineMs
        )
        if (d.booleanField("success") != true || looksPreview(d, hit)) return null

        val encrypted = d.booleanField("deezer_encrypted")
        val needsDecryption = d.booleanField("requires_client_decryption")
        val direct = d.booleanField("direct_downloadable")
        if (encrypted == true || needsDecryption == true || direct == false ||
            !(encrypted == false || needsDecryption == false || direct == true)
        ) {
            sessionManager.log("Deezer: il file richiede decodifica non disponibile.")
            return null
        }
        val format = (d.stringField("deezer_format") ?: d.stringField("format"))?.lowercase(Locale.ROOT).orEmpty()
        if (format.isNotEmpty() && format != "flac") return null

        val rawUrl = (if (direct == true) d.stringField("direct_download_url") else null)
            ?: d.stringField("download_url")
            ?: d.stringField("direct_download_url")
            ?: return null
        val verifiedUrl = verifyPlainFlacUrl("deezer", rawUrl, deadlineMs)
        if (verifiedUrl.isEmpty()) {
            sessionManager.log("Deezer: nessun FLAC diretto verificabile.")
            return null
        }
        return buildResolvedStream(
            provider = provider,
            hit = hit,
            url = verifiedUrl,
            dashManifest = null,
            rawBits = d.doubleField("bit_depth"),
            rawSampleRate = d.doubleField("sample_rate")
        )
    }

    private suspend fun amazonHit(spec: TrackMatchSpec, deadlineMs: Long): CatalogueHit? {
        val songlinkPath = when {
            spec.spotifyId.length == 22 -> "/s/${spec.spotifyId}"
            spec.youtubeId.length == 11 -> "/y/${spec.youtubeId}"
            else -> return null
        }
        val timeout = sessionManager.remainingTimeoutMs(deadlineMs, 2500L)
        val callClient = httpClient.newBuilder()
            .callTimeout(timeout, TimeUnit.MILLISECONDS)
            .connectTimeout(timeout, TimeUnit.MILLISECONDS)
            .readTimeout(timeout, TimeUnit.MILLISECONDS)
            .build()
        val req = Request.Builder()
            .url(songlinkBase.trimEnd('/') + songlinkPath)
            .header("User-Agent", ZarzSessionManager.MODERN_CHROME_UA)
            .header("Accept", "text/html")
            .get()
            .build()

        val html = callClient.newCall(req).await().use { r ->
            if (!r.isSuccessful) return null
            runCatching { r.body.string() }.getOrDefault("")
        }
        if (html.isEmpty()) return null

        var nextDataRaw = ""
        for (match in SCRIPT_TAG_REGEX.findAll(html)) {
            if (NEXT_DATA_ID_REGEX.containsMatchIn(match.groupValues[1])) {
                nextDataRaw = match.groupValues[2]
                break
            }
        }
        if (nextDataRaw.isEmpty()) return null

        val parsed = runCatching { ZarzSessionManager.json.parseToJsonElement(nextDataRaw).jsonObject }.getOrNull()
            ?: return null
        val sections = (((parsed["props"] as? JsonObject)
            ?.get("pageProps") as? JsonObject)
            ?.get("pageData") as? JsonObject)
            ?.get("sections") as? JsonArray
            ?: return null

        for (sec in sections) {
            val links = (sec as? JsonObject)?.get("links") as? JsonArray ?: continue
            for (linkElem in links) {
                val l = linkElem as? JsonObject ?: continue
                if (l.stringField("platform") == "amazonMusic" && l.booleanField("show") == true) {
                    val asin = TrackMatcher.extractAmazonAsin(l.stringField("url"))
                    if (asin.isNotEmpty()) {
                        return CatalogueHit(
                            id = asin,
                            durationMs = TrackMatcher.validDurationMs(spec.durationMs),
                            isrcMatched = false,
                            providerKey = "amazon"
                        )
                    }
                }
            }
        }
        return null
    }

    private suspend fun resolveAmazon(
        spec: TrackMatchSpec,
        provider: ProviderSpec,
        session: StoredSession,
        deadlineMs: Long
    ): ResolvedFlacStream? {
        val hit = amazonHit(spec, deadlineMs) ?: return null
        val ticket = requestTicket(provider, session, provider.ticketAlias, hit.id, deadlineMs)
        val payload = JsonObject(
            mapOf(
                "asin" to JsonPrimitive(hit.id),
                "codec" to JsonPrimitive("flac")
            )
        )
        val rawResp = sessionManager.signedRequest(
            provider = provider,
            session = session,
            method = "POST",
            path = provider.dlPath,
            payload = payload,
            extraHeaders = mapOf("X-Zarz-Ticket" to ticket),
            deadlineMs = deadlineMs
        )
        val d = if (rawResp["_items"] is JsonArray) {
            (rawResp["_items"] as JsonArray).firstOrNull() as? JsonObject
        } else {
            rawResp
        } ?: return null

        val audio = d["audio"] as? JsonObject
        if (audio == null ||
            !audio.stringField("key").isNullOrEmpty() ||
            audio.booleanField("encrypted") == true ||
            audio.stringField("codec")?.lowercase(Locale.ROOT) != "flac"
        ) {
            sessionManager.log("Amazon Music: file cifrato o formato non compatibile.")
            return null
        }
        if (looksPreview(d, hit) || looksPreview(audio, hit)) return null

        val wantedIsrc = TrackMatcher.cleanIsrc(spec.isrc)
        val gotIsrc = TrackMatcher.cleanIsrc((d["meta"] as? JsonObject)?.stringField("isrc"))
        if (wantedIsrc.isNotEmpty() && gotIsrc.isNotEmpty() && wantedIsrc != gotIsrc) {
            return null
        }

        val rawUrl = audio.stringField("url") ?: return null
        val verifiedUrl = verifyPlainFlacUrl("amazon", rawUrl, deadlineMs)
        if (verifiedUrl.isEmpty()) {
            sessionManager.log("Amazon Music: nessun FLAC diretto verificabile.")
            return null
        }
        return buildResolvedStream(
            provider = provider,
            hit = hit,
            url = verifiedUrl,
            dashManifest = null,
            rawBits = audio.doubleField("bitDepth") ?: audio.doubleField("bitsPerSample"),
            rawSampleRate = audio.doubleField("sampleRate")
        )
    }

    /**
     * Verifies a direct FLAC URL by checking `fLaC` magic bytes via `Range: bytes=0-3`
     * while manually validating every redirect target against `trustedUrlChecker`.
     */
    suspend fun verifyPlainFlacUrl(kind: String, rawUrl: String?, deadlineMs: Long): String {
        var currentUrl = rawUrl?.trim().orEmpty()
        for (hop in 0..<3) {
            if (!trustedUrlChecker(kind, currentUrl)) return ""
            val timeout = sessionManager.remainingTimeoutMs(deadlineMs, 2200L)
            val noRedirectClient = httpClient.newBuilder()
                .followRedirects(false)
                .followSslRedirects(false)
                .callTimeout(timeout, TimeUnit.MILLISECONDS)
                .connectTimeout(timeout, TimeUnit.MILLISECONDS)
                .readTimeout(timeout, TimeUnit.MILLISECONDS)
                .build()
            val req = Request.Builder()
                .url(currentUrl)
                .header("User-Agent", ZarzSessionManager.MODERN_CHROME_UA)
                .header("Range", "bytes=0-3")
                .header("Accept", "audio/flac,*/*")
                .get()
                .build()

            val redirectOrVerified = noRedirectClient.newCall(req).await().use { r ->
                if (r.code in 300..399) {
                    val loc = r.header("Location")?.trim().orEmpty()
                    val resolved = if (loc.startsWith("/") && !loc.startsWith("//")) {
                        val parsedBase = currentUrl.toHttpUrlOrNull()
                        if (parsedBase != null) "${parsedBase.scheme}://${parsedBase.host}${if (parsedBase.port != 443 && parsedBase.port != 80) ":${parsedBase.port}" else ""}$loc"
                        else ""
                    } else loc
                    return@use "REDIRECT:$resolved"
                }
                if (r.code != 206) return@use ""
                val contentRange = r.header("Content-Range")?.trim().orEmpty()
                if (contentRange.isNotEmpty() && !CONTENT_RANGE_ZERO.containsMatchIn(contentRange)) {
                    return@use ""
                }
                val headerBytes = ByteArray(4)
                val stream = r.body.byteStream()
                var readTotal = 0
                while (readTotal < 4) {
                    val n = stream.read(headerBytes, readTotal, 4 - readTotal)
                    if (n <= 0) break
                    readTotal += n
                }
                val isFlacMagic = readTotal == 4 &&
                    headerBytes[0] == 0x66.toByte() && // 'f'
                    headerBytes[1] == 0x4C.toByte() && // 'L'
                    headerBytes[2] == 0x61.toByte() && // 'a'
                    headerBytes[3] == 0x43.toByte()    // 'C'
                if (isFlacMagic) currentUrl else ""
            }
            if (redirectOrVerified.startsWith("REDIRECT:")) {
                currentUrl = redirectOrVerified.removePrefix("REDIRECT:")
                continue
            }
            return redirectOrVerified
        }
        return ""
    }

    private fun buildResolvedStream(
        provider: ProviderSpec,
        hit: CatalogueHit,
        url: String?,
        dashManifest: String?,
        rawBits: Double?,
        rawSampleRate: Double?
    ): ResolvedFlacStream {
        val bdInt = rawBits?.toInt()?.takeIf { it.toDouble() == rawBits && it in 8..32 }
        var srDouble = rawSampleRate ?: 0.0
        if (srDouble > 0.0 && srDouble < 1000.0) {
            srDouble = kotlin.math.round(srDouble * 1000.0)
        }
        val srInt = srDouble.toInt().takeIf { it.toDouble() == srDouble && it in 8000..768_000 }
        return ResolvedFlacStream(
            providerKey = provider.key,
            providerName = provider.name,
            url = url,
            dashManifestBase64 = dashManifest,
            durationMs = hit.durationMs,
            isrcMatched = hit.isrcMatched,
            bitDepth = bdInt,
            sampleRate = srInt,
            resolvedAtMs = clock()
        )
    }

    /**
     * Builds a `Streamable.InputProvider` that sequentially streams the fMP4 FLAC segments
     * of a verified Tidal DASH MPD manifest when downloading (`isDownload == true`).
     */
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
            val streams = urls.map { segUrl ->
                if (!trustedUrlChecker("tidal", segUrl)) {
                    throw IllegalStateException("Host segmento DASH non autorizzato")
                }
                val resp = httpClient.newCall(
                    Request.Builder()
                        .url(segUrl)
                        .header("User-Agent", ZarzSessionManager.MODERN_CHROME_UA)
                        .get()
                        .build()
                ).execute()
                if (!resp.isSuccessful) {
                    resp.close()
                    throw IllegalStateException("Download segmento DASH fallito: HTTP ${resp.code}")
                }
                resp.body.byteStream()
            }
            val combined: InputStream = if (streams.isEmpty()) {
                ByteArrayInputStream(ByteArray(0))
            } else {
                SequenceInputStream(Collections.enumeration(streams))
            }
            if (position > 0L) {
                combined.skip(position)
            }
            combined to -1L
        }
    }

    // --- Standalone Mode Catalogue Search & Home Feed ---

    suspend fun searchStandaloneFeed(query: String): Feed<Shelf> {
        val trimmed = query.trim()
        val tabs = listOf(
            Tab("all", "Tutti"),
            Tab("tidal", "Tidal"),
            Tab("qobuz", "Qobuz"),
            Tab("deezer", "Deezer")
        )
        return Feed(tabs) { tab ->
            if (trimmed.isEmpty()) {
                return@Feed emptyList<Shelf>().toFeedData()
            }
            val shelves = coroutineScope {
                val wantAll = tab == null || tab.id == "all"
                val tidalJob = if (wantAll || tab?.id == "tidal") async { searchTidalTracks(trimmed) } else null
                val qobuzJob = if (wantAll || tab?.id == "qobuz") async { searchQobuzTracks(trimmed) } else null
                val deezerJob = if (wantAll || tab?.id == "deezer") async { searchDeezerTracksAndAlbums(trimmed) } else null

                val tidalTracks = tidalJob?.await().orEmpty()
                val qobuzTracks = qobuzJob?.await().orEmpty()
                val (deezerTracks, deezerAlbums) = deezerJob?.await() ?: (emptyList<Track>() to emptyList<Album>())

                buildList {
                    if (wantAll) {
                        val merged = mergeStandaloneTracks(tidalTracks, qobuzTracks, deezerTracks)
                        if (merged.isNotEmpty()) {
                            add(Shelf.Lists.Tracks("flac_tracks_all", "Brani", merged))
                        }
                        if (deezerAlbums.isNotEmpty()) {
                            add(Shelf.Lists.Items("flac_albums_all", "Album", deezerAlbums))
                        }
                    } else {
                        val singleTracks = tidalTracks + qobuzTracks + deezerTracks
                        if (singleTracks.isNotEmpty()) {
                            add(Shelf.Lists.Tracks("flac_tracks_${tab.id}", "Brani (${tab.title})", singleTracks))
                        }
                        if (deezerAlbums.isNotEmpty()) {
                            add(Shelf.Lists.Items("flac_albums_dzr", "Album", deezerAlbums))
                        }
                    }
                }
            }
            shelves.toFeedData()
        }
    }

    suspend fun loadStandaloneHomeFeed(): Feed<Shelf> {
        return PagedStandaloneHome()
    }

    private suspend fun PagedStandaloneHome(): Feed<Shelf> {
        val activeNames = ProviderSpec.ALL
            .filter { sessionManager.getStoredSession(it) != null }
            .joinToString(", ") { it.name }
        val statusSubtitle = if (activeNames.isNotEmpty()) {
            "Sorgenti verificate attive: $activeNames"
        } else {
            "Nessuna sorgente verificata: apri le Impostazioni (icona ingranaggio) per verificare Tidal o Qobuz"
        }

        val deadline = clock() + 6000L
        val chartObj = runCatching {
            requestJson("${deezerApiBase.trimEnd('/')}/chart/0?limit=20", deadlineMs = deadline, timeoutMs = 4000L)
        }.getOrNull()

        val chartTracks = ((chartObj?.get("tracks") as? JsonObject)?.get("data") as? JsonArray)
            ?.mapNotNull { (it as? JsonObject)?.let(::deezerJsonToEchoTrack) }
            .orEmpty()
        val chartAlbums = ((chartObj?.get("albums") as? JsonObject)?.get("data") as? JsonArray)
            ?.mapNotNull { (it as? JsonObject)?.let(::deezerJsonToEchoAlbum) }
            .orEmpty()

        val shelves = buildList<Shelf> {
            add(
                Shelf.Category(
                    id = "flac_bridge_status",
                    title = "FLAC Bridge Multi",
                    subtitle = statusSubtitle,
                    backgroundColor = "#0E151B"
                )
            )
            if (chartTracks.isNotEmpty()) {
                add(Shelf.Lists.Tracks("chart_tracks", "Brani popolari (Catalogo FLAC)", chartTracks))
            }
            if (chartAlbums.isNotEmpty()) {
                add(Shelf.Lists.Items("chart_albums", "Album in evidenza", chartAlbums))
            }
        }
        return shelves.toFeed()
    }

    suspend fun loadStandaloneAlbum(album: Album): Album {
        val dzrId = album.id.removePrefix("dzr:album:").takeIf { it != album.id } ?: return album
        val obj = runCatching {
            requestJson("${deezerApiBase.trimEnd('/')}/album/$dzrId", deadlineMs = clock() + 5000L, timeoutMs = 3500L)
        }.getOrNull() ?: return album
        return deezerJsonToEchoAlbum(obj) ?: album
    }

    suspend fun loadStandaloneAlbumTracks(album: Album): Feed<Track>? {
        val dzrId = album.id.removePrefix("dzr:album:").takeIf { it != album.id } ?: return null
        val obj = runCatching {
            requestJson("${deezerApiBase.trimEnd('/')}/album/$dzrId", deadlineMs = clock() + 5000L, timeoutMs = 3500L)
        }.getOrNull() ?: return null
        val coverUrl = obj.stringField("cover_xl") ?: obj.stringField("cover_big") ?: ""
        val albumModel = deezerJsonToEchoAlbum(obj) ?: album
        val items = ((obj["tracks"] as? JsonObject)?.get("data") as? JsonArray)
            ?.mapNotNull { elem ->
                val tObj = elem as? JsonObject ?: return@mapNotNull null
                deezerJsonToEchoTrack(tObj, fallbackAlbum = albumModel, fallbackCover = coverUrl)
            }
            .orEmpty()
        return items.toFeed()
    }

    suspend fun loadStandaloneArtist(artist: Artist): Artist {
        val dzrId = artist.id.removePrefix("dzr:artist:").takeIf { it != artist.id } ?: return artist
        val obj = runCatching {
            requestJson("${deezerApiBase.trimEnd('/')}/artist/$dzrId", deadlineMs = clock() + 5000L, timeoutMs = 3500L)
        }.getOrNull() ?: return artist
        val name = obj.stringField("name") ?: artist.name
        val pic = obj.stringField("picture_xl") ?: obj.stringField("picture_big")
        val fans = obj.doubleField("nb_fan")?.toLong()
        return artist.copy(
            name = name,
            cover = pic?.takeIf(String::isNotBlank)?.toImageHolder() ?: artist.cover,
            subtitle = fans?.let { "$it fan" } ?: artist.subtitle
        )
    }

    suspend fun loadStandaloneArtistFeed(artist: Artist): Feed<Shelf> {
        val dzrId = artist.id.removePrefix("dzr:artist:").takeIf { it != artist.id }
            ?: return emptyList<Shelf>().toFeed()
        val obj = runCatching {
            requestJson("${deezerApiBase.trimEnd('/')}/artist/$dzrId/top?limit=25", deadlineMs = clock() + 5000L, timeoutMs = 3500L)
        }.getOrNull()
        val tracks = (obj?.get("data") as? JsonArray)
            ?.mapNotNull { (it as? JsonObject)?.let(::deezerJsonToEchoTrack) }
            .orEmpty()
        val shelves = if (tracks.isNotEmpty()) {
            listOf<Shelf>(Shelf.Lists.Tracks("artist_top_$dzrId", "Brani popolari", tracks))
        } else emptyList()
        return shelves.toFeed()
    }

    private suspend fun searchTidalTracks(query: String): List<Track> = runCatching {
        val url = (tidalApiBase.trimEnd('/') + "/search/tracks").toHttpUrl().newBuilder()
            .addQueryParameter("query", query.take(240))
            .addQueryParameter("countryCode", "US")
            .addQueryParameter("limit", "20")
            .build()
            .toString()
        val headers = mapOf("Accept" to "application/json", "x-tidal-token" to TIDAL_PUBLIC_TOKEN)
        val obj = requestJson(url, headers, clock() + 5000L, 3500L) ?: return@runCatching emptyList()
        val items = (obj["items"] as? JsonArray).orEmpty()
        items.mapNotNull { elem ->
            val t = elem as? JsonObject ?: return@mapNotNull null
            val id = TrackMatcher.validNumericId(t.stringField("id")).takeIf(String::isNotEmpty) ?: return@mapNotNull null
            val title = buildString {
                append(t.stringField("title").orEmpty())
                val ver = t.stringField("version").orEmpty()
                if (ver.isNotEmpty()) append(" (").append(ver).append(')')
            }.trim().takeIf(String::isNotEmpty) ?: return@mapNotNull null
            val artists = ((t["artists"] as? JsonArray)?.mapNotNull { a ->
                val aObj = a as? JsonObject ?: return@mapNotNull null
                val aName = aObj.stringField("name")?.takeIf(String::isNotEmpty) ?: return@mapNotNull null
                val aId = aObj.stringField("id").orEmpty()
                Artist("tid:artist:$aId", aName)
            }).orEmpty()
            val albumObj = t["album"] as? JsonObject
            val coverUuid = albumObj?.stringField("cover").orEmpty()
            val cover = if (coverUuid.isNotEmpty()) {
                "https://resources.tidal.com/images/${coverUuid.replace('-', '/')}/640x640.jpg".toImageHolder()
            } else null
            val album = albumObj?.stringField("title")?.takeIf(String::isNotEmpty)?.let { albTitle ->
                Album(id = "tid:album:${albumObj.stringField("id").orEmpty()}", title = albTitle, cover = cover, artists = artists)
            }
            val durationMs = ((t.doubleField("duration") ?: 0.0) * 1000.0).toLong().takeIf { it > 0L }
            val isrc = TrackMatcher.cleanIsrc(t.stringField("isrc")).ifEmpty { null }
            Track(
                id = "tid:track:$id",
                title = title,
                cover = cover,
                artists = artists,
                album = album,
                duration = durationMs,
                isrc = isrc,
                isExplicit = t.booleanField("explicit") == true,
                extras = buildMap {
                    put("flac_provider_hint", "tidal")
                    put("flac_provider_track_id", id)
                    if (isrc != null) put("isrc", isrc)
                }
            )
        }
    }.getOrDefault(emptyList())

    private suspend fun searchQobuzTracks(query: String): List<Track> = runCatching {
        val url = (qobuzApiBase.trimEnd('/') + "/track/search").toHttpUrl().newBuilder()
            .addQueryParameter("query", query.take(240))
            .addQueryParameter("limit", "20")
            .addQueryParameter("offset", "0")
            .build()
            .toString()
        val headers = mapOf("Accept" to "application/json", "X-App-Id" to QOBUZ_PUBLIC_APP_ID)
        val obj = requestJson(url, headers, clock() + 5000L, 3500L) ?: return@runCatching emptyList()
        val items = ((obj["tracks"] as? JsonObject)?.get("items") as? JsonArray).orEmpty()
        items.mapNotNull { elem ->
            val t = elem as? JsonObject ?: return@mapNotNull null
            val id = TrackMatcher.validNumericId(t.stringField("id")).takeIf(String::isNotEmpty) ?: return@mapNotNull null
            val title = buildString {
                append(t.stringField("title").orEmpty())
                val ver = t.stringField("version").orEmpty()
                if (ver.isNotEmpty()) append(" (").append(ver).append(')')
            }.trim().takeIf(String::isNotEmpty) ?: return@mapNotNull null
            val performer = (t["performer"] as? JsonObject)?.stringField("name")
                ?: ((t["album"] as? JsonObject)?.get("artist") as? JsonObject)?.stringField("name")
            val artists = performer?.takeIf(String::isNotEmpty)?.let { listOf(Artist("qbz:artist:$it", it)) }.orEmpty()
            val albumObj = t["album"] as? JsonObject
            val imgObj = albumObj?.get("image") as? JsonObject
            val coverUrl = imgObj?.stringField("large") ?: imgObj?.stringField("small")
            val cover = coverUrl?.takeIf(String::isNotBlank)?.toImageHolder()
            val album = albumObj?.stringField("title")?.takeIf(String::isNotEmpty)?.let { albTitle ->
                Album(id = "qbz:album:${albumObj.stringField("id").orEmpty()}", title = albTitle, cover = cover, artists = artists)
            }
            val durationMs = ((t.doubleField("duration") ?: 0.0) * 1000.0).toLong().takeIf { it > 0L }
            val isrc = TrackMatcher.cleanIsrc(t.stringField("isrc")).ifEmpty { null }
            Track(
                id = "qbz:track:$id",
                title = title,
                cover = cover,
                artists = artists,
                album = album,
                duration = durationMs,
                isrc = isrc,
                isExplicit = t.booleanField("parental_warning") == true,
                extras = buildMap {
                    put("flac_provider_hint", "qobuz")
                    put("flac_provider_track_id", id)
                    if (isrc != null) put("isrc", isrc)
                }
            )
        }
    }.getOrDefault(emptyList())

    private suspend fun searchDeezerTracksAndAlbums(query: String): Pair<List<Track>, List<Album>> = coroutineScope {
        val tracksDeferred = async {
            runCatching {
                val url = (deezerApiBase.trimEnd('/') + "/search").toHttpUrl().newBuilder()
                    .addQueryParameter("q", query.take(240))
                    .addQueryParameter("limit", "20")
                    .build()
                    .toString()
                val obj = requestJson(url, deadlineMs = clock() + 5000L, timeoutMs = 3500L)
                ((obj?.get("data") as? JsonArray)?.mapNotNull { (it as? JsonObject)?.let(::deezerJsonToEchoTrack) }).orEmpty()
            }.getOrDefault(emptyList())
        }
        val albumsDeferred = async {
            runCatching {
                val url = (deezerApiBase.trimEnd('/') + "/search/album").toHttpUrl().newBuilder()
                    .addQueryParameter("q", query.take(240))
                    .addQueryParameter("limit", "10")
                    .build()
                    .toString()
                val obj = requestJson(url, deadlineMs = clock() + 5000L, timeoutMs = 3500L)
                ((obj?.get("data") as? JsonArray)?.mapNotNull { (it as? JsonObject)?.let(::deezerJsonToEchoAlbum) }).orEmpty()
            }.getOrDefault(emptyList())
        }
        tracksDeferred.await() to albumsDeferred.await()
    }

    private fun deezerJsonToEchoTrack(
        t: JsonObject,
        fallbackAlbum: Album? = null,
        fallbackCover: String = ""
    ): Track? {
        val id = TrackMatcher.validNumericId(t.stringField("id")).takeIf(String::isNotEmpty) ?: return null
        val title = (t.stringField("title") ?: t.stringField("title_short"))?.takeIf(String::isNotEmpty) ?: return null
        val artistObj = t["artist"] as? JsonObject
        val artists = artistObj?.stringField("name")?.takeIf(String::isNotEmpty)?.let { aName ->
            listOf(Artist("dzr:artist:${artistObj.stringField("id").orEmpty()}", aName))
        }.orEmpty()
        val albumObj = t["album"] as? JsonObject
        val coverUrl = albumObj?.stringField("cover_xl")
            ?: albumObj?.stringField("cover_big")
            ?: albumObj?.stringField("cover_medium")
            ?: fallbackCover
        val cover = coverUrl.takeIf(String::isNotBlank)?.toImageHolder() ?: fallbackAlbum?.cover
        val album = albumObj?.stringField("title")?.takeIf(String::isNotEmpty)?.let { albTitle ->
            Album(
                id = "dzr:album:${albumObj.stringField("id").orEmpty()}",
                title = albTitle,
                cover = cover,
                artists = artists
            )
        } ?: fallbackAlbum
        val durationMs = ((t.doubleField("duration") ?: 0.0) * 1000.0).toLong().takeIf { it > 0L }
        val isrc = TrackMatcher.cleanIsrc(t.stringField("isrc")).ifEmpty { null }
        return Track(
            id = "dzr:track:$id",
            title = title,
            cover = cover,
            artists = artists,
            album = album,
            duration = durationMs,
            isrc = isrc,
            isExplicit = t.booleanField("explicit_lyrics") == true,
            extras = buildMap {
                put("flac_provider_hint", "deezer")
                put("flac_provider_track_id", id)
                if (isrc != null) put("isrc", isrc)
            }
        )
    }

    private fun deezerJsonToEchoAlbum(a: JsonObject): Album? {
        val id = TrackMatcher.validNumericId(a.stringField("id")).takeIf(String::isNotEmpty) ?: return null
        val title = a.stringField("title")?.takeIf(String::isNotEmpty) ?: return null
        val coverUrl = a.stringField("cover_xl") ?: a.stringField("cover_big") ?: a.stringField("cover_medium")
        val artistObj = a["artist"] as? JsonObject
        val artists = artistObj?.stringField("name")?.takeIf(String::isNotEmpty)?.let { aName ->
            listOf(Artist("dzr:artist:${artistObj.stringField("id").orEmpty()}", aName))
        }.orEmpty()
        return Album(
            id = "dzr:album:$id",
            title = title,
            cover = coverUrl?.takeIf(String::isNotBlank)?.toImageHolder(),
            artists = artists,
            trackCount = a.doubleField("nb_tracks")?.toLong(),
            isExplicit = a.booleanField("explicit_lyrics") == true
        )
    }

    private fun mergeStandaloneTracks(
        tidal: List<Track>,
        qobuz: List<Track>,
        deezer: List<Track>
    ): List<Track> {
        val seenKeys = LinkedHashSet<String>()
        val preferredOrder = sessionManager.preferenceOrder().map { it.key }
        val byProvider = mapOf("tidal" to tidal, "qobuz" to qobuz, "deezer" to deezer)
        val orderedLists = preferredOrder.mapNotNull { byProvider[it] } +
            listOf(tidal, qobuz, deezer)
        val out = mutableListOf<Track>()
        for (list in orderedLists) {
            for (track in list) {
                val key = track.isrc?.takeIf { it.length == 12 }
                    ?: "${TrackMatcher.normalize(track.title)}|${TrackMatcher.normalize(track.artists.firstOrNull()?.name)}"
                if (seenKeys.add(key)) {
                    out.add(track)
                }
            }
        }
        return out
    }
}
