package dev.brahmkshatriya.echo.extension

import dev.brahmkshatriya.echo.common.MusicExtension
import dev.brahmkshatriya.echo.common.clients.ExtensionClient
import dev.brahmkshatriya.echo.common.clients.HomeFeedClient
import dev.brahmkshatriya.echo.common.clients.SearchFeedClient
import dev.brahmkshatriya.echo.common.clients.TrackClient
import dev.brahmkshatriya.echo.common.helpers.Injectable
import dev.brahmkshatriya.echo.common.helpers.WebViewClient
import dev.brahmkshatriya.echo.common.helpers.WebViewRequest
import dev.brahmkshatriya.echo.common.models.Artist
import dev.brahmkshatriya.echo.common.models.ExtensionType
import dev.brahmkshatriya.echo.common.models.Feed
import dev.brahmkshatriya.echo.common.models.Feed.Companion.loadAll
import dev.brahmkshatriya.echo.common.models.Feed.Companion.toFeed
import dev.brahmkshatriya.echo.common.models.ImportType
import dev.brahmkshatriya.echo.common.models.Message
import dev.brahmkshatriya.echo.common.models.Metadata
import dev.brahmkshatriya.echo.common.models.NetworkRequest
import dev.brahmkshatriya.echo.common.models.Shelf
import dev.brahmkshatriya.echo.common.models.Streamable
import dev.brahmkshatriya.echo.common.models.Streamable.Media.Companion.toServerMedia
import dev.brahmkshatriya.echo.common.models.Track
import dev.brahmkshatriya.echo.common.settings.Setting
import dev.brahmkshatriya.echo.common.settings.SettingCategory
import dev.brahmkshatriya.echo.common.settings.SettingOnClick
import dev.brahmkshatriya.echo.common.settings.Settings
import dev.brahmkshatriya.echo.extension.bridge.DiagnosticLogBuffer
import dev.brahmkshatriya.echo.extension.bridge.FlacStreamResolver
import dev.brahmkshatriya.echo.extension.bridge.ProviderSpec
import dev.brahmkshatriya.echo.extension.bridge.StoredSession
import dev.brahmkshatriya.echo.extension.bridge.TrackMatcher
import dev.brahmkshatriya.echo.extension.bridge.ZarzCrypto
import dev.brahmkshatriya.echo.extension.bridge.ZarzSessionManager
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runTest
import okhttp3.Interceptor
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Response
import okhttp3.ResponseBody.Companion.toResponseBody
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

@OptIn(ExperimentalCoroutinesApi::class)
class FlacBridgeExtensionTest {

    private val flacHeaderAndPayload = byteArrayOf(
        0x66, 0x4C, 0x61, 0x43, // "fLaC"
        0x00, 0x00, 0x00, 0x22
    )

    @Test
    fun zarzCryptoProducesDeterministicRollingHmacAndParsesCallbacks() {
        val session = StoredSession(
            id = "sess_123",
            secret = "super_secret_abc",
            expiresAt = "2026-11-08T12:00:00.000Z",
            installId = "0123456789abcdef0123456789abcdef"
        )
        // Exact multiple of 1000ms to ensure .000Z is preserved identically to JS toISOString()
        val nowMs = 1_762_560_000_000L
        val ts = ZarzCrypto.formatIsoTimestamp(nowMs)
        assertTrue("Timestamp must include .000Z: $ts", ts.endsWith(".000Z"))

        val headers = ZarzCrypto.signedHeaders(
            provider = ProviderSpec.TIDAL,
            session = session,
            method = "POST",
            path = "/tickets",
            body = """{"capability":"download_ticket"}""",
            nowMs = nowMs,
            explicitNonce = "fixed_nonce_000000000001"
        )
        assertEquals("sess_123", headers["X-Zarz-Session"])
        assertEquals(ts, headers["X-Zarz-Timestamp"])
        assertEquals("tidal-web@1.2.5", headers["X-Zarz-App-Version"])
        assertEquals("extension", headers["X-Zarz-Platform"])
        assertEquals(43, headers["X-Zarz-Signature"]?.length)

        // Nested & fragment callback parsing
        val direct = ZarzCrypto.parseCallbackValues("spotiflac://session-grant?cb_version=v2grant&state=st1&grant=gr_ok")
        assertEquals("st1", direct["state"])
        assertEquals("gr_ok", direct["grant"])

        val fragment = ZarzCrypto.parseCallbackValues("spotiflac://session-grant#state=st2&code=cd_ok")
        assertEquals("st2", fragment["state"])
        assertEquals("cd_ok", fragment["code"])
    }

    @Test
    fun spotifyAndAmazonMatchersHandleEchoSpotifyUris() {
        val echoSpotifyTrack = Track(
            id = "spotify:track:3AJwUDP919kvQ9QcozQPxg",
            title = "Yellow",
            artists = listOf(Artist("spotify:artist:4gzpq5DPGxSnKTe4SA8HAU", "Coldplay")),
            duration = 266_773L,
            isrc = "GBAYE0000267"
        )
        val spec = TrackMatcher.toMatchSpec(echoSpotifyTrack)
        assertEquals("3AJwUDP919kvQ9QcozQPxg", spec.spotifyId)
        assertEquals("GBAYE0000267", spec.isrc)

        assertEquals(
            "B08XYZ1234",
            TrackMatcher.extractAmazonAsin("https://music.amazon.it/albums/B0001?trackAsin=b08xyz1234&do=play")
        )
        assertEquals(
            "B07ABCDEFG",
            TrackMatcher.extractAmazonAsin("https://music.amazon.com/tracks/B07ABCDEFG")
        )
        assertEquals("", TrackMatcher.extractAmazonAsin("https://evil.example.com/tracks/B07ABCDEFG"))
    }

    @Test
    fun bootstrapWebViewVerificationRenewal429AndReadOnlyDiagnosticsWork() = runTest {
        val now = AtomicLong(1_762_560_000_000L)
        val networkCalls = AtomicInteger(0)
        var nextBootstrap429 = false
        var nextRefresh429 = false

        val client = mockHttpClient { req ->
            networkCalls.incrementAndGet()
            val path = req.url.encodedPath
            when {
                path == "/v2/bootstrap" -> {
                    if (nextBootstrap429) {
                        jsonResponse(req, 429, """{"retry_after_seconds":1200}""", mapOf("Retry-After" to "1200"))
                    } else {
                        val app = req.url.queryParameter("app_version").orEmpty()
                        if (app.startsWith("tidal")) {
                            // Require interactive WebView challenge for Tidal
                            jsonResponse(req, 200, """{"challenge_id":"ch_tid_1","server_nonce":"nonce_1","expires_in":180}""")
                        } else {
                            // Immediate grant for others
                            jsonResponse(
                                req,
                                200,
                                """{"session_id":"s_${app.take(3)}","session_secret":"sec_$app","expires_at":"2026-11-08T10:00:00.000Z"}"""
                            )
                        }
                    }
                }
                path == "/v2/session/exchange" -> {
                    jsonResponse(
                        req,
                        200,
                        """{"session_id":"s_tidal_verified","session_secret":"sec_tidal","expires_at":"2026-11-08T10:00:00.000Z"}"""
                    )
                }
                path == "/v2/session/refresh" -> {
                    if (nextRefresh429) {
                        jsonResponse(req, 429, """{"retry_after_seconds":900}""", mapOf("Retry-After" to "900"))
                    } else {
                        jsonResponse(
                            req,
                            200,
                            """{"session_id":"s_tidal_refreshed","session_secret":"sec_tidal_2","expires_at":"2026-11-09T10:00:00.000Z"}"""
                        )
                    }
                }
                else -> jsonResponse(req, 404, """{"code":"NOT_FOUND"}""")
            }
        }

        val logs = DiagnosticLogBuffer(40)
        val notifications = mutableListOf<String>()
        val settings = MockedSettings()
        val scope = CoroutineScope(SupervisorJob() + UnconfinedTestDispatcher(testScheduler))
        val manager = ZarzSessionManager(
            baseHttpClient = client,
            logs = logs,
            onNotify = { notifications.add(it) },
            clock = { now.get() }
        )
        manager.attachSettings(settings)
        manager.attachWebViewClient(object : WebViewClient {
            override suspend fun await(
                showWebView: Boolean,
                reason: String,
                request: WebViewRequest<String>
            ): Result<String?> {
                // Verify modern User-Agent override is present so Turnstile/Cloudflare challenges work
                val ua = request.initialUrl.lowerCaseHeaders["user-agent"].orEmpty()
                assertTrue("Modern Chrome UA must be set", ua.contains("Chrome/131"))
                val cbRaw = request.initialUrl.url.substringAfter("cb=")
                val decodedCb = URLDecoder.decode(cbRaw, StandardCharsets.UTF_8)
                val fullCallback = "$decodedCb&grant=valid_grant_token_123"
                val intercepted = listOf(NetworkRequest(fullCallback))
                val out = (request as WebViewRequest.Headers<String>).onStop(intercepted)
                return Result.success(out)
            }
        })

        // Startup automatic verification verifies all 4 providers (Tidal via WebView, others via bootstrap)
        assertTrue(manager.startAutomatic(scope, reset = false, reason = "avvio"))
        manager.awaitAutomaticIdle()
        assertEquals("s_tidal_verified", manager.getStoredSession(ProviderSpec.TIDAL)?.id)
        assertNotNull(manager.getStoredSession(ProviderSpec.QOBUZ))

        // Show waits is strictly read-only: must perform ZERO network calls
        val beforeReadOnly = networkCalls.get()
        manager.showVerificationWaits()
        assertEquals(beforeReadOnly, networkCalls.get())

        // Advance clock by 6 hours + 1 minute -> periodic renewal becomes due
        now.addAndGet(6L * 60L * 60L * 1000L + 60_000L)
        assertTrue(manager.scheduleRenewalIfDue(scope))
        manager.awaitAutomaticIdle()
        assertEquals("s_tidal_refreshed", manager.getStoredSession(ProviderSpec.TIDAL)?.id)

        // Advance another 6 hours + 1 minute, but simulate 429 on /session/refresh -> session MUST be preserved!
        nextRefresh429 = true
        now.addAndGet(6L * 60L * 60L * 1000L + 60_000L)
        assertTrue(manager.scheduleRenewalIfDue(scope))
        manager.awaitAutomaticIdle()
        assertEquals("s_tidal_refreshed", manager.getStoredSession(ProviderSpec.TIDAL)?.id)
        assertTrue("429 cooldown must be recorded", manager.verificationWaitMs() >= 15L * 60L * 1000L)

        // Forget all sessions clears sessions & pauses auto_verify, while keeping 429 server cooldown
        manager.forgetAllSessions()
        assertNull(manager.getStoredSession(ProviderSpec.TIDAL))
        assertFalse(manager.automaticAllowed())
        assertTrue("Server 429 cooldown must survive forget", manager.verificationWaitMs() > 0L)

        // Explicit Force Verify can send ONE early attempt despite stored server wait (and respects 429 if server refuses)
        nextBootstrap429 = true
        val ext = FlacBridgeExtension(client, logs, manager, null, scope, startHeartbeat = false)
        ext.setSettings(settings)
        val buttons = ext.getSettingItems()
            .filterIsInstance<SettingCategory>()
            .flatMap { it.items }
            .filterIsInstance<SettingOnClick>()
            .associateBy { it.key }
        buttons.getValue("force_verify").onClick.invoke()
        assertTrue(logs.snapshot().any { it.contains("HTTP 429") })
    }

    @Test
    fun allFourProvidersBridgeModeFallbackAndDashDownloadWorkEndToEnd() = runTest {
        val now = AtomicLong(1_762_560_000_000L)
        var qobuzReturn503 = false
        var tidalReturnDash = false
        var tidalHasTrack = true
        var qobuzSamplePreview = false
        var deezerEncrypted = false

        val validDashMpd = """
            <?xml version="1.0" encoding="UTF-8"?>
            <MPD xmlns="urn:mpeg:dash:schema:mpd:2011" profiles="urn:mpeg:dash:profile:isoff-main:2011">
              <Period duration="PT266S">
                <AdaptationSet mimeType="audio/mp4" codecs="flac">
                  <Representation id="1" bandwidth="1411200" codecs="flac" audioSamplingRate="44100">
                    <SegmentTemplate initialization="https://sp-pr-cf.audio.tidal.com/init.mp4" media="https://sp-pr-cf.audio.tidal.com/seg_${'$'}Number${'$'}.m4s" startNumber="1">
                      <SegmentTimeline>
                        <S d="176400" r="1"/>
                      </SegmentTimeline>
                    </SegmentTemplate>
                  </Representation>
                </AdaptationSet>
              </Period>
            </MPD>
        """.trimIndent()
        val encodedDash = Base64.getEncoder().encodeToString(validDashMpd.toByteArray(StandardCharsets.UTF_8))

        val btsJson = """{"mimeType":"audio/flac","codecs":"flac","encryptionType":"NONE","urls":["https://sp-pr-cf.audio.tidal.com/direct_track.flac"]}"""
        val encodedBts = Base64.getEncoder().encodeToString(btsJson.toByteArray(StandardCharsets.UTF_8))

        val client = mockHttpClient { req ->
            val host = req.url.host
            val path = req.url.encodedPath
            when {
                host == "tidal.com" && path == "/v1/tracks" -> {
                    if (!tidalHasTrack) jsonResponse(req, 200, """{"items":[]}""")
                    else jsonResponse(
                        req,
                        200,
                        """{"items":[{"id":998877,"title":"Yellow","duration":267,"isrc":"GBAYE0000267","audioQuality":"HI_RES_LOSSLESS","streamReady":true,"allowStreaming":true,"artists":[{"id":1,"name":"Coldplay"}]}]}"""
                    )
                }
                host == "www.qobuz.com" && path.endsWith("/track/search") -> {
                    jsonResponse(
                        req,
                        200,
                        """{"tracks":{"items":[{"id":554433,"title":"Yellow","duration":267,"isrc":"GBAYE0000267","streamable":true,"performer":{"name":"Coldplay"}}]}}"""
                    )
                }
                host == "api.deezer.com" && path.startsWith("/track/isrc:") -> {
                    jsonResponse(
                        req,
                        200,
                        """{"id":332211,"title":"Yellow","duration":267,"isrc":"GBAYE0000267","readable":true,"artist":{"id":10,"name":"Coldplay"}}"""
                    )
                }
                host == "song.link" && path == "/s/3AJwUDP919kvQ9QcozQPxg" -> {
                    val html = """
                        <html><body>
                        <script id="__NEXT_DATA__" type="application/json">
                        {"props":{"pageProps":{"pageData":{"sections":[{"links":[{"platform":"amazonMusic","show":true,"url":"https://music.amazon.it/albums/B001?trackAsin=B00YELLOW1"}]}]}}}}
                        </script>
                        </body></html>
                    """.trimIndent()
                    Response.Builder()
                        .request(req)
                        .protocol(Protocol.HTTP_1_1)
                        .code(200)
                        .message("OK")
                        .body(html.toResponseBody("text/html".toMediaType()))
                        .build()
                }
                host == "api.zarz.moe" && path == "/v2/tickets" -> {
                    jsonResponse(req, 200, """{"ticket_id":"tkt_valid_001"}""")
                }
                host == "api.zarz.moe" && path == "/v2/dl/tid" -> {
                    val man = if (tidalReturnDash) encodedDash else encodedBts
                    jsonResponse(
                        req,
                        200,
                        """{"data":{"assetPresentation":"FULL","audioQuality":"HI_RES_LOSSLESS","bitDepth":24,"sampleRate":96000,"manifest":"$man"}}"""
                    )
                }
                host == "api.zarz.moe" && path == "/v2/dl/qbz" -> {
                    if (qobuzReturn503) {
                        jsonResponse(req, 503, """{"code":"PROVIDER_UNAVAILABLE","retry_after_seconds":120}""")
                    } else if (qobuzSamplePreview) {
                        jsonResponse(req, 200, """{"sample":true,"duration":30,"url":"https://streaming-qobuz-std.akamaized.net/preview.flac"}""")
                    } else {
                        jsonResponse(
                            req,
                            200,
                            """{"url":"https://streaming-qobuz-std.akamaized.net/yellow_cd.flac","bit_depth":16,"sampling_rate":44.1}"""
                        )
                    }
                }
                host == "api.zarz.moe" && path == "/v2/dl/dzr" -> {
                    if (deezerEncrypted) {
                        jsonResponse(req, 200, """{"success":true,"deezer_encrypted":true,"requires_client_decryption":true}""")
                    } else {
                        jsonResponse(
                            req,
                            200,
                            """{"success":true,"deezer_encrypted":false,"direct_downloadable":true,"format":"flac","direct_download_url":"https://cdns-preview-1.dzcdn.net/stream/yellow.flac","bit_depth":16,"sample_rate":44100}"""
                        )
                    }
                }
                host == "api.zarz.moe" && path == "/v2/dl/amazeamazeamaze" -> {
                    jsonResponse(
                        req,
                        200,
                        """[{"meta":{"isrc":"GBAYE0000267"},"audio":{"codec":"flac","encrypted":false,"bitDepth":24,"sampleRate":48000,"url":"https://d12345.cloudfront.net/amzn_yellow.flac"}}]"""
                    )
                }
                host == "sp-pr-cf.audio.tidal.com" && (path.endsWith(".mp4") || path.endsWith(".m4s")) -> {
                    binaryResponse(req, 200, byteArrayOf(0x01, 0x02, 0x03, 0x04))
                }
                req.header("Range") == "bytes=0-3" -> {
                    binaryResponse(req, 206, flacHeaderAndPayload, mapOf("Content-Range" to "bytes 0-3/25000000"))
                }
                else -> jsonResponse(req, 404, """{"error":"not_found"}""")
            }
        }

        val logs = DiagnosticLogBuffer(40)
        val settings = MockedSettings()
        val scope = CoroutineScope(SupervisorJob() + UnconfinedTestDispatcher(testScheduler))
        val manager = ZarzSessionManager(client, logs, onNotify = {}, clock = { now.get() })
        manager.attachSettings(settings)

        // Seed valid sessions for all 4 sources
        val installId = manager.installId()
        ProviderSpec.ALL.forEach { p ->
            val sessionJson = ZarzSessionManager.json.parseToJsonElement(
                """{"session_id":"sid_${p.key}","session_secret":"sec_${p.key}","expires_at":"2026-11-10T00:00:00.000Z"}"""
            ).let { it as kotlinx.serialization.json.JsonObject }
            assertTrue(manager.saveSession(p, sessionJson, installId, null))
        }

        val resolver = FlacStreamResolver(client, manager, clock = { now.get() })
        val bridge = FlacBridgeExtension(client, logs, manager, resolver, scope, startHeartbeat = false)
        bridge.setSettings(settings)

        // Attach a simulated Spotify Metadata Extension with both OGG server and Canvas background!
        val fakeSpotify = FakeMetadataExtension()
        val spotifyMusicExt = MusicExtension(
            Metadata("", "", ImportType.File, ExtensionType.MUSIC, "echo_spotify", "Spotify", "1.0", "", ""),
            Injectable<ExtensionClient>({ fakeSpotify }, emptyList())
        )
        bridge.setMusicExtensions(listOf(spotifyMusicExt))

        val inputTrack = Track(
            id = "spotify:track:3AJwUDP919kvQ9QcozQPxg",
            title = "Yellow",
            artists = listOf(Artist("spotify:artist:1", "Coldplay"))
        )

        // 1. Tidal preferred -> exposes Auto + manual server chips (Tidal, Qobuz, Deezer, Amazon) + Spotify fallback + Canvas!
        assertEquals("highest", settings.getString(ZarzSessionManager.KEY_ECHO_STREAM_QUALITY))
        val loadedTidal = bridge.loadTrack(inputTrack, isDownload = false)
        assertEquals("Auto · Tidal", loadedTidal.servers.maxBy { it.quality }.title)
        assertEquals(
            listOf(
                "Auto · Tidal",
                "Tidal · FLAC · 24-bit · 96 kHz",
                "Qobuz · FLAC",
                "Deezer · FLAC",
                "Amazon Music · FLAC",
                "Spotify · OGG Vorbis 320"
            ),
            loadedTidal.servers.map { it.title }
        )
        assertEquals(1, loadedTidal.backgrounds.size) // Spotify Canvas preserved!

        val mediaTidal = bridge.loadStreamableMedia(loadedTidal.servers.first(), isDownload = false) as Streamable.Media.Server
        val sourceTidal = mediaTidal.sources.single() as Streamable.Source.Http
        assertEquals(Streamable.SourceType.Progressive, sourceTidal.type)
        assertEquals("https://sp-pr-cf.audio.tidal.com/direct_track.flac", sourceTidal.request.url)

        // 1b. Manual server switching directly from player chips!
        val manualQobuzChip = loadedTidal.servers.first { it.title == "Qobuz · FLAC" }
        val manualQobuzMedia = bridge.loadStreamableMedia(manualQobuzChip, isDownload = false) as Streamable.Media.Server
        assertEquals(
            "https://streaming-qobuz-std.akamaized.net/yellow_cd.flac",
            (manualQobuzMedia.sources.single() as Streamable.Source.Http).request.url
        )

        val manualDeezerChip = loadedTidal.servers.first { it.title == "Deezer · FLAC" }
        val manualDeezerMedia = bridge.loadStreamableMedia(manualDeezerChip, isDownload = false) as Streamable.Media.Server
        assertEquals(
            "https://cdns-preview-1.dzcdn.net/stream/yellow.flac",
            (manualDeezerMedia.sources.single() as Streamable.Source.Http).request.url
        )

        val manualAmazonChip = loadedTidal.servers.first { it.title == "Amazon Music · FLAC" }
        val manualAmazonMedia = bridge.loadStreamableMedia(manualAmazonChip, isDownload = false) as Streamable.Media.Server
        assertEquals(
            "https://d12345.cloudfront.net/amzn_yellow.flac",
            (manualAmazonMedia.sources.single() as Streamable.Source.Http).request.url
        )

        // 2. Tidal DASH manifest: streaming returns SourceType.DASH data: URI, download returns concatenated Raw stream!
        tidalReturnDash = true
        now.addAndGet(300_000L) // Expire 4-min cache
        val loadedDash = bridge.loadTrack(inputTrack, isDownload = false)
        val dashStreamMedia = bridge.loadStreamableMedia(loadedDash.servers.first(), isDownload = false) as Streamable.Media.Server
        val dashHttpSource = dashStreamMedia.sources.single() as Streamable.Source.Http
        assertEquals(Streamable.SourceType.DASH, dashHttpSource.type)
        assertTrue(dashHttpSource.request.url.startsWith("data:application/dash+xml;base64,"))

        val dashDownloadMedia = bridge.loadStreamableMedia(loadedDash.servers.first(), isDownload = true) as Streamable.Media.Server
        val dashRawSource = dashDownloadMedia.sources.single() as Streamable.Source.Raw
        val (dlStream, _) = dashRawSource.streamProvider!!.provide(0L, -1L)
        val downloadedBytes = dlStream.readBytes()
        assertEquals(12, downloadedBytes.size) // init (4B) + seg_1 (4B) + seg_2 (4B)

        // 3. Qobuz preferred -> 503 pauses Qobuz temporarily and falls back cleanly; normal Qobuz verifies fLaC bytes
        settings.putString(ZarzSessionManager.KEY_PREFERRED, "Qobuz")
        tidalHasTrack = false
        qobuzSamplePreview = true // Reject 30s sample preview -> should fall through to Deezer!
        deezerEncrypted = true    // Encrypted Deezer skipped -> should fall through to Amazon Music!
        now.addAndGet(300_000L)

        val loadedAmazon = bridge.loadTrack(inputTrack, isDownload = false)
        assertEquals("Auto · Amazon Music", loadedAmazon.servers.first().title)
        assertTrue(loadedAmazon.servers.any { it.title == "Amazon Music · FLAC · 24-bit · 48 kHz" })

        // Strict manual click on Tidal when track is missing on Tidal must fail explicitly (never fake with another server)
        val missingTidalChip = loadedAmazon.servers.first { it.title == "Tidal · FLAC" }
        val manualMiss = runCatching { bridge.loadStreamableMedia(missingTidalChip, isDownload = false) }
        assertTrue("Manual server miss must report unavailable instead of faking", manualMiss.isFailure)

        // Enable unencrypted Deezer and test Deezer resolution
        deezerEncrypted = false
        settings.putString(ZarzSessionManager.KEY_PREFERRED, "Deezer")
        now.addAndGet(300_000L)
        val loadedDeezer = bridge.loadTrack(inputTrack, isDownload = false)
        assertEquals("Auto · Deezer", loadedDeezer.servers.first().title)
        assertTrue(loadedDeezer.servers.any { it.title == "Deezer · FLAC · 16-bit · 44.1 kHz" })

        // Enable normal Qobuz CD FLAC and test Qobuz resolution (44.1 -> 44100 Hz conversion)
        qobuzSamplePreview = false
        settings.putString(ZarzSessionManager.KEY_PREFERRED, "Qobuz")
        now.addAndGet(300_000L)
        val loadedQobuz = bridge.loadTrack(inputTrack, isDownload = false)
        assertEquals("Auto · Qobuz", loadedQobuz.servers.first().title)
        assertTrue(loadedQobuz.servers.any { it.title == "Qobuz · FLAC · 16-bit · 44.1 kHz" })

        // Simulate Qobuz 503 -> Qobuz paused in memory, falls back to Deezer automatically
        qobuzReturn503 = true
        now.addAndGet(300_000L)
        val after503 = bridge.loadTrack(inputTrack, isDownload = false)
        assertEquals("Auto · Deezer", after503.servers.first().title)
        assertTrue(manager.qobuzUnavailableUntil > now.get())

        // 4. When NO FLAC provider matches, Spotify original server has highest quality (900 > 800) so Echo selects Spotify immediately!
        manager.forgetAllSessions()
        now.addAndGet(300_000L)
        val fallbackTrack = bridge.loadTrack(inputTrack, isDownload = false)
        val topFallbackServer = fallbackTrack.servers.maxBy { it.quality }
        assertEquals("Spotify · OGG Vorbis 320", topFallbackServer.title)
        val spotifyMedia = bridge.loadStreamableMedia(topFallbackServer, isDownload = false) as Streamable.Media.Server
        assertEquals("https://audio-ak-spotify.akamaized.net/ogg320", (spotifyMedia.sources.single() as Streamable.Source.Http).request.url)
    }

    @Test
    fun standaloneModeSearchWorksWithoutMetadataExtension() = runTest {
        val client = mockHttpClient { req ->
            val host = req.url.host
            val path = req.url.encodedPath
            when {
                host == "tidal.com" && path == "/v1/search/tracks" -> jsonResponse(
                    req,
                    200,
                    """{"items":[{"id":998877,"title":"Yellow","duration":267,"isrc":"GBAYE0000267","explicit":false,"artists":[{"id":1,"name":"Coldplay"}],"album":{"id":10,"title":"Parachutes","cover":"11111111-2222-3333-4444-555555555555"}}]}"""
                )
                host == "www.qobuz.com" -> jsonResponse(req, 200, """{"tracks":{"items":[]}}""")
                host == "api.deezer.com" -> jsonResponse(req, 200, """{"data":[]}""")
                else -> jsonResponse(req, 404, "{}")
            }
        }
        val logs = DiagnosticLogBuffer(20)
        val settings = MockedSettings()
        val scope = CoroutineScope(SupervisorJob() + UnconfinedTestDispatcher(testScheduler))
        val manager = ZarzSessionManager(client, logs, onNotify = {})
        manager.attachSettings(settings)
        val resolver = FlacStreamResolver(client, manager)
        val standalone = FlacBridgeExtension(client, logs, manager, resolver, scope, startHeartbeat = false)
        standalone.setSettings(settings)

        val feed = standalone.loadSearchFeed("Coldplay Yellow")
        val shelves = feed.getPagedData(feed.tabs.first()).pagedData.loadAll()
        val tracks = shelves.filterIsInstance<Shelf.Lists.Tracks>().flatMap { it.list }
        assertEquals(1, tracks.size)
        assertEquals("tid:track:998877", tracks.single().id)
        assertEquals("GBAYE0000267", tracks.single().isrc)
    }

    private class FakeMetadataExtension : ExtensionClient, HomeFeedClient, SearchFeedClient, TrackClient {
        override suspend fun getSettingItems(): List<Setting> = emptyList()
        override fun setSettings(settings: Settings) {}

        override suspend fun loadHomeFeed(): Feed<Shelf> = emptyList<Shelf>().toFeed()
        override suspend fun loadSearchFeed(query: String): Feed<Shelf> = emptyList<Shelf>().toFeed()

        override suspend fun loadTrack(track: Track, isDownload: Boolean): Track {
            return Track(
                id = track.id,
                title = "Yellow",
                artists = listOf(Artist("spotify:artist:1", "Coldplay")),
                duration = 266_773L,
                isrc = "GBAYE0000267",
                streamables = listOf(
                    Streamable.server("sp_ogg_320", 320, "OGG Vorbis 320"),
                    Streamable.background("sp_canvas_1", 100, "Spotify Canvas")
                )
            )
        }

        override suspend fun loadStreamableMedia(streamable: Streamable, isDownload: Boolean): Streamable.Media {
            return when (streamable.type) {
                Streamable.MediaType.Background ->
                    Streamable.Media.Background(NetworkRequest("https://canvaz.scdn.co/canvas.mp4"))
                else ->
                    "https://audio-ak-spotify.akamaized.net/ogg320".toServerMedia()
            }
        }

        override suspend fun loadFeed(track: Track): Feed<Shelf>? = null
    }

    private fun mockHttpClient(handler: (okhttp3.Request) -> Response): OkHttpClient =
        OkHttpClient.Builder()
            .addInterceptor(Interceptor { chain ->
                val req = chain.request()
                val ua = req.header("User-Agent").orEmpty()
                if (!ua.contains("Chrome/131") || ua.startsWith("okhttp/", ignoreCase = true)) {
                    // Simulate Cloudflare WAF blocking non-browser / OkHttp User-Agent with HTTP 403
                    return@Interceptor jsonResponse(req, 403, """{"error":"Forbidden"}""")
                }
                handler(req)
            })
            .build()

    private fun jsonResponse(
        req: okhttp3.Request,
        code: Int,
        body: String,
        headers: Map<String, String> = emptyMap()
    ): Response {
        val builder = Response.Builder()
            .request(req)
            .protocol(Protocol.HTTP_1_1)
            .code(code)
            .message("HTTP $code")
            .header("Content-Type", "application/json")
        headers.forEach { (k, v) -> builder.header(k, v) }
        return builder.body(body.toResponseBody("application/json".toMediaType())).build()
    }

    private fun binaryResponse(
        req: okhttp3.Request,
        code: Int,
        bytes: ByteArray,
        headers: Map<String, String> = emptyMap()
    ): Response {
        val builder = Response.Builder()
            .request(req)
            .protocol(Protocol.HTTP_1_1)
            .code(code)
            .message("HTTP $code")
            .header("Content-Type", "audio/flac")
        headers.forEach { (k, v) -> builder.header(k, v) }
        return builder.body(bytes.toResponseBody("audio/flac".toMediaType())).build()
    }
}
