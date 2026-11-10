package dev.brahmkshatriya.echo.extension

import dev.brahmkshatriya.echo.common.models.Album
import dev.brahmkshatriya.echo.common.models.Artist
import dev.brahmkshatriya.echo.common.models.Track
import dev.brahmkshatriya.echo.extension.tidal.DiagnosticLogBuffer
import dev.brahmkshatriya.echo.extension.tidal.StoredSession
import dev.brahmkshatriya.echo.extension.tidal.TidalApi
import dev.brahmkshatriya.echo.extension.tidal.TidalConverter
import dev.brahmkshatriya.echo.extension.tidal.TidalCrypto
import dev.brahmkshatriya.echo.extension.tidal.TidalSessionManager
import dev.brahmkshatriya.echo.extension.tidal.TidalStreamResolver
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import okhttp3.OkHttpClient
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.nio.charset.StandardCharsets
import java.util.Base64

@OptIn(ExperimentalCoroutinesApi::class)
class TidalExtensionTest {

    @Test
    fun testTidalCryptoSignAndParseCallback() {
        val installId = TidalCrypto.generateInstallId()
        assertEquals(32, installId.length)
        assertTrue(installId.matches(Regex("^[a-f0-9]{32}$")))

        val session = StoredSession(
            id = "test-session",
            secret = "secret123",
            expiresAt = "2026-12-31T23:59:59Z",
            installId = "0123456789abcdef0123456789abcdef"
        )
        val headers = TidalCrypto.signedHeaders(
            session = session,
            method = "POST",
            path = "/ticket",
            body = """{"alias":"tid"}""",
            nowMs = 1700000000000L
        )

        assertEquals("test-session", headers["X-Zarz-Session"])
        assertNotNull(headers["X-Zarz-Signature"])

        val callbackUrl = "spotiflac://session-grant?grant=my-grant-code&state=xyz123"
        val parsed = TidalCrypto.parseCallback(callbackUrl)
        assertNotNull(parsed)
        assertEquals("my-grant-code", parsed?.first)
        assertEquals("xyz123", parsed?.second)

        assertNull(TidalCrypto.parseCallback("https://example.com"))
    }

    @Test
    fun testStoredSessionValidation() {
        val valid = StoredSession(
            id = "sess-1",
            secret = "sec-1",
            expiresAt = "2026-12-31T23:59:59Z",
            installId = "0123456789abcdef0123456789abcdef"
        )
        assertTrue(valid.isValid())
        assertNotNull(valid.expiresAtEpochMs())

        val invalid = StoredSession(
            id = "",
            secret = "sec-1",
            installId = "short"
        )
        assertFalse(invalid.isValid())
    }

    @Test
    fun testTidalConverterIdExtraction() {
        assertEquals("55391466", TidalConverter.extractNumericId("tid:track:55391466"))
        assertEquals("55391447", TidalConverter.extractNumericId("tid:album:55391447"))
        assertEquals("9706", TidalConverter.extractNumericId("tid:artist:9706"))
        assertEquals("94fe2b9b-096d-4b39-8129-d5b8e774e9b3", TidalConverter.extractNumericId("tid:playlist:94fe2b9b-096d-4b39-8129-d5b8e774e9b3"))
        assertEquals("12345", TidalConverter.extractNumericId("12345"))
    }

    @Test
    fun testTidalStreamResolverDecodeDirectFlac() {
        val resolver = TidalStreamResolver(
            baseHttpClient = OkHttpClient(),
            sessionManager = TidalSessionManager(OkHttpClient(), DiagnosticLogBuffer())
        )

        val directJson = """
            {
                "mimeType": "audio/flac",
                "codecs": "flac",
                "encryptionType": "NONE",
                "urls": ["https://audio.tidal.com/stream/track.flac"]
            }
        """.trimIndent()
        val base64Direct = Base64.getEncoder().encodeToString(directJson.toByteArray(StandardCharsets.UTF_8))

        val dataObj = JsonObject(
            mapOf(
                "audioQuality" to JsonPrimitive("LOSSLESS"),
                "manifest" to JsonPrimitive(base64Direct),
                "bitDepth" to JsonPrimitive(16),
                "sampleRate" to JsonPrimitive(44100)
            )
        )

        val decoded = resolver.decodeTidalData(dataObj)
        assertNotNull(decoded)
        assertEquals("https://audio.tidal.com/stream/track.flac", decoded?.first)
        assertNull(decoded?.second)
    }

    @Test
    fun testTidalStreamResolverDecodeDashMpd() {
        val resolver = TidalStreamResolver(
            baseHttpClient = OkHttpClient(),
            sessionManager = TidalSessionManager(OkHttpClient(), DiagnosticLogBuffer())
        )

        val mpdXml = """
            <MPD xmlns="urn:mpeg:dash:schema:mpd:2011">
                <Period>
                    <AdaptationSet>
                        <Representation codecs="flac">
                            <SegmentTemplate initialization="https://audio.tidal.com/init.mp4" media="https://audio.tidal.com/seg_${'$'}Number${'$'}.mp4" startNumber="1">
                                <SegmentTimeline>
                                    <S d="1000" r="2" />
                                </SegmentTimeline>
                            </SegmentTemplate>
                        </Representation>
                    </AdaptationSet>
                </Period>
            </MPD>
        """.trimIndent()
        val base64Mpd = Base64.getEncoder().encodeToString(mpdXml.toByteArray(StandardCharsets.UTF_8))

        val dataObj = JsonObject(
            mapOf(
                "audioQuality" to JsonPrimitive("LOSSLESS"),
                "manifest" to JsonPrimitive(base64Mpd)
            )
        )

        val decoded = resolver.decodeTidalData(dataObj)
        assertNotNull(decoded)
        assertNull(decoded?.first)
        assertEquals(base64Mpd, decoded?.second)
    }

    @Test
    fun testTidalApiLiveSearchAndMetadata() = runTest {
        val api = TidalApi(OkHttpClient())
        val tracks = api.searchTracks("Pink Floyd", limit = 3)
        assertTrue(tracks.isNotEmpty())
        val firstTrack = tracks.first()
        assertTrue(firstTrack.title.isNotEmpty())
        assertTrue(firstTrack.id.startsWith("tid:track:"))
        assertTrue(firstTrack.artists.isNotEmpty())

        val albums = api.searchAlbums("Dark Side of the Moon", limit = 2)
        assertTrue(albums.isNotEmpty())
        val firstAlbum = albums.first()
        assertTrue(firstAlbum.title.isNotEmpty())
        assertTrue(firstAlbum.id.startsWith("tid:album:"))

        val trackDetails = api.getTrack(firstTrack.id)
        assertNotNull(trackDetails)
        assertEquals(firstTrack.id, trackDetails?.id)
    }

    @Test
    fun testTidalExtensionSettingsAndInitialization() = runTest {
        val ext = TidalExtension()
        val settings = MockedSettings()
        ext.setSettings(settings)
        ext.onInitialize()

        val settingItems = ext.getSettingItems()
        assertEquals(3, settingItems.size)
        assertEquals("cat_session", settingItems[0].key)
        assertEquals("cat_prefs", settingItems[1].key)
        assertEquals("cat_logs", settingItems[2].key)
    }
}
