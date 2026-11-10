package dev.brahmkshatriya.echo.extension.bridge

import dev.brahmkshatriya.echo.common.models.Track
import java.text.Normalizer
import java.util.Locale
import kotlin.math.abs

object TrackMatcher {
    private val NON_ALNUM_UPPER = Regex("[^A-Z0-9]")
    private val DIACRITICS = Regex("[\\u0300-\\u036f]+")
    private val ALLOWED_CHARS =
        Regex("[^a-z0-9\\u00c0-\\u024f\\u0370-\\u04ff\\u3040-\\u30ff\\u3400-\\u9fff\\uac00-\\ud7af]+")
    private val WHITESPACE = Regex("\\s+")
    private val NUMERIC_ID = Regex("^[0-9]{1,16}$")

    private val SPOTIFY_URI_ID = Regex("^spotify:track:([A-Za-z0-9]{22})$")
    private val SPOTIFY_URL_ID =
        Regex("^https?://open\\.spotify\\.com/(?:intl-[a-z-]+/)?track/([A-Za-z0-9]{22})(?:[/?#].*)?$", RegexOption.IGNORE_CASE)
    private val BARE_SPOTIFY_ID = Regex("^[A-Za-z0-9]{22}$")

    private val YT_URI_ID = Regex("^(?:youtube:video:|yt:)([A-Za-z0-9_-]{11})$")
    private val YT_URL_ID =
        Regex("^https?://(?:(?:www|music)\\.youtube\\.com/watch\\?.*v=|youtu\\.be/)([A-Za-z0-9_-]{11})(?:[&#?].*)?$", RegexOption.IGNORE_CASE)
    private val BARE_YT_ID = Regex("^[A-Za-z0-9_-]{11}$")

    private val AMAZON_HOST =
        Regex("^https://music\\.amazon\\.(?:com|co\\.uk|co\\.jp|de|fr|it|es|in|com\\.br|com\\.mx|com\\.au)(?:/|$)", RegexOption.IGNORE_CASE)
    private val AMAZON_QUERY_ASIN = Regex("[?&]trackasin=([a-z0-9]{10})(?:[&#]|$)", RegexOption.IGNORE_CASE)
    private val AMAZON_PATH_ASIN = Regex("^https://[^/]+/tracks?/([a-z0-9]{10})(?:[/?#]|$)", RegexOption.IGNORE_CASE)

    private val TRUSTED_HOST_URL = Regex("^https://([a-z0-9-]+(?:\\.[a-z0-9-]+)+)(?::443)?(?:/|$)", RegexOption.IGNORE_CASE)
    private val TIDAL_AUDIO_HOST = Regex("^(?:[a-z0-9-]+\\.)*audio\\.tidal\\.com$")
    private val DEEZER_CDN_HOST = Regex("^(?:[a-z0-9-]+\\.)+dzcdn\\.net$")
    private val CLOUDFRONT_HOST = Regex("^(?:[a-z0-9-]+\\.)+cloudfront\\.net$")

    fun cleanIsrc(value: String?): String {
        val s = value?.trim()?.uppercase(Locale.ROOT)?.replace(NON_ALNUM_UPPER, "").orEmpty()
        return if (s.length == 12) s else ""
    }

    fun normalize(value: String?): String {
        var s = value?.trim()?.lowercase(Locale.ROOT).orEmpty()
        if (s.isEmpty()) return ""
        s = runCatching {
            Normalizer.normalize(s, Normalizer.Form.NFD).replace(DIACRITICS, "")
        }.getOrDefault(s)
        return s
            .replace("&", " and ")
            .replace(ALLOWED_CHARS, " ")
            .replace(WHITESPACE, " ")
            .trim()
    }

    fun validDurationMs(durationMs: Long?): Long {
        val d = durationMs ?: return 0L
        return if (d in 1L..<86_400_000L) d else 0L
    }

    fun validNumericId(value: String?): String {
        val s = value?.trim().orEmpty()
        if (!NUMERIC_ID.matches(s)) return ""
        val asLong = s.toLongOrNull() ?: return ""
        return if (asLong in 1L..9_007_199_254_740_991L) s else ""
    }

    fun extractSpotifyTrackId(track: Track): String {
        val candidates = listOfNotNull(
            track.extras["spotify_id"],
            track.extras["spotify_uri"],
            track.id
        )
        for (raw in candidates) {
            val s = raw.trim()
            SPOTIFY_URI_ID.matchEntire(s)?.groupValues?.getOrNull(1)?.let { return it }
            SPOTIFY_URL_ID.matchEntire(s)?.groupValues?.getOrNull(1)?.let { return it }
            if (BARE_SPOTIFY_ID.matches(s)) return s
        }
        return ""
    }

    fun extractYoutubeVideoId(track: Track): String {
        val candidates = listOfNotNull(
            track.extras["yt_id"],
            track.extras["video_id"],
            track.id
        )
        for (raw in candidates) {
            val s = raw.trim()
            if (s.startsWith("spotify:") || s.startsWith("tid:") || s.startsWith("qbz:") || s.startsWith("dzr:")) {
                continue
            }
            YT_URI_ID.matchEntire(s)?.groupValues?.getOrNull(1)?.let { return it }
            YT_URL_ID.matchEntire(s)?.groupValues?.getOrNull(1)?.let { return it }
            if (BARE_YT_ID.matches(s)) return s
        }
        return ""
    }

    fun toMatchSpec(track: Track): TrackMatchSpec {
        val artistList = track.artists.mapNotNull { it.name.trim().takeIf(String::isNotEmpty) }
        val primaryArtist = artistList.firstOrNull().orEmpty()
        return TrackMatchSpec(
            id = track.id,
            title = track.title.trim(),
            artist = primaryArtist,
            artists = artistList,
            durationMs = validDurationMs(track.duration),
            isrc = cleanIsrc(track.isrc ?: track.extras["isrc"]),
            spotifyId = extractSpotifyTrackId(track),
            youtubeId = extractYoutubeVideoId(track),
            preferredProviderKey = track.extras["flac_provider_hint"].orEmpty(),
            providerTrackId = track.extras["flac_provider_track_id"].orEmpty()
        )
    }

    fun extractAmazonAsin(rawUrl: String?): String {
        val url = rawUrl?.trim()?.replace("&amp;", "&").orEmpty()
        if (!AMAZON_HOST.containsMatchIn(url)) return ""
        val q = AMAZON_QUERY_ASIN.find(url)?.groupValues?.getOrNull(1)
        val p = AMAZON_PATH_ASIN.find(url)?.groupValues?.getOrNull(1)
        return (q ?: p).orEmpty().trim().uppercase(Locale.ROOT)
    }

    fun isTrustedMediaUrl(kind: String, rawUrl: String?): Boolean {
        val url = rawUrl?.trim().orEmpty()
        val host = TRUSTED_HOST_URL.find(url)?.groupValues?.getOrNull(1)?.lowercase(Locale.ROOT)
            ?: return false
        return when (kind) {
            "tidal" -> TIDAL_AUDIO_HOST.matches(host)
            "qobuz" -> host == "streaming-qobuz-std.akamaized.net"
            "deezer" -> host == "dl.musicdl.me" || DEEZER_CDN_HOST.matches(host)
            "amazon" -> CLOUDFRONT_HOST.matches(host)
            else -> false
        }
    }

    fun matchCatalogueCandidate(
        wanted: TrackMatchSpec,
        rawId: String?,
        rawTitle: String?,
        rawVersion: String?,
        candidateArtists: List<String>,
        durationSeconds: Double?,
        rawIsrc: String?,
        audioQuality: String?,
        streamable: Boolean,
        kind: String,
        albumTitle: String = "",
        albumId: String = "",
        coverUrl: String = "",
        isExplicit: Boolean = false
    ): CatalogueHit? {
        if (!streamable) return null
        val number = validNumericId(rawId)
        if (number.isEmpty()) return null

        val expected = validDurationMs(wanted.durationMs)
        var actual = ((durationSeconds ?: 0.0) * 1000.0).toLong()
        if (actual <= 0L || actual > 86_400_000L) actual = 0L

        val wantedIsrc = cleanIsrc(wanted.isrc)
        val foundIsrc = cleanIsrc(rawIsrc)
        val isrcMatched = wantedIsrc.isNotEmpty() && wantedIsrc == foundIsrc

        if (wantedIsrc.isNotEmpty()) {
            if (!isrcMatched || (expected > 0L && actual > 0L && abs(expected - actual) > 10_000L)) {
                return null
            }
        } else {
            val fullTitle = buildString {
                append(rawTitle?.trim().orEmpty())
                val ver = rawVersion?.trim().orEmpty()
                if (ver.isNotEmpty()) {
                    append(' ').append(ver)
                }
            }
            val normWantedTitle = normalize(wanted.title)
            if (normWantedTitle.isEmpty() || normWantedTitle != normalize(fullTitle)) {
                return null
            }
            val normWantedArtist = normalize(wanted.primaryArtist())
            val sameArtist = candidateArtists.any { normalize(it) == normWantedArtist }
            if (normWantedArtist.isEmpty() || !sameArtist || expected <= 0L || actual <= 0L || abs(expected - actual) > 3_000L) {
                return null
            }
        }

        val diff = if (expected > 0L && actual > 0L) abs(expected - actual) else 0L
        val lossless = kind == "tidal" && audioQuality?.contains("LOSSLESS", ignoreCase = true) == true
        return CatalogueHit(
            id = number,
            durationMs = actual,
            isrcMatched = isrcMatched,
            differenceMs = diff,
            lossless = lossless,
            title = rawTitle?.trim().orEmpty(),
            artist = candidateArtists.firstOrNull().orEmpty(),
            artists = candidateArtists,
            albumTitle = albumTitle,
            albumId = albumId,
            coverUrl = coverUrl,
            isrc = foundIsrc,
            isExplicit = isExplicit,
            providerKey = kind
        )
    }

    fun selectBestHit(hits: List<CatalogueHit>): CatalogueHit? {
        if (hits.isEmpty()) return null
        return hits.sortedWith(
            compareByDescending<CatalogueHit> { it.lossless }
                .thenBy { it.differenceMs }
        ).firstOrNull()
    }
}
