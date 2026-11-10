package dev.brahmkshatriya.echo.extension.tidal

import dev.brahmkshatriya.echo.common.helpers.ContinuationCallback.Companion.await
import dev.brahmkshatriya.echo.common.models.Album
import dev.brahmkshatriya.echo.common.models.Artist
import dev.brahmkshatriya.echo.common.models.Playlist
import dev.brahmkshatriya.echo.common.models.Track
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import java.util.concurrent.TimeUnit

class TidalApi(
    baseHttpClient: OkHttpClient,
    private val apiBaseUrl: String = "https://api.tidal.com/v1",
    private val apiToken: String = "49YxDN9a2aFV6RTG"
) {
    private val httpClient = baseHttpClient.newBuilder()
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(15, TimeUnit.SECONDS)
        .build()

    private suspend fun requestJson(url: String): JsonObject? = withContext(Dispatchers.IO) {
        val req = Request.Builder()
            .url(url)
            .header("Accept", "application/json")
            .header("x-tidal-token", apiToken)
            .header("User-Agent", TidalSessionManager.MODERN_CHROME_UA)
            .get()
            .build()
        try {
            httpClient.newCall(req).await().use { r ->
                if (!r.isSuccessful) return@withContext null
                val body = r.body.string()
                runCatching { TidalSessionManager.json.parseToJsonElement(body).jsonObject }.getOrNull()
            }
        } catch (ce: CancellationException) {
            throw ce
        } catch (_: Throwable) {
            null
        }
    }

    private suspend fun requestJsonArray(url: String): JsonArray? = withContext(Dispatchers.IO) {
        val req = Request.Builder()
            .url(url)
            .header("Accept", "application/json")
            .header("x-tidal-token", apiToken)
            .header("User-Agent", TidalSessionManager.MODERN_CHROME_UA)
            .get()
            .build()
        try {
            httpClient.newCall(req).await().use { r ->
                if (!r.isSuccessful) return@withContext null
                val body = r.body.string()
                runCatching { TidalSessionManager.json.parseToJsonElement(body).jsonArray }.getOrNull()
            }
        } catch (ce: CancellationException) {
            throw ce
        } catch (_: Throwable) {
            null
        }
    }

    suspend fun searchTracks(
        query: String,
        countryCode: String = "US",
        limit: Int = 25,
        offset: Int = 0
    ): List<Track> {
        val url = "$apiBaseUrl/search/tracks".toHttpUrl().newBuilder()
            .addQueryParameter("query", query.take(240))
            .addQueryParameter("countryCode", countryCode)
            .addQueryParameter("limit", limit.toString())
            .addQueryParameter("offset", offset.toString())
            .build()
            .toString()

        val obj = requestJson(url) ?: return emptyList()
        val items = (obj["items"] as? JsonArray).orEmpty()
        return items.mapNotNull { (it as? JsonObject)?.let(TidalConverter::jsonToTrack) }
    }

    suspend fun searchAlbums(
        query: String,
        countryCode: String = "US",
        limit: Int = 20,
        offset: Int = 0
    ): List<Album> {
        val url = "$apiBaseUrl/search/albums".toHttpUrl().newBuilder()
            .addQueryParameter("query", query.take(240))
            .addQueryParameter("countryCode", countryCode)
            .addQueryParameter("limit", limit.toString())
            .addQueryParameter("offset", offset.toString())
            .build()
            .toString()

        val obj = requestJson(url) ?: return emptyList()
        val items = (obj["items"] as? JsonArray).orEmpty()
        return items.mapNotNull { (it as? JsonObject)?.let(TidalConverter::jsonToAlbum) }
    }

    suspend fun searchArtists(
        query: String,
        countryCode: String = "US",
        limit: Int = 20,
        offset: Int = 0
    ): List<Artist> {
        val url = "$apiBaseUrl/search/artists".toHttpUrl().newBuilder()
            .addQueryParameter("query", query.take(240))
            .addQueryParameter("countryCode", countryCode)
            .addQueryParameter("limit", limit.toString())
            .addQueryParameter("offset", offset.toString())
            .build()
            .toString()

        val obj = requestJson(url) ?: return emptyList()
        val items = (obj["items"] as? JsonArray).orEmpty()
        return items.mapNotNull { (it as? JsonObject)?.let(TidalConverter::jsonToArtist) }
    }

    suspend fun searchPlaylists(
        query: String,
        countryCode: String = "US",
        limit: Int = 20,
        offset: Int = 0
    ): List<Playlist> {
        val url = "$apiBaseUrl/search/playlists".toHttpUrl().newBuilder()
            .addQueryParameter("query", query.take(240))
            .addQueryParameter("countryCode", countryCode)
            .addQueryParameter("limit", limit.toString())
            .addQueryParameter("offset", offset.toString())
            .build()
            .toString()

        val obj = requestJson(url) ?: return emptyList()
        val items = (obj["items"] as? JsonArray).orEmpty()
        return items.mapNotNull { (it as? JsonObject)?.let(TidalConverter::jsonToPlaylist) }
    }

    suspend fun getTrack(id: String, countryCode: String = "US"): Track? {
        val cleanId = TidalConverter.extractNumericId(id)
        if (cleanId.isEmpty()) return null
        val url = "$apiBaseUrl/tracks/$cleanId".toHttpUrl().newBuilder()
            .addQueryParameter("countryCode", countryCode)
            .build()
            .toString()
        val obj = requestJson(url) ?: return null
        return TidalConverter.jsonToTrack(obj)
    }

    suspend fun getTrackRadio(id: String, countryCode: String = "US", limit: Int = 50): List<Track> {
        val cleanId = TidalConverter.extractNumericId(id)
        if (cleanId.isEmpty()) return emptyList()
        val url = "$apiBaseUrl/tracks/$cleanId/radio".toHttpUrl().newBuilder()
            .addQueryParameter("countryCode", countryCode)
            .addQueryParameter("limit", limit.toString())
            .build()
            .toString()
        val obj = requestJson(url) ?: return emptyList()
        val items = (obj["items"] as? JsonArray).orEmpty()
        return items.mapNotNull { (it as? JsonObject)?.let(TidalConverter::jsonToTrack) }
    }

    suspend fun getAlbum(id: String, countryCode: String = "US"): Album? {
        val cleanId = TidalConverter.extractNumericId(id)
        if (cleanId.isEmpty()) return null
        val url = "$apiBaseUrl/albums/$cleanId".toHttpUrl().newBuilder()
            .addQueryParameter("countryCode", countryCode)
            .build()
            .toString()
        val obj = requestJson(url) ?: return null
        return TidalConverter.jsonToAlbum(obj)
    }

    suspend fun getAlbumTracks(id: String, countryCode: String = "US", limit: Int = 100): List<Track> {
        val cleanId = TidalConverter.extractNumericId(id)
        if (cleanId.isEmpty()) return emptyList()
        val url = "$apiBaseUrl/albums/$cleanId/tracks".toHttpUrl().newBuilder()
            .addQueryParameter("countryCode", countryCode)
            .addQueryParameter("limit", limit.toString())
            .build()
            .toString()
        val obj = requestJson(url) ?: return emptyList()
        val items = (obj["items"] as? JsonArray).orEmpty()
        return items.mapNotNull { (it as? JsonObject)?.let(TidalConverter::jsonToTrack) }
    }

    suspend fun getArtist(id: String, countryCode: String = "US"): Artist? {
        val cleanId = TidalConverter.extractNumericId(id)
        if (cleanId.isEmpty()) return null
        val url = "$apiBaseUrl/artists/$cleanId".toHttpUrl().newBuilder()
            .addQueryParameter("countryCode", countryCode)
            .build()
            .toString()
        val obj = requestJson(url) ?: return null
        return TidalConverter.jsonToArtist(obj)
    }

    suspend fun getArtistTopTracks(id: String, countryCode: String = "US", limit: Int = 25): List<Track> {
        val cleanId = TidalConverter.extractNumericId(id)
        if (cleanId.isEmpty()) return emptyList()
        val url = "$apiBaseUrl/artists/$cleanId/toptracks".toHttpUrl().newBuilder()
            .addQueryParameter("countryCode", countryCode)
            .addQueryParameter("limit", limit.toString())
            .build()
            .toString()
        val obj = requestJson(url) ?: return emptyList()
        val items = (obj["items"] as? JsonArray).orEmpty()
        return items.mapNotNull { (it as? JsonObject)?.let(TidalConverter::jsonToTrack) }
    }

    suspend fun getArtistAlbums(id: String, countryCode: String = "US", limit: Int = 50): List<Album> {
        val cleanId = TidalConverter.extractNumericId(id)
        if (cleanId.isEmpty()) return emptyList()
        val url = "$apiBaseUrl/artists/$cleanId/albums".toHttpUrl().newBuilder()
            .addQueryParameter("countryCode", countryCode)
            .addQueryParameter("limit", limit.toString())
            .build()
            .toString()
        val obj = requestJson(url) ?: return emptyList()
        val items = (obj["items"] as? JsonArray).orEmpty()
        return items.mapNotNull { (it as? JsonObject)?.let(TidalConverter::jsonToAlbum) }
    }

    suspend fun getPlaylist(uuid: String, countryCode: String = "US"): Playlist? {
        val cleanUuid = TidalConverter.extractNumericId(uuid)
        if (cleanUuid.isEmpty()) return null
        val url = "$apiBaseUrl/playlists/$cleanUuid".toHttpUrl().newBuilder()
            .addQueryParameter("countryCode", countryCode)
            .build()
            .toString()
        val obj = requestJson(url) ?: return null
        return TidalConverter.jsonToPlaylist(obj)
    }

    suspend fun getPlaylistTracks(uuid: String, countryCode: String = "US", limit: Int = 100): List<Track> {
        val cleanUuid = TidalConverter.extractNumericId(uuid)
        if (cleanUuid.isEmpty()) return emptyList()
        val url = "$apiBaseUrl/playlists/$cleanUuid/tracks".toHttpUrl().newBuilder()
            .addQueryParameter("countryCode", countryCode)
            .addQueryParameter("limit", limit.toString())
            .build()
            .toString()
        val obj = requestJson(url) ?: return emptyList()
        val items = (obj["items"] as? JsonArray).orEmpty()
        return items.mapNotNull { (it as? JsonObject)?.let(TidalConverter::jsonToTrack) }
    }

    suspend fun getFeaturedNewAlbums(countryCode: String = "US", limit: Int = 20): List<Album> {
        val url = "$apiBaseUrl/featured/new/albums".toHttpUrl().newBuilder()
            .addQueryParameter("countryCode", countryCode)
            .addQueryParameter("limit", limit.toString())
            .build()
            .toString()
        val obj = requestJson(url) ?: return emptyList()
        val items = (obj["items"] as? JsonArray).orEmpty()
        return items.mapNotNull { (it as? JsonObject)?.let(TidalConverter::jsonToAlbum) }
    }

    suspend fun getFeaturedTopTracks(countryCode: String = "US", limit: Int = 25): List<Track> {
        val url = "$apiBaseUrl/featured/top/tracks".toHttpUrl().newBuilder()
            .addQueryParameter("countryCode", countryCode)
            .addQueryParameter("limit", limit.toString())
            .build()
            .toString()
        val obj = requestJson(url) ?: return emptyList()
        val items = (obj["items"] as? JsonArray).orEmpty()
        return items.mapNotNull { (it as? JsonObject)?.let(TidalConverter::jsonToTrack) }
    }

    suspend fun getFeaturedNewTracks(countryCode: String = "US", limit: Int = 25): List<Track> {
        val url = "$apiBaseUrl/featured/new/tracks".toHttpUrl().newBuilder()
            .addQueryParameter("countryCode", countryCode)
            .addQueryParameter("limit", limit.toString())
            .build()
            .toString()
        val obj = requestJson(url) ?: return emptyList()
        val items = (obj["items"] as? JsonArray).orEmpty()
        return items.mapNotNull { (it as? JsonObject)?.let(TidalConverter::jsonToTrack) }
    }

    suspend fun getGenrePlaylists(genrePath: String, countryCode: String = "US", limit: Int = 20): List<Playlist> {
        val url = "$apiBaseUrl/genres/$genrePath/playlists".toHttpUrl().newBuilder()
            .addQueryParameter("countryCode", countryCode)
            .addQueryParameter("limit", limit.toString())
            .build()
            .toString()
        val obj = requestJson(url) ?: return emptyList()
        val items = (obj["items"] as? JsonArray).orEmpty()
        return items.mapNotNull { (it as? JsonObject)?.let(TidalConverter::jsonToPlaylist) }
    }
}
