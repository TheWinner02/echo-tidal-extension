package dev.brahmkshatriya.echo.extension.tidal

import dev.brahmkshatriya.echo.common.models.Album
import dev.brahmkshatriya.echo.common.models.Artist
import dev.brahmkshatriya.echo.common.models.ImageHolder.Companion.toImageHolder
import dev.brahmkshatriya.echo.common.models.Playlist
import dev.brahmkshatriya.echo.common.models.Track
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

object TidalConverter {

    fun imageCoverUrl(uuid: String?, size: Int = 640): String? {
        val clean = uuid?.trim().orEmpty()
        if (clean.isEmpty()) return null
        return "https://resources.tidal.com/images/${clean.replace('-', '/')}/${size}x${size}.jpg"
    }

    fun artistPictureUrl(uuid: String?, size: Int = 750): String? {
        val clean = uuid?.trim().orEmpty()
        if (clean.isEmpty()) return null
        return "https://resources.tidal.com/images/${clean.replace('-', '/')}/${size}x${size}.jpg"
    }

    fun jsonToTrack(obj: JsonObject): Track? {
        val rawId = obj["id"]?.jsonPrimitive?.contentOrNull ?: return null
        val title = obj["title"]?.jsonPrimitive?.contentOrNull?.trim() ?: return null
        val version = obj["version"]?.jsonPrimitive?.contentOrNull?.trim()
        val fullTitle = if (!version.isNullOrEmpty()) "$title ($version)" else title

        val artists = buildList {
            (obj["artists"] as? JsonArray)?.forEach { a ->
                (a as? JsonObject)?.let { aObj ->
                    val aId = aObj["id"]?.jsonPrimitive?.contentOrNull
                    val aName = aObj["name"]?.jsonPrimitive?.contentOrNull?.trim()
                    val aPic = aObj["picture"]?.jsonPrimitive?.contentOrNull
                    if (!aId.isNullOrEmpty() && !aName.isNullOrEmpty()) {
                        add(
                            Artist(
                                id = "tid:artist:$aId",
                                name = aName,
                                cover = artistPictureUrl(aPic)?.toImageHolder()
                            )
                        )
                    }
                }
            }
            if (isEmpty()) {
                (obj["artist"] as? JsonObject)?.let { aObj ->
                    val aId = aObj["id"]?.jsonPrimitive?.contentOrNull
                    val aName = aObj["name"]?.jsonPrimitive?.contentOrNull?.trim()
                    val aPic = aObj["picture"]?.jsonPrimitive?.contentOrNull
                    if (!aId.isNullOrEmpty() && !aName.isNullOrEmpty()) {
                        add(
                            Artist(
                                id = "tid:artist:$aId",
                                name = aName,
                                cover = artistPictureUrl(aPic)?.toImageHolder()
                            )
                        )
                    }
                }
            }
        }

        val albumObj = obj["album"] as? JsonObject
        val albumCoverUuid = albumObj?.get("cover")?.jsonPrimitive?.contentOrNull
        val cover = imageCoverUrl(albumCoverUuid)?.toImageHolder()
        val album = albumObj?.let { alb ->
            val albId = alb["id"]?.jsonPrimitive?.contentOrNull
            val albTitle = alb["title"]?.jsonPrimitive?.contentOrNull?.trim()
            if (!albId.isNullOrEmpty() && !albTitle.isNullOrEmpty()) {
                Album(
                    id = "tid:album:$albId",
                    title = albTitle,
                    cover = cover,
                    artists = artists
                )
            } else null
        }

        val durationSec = obj["duration"]?.jsonPrimitive?.doubleOrNull ?: 0.0
        val durationMs = (durationSec * 1000.0).toLong().takeIf { it > 0L }

        return Track(
            id = "tid:track:$rawId",
            title = fullTitle,
            artists = artists,
            album = album,
            cover = cover,
            duration = durationMs,
            plays = obj["popularity"]?.jsonPrimitive?.contentOrNull?.toLongOrNull()
        )
    }

    fun jsonToAlbum(obj: JsonObject): Album? {
        val rawId = obj["id"]?.jsonPrimitive?.contentOrNull ?: return null
        val title = obj["title"]?.jsonPrimitive?.contentOrNull?.trim() ?: return null
        val coverUuid = obj["cover"]?.jsonPrimitive?.contentOrNull
        val cover = imageCoverUrl(coverUuid)?.toImageHolder()

        val artists = buildList {
            (obj["artists"] as? JsonArray)?.forEach { a ->
                (a as? JsonObject)?.let { aObj ->
                    val aId = aObj["id"]?.jsonPrimitive?.contentOrNull
                    val aName = aObj["name"]?.jsonPrimitive?.contentOrNull?.trim()
                    if (!aId.isNullOrEmpty() && !aName.isNullOrEmpty()) {
                        add(Artist(id = "tid:artist:$aId", name = aName))
                    }
                }
            }
            if (isEmpty()) {
                (obj["artist"] as? JsonObject)?.let { aObj ->
                    val aId = aObj["id"]?.jsonPrimitive?.contentOrNull
                    val aName = aObj["name"]?.jsonPrimitive?.contentOrNull?.trim()
                    if (!aId.isNullOrEmpty() && !aName.isNullOrEmpty()) {
                        add(Artist(id = "tid:artist:$aId", name = aName))
                    }
                }
            }
        }

        val trackCount = obj["numberOfTracks"]?.jsonPrimitive?.contentOrNull?.toLongOrNull()
        val releaseYear = obj["releaseDate"]?.jsonPrimitive?.contentOrNull?.take(4)?.toIntOrNull()
        val releaseDate = releaseYear?.let { dev.brahmkshatriya.echo.common.models.Date(it) }

        return Album(
            id = "tid:album:$rawId",
            title = title,
            cover = cover,
            artists = artists,
            trackCount = trackCount,
            releaseDate = releaseDate
        )
    }

    fun jsonToArtist(obj: JsonObject): Artist? {
        val rawId = obj["id"]?.jsonPrimitive?.contentOrNull ?: return null
        val name = obj["name"]?.jsonPrimitive?.contentOrNull?.trim() ?: return null
        val pictureUuid = obj["picture"]?.jsonPrimitive?.contentOrNull
        val cover = artistPictureUrl(pictureUuid)?.toImageHolder()

        return Artist(
            id = "tid:artist:$rawId",
            name = name,
            cover = cover
        )
    }

    fun jsonToPlaylist(obj: JsonObject): Playlist? {
        val uuid = obj["uuid"]?.jsonPrimitive?.contentOrNull ?: return null
        val title = obj["title"]?.jsonPrimitive?.contentOrNull?.trim() ?: return null
        val imageUuid = obj["squareImage"]?.jsonPrimitive?.contentOrNull
            ?: obj["image"]?.jsonPrimitive?.contentOrNull
        val cover = imageCoverUrl(imageUuid)?.toImageHolder()
        val trackCount = obj["numberOfTracks"]?.jsonPrimitive?.contentOrNull?.toLongOrNull()
        val desc = obj["description"]?.jsonPrimitive?.contentOrNull

        return Playlist(
            id = "tid:playlist:$uuid",
            title = title,
            isEditable = false,
            isPrivate = false,
            cover = cover,
            authors = listOf(Artist(id = "tid:editorial", name = "Tidal")),
            trackCount = trackCount,
            description = desc,
            isRadioSupported = true
        )
    }

    fun extractNumericId(raw: String): String {
        return raw.trim()
            .removePrefix("tid:track:")
            .removePrefix("tid:album:")
            .removePrefix("tid:artist:")
            .removePrefix("tid:playlist:")
            .substringAfterLast("track/")
            .substringAfterLast("album/")
            .substringAfterLast("artist/")
            .substringAfterLast("playlist/")
            .substringBefore("?")
            .substringBefore("/")
            .trim()
    }
}
