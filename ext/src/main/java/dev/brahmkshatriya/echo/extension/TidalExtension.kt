package dev.brahmkshatriya.echo.extension

import dev.brahmkshatriya.echo.common.clients.AlbumClient
import dev.brahmkshatriya.echo.common.clients.ArtistClient
import dev.brahmkshatriya.echo.common.clients.ExtensionClient
import dev.brahmkshatriya.echo.common.clients.HomeFeedClient
import dev.brahmkshatriya.echo.common.clients.PlaylistClient
import dev.brahmkshatriya.echo.common.clients.RadioClient
import dev.brahmkshatriya.echo.common.clients.SearchFeedClient
import dev.brahmkshatriya.echo.common.clients.SettingsChangeListenerClient
import dev.brahmkshatriya.echo.common.clients.TrackClient
import dev.brahmkshatriya.echo.common.helpers.WebViewClient
import dev.brahmkshatriya.echo.common.models.Album
import dev.brahmkshatriya.echo.common.models.Artist
import dev.brahmkshatriya.echo.common.models.EchoMediaItem
import dev.brahmkshatriya.echo.common.models.Feed
import dev.brahmkshatriya.echo.common.models.Feed.Companion.toFeed
import dev.brahmkshatriya.echo.common.models.Feed.Companion.toFeedData
import dev.brahmkshatriya.echo.common.models.Message
import dev.brahmkshatriya.echo.common.models.NetworkConnection
import dev.brahmkshatriya.echo.common.models.Playlist
import dev.brahmkshatriya.echo.common.models.Radio
import dev.brahmkshatriya.echo.common.models.Shelf
import dev.brahmkshatriya.echo.common.models.Streamable
import dev.brahmkshatriya.echo.common.models.Streamable.Media.Companion.toMedia
import dev.brahmkshatriya.echo.common.models.Streamable.Media.Companion.toServerMedia
import dev.brahmkshatriya.echo.common.models.Tab
import dev.brahmkshatriya.echo.common.models.Track
import dev.brahmkshatriya.echo.common.providers.MessageFlowProvider
import dev.brahmkshatriya.echo.common.providers.NetworkConnectionProvider
import dev.brahmkshatriya.echo.common.providers.WebViewClientProvider
import dev.brahmkshatriya.echo.common.settings.Setting
import dev.brahmkshatriya.echo.common.settings.SettingCategory
import dev.brahmkshatriya.echo.common.settings.SettingList
import dev.brahmkshatriya.echo.common.settings.SettingOnClick
import dev.brahmkshatriya.echo.common.settings.SettingSwitch
import dev.brahmkshatriya.echo.common.settings.Settings
import dev.brahmkshatriya.echo.extension.tidal.DiagnosticLogBuffer
import dev.brahmkshatriya.echo.extension.tidal.TidalApi
import dev.brahmkshatriya.echo.extension.tidal.TidalConverter
import dev.brahmkshatriya.echo.extension.tidal.TidalSessionManager
import dev.brahmkshatriya.echo.extension.tidal.TidalStreamResolver
import dev.brahmkshatriya.echo.extension.tidal.VerificationStatus
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import okhttp3.OkHttpClient
import java.util.concurrent.TimeUnit

open class TidalExtension internal constructor(
    private val httpClient: OkHttpClient,
    internal val logs: DiagnosticLogBuffer,
    internal val sessionManager: TidalSessionManager?,
    internal val streamResolver: TidalStreamResolver?,
    internal val tidalApi: TidalApi?,
    private val extensionScope: CoroutineScope,
    private val startHeartbeat: Boolean
) : ExtensionClient,
    HomeFeedClient,
    SearchFeedClient,
    TrackClient,
    AlbumClient,
    ArtistClient,
    PlaylistClient,
    RadioClient,
    WebViewClientProvider,
    MessageFlowProvider,
    NetworkConnectionProvider,
    SettingsChangeListenerClient {

    @Suppress("unused")
    constructor() : this(
        httpClient = TidalSessionManager.withBrowserHeaders(
            OkHttpClient.Builder()
                .connectTimeout(10, TimeUnit.SECONDS)
                .readTimeout(15, TimeUnit.SECONDS)
                .build()
        ),
        logs = DiagnosticLogBuffer(40),
        sessionManager = null,
        streamResolver = null,
        tidalApi = null,
        extensionScope = CoroutineScope(SupervisorJob() + Dispatchers.IO),
        startHeartbeat = true
    )

    companion object {
        const val KEY_COUNTRY_CODE = "tidal_country_code"
        private const val DEFAULT_COUNTRY_CODE = "US"
        private const val HEARTBEAT_INTERVAL_MS = 5L * 60L * 1000L
    }

    private lateinit var settings: Settings
    private var messageFlow: MutableSharedFlow<Message>? = null
    private var heartbeatJob: Job? = null

    internal val manager: TidalSessionManager by lazy {
        sessionManager ?: TidalSessionManager(
            baseHttpClient = httpClient,
            logs = logs,
            onNotify = ::emitNotification
        )
    }

    internal val resolver: TidalStreamResolver by lazy {
        streamResolver ?: TidalStreamResolver(
            baseHttpClient = httpClient,
            sessionManager = manager
        )
    }

    internal val api: TidalApi by lazy {
        tidalApi ?: TidalApi(
            baseHttpClient = httpClient
        )
    }

    private fun getCountryCode(): String {
        return runCatching { settings.getString(KEY_COUNTRY_CODE) }
            .getOrNull()?.ifBlank { null } ?: DEFAULT_COUNTRY_CODE
    }

    override fun setSettings(settings: Settings) {
        this.settings = settings
        manager.attachSettings(settings)
    }

    override fun setWebViewClient(webViewClient: WebViewClient) {
        manager.attachWebViewClient(webViewClient)
    }

    override fun setMessageFlow(messageFlow: MutableSharedFlow<Message>) {
        this.messageFlow = messageFlow
    }

    override fun setNetworkConnection(networkConnection: NetworkConnection) {
        manager.updateNetworkConnection(networkConnection, extensionScope)
    }

    private fun emitNotification(text: String) {
        val flow = messageFlow ?: return
        if (!flow.tryEmit(Message(text))) {
            extensionScope.launch {
                runCatching { flow.emit(Message(text)) }
            }
        }
    }

    override suspend fun onInitialize() {
        runCatching {
            manager.clearStalePendingOnStartup()
            manager.startAutomatic(extensionScope, reset = false, reason = "avvio")
        }.onFailure {
            manager.log("Impossibile inizializzare la verifica automatica Tidal.")
        }

        if (startHeartbeat && heartbeatJob?.isActive != true) {
            heartbeatJob = extensionScope.launch {
                while (isActive) {
                    delay(HEARTBEAT_INTERVAL_MS)
                    runCatching { manager.scheduleRenewalIfDue(extensionScope) }
                }
            }
        }
    }

    override suspend fun onExtensionSelected() {
        runCatching { manager.scheduleRenewalIfDue(extensionScope) }
    }

    override suspend fun onSettingsChanged(settings: Settings, key: String?) {
        this.settings = settings
        manager.attachSettings(settings)
        if (key == TidalSessionManager.KEY_AUTO_VERIFY) {
            if (manager.automaticEnabled()) {
                manager.startAutomatic(extensionScope, reset = true)
            } else {
                manager.log("Verifica automatica Tidal disattivata.")
            }
        }
    }

    override suspend fun getSettingItems(): List<Setting> {
        val sessionCategory = SettingCategory(
            title = "Sessione Tidal FLAC",
            key = "cat_session",
            items = listOf(
                SettingSwitch(
                    title = "Rinnovo automatico silenzioso in background",
                    key = TidalSessionManager.KEY_AUTO_VERIFY,
                    summary = "Mantiene attiva la sessione Tidal rinnovandola silenziosamente in background prima della scadenza, senza aprire la WebView.",
                    defaultValue = true
                ),
                SettingOnClick(
                    title = "Verifica sessione Tidal (WebView)",
                    key = "verify_tidal",
                    summary = manager.sessionSummaryForUi()
                ) {
                    extensionScope.launch {
                        manager.notice("Avvio verifica Turnstile... Completa il controllo nella finestra WebView.")
                        val status = manager.manualVerify(force = false)
                        when (status) {
                            VerificationStatus.OPENED -> manager.notify("Completa la verifica nella finestra aperta.")
                            VerificationStatus.BUSY, VerificationStatus.RATE_LIMITED -> {}
                            VerificationStatus.COMPLETE -> manager.notify("Sessione verificata con successo!")
                            else -> manager.notify("Verifica non completata o annullata.")
                        }
                    }
                },
                SettingOnClick(
                    title = "Forza nuova verifica manuale",
                    key = "force_verify",
                    summary = "Tenta immediatamente una nuova verifica della sessione anche prima della scadenza."
                ) {
                    extensionScope.launch {
                        manager.notice("Avvio verifica forzata... Completa il controllo nella finestra WebView.")
                        manager.manualVerify(force = true)
                    }
                }
            )
        )

        val prefCategory = SettingCategory(
            title = "Preferenze catalogo",
            key = "cat_prefs",
            items = listOf(
                SettingList(
                    title = "Paese catalogo Tidal",
                    key = KEY_COUNTRY_CODE,
                    summary = "Imposta la nazione per il catalogo e le nuove uscite.",
                    entryTitles = listOf("Stati Uniti (US)", "Italia (IT)", "Regno Unito (GB)", "Germania (DE)", "Francia (FR)"),
                    entryValues = listOf("US", "IT", "GB", "DE", "FR"),
                    defaultEntryIndex = 0
                )
            )
        )

        val logCategory = SettingCategory(
            title = "Diagnostica e Log",
            key = "cat_logs",
            items = listOf(
                SettingOnClick(
                    title = "Mostra ultimi eventi",
                    key = "show_logs",
                    summary = logs.snapshot().firstOrNull() ?: "Nessun evento recente."
                ) {
                    val snapshot = logs.snapshot()
                    val msg = if (snapshot.isEmpty()) "Nessun log disponibile."
                    else snapshot.take(3).joinToString("\n")
                    manager.notify(msg)
                },
                SettingOnClick(
                    title = "Cancella log diagnostici",
                    key = "clear_logs",
                    summary = "Azzera la cronologia degli eventi."
                ) {
                    logs.clear()
                    manager.notify("Log cancellati.")
                }
            )
        )

        return listOf(sessionCategory, prefCategory, logCategory)
    }

    // --- HOME FEED ---

    override suspend fun loadHomeFeed(): Feed<Shelf> {
        val country = getCountryCode()
        val shelves = coroutineScope {
            val newAlbumsJob = async { runCatching { api.getFeaturedNewAlbums(country, 20) }.getOrDefault(emptyList()) }
            val topTracksJob = async { runCatching { api.getFeaturedTopTracks(country, 20) }.getOrDefault(emptyList()) }
            val newTracksJob = async { runCatching { api.getFeaturedNewTracks(country, 20) }.getOrDefault(emptyList()) }
            val playlistsJob = async { runCatching { api.getGenrePlaylists("Rock", country, 15) }.getOrDefault(emptyList()) }

            val newAlbums = newAlbumsJob.await()
            val topTracks = topTracksJob.await()
            val newTracks = newTracksJob.await()
            val playlists = playlistsJob.await()

            buildList {
                if (newAlbums.isNotEmpty()) {
                    add(Shelf.Lists.Items("tidal_new_albums", "Nuove uscite · Album", newAlbums))
                }
                if (topTracks.isNotEmpty()) {
                    add(Shelf.Lists.Tracks("tidal_top_tracks", "Brani più ascoltati", topTracks))
                }
                if (newTracks.isNotEmpty()) {
                    add(Shelf.Lists.Tracks("tidal_new_tracks", "Nuove uscite · Singoli", newTracks))
                }
                if (playlists.isNotEmpty()) {
                    add(Shelf.Lists.Items("tidal_playlists", "Playlist consigliate", playlists))
                }
            }
        }
        return shelves.toFeed()
    }

    // --- SEARCH FEED ---

    override suspend fun loadSearchFeed(query: String): Feed<Shelf> {
        val trimmed = query.trim()
        val tabs = listOf(
            Tab("all", "Tutto"),
            Tab("tracks", "Brani"),
            Tab("albums", "Album"),
            Tab("artists", "Artisti"),
            Tab("playlists", "Playlist")
        )

        return Feed(tabs) { tab ->
            if (trimmed.isEmpty()) return@Feed emptyList<Shelf>().toFeedData()
            val country = getCountryCode()
            val tabId = tab?.id ?: "all"

            val shelves = coroutineScope {
                val wantAll = tabId == "all"
                val tracksJob = if (wantAll || tabId == "tracks") {
                    async { runCatching { api.searchTracks(trimmed, country) }.getOrDefault(emptyList()) }
                } else {
                    CompletableDeferred(emptyList())
                }
                val albumsJob = if (wantAll || tabId == "albums") {
                    async { runCatching { api.searchAlbums(trimmed, country) }.getOrDefault(emptyList()) }
                } else {
                    CompletableDeferred(emptyList())
                }
                val artistsJob = if (wantAll || tabId == "artists") {
                    async { runCatching { api.searchArtists(trimmed, country) }.getOrDefault(emptyList()) }
                } else {
                    CompletableDeferred(emptyList())
                }
                val playlistsJob = if (wantAll || tabId == "playlists") {
                    async { runCatching { api.searchPlaylists(trimmed, country) }.getOrDefault(emptyList()) }
                } else {
                    CompletableDeferred(emptyList())
                }

                val tracks = tracksJob.await()
                val albums = albumsJob.await()
                val artists = artistsJob.await()
                val playlists = playlistsJob.await()

                buildList {
                    if (tracks.isNotEmpty()) {
                        add(Shelf.Lists.Tracks("search_tracks", if (wantAll) "Brani" else "Risultati Brani", tracks))
                    }
                    if (albums.isNotEmpty()) {
                        add(Shelf.Lists.Items("search_albums", if (wantAll) "Album" else "Risultati Album", albums))
                    }
                    if (artists.isNotEmpty()) {
                        add(Shelf.Lists.Items("search_artists", if (wantAll) "Artisti" else "Risultati Artisti", artists))
                    }
                    if (playlists.isNotEmpty()) {
                        add(Shelf.Lists.Items("search_playlists", if (wantAll) "Playlist" else "Risultati Playlist", playlists))
                    }
                }
            }
            shelves.toFeedData()
        }
    }

    // --- TRACK CLIENT ---

    override suspend fun loadTrack(track: Track, isDownload: Boolean): Track {
        val id = TidalConverter.extractNumericId(track.id)
        val fetched = api.getTrack(id, getCountryCode()) ?: track
        val serverStream = Streamable.server(
            id = id,
            quality = 1411,
            title = "Tidal FLAC",
            extras = mapOf("id" to id)
        )
        return fetched.copy(streamables = listOf(serverStream))
    }

    override suspend fun loadStreamableMedia(streamable: Streamable, isDownload: Boolean): Streamable.Media {
        val trackId = streamable.extras["id"] ?: streamable.id
        val stream = resolver.resolveStream(trackId)
            ?: throw IllegalStateException("Impossibile risolvere il flusso FLAC per il brano Tidal ($trackId)")

        return if (!stream.url.isNullOrBlank()) {
            stream.url.toServerMedia(type = Streamable.SourceType.Progressive)
        } else if (!stream.dashManifestBase64.isNullOrBlank()) {
            if (isDownload) {
                val inputProvider = resolver.createDashDownloadInputProvider(stream.dashManifestBase64)
                Streamable.Source.Raw(
                    streamProvider = inputProvider,
                    id = "tidal_${trackId}_flac",
                    quality = 1411,
                    title = stream.label()
                ).toMedia()
            } else {
                "data:application/dash+xml;base64,${stream.dashManifestBase64}".toServerMedia(
                    type = Streamable.SourceType.DASH
                )
            }
        } else {
            throw IllegalStateException("Stream FLAC non disponibile")
        }
    }

    override suspend fun loadFeed(track: Track): Feed<Shelf>? = null

    // --- ALBUM CLIENT ---

    override suspend fun loadAlbum(album: Album): Album {
        val id = TidalConverter.extractNumericId(album.id)
        return api.getAlbum(id, getCountryCode()) ?: album
    }

    override suspend fun loadTracks(album: Album): Feed<Track> {
        val id = TidalConverter.extractNumericId(album.id)
        val tracks = api.getAlbumTracks(id, getCountryCode())
        return tracks.toFeed()
    }

    override suspend fun loadFeed(album: Album): Feed<Shelf>? = null

    // --- ARTIST CLIENT ---

    override suspend fun loadArtist(artist: Artist): Artist {
        val id = TidalConverter.extractNumericId(artist.id)
        return api.getArtist(id, getCountryCode()) ?: artist
    }

    override suspend fun loadFeed(artist: Artist): Feed<Shelf> {
        val id = TidalConverter.extractNumericId(artist.id)
        val country = getCountryCode()
        val shelves = coroutineScope {
            val topTracksJob = async { runCatching { api.getArtistTopTracks(id, country) }.getOrDefault(emptyList()) }
            val albumsJob = async { runCatching { api.getArtistAlbums(id, country) }.getOrDefault(emptyList()) }

            val topTracks = topTracksJob.await()
            val albums = albumsJob.await()

            buildList {
                if (topTracks.isNotEmpty()) {
                    add(Shelf.Lists.Tracks("artist_top_$id", "Brani popolari", topTracks))
                }
                if (albums.isNotEmpty()) {
                    add(Shelf.Lists.Items("artist_albums_$id", "Album", albums))
                }
            }
        }
        return shelves.toFeed()
    }

    // --- PLAYLIST CLIENT ---

    override suspend fun loadPlaylist(playlist: Playlist): Playlist {
        val uuid = TidalConverter.extractNumericId(playlist.id)
        return api.getPlaylist(uuid, getCountryCode()) ?: playlist
    }

    override suspend fun loadTracks(playlist: Playlist): Feed<Track> {
        val uuid = TidalConverter.extractNumericId(playlist.id)
        val tracks = api.getPlaylistTracks(uuid, getCountryCode())
        return tracks.toFeed()
    }

    override suspend fun loadFeed(playlist: Playlist): Feed<Shelf>? = null

    // --- RADIO CLIENT ---

    override suspend fun radio(item: EchoMediaItem, context: EchoMediaItem?): Radio {
        val trackId = TidalConverter.extractNumericId(item.id)
        return Radio(
            id = "tid:radio:$trackId",
            title = "Radio · ${item.title}",
            cover = item.cover
        )
    }

    override suspend fun loadRadio(radio: Radio): Radio = radio

    override suspend fun loadTracks(radio: Radio): Feed<Track> {
        val trackId = TidalConverter.extractNumericId(radio.id)
        val tracks = api.getTrackRadio(trackId, getCountryCode())
        return tracks.toFeed()
    }
}
