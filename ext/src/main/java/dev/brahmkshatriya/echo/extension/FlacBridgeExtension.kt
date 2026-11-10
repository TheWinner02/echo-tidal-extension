package dev.brahmkshatriya.echo.extension

import dev.brahmkshatriya.echo.common.MusicExtension
import dev.brahmkshatriya.echo.common.clients.AlbumClient
import dev.brahmkshatriya.echo.common.clients.ArtistClient
import dev.brahmkshatriya.echo.common.clients.ExtensionClient
import dev.brahmkshatriya.echo.common.clients.FollowClient
import dev.brahmkshatriya.echo.common.clients.HideClient
import dev.brahmkshatriya.echo.common.clients.HomeFeedClient
import dev.brahmkshatriya.echo.common.clients.LibraryFeedClient
import dev.brahmkshatriya.echo.common.clients.LikeClient
import dev.brahmkshatriya.echo.common.clients.PlaylistClient
import dev.brahmkshatriya.echo.common.clients.PlaylistEditClient
import dev.brahmkshatriya.echo.common.clients.RadioClient
import dev.brahmkshatriya.echo.common.clients.SaveClient
import dev.brahmkshatriya.echo.common.clients.SearchFeedClient
import dev.brahmkshatriya.echo.common.clients.SettingsChangeListenerClient
import dev.brahmkshatriya.echo.common.clients.ShareClient
import dev.brahmkshatriya.echo.common.clients.TrackClient
import dev.brahmkshatriya.echo.common.helpers.WebViewClient
import dev.brahmkshatriya.echo.common.models.Album
import dev.brahmkshatriya.echo.common.models.Artist
import dev.brahmkshatriya.echo.common.models.EchoMediaItem
import dev.brahmkshatriya.echo.common.models.Feed
import dev.brahmkshatriya.echo.common.models.Feed.Companion.toFeed
import dev.brahmkshatriya.echo.common.models.Message
import dev.brahmkshatriya.echo.common.models.NetworkConnection
import dev.brahmkshatriya.echo.common.models.NetworkRequest.Companion.toGetRequest
import dev.brahmkshatriya.echo.common.models.Playlist
import dev.brahmkshatriya.echo.common.models.Radio
import dev.brahmkshatriya.echo.common.models.Shelf
import dev.brahmkshatriya.echo.common.models.Streamable
import dev.brahmkshatriya.echo.common.models.Streamable.Media.Companion.toMedia
import dev.brahmkshatriya.echo.common.models.Track
import dev.brahmkshatriya.echo.common.providers.MessageFlowProvider
import dev.brahmkshatriya.echo.common.providers.MusicExtensionsProvider
import dev.brahmkshatriya.echo.common.providers.NetworkConnectionProvider
import dev.brahmkshatriya.echo.common.providers.WebViewClientProvider
import dev.brahmkshatriya.echo.common.settings.Setting
import dev.brahmkshatriya.echo.common.settings.SettingCategory
import dev.brahmkshatriya.echo.common.settings.SettingItem
import dev.brahmkshatriya.echo.common.settings.SettingList
import dev.brahmkshatriya.echo.common.settings.SettingMultipleChoice
import dev.brahmkshatriya.echo.common.settings.SettingOnClick
import dev.brahmkshatriya.echo.common.settings.SettingSwitch
import dev.brahmkshatriya.echo.common.settings.Settings
import dev.brahmkshatriya.echo.extension.bridge.DiagnosticLogBuffer
import dev.brahmkshatriya.echo.extension.bridge.FlacStreamResolver
import dev.brahmkshatriya.echo.extension.bridge.ProviderSpec
import dev.brahmkshatriya.echo.extension.bridge.ResolvedFlacStream
import dev.brahmkshatriya.echo.extension.bridge.TrackMatchSpec
import dev.brahmkshatriya.echo.extension.bridge.TrackMatcher
import dev.brahmkshatriya.echo.extension.bridge.VerificationStatus
import dev.brahmkshatriya.echo.extension.bridge.ZarzSessionManager
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.serialization.encodeToString
import okhttp3.OkHttpClient
import java.util.concurrent.TimeUnit

/**
 * FLAC Bridge Multi per Echo — Porting nativo Kotlin di FLAC-Bridge-Multi-Myspoti (GPL-3.0).
 *
 * Funziona sia come Bridge integrato sopra un'estensione metadati installata (es. Spotify, YouTube Music)
 * con fallback automatico all'audio originale, sia come sorgente FLAC autonoma o abbinata a Combine Extension.
 */
class FlacBridgeExtension internal constructor(
    private val httpClient: OkHttpClient,
    internal val logs: DiagnosticLogBuffer,
    internal val sessionManager: ZarzSessionManager?,
    internal val streamResolver: FlacStreamResolver?,
    private val extensionScope: CoroutineScope,
    private val startHeartbeat: Boolean
) : ExtensionClient,
    SettingsChangeListenerClient,
    WebViewClientProvider,
    MessageFlowProvider,
    NetworkConnectionProvider,
    MusicExtensionsProvider,
    HomeFeedClient,
    LibraryFeedClient,
    SearchFeedClient,
    TrackClient,
    AlbumClient,
    ArtistClient,
    PlaylistClient,
    PlaylistEditClient,
    SaveClient,
    LikeClient,
    ShareClient,
    FollowClient,
    HideClient,
    RadioClient {

    @Suppress("unused")
    constructor() : this(
        httpClient = ZarzSessionManager.withBrowserHeaders(
            OkHttpClient.Builder()
                .connectTimeout(10, TimeUnit.SECONDS)
                .readTimeout(15, TimeUnit.SECONDS)
                .build()
        ),
        logs = DiagnosticLogBuffer(40),
        sessionManager = null,
        streamResolver = null,
        extensionScope = CoroutineScope(SupervisorJob() + Dispatchers.IO),
        startHeartbeat = true
    )

    companion object {
        const val EXTENSION_ID = "echo_flac_bridge"
        private const val COMBINE_ID = "echo_combine"

        const val MODE_AUTO = "auto"
        const val MODE_BRIDGE = "bridge"
        const val MODE_STANDALONE = "standalone"

        const val EXTRA_IS_FLAC_BRIDGE = "flac_bridge_stream"
        const val EXTRA_PROVIDER_KEY = "flac_bridge_provider"
        const val EXTRA_STRICT_PROVIDER = "flac_bridge_strict_provider"
        const val EXTRA_MATCH_SPEC = "flac_bridge_match_spec"
        const val EXTRA_META_EXT_ID = "flac_bridge_meta_ext_id"
        const val EXTRA_ORIGINAL_STREAM = "flac_bridge_original_stream"
        const val EXTRA_FALLBACK_EXT_ID = "flac_bridge_fallback_ext_id"
        const val EXTRA_FALLBACK_STREAM = "flac_bridge_fallback_stream"

        private const val HEARTBEAT_INTERVAL_MS = 5L * 60L * 1000L
    }

    private lateinit var settings: Settings
    private var messageFlow: MutableSharedFlow<Message>? = null
    private var heartbeatJob: Job? = null

    internal val manager: ZarzSessionManager by lazy {
        sessionManager ?: ZarzSessionManager(
            baseHttpClient = httpClient,
            logs = logs,
            onNotify = ::emitNotification
        )
    }

    internal val resolver: FlacStreamResolver by lazy {
        streamResolver ?: FlacStreamResolver(
            baseHttpClient = httpClient,
            sessionManager = manager
        )
    }

    override val requiredMusicExtensions: List<String> = emptyList()
    private val extensionsFlow = MutableStateFlow<List<MusicExtension>>(emptyList())

    override fun setSettings(settings: Settings) {
        this.settings = settings
        // Default Echo's per-extension stream_quality to "highest" (if unset) so Echo's
        // PlayerService.selectServerIndex selects our top-priority server (quality = 10_000)
        // instead of the middle item when multiple manual server chips are exposed.
        runCatching {
            if (settings.getString(ZarzSessionManager.KEY_ECHO_STREAM_QUALITY) == null) {
                settings.putString(ZarzSessionManager.KEY_ECHO_STREAM_QUALITY, "highest")
            }
        }
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

    override fun setMusicExtensions(extensions: List<MusicExtension>) {
        extensionsFlow.value = extensions.filter {
            it.id != EXTENSION_ID && it.id != COMBINE_ID && it.isEnabled
        }
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
            manager.log("Impossibile inizializzare la verifica automatica.")
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
        if (key == ZarzSessionManager.KEY_AUTO_VERIFY) {
            if (manager.automaticEnabled()) {
                manager.startAutomatic(extensionScope, reset = true)
            } else {
                manager.stopAutomatic(0L)
                manager.log("Verifica automatica disattivata.")
            }
        }
    }

    override suspend fun getSettingItems(): List<Setting> {
        val availableExts = extensionsFlow.value
        val metaIds = listOf("auto") + availableExts.map { it.id }
        val metaNames = listOf(
            if (availableExts.isNotEmpty()) "Automatica (${availableExts.first().name})"
            else "Nessuna altra estensione installata"
        ) + availableExts.map { it.name }

        val sourceCategory = SettingCategory(
            title = "Modalità e Sorgenti FLAC",
            key = "cat_sources",
            items = listOf(
                SettingList(
                    title = "Modalità operativa",
                    key = ZarzSessionManager.KEY_BRIDGE_MODE,
                    summary = "Bridge avvolge Spotify/altra estensione e sostituisce l'audio con FLAC; Autonoma espone direttamente i cataloghi FLAC (anche per Combine).",
                    entryTitles = listOf(
                        "Automatica (Bridge se disponibile, altrimenti Autonoma)",
                        "Bridge integrato (usa estensione metadati)",
                        "Sorgente FLAC autonoma (o per Combine Extension)"
                    ),
                    entryValues = listOf(MODE_AUTO, MODE_BRIDGE, MODE_STANDALONE),
                    defaultEntryIndex = 0
                ),
                SettingList(
                    title = "Estensione metadati (Modalità Bridge)",
                    key = ZarzSessionManager.KEY_METADATA_EXT,
                    summary = "Estensione da cui leggere libreria, playlist, ricerca e copertine (es. Spotify).",
                    entryTitles = metaNames,
                    entryValues = metaIds,
                    defaultEntryIndex = 0
                ),
                SettingSwitch(
                    title = "Ritorna all'audio originale se FLAC assente",
                    key = ZarzSessionManager.KEY_FALLBACK_ORIGINAL,
                    summary = "Come su Myspoti: se nessuna sorgente restituisce un FLAC compatibile, riproduce lo stream dell'estensione metadati.",
                    defaultValue = true
                ),
                SettingList(
                    title = "Prima sorgente (server predefinito)",
                    key = ZarzSessionManager.KEY_PREFERRED,
                    summary = "Server FLAC usato per primo in riproduzione automatica.",
                    entryTitles = ProviderSpec.ALL.map { it.name },
                    entryValues = ProviderSpec.ALL.map { it.name },
                    defaultEntryIndex = 0
                ),
                SettingSwitch(
                    title = "Scelta manuale server nel player",
                    key = ZarzSessionManager.KEY_MANUAL_SERVER_PICKER,
                    summary = "Mostra nel menu Qualità/Server del player tutti i server abilitati (Auto, Tidal, Qobuz, Deezer, Amazon Music, sorgente originale) per cambiare server al volo.",
                    defaultValue = true
                ),
                SettingSwitch(
                    title = "Usa solo la prima sorgente in automatico",
                    key = ZarzSessionManager.KEY_STRICT_PREFERRED,
                    summary = "Se attivo, non passa automaticamente agli altri server FLAC quando il brano manca sulla Prima sorgente (puoi comunque scegliere manualmente qualsiasi server nel player).",
                    defaultValue = false
                ),
                SettingMultipleChoice(
                    title = "Sorgenti abilitate",
                    key = ZarzSessionManager.KEY_ENABLED_SOURCES,
                    summary = "Seleziona quali sorgenti controllare, mostrare nel player e interrogare.",
                    entryTitles = ProviderSpec.ALL.map {
                        if (it.experimental) "${it.name} (sperimentale)" else it.name
                    },
                    entryValues = ProviderSpec.ALL.map { it.key },
                    defaultEntryIndices = setOf(0, 1, 2, 3)
                )
            )
        )

        val verifyCategory = SettingCategory(
            title = "Verifica e Sessioni (rinnovo ~6 ore)",
            key = "cat_verify",
            items = listOf(
                SettingSwitch(
                    title = "Verifica automatica all’avvio",
                    key = ZarzSessionManager.KEY_AUTO_VERIFY,
                    summary = "All’avvio e circa ogni 6 ore mentre Echo è attiva, controlla e tenta il rinnovo delle sessioni rispettando i limiti del server.",
                    defaultValue = true
                ),
                SettingOnClick(
                    title = "Attiva Tidal",
                    key = "verify_tidal",
                    summary = manager.sessionSummaryForUi(ProviderSpec.TIDAL)
                ) { handleManualSourceVerify(ProviderSpec.TIDAL) },
                SettingOnClick(
                    title = "Attiva Qobuz",
                    key = "verify_qobuz",
                    summary = manager.sessionSummaryForUi(ProviderSpec.QOBUZ)
                ) { handleManualSourceVerify(ProviderSpec.QOBUZ) },
                SettingOnClick(
                    title = "Attiva Deezer (sperimentale)",
                    key = "verify_deezer",
                    summary = manager.sessionSummaryForUi(ProviderSpec.DEEZER)
                ) { handleManualSourceVerify(ProviderSpec.DEEZER) },
                SettingOnClick(
                    title = "Attiva Amazon Music (sperimentale)",
                    key = "verify_amazon",
                    summary = manager.sessionSummaryForUi(ProviderSpec.AMAZON)
                ) { handleManualSourceVerify(ProviderSpec.AMAZON) },
                SettingOnClick(
                    title = "Riprova verifica automatica",
                    key = "verify_all",
                    summary = "Riprende i controlli: verifica le sorgenti necessarie e rinnova le sessioni dovute, rispettando le pause del server."
                ) {
                    if (manager.startAutomatic(extensionScope, reset = true)) {
                        manager.notify("Verifiche richieste: attendi la rete e completa le eventuali pagine.")
                    }
                },
                SettingOnClick(
                    title = "Forza verifica manuale",
                    key = "force_verify",
                    summary = "Un tentativo per la Prima sorgente anche prima della scadenza memorizzata; il server può rispondere 429."
                ) {
                    val chosen = manager.preferenceOrder().firstOrNull() ?: ProviderSpec.TIDAL
                    manager.log("Tentativo manuale richiesto per ${chosen.name}.")
                    val status = manager.beginVerification(
                        provider = chosen,
                        automatic = false,
                        deadlineMs = System.currentTimeMillis() + 20_000L,
                        forceManual = true,
                        overrideStoredServerWait = true
                    )
                    if (status == VerificationStatus.RATE_LIMITED && manager.verificationWaitMs() > 0L && manager.autoRunning) {
                        manager.stopAutomatic(0L)
                    } else if (status == VerificationStatus.COMPLETE && manager.autoRunning && manager.automaticAllowed()) {
                        manager.wakeAutomatic(extensionScope)
                    }
                },
                SettingOnClick(
                    title = "Mostra tempi di attesa",
                    key = "show_waits",
                    summary = "Mostra le pause e il funzionamento del rinnovo periodico nel Registro qui sotto, senza inviare richieste."
                ) {
                    manager.showVerificationWaits()
                },
                SettingOnClick(
                    title = "Cancella tutte le sessioni",
                    key = "forget",
                    summary = "Rimuove le sessioni locali e sospende la verifica automatica finché non la riattivi."
                ) {
                    manager.forgetAllSessions()
                }
            )
        )

        val recentLogs = logs.snapshot().take(12)
        val logSummary = if (recentLogs.isEmpty()) {
            "Nessun evento registrato in questa sessione."
        } else {
            recentLogs.joinToString("\n")
        }

        val diagnosticsCategory = SettingCategory(
            title = "Registro Diagnostico",
            key = "cat_logs",
            items = listOf(
                SettingItem(
                    title = "Ultimi eventi [FLAC Bridge]",
                    key = "diagnostic_log_view",
                    summary = logSummary
                ),
                SettingOnClick(
                    title = "Pulisci registro diagnostico",
                    key = "clear_logs",
                    summary = "Svuota solo le righe diagnostiche mostrate sopra (non tocca le sessioni)."
                ) {
                    logs.clear()
                    manager.notify("Registro diagnostico ripulito.")
                }
            )
        )

        return listOf(sourceCategory, verifyCategory, diagnosticsCategory)
    }

    private suspend fun handleManualSourceVerify(provider: ProviderSpec) {
        manager.log("${provider.name}: avvio della verifica manuale richiesto.")
        val status = manager.beginVerification(
            provider = provider,
            automatic = false,
            deadlineMs = System.currentTimeMillis() + 20_000L,
            forceManual = true,
            overrideStoredServerWait = false
        )
        when {
            status == VerificationStatus.RATE_LIMITED && manager.verificationWaitMs() > 0L && manager.autoRunning ->
                manager.stopAutomatic(0L)
            status == VerificationStatus.COMPLETE && manager.autoRunning && manager.automaticAllowed() ->
                manager.wakeAutomatic(extensionScope)
            status == VerificationStatus.AUTH_UNAVAILABLE && manager.autoRunning ->
                manager.stopAutomatic(15L * 60L * 1000L)
        }
    }

    // --- Mode & Metadata Extension Resolution ---

    private fun configuredMode(): String =
        runCatching { settings.getString(ZarzSessionManager.KEY_BRIDGE_MODE)?.trim().orEmpty() }
            .getOrDefault("")
            .ifEmpty { MODE_AUTO }

    private fun fallbackToOriginalEnabled(): Boolean =
        runCatching { settings.getBoolean(ZarzSessionManager.KEY_FALLBACK_ORIGINAL) != false }
            .getOrDefault(true)

    private fun manualServerPickerEnabled(): Boolean =
        runCatching { settings.getBoolean(ZarzSessionManager.KEY_MANUAL_SERVER_PICKER) != false }
            .getOrDefault(true)

    private fun strictPreferredEnabled(): Boolean =
        runCatching { settings.getBoolean(ZarzSessionManager.KEY_STRICT_PREFERRED) == true }
            .getOrDefault(false)

    private fun activeMetadataExtensionOrNull(): MusicExtension? {
        val mode = configuredMode()
        if (mode == MODE_STANDALONE) return null
        val available = extensionsFlow.value
        if (available.isEmpty()) return null
        val selected = runCatching { settings.getString(ZarzSessionManager.KEY_METADATA_EXT)?.trim() }.getOrNull()
        if (selected.isNullOrEmpty() || selected == "auto") {
            return available.firstOrNull()
        }
        return available.firstOrNull { it.id == selected || it.name == selected } ?: available.firstOrNull()
    }

    private fun requireMetadataExtension(): MusicExtension =
        activeMetadataExtensionOrNull()
            ?: throw IllegalStateException("Nessuna estensione metadati attiva. Installa es. Spotify in Echo o passa alla modalità Sorgente FLAC autonoma.")

    @Suppress("UNCHECKED_CAST")
    private suspend inline fun <reified T> MusicExtension.client(): T =
        instance.value().getOrThrow() as T

    private suspend inline fun <reified T> MusicExtension.clientOrNull(): T? =
        instance.value().getOrNull() as? T

    private fun isStandaloneItemId(id: String): Boolean =
        id.startsWith("tid:") || id.startsWith("qbz:") || id.startsWith("dzr:")

    // --- Browsing & Search ---

    override suspend fun loadHomeFeed(): Feed<Shelf> {
        val metaExt = activeMetadataExtensionOrNull()
        if (metaExt != null) {
            val homeClient = metaExt.clientOrNull<HomeFeedClient>()
            if (homeClient != null) return homeClient.loadHomeFeed()
        }
        return resolver.loadStandaloneHomeFeed()
    }

    override suspend fun loadLibraryFeed(): Feed<Shelf> {
        val metaExt = activeMetadataExtensionOrNull()
            ?: return emptyList<Shelf>().toFeed()
        val libClient = metaExt.clientOrNull<LibraryFeedClient>()
            ?: return emptyList<Shelf>().toFeed()
        return libClient.loadLibraryFeed()
    }

    override suspend fun loadSearchFeed(query: String): Feed<Shelf> {
        val metaExt = activeMetadataExtensionOrNull()
        if (metaExt != null) {
            val searchClient = metaExt.clientOrNull<SearchFeedClient>()
            if (searchClient != null) return searchClient.loadSearchFeed(query)
        }
        return resolver.searchStandaloneFeed(query)
    }

    // --- Core Audio Bridge & Stream Resolution ---

    override suspend fun loadTrack(track: Track, isDownload: Boolean): Track {
        val metaExt = if (isStandaloneItemId(track.id)) null else activeMetadataExtensionOrNull()

        if (metaExt != null) {
            val metaTrack = runCatching {
                metaExt.client<TrackClient>().loadTrack(track, isDownload)
            }.getOrElse { track }

            // Podcasts / pure videos should play directly from metadata extension
            if (metaTrack.type == Track.Type.Podcast) {
                return wrapMetadataTrackOriginalsOnly(metaTrack, metaExt)
            }

            val spec = mergeMatchSpecs(primary = TrackMatcher.toMatchSpec(metaTrack), secondary = TrackMatcher.toMatchSpec(track))
            val flacCandidates = resolveOrderedCandidates(spec, isDownload)

            val allowFallback = fallbackToOriginalEnabled()
            val wrappedOriginalServers = if (allowFallback) {
                wrapMetadataServers(metaTrack.servers, metaExt)
            } else {
                emptyList()
            }
            val primaryOriginalFallback = wrappedOriginalServers.maxByOrNull { it.quality }

            val flacServers = buildTrackFlacServers(
                flacCandidates = flacCandidates,
                spec = spec,
                isDownload = isDownload,
                hasOriginalFallback = wrappedOriginalServers.isNotEmpty(),
                fallbackMetaExtId = if (allowFallback) metaExt.id else null,
                fallbackOriginal = primaryOriginalFallback?.extras?.get(EXTRA_ORIGINAL_STREAM)
            )

            val combinedServers = flacServers + wrappedOriginalServers
            val preservedNonServers = metaTrack.backgrounds.map { wrapMetadataStreamable(it, metaExt.id) } +
                metaTrack.subtitles.map { wrapMetadataStreamable(it, metaExt.id) }

            val resolvedDuration = metaTrack.duration ?: flacCandidates.firstOrNull()?.durationMs?.takeIf { it > 0L }
            return metaTrack.copy(
                duration = resolvedDuration,
                isPlayable = if (flacCandidates.isNotEmpty() || wrappedOriginalServers.isNotEmpty()) Track.Playable.Yes else metaTrack.isPlayable,
                streamables = combinedServers + preservedNonServers
            )
        }

        // Standalone Mode (or standalone track ID)
        val spec = TrackMatcher.toMatchSpec(track)
        val flacCandidates = resolveOrderedCandidates(spec, isDownload)
        val flacServers = buildTrackFlacServers(
            flacCandidates = flacCandidates,
            spec = spec,
            isDownload = isDownload,
            hasOriginalFallback = false,
            fallbackMetaExtId = null,
            fallbackOriginal = null
        )
        val resolvedDuration = track.duration ?: flacCandidates.firstOrNull()?.durationMs?.takeIf { it > 0L }
        return track.copy(
            duration = resolvedDuration,
            streamables = flacServers
        )
    }

    override suspend fun loadStreamableMedia(streamable: Streamable, isDownload: Boolean): Streamable.Media {
        // Delegate metadata extension streamables (original fallback audio, Canvas backgrounds, subtitles)
        val metaExtId = streamable.extras[EXTRA_META_EXT_ID]
        if (metaExtId != null && streamable.extras[EXTRA_IS_FLAC_BRIDGE] != "true") {
            val ext = findExtensionById(metaExtId)
                ?: throw IllegalStateException("Estensione metadati '$metaExtId' non disponibile.")
            val unwrapped = unwrapMetadataStreamable(streamable)
            return ext.client<TrackClient>().loadStreamableMedia(unwrapped, isDownload)
        }

        val providerKey = streamable.extras[EXTRA_PROVIDER_KEY].orEmpty()
        val strictManual = streamable.extras[EXTRA_STRICT_PROVIDER] == "true"
        val specJson = streamable.extras[EXTRA_MATCH_SPEC]
            ?: throw IllegalStateException("Dati di corrispondenza FLAC mancanti nello streamable.")
        val spec = ZarzSessionManager.json.decodeFromString<TrackMatchSpec>(specJson)

        if (strictManual) {
            val chosenProvider = ProviderSpec.byKey(providerKey)
                ?: throw IllegalStateException("Server FLAC '$providerKey' non riconosciuto.")
            val manualResolved = resolver.resolveSingleProvider(
                spec = spec,
                provider = chosenProvider,
                budgetMs = if (isDownload) 27_000L else 13_200L,
                scope = extensionScope
            )
            return toEchoServerMedia(manualResolved, spec, streamable.quality, isDownload)
        }

        val resolved: ResolvedFlacStream? = (if (providerKey.isNotEmpty()) resolver.getCachedStream(providerKey, spec) else null)
            ?: run {
                val freshList = resolver.resolve(
                    spec = if (providerKey.isNotEmpty()) spec.copy(preferredProviderKey = providerKey) else spec,
                    budgetMs = if (isDownload) 27_000L else 13_200L,
                    collectAll = false,
                    scope = extensionScope,
                    onlyFirstPreferred = strictPreferredEnabled()
                )
                freshList.firstOrNull()
            }

        if (resolved != null) {
            return toEchoServerMedia(resolved, spec, streamable.quality, isDownload)
        }

        // Optional runtime fallback to metadata extension's original stream if FLAC expired and could not re-resolve
        val fallbackExtId = streamable.extras[EXTRA_FALLBACK_EXT_ID]
        val fallbackStreamRaw = streamable.extras[EXTRA_FALLBACK_STREAM]
        if (fallbackExtId != null && fallbackStreamRaw != null) {
            val ext = findExtensionById(fallbackExtId)
            if (ext != null) {
                val orig = ZarzSessionManager.json.decodeFromString<Streamable>(fallbackStreamRaw)
                return ext.client<TrackClient>().loadStreamableMedia(orig, isDownload)
            }
        }

        throw IllegalStateException("Nessuno stream FLAC verificato disponibile per '${spec.title}'.")
    }

    override suspend fun loadFeed(track: Track): Feed<Shelf>? {
        if (isStandaloneItemId(track.id)) return null
        val metaExt = activeMetadataExtensionOrNull() ?: return null
        return metaExt.clientOrNull<TrackClient>()?.loadFeed(track)
    }

    private suspend fun resolveOrderedCandidates(spec: TrackMatchSpec, isDownload: Boolean): List<ResolvedFlacStream> {
        val budget = if (isDownload) 27_000L else 13_200L
        val found = resolver.resolve(
            spec = spec,
            budgetMs = budget,
            collectAll = isDownload,
            scope = extensionScope,
            onlyFirstPreferred = !isDownload && strictPreferredEnabled()
        )
        // For downloads, prefer direct Progressive .flac files over DASH fMP4 manifests when multiple exist
        return if (isDownload && found.size > 1) {
            found.sortedBy { if (it.isDash) 1 else 0 }
        } else {
            found
        }
    }

    private fun buildTrackFlacServers(
        flacCandidates: List<ResolvedFlacStream>,
        spec: TrackMatchSpec,
        isDownload: Boolean,
        hasOriginalFallback: Boolean,
        fallbackMetaExtId: String?,
        fallbackOriginal: String?
    ): List<Streamable> {
        if (isDownload || !manualServerPickerEnabled()) {
            return flacCandidates.mapIndexed { index, found ->
                buildFlacStreamable(
                    id = "flac_bridge:${found.providerKey}:${spec.id}",
                    title = found.label(),
                    providerKey = found.providerKey,
                    strictProvider = false,
                    spec = spec,
                    qualityRank = 10_000 - index * 10,
                    fallbackMetaExtId = fallbackMetaExtId,
                    fallbackOriginal = fallbackOriginal
                )
            }
        }

        val autoCandidate = flacCandidates.firstOrNull()
        val canResolveAmazon = spec.spotifyId.length == 22 || spec.youtubeId.length == 11 || autoCandidate?.providerKey == "amazon"
        val manualProviders = manager.preferenceOrder().filter { p ->
            p.key != "amazon" || canResolveAmazon
        }

        return buildList {
            // 1. Default Automatic server chip (quality = 10_000 when FLAC found or no original fallback)
            if (autoCandidate != null || !hasOriginalFallback) {
                add(
                    buildFlacStreamable(
                        id = "flac_bridge:auto:${spec.id}",
                        title = if (autoCandidate != null) "Auto · ${autoCandidate.providerName}" else "Auto · FLAC",
                        providerKey = autoCandidate?.providerKey.orEmpty(),
                        strictProvider = false,
                        spec = spec,
                        qualityRank = 10_000,
                        fallbackMetaExtId = fallbackMetaExtId,
                        fallbackOriginal = fallbackOriginal
                    )
                )
            }

            // 2. Individual manual server chips for every enabled source (on-demand resolution on tap)
            val manualBaseQuality = if (autoCandidate != null || !hasOriginalFallback) 9_900 else 800
            manualProviders.forEachIndexed { idx, p ->
                val chipTitle = if (autoCandidate?.providerKey == p.key) {
                    autoCandidate.label()
                } else {
                    "${p.name} · FLAC"
                }
                add(
                    buildFlacStreamable(
                        id = "flac_bridge:manual:${p.key}:${spec.id}",
                        title = chipTitle,
                        providerKey = p.key,
                        strictProvider = true,
                        spec = spec,
                        qualityRank = (manualBaseQuality - idx * 10).coerceAtLeast(10),
                        fallbackMetaExtId = null,
                        fallbackOriginal = null
                    )
                )
            }
        }
    }

    private fun toEchoServerMedia(
        found: ResolvedFlacStream,
        spec: TrackMatchSpec,
        qualityRank: Int,
        isDownload: Boolean
    ): Streamable.Media.Server {
        val label = found.label()
        val playerHeaders = mapOf("User-Agent" to ZarzSessionManager.MODERN_CHROME_UA)
        val directUrl = found.url
        if (!directUrl.isNullOrBlank()) {
            return Streamable.Source.Http(
                request = directUrl.toGetRequest(playerHeaders),
                type = Streamable.SourceType.Progressive,
                quality = qualityRank,
                title = label,
                isVideo = false,
                isLive = false
            ).toMedia()
        }

        val manifestB64 = found.dashManifestBase64
            ?: throw IllegalStateException("Risposta Tidal priva di URL e manifest DASH.")
        if (isDownload) {
            return Streamable.Source.Raw(
                streamProvider = resolver.createDashDownloadInputProvider(manifestB64),
                id = "tidal_dash_dl:${spec.id}",
                quality = qualityRank,
                title = label,
                isVideo = false,
                isLive = false
            ).toMedia()
        }

        val dataUri = "data:application/dash+xml;base64,$manifestB64"
        return Streamable.Source.Http(
            request = dataUri.toGetRequest(playerHeaders),
            type = Streamable.SourceType.DASH,
            quality = qualityRank,
            title = label,
            isVideo = false,
            isLive = true
        ).toMedia()
    }

    private fun buildFlacStreamable(
        id: String,
        title: String,
        providerKey: String,
        strictProvider: Boolean,
        spec: TrackMatchSpec,
        qualityRank: Int,
        fallbackMetaExtId: String?,
        fallbackOriginal: String?
    ): Streamable {
        val extras = buildMap {
            put(EXTRA_IS_FLAC_BRIDGE, "true")
            put(EXTRA_PROVIDER_KEY, providerKey)
            put(EXTRA_STRICT_PROVIDER, if (strictProvider) "true" else "false")
            put(EXTRA_MATCH_SPEC, ZarzSessionManager.json.encodeToString(spec))
            if (fallbackMetaExtId != null && fallbackOriginal != null) {
                put(EXTRA_FALLBACK_EXT_ID, fallbackMetaExtId)
                put(EXTRA_FALLBACK_STREAM, fallbackOriginal)
            }
        }
        return Streamable.server(
            id = id,
            quality = qualityRank,
            title = title,
            extras = extras
        )
    }

    private fun wrapMetadataServers(servers: List<Streamable>, metaExt: MusicExtension): List<Streamable> {
        val sorted = servers.sortedByDescending { it.quality }
        return sorted.mapIndexed { idx, orig ->
            val origTitle = orig.title?.takeIf(String::isNotBlank) ?: "Audio"
            orig.copy(
                quality = (900 - idx * 10).coerceAtLeast(1),
                title = "${metaExt.name} · $origTitle",
                extras = orig.extras + mapOf(
                    EXTRA_META_EXT_ID to metaExt.id,
                    EXTRA_ORIGINAL_STREAM to ZarzSessionManager.json.encodeToString(orig)
                )
            )
        }
    }

    private fun wrapMetadataStreamable(streamable: Streamable, metaExtId: String): Streamable =
        streamable.copy(
            extras = streamable.extras + mapOf(
                EXTRA_META_EXT_ID to metaExtId,
                EXTRA_ORIGINAL_STREAM to ZarzSessionManager.json.encodeToString(streamable)
            )
        )

    private fun wrapMetadataTrackOriginalsOnly(metaTrack: Track, metaExt: MusicExtension): Track =
        metaTrack.copy(
            streamables = metaTrack.streamables.map { wrapMetadataStreamable(it, metaExt.id) }
        )

    private fun unwrapMetadataStreamable(streamable: Streamable): Streamable =
        streamable.extras[EXTRA_ORIGINAL_STREAM]?.let {
            ZarzSessionManager.json.decodeFromString<Streamable>(it)
        } ?: streamable.copy(extras = streamable.extras - EXTRA_META_EXT_ID)

    private fun mergeMatchSpecs(primary: TrackMatchSpec, secondary: TrackMatchSpec): TrackMatchSpec =
        primary.copy(
            title = primary.title.ifEmpty { secondary.title },
            artist = primary.artist.ifEmpty { secondary.artist },
            artists = primary.artists.ifEmpty { secondary.artists },
            durationMs = if (primary.durationMs > 0L) primary.durationMs else secondary.durationMs,
            isrc = primary.isrc.ifEmpty { secondary.isrc },
            spotifyId = primary.spotifyId.ifEmpty { secondary.spotifyId },
            youtubeId = primary.youtubeId.ifEmpty { secondary.youtubeId }
        )

    private fun findExtensionById(id: String): MusicExtension? =
        extensionsFlow.value.firstOrNull { it.id == id }

    // --- Delegated Clients (Album, Artist, Playlist, Library Actions, Radio) ---

    override suspend fun loadAlbum(album: Album): Album {
        if (isStandaloneItemId(album.id)) return resolver.loadStandaloneAlbum(album)
        val metaExt = activeMetadataExtensionOrNull() ?: return resolver.loadStandaloneAlbum(album)
        return metaExt.client<AlbumClient>().loadAlbum(album)
    }

    override suspend fun loadTracks(album: Album): Feed<Track>? {
        if (isStandaloneItemId(album.id)) return resolver.loadStandaloneAlbumTracks(album)
        val metaExt = activeMetadataExtensionOrNull() ?: return resolver.loadStandaloneAlbumTracks(album)
        return metaExt.client<AlbumClient>().loadTracks(album)
    }

    override suspend fun loadFeed(album: Album): Feed<Shelf>? {
        if (isStandaloneItemId(album.id)) return null
        val metaExt = activeMetadataExtensionOrNull() ?: return null
        return metaExt.client<AlbumClient>().loadFeed(album)
    }

    override suspend fun loadArtist(artist: Artist): Artist {
        if (isStandaloneItemId(artist.id)) return resolver.loadStandaloneArtist(artist)
        val metaExt = activeMetadataExtensionOrNull() ?: return resolver.loadStandaloneArtist(artist)
        return metaExt.client<ArtistClient>().loadArtist(artist)
    }

    override suspend fun loadFeed(artist: Artist): Feed<Shelf> {
        if (isStandaloneItemId(artist.id)) return resolver.loadStandaloneArtistFeed(artist)
        val metaExt = activeMetadataExtensionOrNull() ?: return resolver.loadStandaloneArtistFeed(artist)
        return metaExt.client<ArtistClient>().loadFeed(artist)
    }

    override suspend fun loadPlaylist(playlist: Playlist): Playlist =
        requireMetadataExtension().client<PlaylistClient>().loadPlaylist(playlist)

    override suspend fun loadTracks(playlist: Playlist): Feed<Track> =
        requireMetadataExtension().client<PlaylistClient>().loadTracks(playlist)

    override suspend fun loadFeed(playlist: Playlist): Feed<Shelf>? =
        requireMetadataExtension().client<PlaylistClient>().loadFeed(playlist)

    override suspend fun listEditablePlaylists(track: Track?): List<Pair<Playlist, Boolean>> =
        activeMetadataExtensionOrNull()?.clientOrNull<PlaylistEditClient>()?.listEditablePlaylists(track) ?: emptyList()

    override suspend fun createPlaylist(title: String, description: String?): Playlist =
        requireMetadataExtension().clientOrNull<PlaylistEditClient>()?.createPlaylist(title, description)
            ?: throw UnsupportedOperationException("Operazione non supportata dall'estensione metadati.")

    override suspend fun addTracksToPlaylist(playlist: Playlist, tracks: List<Track>, index: Int, new: List<Track>) =
        requireMetadataExtension().clientOrNull<PlaylistEditClient>()?.addTracksToPlaylist(playlist, tracks, index, new)
            ?: throw UnsupportedOperationException("Operazione non supportata dall'estensione metadati.")

    override suspend fun removeTracksFromPlaylist(playlist: Playlist, tracks: List<Track>, indexes: List<Int>) =
        requireMetadataExtension().clientOrNull<PlaylistEditClient>()?.removeTracksFromPlaylist(playlist, tracks, indexes)
            ?: throw UnsupportedOperationException("Operazione non supportata dall'estensione metadati.")

    override suspend fun moveTrackInPlaylist(playlist: Playlist, tracks: List<Track>, fromIndex: Int, toIndex: Int) =
        requireMetadataExtension().clientOrNull<PlaylistEditClient>()?.moveTrackInPlaylist(playlist, tracks, fromIndex, toIndex)
            ?: throw UnsupportedOperationException("Operazione non supportata dall'estensione metadati.")

    override suspend fun editPlaylistMetadata(playlist: Playlist, title: String, description: String?) =
        requireMetadataExtension().clientOrNull<PlaylistEditClient>()?.editPlaylistMetadata(playlist, title, description)
            ?: throw UnsupportedOperationException("Operazione non supportata dall'estensione metadati.")

    override suspend fun deletePlaylist(playlist: Playlist) =
        requireMetadataExtension().clientOrNull<PlaylistEditClient>()?.deletePlaylist(playlist)
            ?: throw UnsupportedOperationException("Operazione non supportata dall'estensione metadati.")

    override suspend fun isItemSaved(item: EchoMediaItem): Boolean =
        activeMetadataExtensionOrNull()?.clientOrNull<SaveClient>()?.isItemSaved(item) ?: false

    override suspend fun saveToLibrary(item: EchoMediaItem, shouldSave: Boolean) =
        requireMetadataExtension().clientOrNull<SaveClient>()?.saveToLibrary(item, shouldSave)
            ?: throw UnsupportedOperationException("Salvataggio non supportato in modalità autonoma.")

    override suspend fun isItemLiked(item: EchoMediaItem): Boolean =
        activeMetadataExtensionOrNull()?.clientOrNull<LikeClient>()?.isItemLiked(item) ?: false

    override suspend fun likeItem(item: EchoMediaItem, shouldLike: Boolean) =
        requireMetadataExtension().clientOrNull<LikeClient>()?.likeItem(item, shouldLike)
            ?: throw UnsupportedOperationException("Mi piace non supportato in modalità autonoma.")

    override suspend fun onShare(item: EchoMediaItem): String {
        val metaExt = activeMetadataExtensionOrNull()
        if (metaExt != null && !isStandaloneItemId(item.id)) {
            metaExt.clientOrNull<ShareClient>()?.onShare(item)?.let { return it }
        }
        return when {
            item.id.startsWith("tid:track:") -> "https://tidal.com/browse/track/${item.id.removePrefix("tid:track:")}"
            item.id.startsWith("qbz:track:") -> "https://open.qobuz.com/track/${item.id.removePrefix("qbz:track:")}"
            item.id.startsWith("dzr:track:") -> "https://www.deezer.com/track/${item.id.removePrefix("dzr:track:")}"
            item.id.startsWith("dzr:album:") -> "https://www.deezer.com/album/${item.id.removePrefix("dzr:album:")}"
            else -> item.title
        }
    }

    override suspend fun isFollowing(item: EchoMediaItem): Boolean =
        activeMetadataExtensionOrNull()?.clientOrNull<FollowClient>()?.isFollowing(item) ?: false

    override suspend fun followItem(item: EchoMediaItem, shouldFollow: Boolean) =
        requireMetadataExtension().clientOrNull<FollowClient>()?.followItem(item, shouldFollow)
            ?: throw UnsupportedOperationException("Segui non supportato in modalità autonoma.")

    override suspend fun getFollowersCount(item: EchoMediaItem): Long? =
        activeMetadataExtensionOrNull()?.clientOrNull<FollowClient>()?.getFollowersCount(item)

    override suspend fun isItemHidden(item: EchoMediaItem): Boolean =
        activeMetadataExtensionOrNull()?.clientOrNull<HideClient>()?.isItemHidden(item) ?: false

    override suspend fun hideItem(item: EchoMediaItem, shouldHide: Boolean) =
        requireMetadataExtension().clientOrNull<HideClient>()?.hideItem(item, shouldHide)
            ?: throw UnsupportedOperationException("Nascondi non supportato in modalità autonoma.")

    override suspend fun radio(item: EchoMediaItem, context: EchoMediaItem?): Radio =
        requireMetadataExtension().clientOrNull<RadioClient>()?.radio(item, context)
            ?: throw UnsupportedOperationException("Radio disponibile solo con estensione metadati in modalità Bridge.")

    override suspend fun loadRadio(radio: Radio): Radio =
        requireMetadataExtension().clientOrNull<RadioClient>()?.loadRadio(radio)
            ?: throw UnsupportedOperationException("Radio disponibile solo con estensione metadati in modalità Bridge.")

    override suspend fun loadTracks(radio: Radio): Feed<Track> =
        requireMetadataExtension().clientOrNull<RadioClient>()?.loadTracks(radio)
            ?: throw UnsupportedOperationException("Radio disponibile solo con estensione metadati in modalità Bridge.")
}
