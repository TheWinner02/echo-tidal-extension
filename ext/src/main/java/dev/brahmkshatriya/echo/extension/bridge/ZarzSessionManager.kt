package dev.brahmkshatriya.echo.extension.bridge

import dev.brahmkshatriya.echo.common.helpers.ContinuationCallback.Companion.await
import dev.brahmkshatriya.echo.common.helpers.WebViewClient
import dev.brahmkshatriya.echo.common.helpers.WebViewRequest
import dev.brahmkshatriya.echo.common.models.NetworkConnection
import dev.brahmkshatriya.echo.common.models.NetworkRequest
import dev.brahmkshatriya.echo.common.models.NetworkRequest.Companion.toGetRequest
import dev.brahmkshatriya.echo.common.settings.Settings
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

class ZarzSessionManager(
    baseHttpClient: OkHttpClient,
    private val logs: DiagnosticLogBuffer,
    private val onNotify: (String) -> Unit,
    private val gateBaseUrl: String = DEFAULT_GATE,
    private val clock: () -> Long = System::currentTimeMillis
) {
    private val httpClient: OkHttpClient = withBrowserHeaders(baseHttpClient)

    companion object {
        const val DEFAULT_GATE = "https://api.zarz.moe/v2"
        const val CALLBACK_URI = "spotiflac://session-grant"

        const val KEY_PREFERRED = "preferred"
        const val KEY_STRICT_PREFERRED = "strict_preferred_only"
        const val KEY_MANUAL_SERVER_PICKER = "manual_server_picker"
        const val KEY_ECHO_STREAM_QUALITY = "stream_quality"
        const val KEY_AUTO_VERIFY = "auto_verify"
        const val KEY_ENABLED_SOURCES = "enabled_sources"
        const val KEY_BRIDGE_MODE = "bridge_mode"
        const val KEY_METADATA_EXT = "metadata_extension"
        const val KEY_FALLBACK_ORIGINAL = "fallback_to_metadata_audio"

        const val INSTALL_KEY = "installation_id"
        const val PENDING_KEY = "verification_pending"
        const val AUTO_PAUSED_KEY = "auto_verification_paused"
        const val AUTO_RETRY_KEY = "auto_verification_retry_after"
        const val VERIFY_LIMIT_KEY = "verification_rate_limit_until"
        const val VERIFY_SERVER_LIMIT_KEY = "verification_server_retry_after_until"
        const val FORCE_GUARD_KEY = "verification_manual_retry_after"
        const val VERIFY_FORCE_KEY = "verification_force_after_limit"
        const val RENEW_CHECKED_PREFIX = "verification_session_checked_"
        const val RENEW_ATTEMPT_PREFIX = "verification_refresh_retry_until_"

        const val RENEW_INTERVAL_MS = 6L * 60L * 60L * 1000L
        const val RENEW_SOON_MS = 60L * 60L * 1000L
        const val RENEW_GUARD_MS = 15L * 60L * 1000L
        const val MANUAL_GUARD_MS = 30_000L
        const val NOTICE_COOLDOWN_MS = 60_000L

        private val SAFE_PATH_REGEX = Regex("^/[a-z0-9/]+$", RegexOption.IGNORE_CASE)
        private val NUMERIC_SECONDS_REGEX = Regex("^\\d+(?:\\.\\d+)?$")
        private val CALLBACK_STOP_REGEX = Regex("^spotiflac://session-grant[?#].*", RegexOption.IGNORE_CASE)

        const val MODERN_CHROME_UA =
            "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"

        /**
         * Ensures no outbound request ever sends OkHttp's default `okhttp/x.y` User-Agent,
         * which Cloudflare WAF in front of `/v2/dl/...` blocks with `HTTP 403 Forbidden`.
         */
        fun withBrowserHeaders(client: OkHttpClient): OkHttpClient =
            client.newBuilder()
                .addInterceptor { chain ->
                    val req = chain.request()
                    val ua = req.header("User-Agent")
                    val updated = if (ua.isNullOrBlank() || ua.startsWith("okhttp/", ignoreCase = true)) {
                        req.newBuilder().header("User-Agent", MODERN_CHROME_UA).build()
                    } else {
                        req
                    }
                    chain.proceed(updated)
                }
                .build()

        val json = Json {
            ignoreUnknownKeys = true
            encodeDefaults = true
        }
    }

    @Volatile
    private var settingsRef: Settings? = null

    @Volatile
    private var webViewClientRef: WebViewClient? = null

    @Volatile
    private var networkState: NetworkConnection = NetworkConnection.Unmetered

    @Volatile
    var qobuzUnavailableUntil: Long = 0L
        private set

    private val verifyMutex = Mutex()
    private val autoMutex = Mutex()
    private val forgetGeneration = AtomicLong(0L)

    @Volatile
    private var lastNoticeMs: Long = 0L

    private data class AutoQueueItem(val key: String, var force: Boolean)

    private val autoQueue = ArrayDeque<AutoQueueItem>()

    @Volatile
    var autoRunning: Boolean = false
        private set

    @Volatile
    private var autoHadFailure: Boolean = false

    @Volatile
    private var autoAnnounced: Boolean = false

    private var autoJob: Job? = null

    fun attachSettings(settings: Settings) {
        this.settingsRef = settings
    }

    fun attachWebViewClient(webViewClient: WebViewClient) {
        this.webViewClientRef = webViewClient
    }

    fun updateNetworkConnection(connection: NetworkConnection, scope: CoroutineScope?) {
        val wasOffline = this.networkState == NetworkConnection.NotConnected
        this.networkState = connection
        if (connection != NetworkConnection.NotConnected && (wasOffline || autoRunning)) {
            if (autoRunning && scope != null) {
                wakeAutomatic(scope)
            }
        }
    }

    fun isOnline(): Boolean = networkState != NetworkConnection.NotConnected

    fun log(message: String) {
        logs.append(message, clock())
    }

    fun notify(message: String) {
        onNotify(message)
    }

    fun notice(message: String) {
        val now = clock()
        if (now - lastNoticeMs >= NOTICE_COOLDOWN_MS) {
            lastNoticeMs = now
            notify(message)
        }
    }

    private fun settingsOrThrow(): Settings =
        settingsRef ?: error("Settings non ancora inizializzate")

    private fun getLong(key: String): Long =
        runCatching { settingsRef?.getString(key)?.trim()?.toLongOrNull() ?: 0L }.getOrDefault(0L)

    private fun putLong(key: String, value: Long) {
        settingsRef?.putString(key, value.toString())
    }

    private fun removeKey(key: String) {
        settingsRef?.putString(key, null)
        settingsRef?.putBoolean(key, null)
        settingsRef?.putStringSet(key, null)
    }

    fun installId(): String {
        val s = settingsOrThrow()
        val current = s.getString(INSTALL_KEY)?.trim().orEmpty()
        if (StoredSession.HEX_32_REGEX.matches(current)) return current
        val generated = ZarzCrypto.generateInstallId(clock())
        s.putString(INSTALL_KEY, generated)
        return generated
    }

    fun getStoredSession(provider: ProviderSpec): StoredSession? {
        val raw = runCatching { settingsRef?.getString(provider.storage) }.getOrNull() ?: return null
        val parsed = runCatching { json.decodeFromString<StoredSession>(raw) }.getOrNull() ?: return null
        return parsed.takeIf { it.isValid() }
    }

    fun saveSession(
        provider: ProviderSpec,
        data: JsonObject?,
        installId: String,
        old: StoredSession?
    ): Boolean {
        val id = data?.stringField("session_id").orEmpty().ifEmpty { old?.id.orEmpty() }
        val secret = data?.stringField("session_secret").orEmpty().ifEmpty { old?.secret.orEmpty() }
        val expiresAt = data?.stringField("expires_at").orEmpty().ifEmpty { old?.expiresAt.orEmpty() }
        val candidate = StoredSession(
            id = id,
            secret = secret,
            expiresAt = expiresAt,
            installId = installId
        )
        if (!candidate.isValid()) return false
        val s = settingsOrThrow()
        s.putString(provider.storage, json.encodeToString(candidate))
        putLong(RENEW_CHECKED_PREFIX + provider.key, clock())
        if (old == null) {
            removeKey(RENEW_ATTEMPT_PREFIX + provider.key)
        }
        return true
    }

    fun clearStalePendingOnStartup() {
        removeKey(PENDING_KEY)
    }

    fun automaticEnabled(): Boolean =
        runCatching { settingsRef?.getBoolean(KEY_AUTO_VERIFY) != false }.getOrDefault(true)

    fun automaticAllowed(): Boolean =
        automaticEnabled() && runCatching { settingsRef?.getBoolean(AUTO_PAUSED_KEY) != true }.getOrDefault(true)

    fun verificationWaitMs(): Long = waitUntil(VERIFY_LIMIT_KEY)

    fun serverRetryWaitMs(): Long = waitUntil(VERIFY_SERVER_LIMIT_KEY)

    fun waitUntil(key: String): Long {
        val until = getLong(key)
        return if (until > 0L) maxOf(0L, until - clock()) else 0L
    }

    fun retryDelayed(): Boolean = getLong(AUTO_RETRY_KEY) > clock()

    fun verificationWaitMessage(): String =
        "Verifiche automatiche in pausa dopo HTTP 429: ancora circa ${FlacBridgeDiagnostics.humanWait(verificationWaitMs())}."

    fun enabledProviderKeys(): Set<String> {
        val configured = runCatching { settingsRef?.getStringSet(KEY_ENABLED_SOURCES) }.getOrNull()
        if (configured.isNullOrEmpty()) {
            return ProviderSpec.ALL.map { it.key }.toSet()
        }
        return configured.filter { ProviderSpec.byKey(it) != null }.toSet()
            .ifEmpty { ProviderSpec.ALL.map { it.key }.toSet() }
    }

    fun preferenceOrder(): List<ProviderSpec> {
        val preferredName = runCatching { settingsRef?.getString(KEY_PREFERRED)?.trim().orEmpty() }.getOrDefault("")
        val enabled = enabledProviderKeys()
        val activeProviders = ProviderSpec.ALL.filter { it.key in enabled }
        val first = activeProviders.firstOrNull {
            it.name.equals(preferredName, ignoreCase = true) || it.key.equals(preferredName, ignoreCase = true)
        } ?: activeProviders.firstOrNull()
        return buildList {
            if (first != null) add(first)
            for (p in activeProviders) {
                if (first == null || p.key != first.key) add(p)
            }
        }
    }

    fun sessionSummaryForUi(provider: ProviderSpec): String {
        val now = clock()
        val s = getStoredSession(provider)
        val serverWait = serverRetryWaitMs()
        val baseStatus = if (s == null) {
            "Non verificata"
        } else {
            val exp = s.expiresAtEpochMs()
            when {
                exp != null && exp <= now -> "Sessione scaduta (rinnovo richiesto)"
                exp != null -> "Sessione attiva (scade tra ~${FlacBridgeDiagnostics.humanWait(exp - now)})"
                else -> "Sessione attiva"
            }
        }
        val suffix = buildString {
            if (provider.experimental) {
                append(" · Solo FLAC diretti non cifrati")
            }
            if (serverWait > 0L) {
                append(" · Attesa server: ${FlacBridgeDiagnostics.humanWait(serverWait)}")
            }
        }
        return "$baseStatus$suffix"
    }

    /**
     * Read-only diagnostic report matching `showVerificationWaits()` in main.js.
     * Never triggers network calls, modifies waits, or clears sessions.
     */
    fun showVerificationWaits() {
        val automatic429 = verificationWaitMs()
        val server = serverRetryWaitMs()
        val cancelled = waitUntil(AUTO_RETRY_KEY)
        val guard = waitUntil(FORCE_GUARD_KEY)
        log(
            if (automatic429 > 0L) "Pausa automatica per HTTP 429: ancora circa ${FlacBridgeDiagnostics.humanWait(automatic429)}."
            else "Pausa automatica per HTTP 429: nessuna attesa memorizzata."
        )
        log(
            if (server > 0L) "Attesa indicata da una risposta precedente del server e memorizzata: ancora circa ${FlacBridgeDiagnostics.humanWait(server)}."
            else "Attesa del server: nessun tempo esplicito memorizzato. Il server può comunque rispondere HTTP 429."
        )
        if (server > 0L) {
            log("I pulsanti Attiva rispettano questa attesa; Forza verifica manuale può chiedere un solo tentativo anticipato.")
        }
        if (cancelled > 0L) {
            log("Altra pausa automatica dopo verifica interrotta: ancora circa ${FlacBridgeDiagnostics.humanWait(cancelled)}.")
        }
        if (guard > 0L) {
            log("Sicurezza tra tentativi manuali: attendi ancora circa ${FlacBridgeDiagnostics.humanWait(guard)}.")
        }
        when {
            !automaticEnabled() -> log("Verifica automatica: disattivata nelle impostazioni.")
            !automaticAllowed() -> log("Verifica automatica: sospesa dopo la cancellazione delle sessioni.")
            else -> log("Rinnovo delle sessioni: controllo previsto ogni 6 ore mentre Echo è attiva, salvo attese e disponibilità del servizio.")
        }
        notify("Tempi di attesa riportati nel Registro; nessuna verifica è stata avviata.")
    }

    fun verificationForceKeys(): List<String> {
        val set = runCatching { settingsRef?.getStringSet(VERIFY_FORCE_KEY) }.getOrNull().orEmpty()
        return ProviderSpec.ALL.map { it.key }.filter { it in set }
    }

    fun markVerificationForce(provider: ProviderSpec?) {
        if (provider == null) return
        val updated = (verificationForceKeys() + provider.key).toSet()
        settingsRef?.putStringSet(VERIFY_FORCE_KEY, updated)
    }

    fun clearVerificationForce(provider: ProviderSpec?) {
        if (provider == null) return
        val updated = verificationForceKeys().filter { it != provider.key }.toSet()
        if (updated.isEmpty()) removeKey(VERIFY_FORCE_KEY)
        else settingsRef?.putStringSet(VERIFY_FORCE_KEY, updated)
    }

    private fun parseRetryAfterMs(response: Response?, data: JsonObject?): Long {
        val headerVal = response?.header("Retry-After")?.trim().orEmpty()
        val fromHeader = when {
            headerVal.isEmpty() -> 0L
            NUMERIC_SECONDS_REGEX.matches(headerVal) -> ((headerVal.toDoubleOrNull() ?: 0.0) * 1000.0).toLong()
            else -> runCatching {
                val parsed = ZonedDateTime.parse(headerVal, DateTimeFormatter.RFC_1123_DATE_TIME)
                    .toInstant().toEpochMilli()
                maxOf(0L, parsed - clock())
            }.getOrDefault(0L)
        }
        val seconds = data?.doubleField("retry_after_seconds")
            ?: data?.doubleField("retryAfterSeconds")
            ?: 0.0
        val fromBody = if (seconds > 0.0 && seconds.isFinite()) (seconds * 1000.0).toLong() else 0L
        return maxOf(fromHeader, fromBody)
    }

    private fun rememberVerificationLimit(response: Response?, data: JsonObject?, provider: ProviderSpec?): Long {
        val requested = parseRetryAfterMs(response, data)
        val now = clock()
        val wait = maxOf(15L * 60L * 1000L, requested)
        val old = getLong(VERIFY_LIMIT_KEY)
        putLong(VERIFY_LIMIT_KEY, maxOf(old, now + wait))
        if (requested > 0L) {
            val serverOld = getLong(VERIFY_SERVER_LIMIT_KEY)
            putLong(VERIFY_SERVER_LIMIT_KEY, maxOf(serverOld, now + requested))
        }
        markVerificationForce(provider)
        synchronized(autoQueue) {
            for (item in autoQueue) {
                if (item.force) markVerificationForce(ProviderSpec.byKey(item.key))
            }
        }
        return requested
    }

    fun renewalNeeded(provider: ProviderSpec, session: StoredSession?): Boolean {
        if (session == null || !session.isValid()) return true
        val now = clock()
        val checked = getLong(RENEW_CHECKED_PREFIX + provider.key)
        val expires = session.expiresAtEpochMs()
        return checked <= 0L ||
            checked > now + 5L * 60L * 1000L ||
            now - checked >= RENEW_INTERVAL_MS ||
            (expires != null && expires - now <= RENEW_SOON_MS)
    }

    fun renewalRetryWait(provider: ProviderSpec): Long =
        waitUntil(RENEW_ATTEMPT_PREFIX + provider.key)

    fun remainingTimeoutMs(deadlineMs: Long, requestedMs: Long): Long {
        val available = deadlineMs - clock() - 350L
        if (available < 1000L) throw BridgeGatewayException("TIME_BUDGET")
        return available.coerceIn(1000L, requestedMs)
    }

    suspend fun signedRequest(
        provider: ProviderSpec,
        session: StoredSession,
        method: String,
        path: String,
        query: Map<String, String>? = null,
        payload: JsonObject? = null,
        extraHeaders: Map<String, String>? = null,
        deadlineMs: Long
    ): JsonObject {
        if (!SAFE_PATH_REGEX.matches(path) || (method != "GET" && method != "POST")) {
            throw IllegalArgumentException("BAD_PATH")
        }
        val bodyStr = if (method == "POST") (payload ?: JsonObject(emptyMap())).toString() else ""
        val now = clock()
        val headers = LinkedHashMap(ZarzCrypto.signedHeaders(provider, session, method, path, bodyStr, now))
        extraHeaders?.forEach { (k, v) -> headers[k] = v }

        val urlBuilder = (gateBaseUrl.trimEnd('/') + path).toHttpUrl().newBuilder()
        query?.forEach { (k, v) -> urlBuilder.addQueryParameter(k, v) }

        val timeoutMs = remainingTimeoutMs(deadlineMs, if (provider.key == "tidal") 3800L else 3000L)
        val reqBuilder = Request.Builder().url(urlBuilder.build())
        headers.forEach { (k, v) -> reqBuilder.header(k, v) }
        if (method == "POST") {
            reqBuilder.post(bodyStr.toRequestBody("application/json".toMediaType()))
        } else {
            reqBuilder.get()
        }

        val callClient = httpClient.newBuilder()
            .callTimeout(timeoutMs, TimeUnit.MILLISECONDS)
            .connectTimeout(timeoutMs, TimeUnit.MILLISECONDS)
            .readTimeout(timeoutMs, TimeUnit.MILLISECONDS)
            .build()

        val response = try {
            callClient.newCall(reqBuilder.build()).await()
        } catch (ce: CancellationException) {
            throw ce
        }

        return response.use { r ->
            val rawBody = runCatching { r.body.string() }.getOrDefault("")
            val data = parseJsonObjectOrNull(rawBody) ?: JsonObject(emptyMap())
            if (r.code != 200) {
                val status = r.code
                val code = data.stringField("code")
                    ?.uppercase(Locale.ROOT)
                    ?.replace(Regex("[^A-Z0-9_]"), "")
                    ?.take(50)
                    .orEmpty()
                log("${provider.name}: server HTTP $status${if (code.isNotEmpty()) " ($code)" else ""}.")

                if (path == "/session/refresh" && status == 429) {
                    rememberVerificationLimit(r, data, null)
                    log("${provider.name}: rinnovo in pausa dopo HTTP 429; sessione conservata.")
                    log(verificationWaitMessage())
                    throw BridgeGatewayException("RATE_LIMITED")
                }
                if (provider.key == "qobuz" && path != "/session/refresh" && status == 503 && code == "PROVIDER_UNAVAILABLE") {
                    val wait = maxOf(60_000L, parseRetryAfterMs(r, data))
                    qobuzUnavailableUntil = maxOf(qobuzUnavailableUntil, clock() + wait)
                    log("Qobuz: sospesa temporaneamente dopo HTTP 503.")
                }
                if (status == 428 || code == "VERIFY_REQUIRED" ||
                    (status == 401 && code == "SESSION_INVALID") ||
                    (status == 403 && code == "REQUEST_AUTH_INVALID")
                ) {
                    if (status == 401 && code == "SESSION_INVALID" && path != "/session/refresh") {
                        removeKey(provider.storage)
                    }
                    throw BridgeGatewayException("VERIFY_REQUIRED")
                }
                throw BridgeGatewayException("GATEWAY_UNAVAILABLE")
            }
            // If payload was a JSON array (e.g. single-item array from /dl/amazeamazeamaze), wrap or unwrap cleanly
            parseJsonElementToObject(rawBody) ?: data
        }
    }

    suspend fun activeSession(
        provider: ProviderSpec,
        deadlineMs: Long,
        scheduledRenewal: Boolean = false
    ): StoredSession? {
        var s = getStoredSession(provider) ?: return null
        val now = clock()
        val expires = s.expiresAtEpochMs()
        val needsRefresh = scheduledRenewal || (expires != null && expires - now <= 300_000L)
        if (!needsRefresh) return s

        if (verificationWaitMs() > 0L || renewalRetryWait(provider) > 0L) {
            if (scheduledRenewal) {
                throw BridgeGatewayException(if (verificationWaitMs() > 0L) "RATE_LIMITED" else "REFRESH_DELAYED")
            }
            if (expires != null && expires <= now) {
                throw BridgeGatewayException("GATEWAY_UNAVAILABLE")
            }
            return s
        }

        remainingTimeoutMs(deadlineMs, 3000L)
        putLong(RENEW_ATTEMPT_PREFIX + provider.key, clock() + RENEW_GUARD_MS)
        try {
            val payload = JsonObject(mapOf("install_id" to JsonPrimitive(s.installId)))
            val fresh = signedRequest(provider, s, "POST", "/session/refresh", null, payload, null, deadlineMs)
            if (!saveSession(provider, fresh, s.installId, s)) {
                throw BridgeGatewayException("REFRESH_FAILED")
            }
            s = getStoredSession(provider) ?: s
            log("${provider.name}: richiesta di rinnovo della sessione accettata.")
        } catch (ce: CancellationException) {
            throw ce
        } catch (err: Throwable) {
            val code = (err as? BridgeGatewayException)?.code.orEmpty()
            if (code == "TIME_BUDGET") {
                removeKey(RENEW_ATTEMPT_PREFIX + provider.key)
                throw err
            }
            if (scheduledRenewal) throw err
            if (code == "VERIFY_REQUIRED") throw BridgeGatewayException("VERIFY_REQUIRED")
            if (expires != null && expires <= clock()) throw BridgeGatewayException("GATEWAY_UNAVAILABLE")
            // Any temporary failure leaves the still-valid session in place.
        }
        return s
    }

    suspend fun beginVerification(
        provider: ProviderSpec,
        automatic: Boolean,
        deadlineMs: Long,
        forceManual: Boolean = false,
        overrideStoredServerWait: Boolean = false
    ): VerificationStatus {
        val generationAtStart = forgetGeneration.get()
        if (!verifyMutex.tryLock()) {
            if (!automatic) {
                log("${provider.name}: una verifica è già aperta; completala o chiudila prima di riprovare.")
                notify("Una verifica è già aperta. Completa o chiudi quella pagina.")
            }
            return VerificationStatus.BUSY
        }
        try {
            if (getPendingVerification() != null) {
                if (!automatic) {
                    log("${provider.name}: una verifica è già aperta; completala o chiudila prima di riprovare.")
                    notify("Una verifica è già aperta. Completa o chiudi quella pagina.")
                }
                return VerificationStatus.BUSY
            }

            if (forceManual) {
                val serverWait = serverRetryWaitMs()
                val guardWait = waitUntil(FORCE_GUARD_KEY)
                if (serverWait > 0L && !overrideStoredServerWait) {
                    log("${provider.name}: verifica manuale in attesa del server; ancora circa ${FlacBridgeDiagnostics.humanWait(serverWait)}.")
                    notify("Il server richiede ancora ${FlacBridgeDiagnostics.humanWait(serverWait)} di attesa.")
                    return VerificationStatus.RATE_LIMITED
                }
                if (guardWait > 0L) {
                    log("${provider.name}: attendi ancora circa ${FlacBridgeDiagnostics.humanWait(guardWait)} prima di un altro tentativo manuale.")
                    notify("Attendi ${FlacBridgeDiagnostics.humanWait(guardWait)} prima di un altro tentativo manuale.")
                    return VerificationStatus.RATE_LIMITED
                }
                if (serverWait > 0L && overrideStoredServerWait) {
                    log("${provider.name}: tentativo anticipato richiesto nonostante l’attesa memorizzata dalla precedente risposta (${FlacBridgeDiagnostics.humanWait(serverWait)}); il server può rifiutarlo.")
                }
                putLong(FORCE_GUARD_KEY, clock() + MANUAL_GUARD_MS)
            } else if (verificationWaitMs() > 0L) {
                if (!automatic) {
                    val msg = verificationWaitMessage()
                    log("${provider.name}: $msg")
                    notify(msg)
                }
                return VerificationStatus.RATE_LIMITED
            }

            val install = installId()
            val bootstrapJson: JsonObject
            try {
                if (overrideStoredServerWait) {
                    log("${provider.name}: richiesta forzata al servizio di verifica in corso (un solo tentativo).")
                }
                val bootUrl = (gateBaseUrl.trimEnd('/') + "/bootstrap").toHttpUrl().newBuilder()
                    .addQueryParameter("app_version", provider.app)
                    .addQueryParameter("install_id", install)
                    .build()
                val timeoutMs = remainingTimeoutMs(deadlineMs, 6000L)
                val callClient = httpClient.newBuilder()
                    .callTimeout(timeoutMs, TimeUnit.MILLISECONDS)
                    .connectTimeout(timeoutMs, TimeUnit.MILLISECONDS)
                    .readTimeout(timeoutMs, TimeUnit.MILLISECONDS)
                    .build()
                val req = Request.Builder()
                    .url(bootUrl)
                    .header("User-Agent", MODERN_CHROME_UA)
                    .header("Accept", "application/json")
                    .get()
                    .build()

                val statusAndBody = callClient.newCall(req).await().use { r ->
                    val raw = runCatching { r.body.string() }.getOrDefault("")
                    val parsed = parseJsonObjectOrNull(raw)
                    if (!r.isSuccessful) {
                        log("${provider.name}: avvio verifica non disponibile (HTTP ${r.code}).")
                        if (r.code == 429) {
                            val indicated = rememberVerificationLimit(r, parsed, provider)
                            if (overrideStoredServerWait) {
                                log("${provider.name}: il servizio ha risposto HTTP 429 al tentativo forzato; verifica non avviata.")
                                log(
                                    if (indicated > 0L) "${provider.name}: questa risposta indica circa ${FlacBridgeDiagnostics.humanWait(indicated)} di attesa."
                                    else "${provider.name}: questa risposta non indica un tempo di attesa esplicito leggibile."
                                )
                            }
                            log(verificationWaitMessage())
                            if (!automatic) {
                                notify(
                                    if (overrideStoredServerWait) "${provider.name}: server HTTP 429. Dettagli nel Registro."
                                    else verificationWaitMessage()
                                )
                            }
                            return VerificationStatus.RATE_LIMITED
                        }
                        if (!automatic) notify("Verifica ${provider.name} non disponibile. Riprova più tardi.")
                        return VerificationStatus.FAILED
                    }
                    parsed
                }

                bootstrapJson = statusAndBody ?: run {
                    log("${provider.name}: risposta di verifica non riconosciuta.")
                    if (!automatic) notify("Verifica ${provider.name} non riconosciuta.")
                    return VerificationStatus.FAILED
                }

                if (forgetGeneration.get() != generationAtStart) return VerificationStatus.FAILED

                if (saveSession(provider, bootstrapJson, install, null)) {
                    clearVerificationForce(provider)
                    log("${provider.name}: sessione verificata.")
                    if (!automatic) notify("${provider.name} attivato.")
                    return VerificationStatus.COMPLETE
                }
            } catch (ce: CancellationException) {
                throw ce
            } catch (err: BridgeGatewayException) {
                if (err.code == "TIME_BUDGET") return VerificationStatus.DEFER
                log("${provider.name}: impossibile raggiungere il server di verifica.")
                if (!automatic) notify("Verifica ${provider.name} non disponibile. Controlla la rete e riprova.")
                return VerificationStatus.FAILED
            } catch (_: Throwable) {
                log("${provider.name}: impossibile raggiungere il server di verifica.")
                if (!automatic) notify("Verifica ${provider.name} non disponibile. Controlla la rete e riprova.")
                return VerificationStatus.FAILED
            }

            val challenge = bootstrapJson.stringField("challenge_id").orEmpty()
            val serverNonce = bootstrapJson.stringField("server_nonce").orEmpty().ifEmpty { challenge }
            if (challenge.isEmpty() || serverNonce.isEmpty()) {
                log("${provider.name}: risposta di verifica non riconosciuta.")
                if (!automatic) notify("Verifica ${provider.name} non riconosciuta.")
                return VerificationStatus.FAILED
            }

            val webView = webViewClientRef ?: run {
                log("${provider.name}: pagina di verifica non disponibile.")
                if (!automatic) {
                    notify("Impossibile aprire la verifica di ${provider.name}. Aggiorna Android System WebView.")
                }
                return VerificationStatus.AUTH_UNAVAILABLE
            }

            val state = ZarzCrypto.generateState(serverNonce, install, clock())
            val callbackParam = "$CALLBACK_URI?cb_version=v2grant&state=" +
                URLEncoder.encode(state, StandardCharsets.UTF_8)
            val challengeUrl = (gateBaseUrl.trimEnd('/') + "/challenge").toHttpUrl().newBuilder()
                .addQueryParameter("id", challenge)
                .addQueryParameter("cb", callbackParam)
                .build()
                .toString()

            val ttlSec = (bootstrapJson.doubleField("expires_in")?.toLong() ?: 300L).coerceIn(15L, 300L)
            val pending = PendingVerification(
                provider = provider.key,
                state = state,
                installId = install,
                until = clock() + ttlSec * 1000L,
                automatic = automatic
            )
            settingsOrThrow().putString(PENDING_KEY, json.encodeToString(pending))
            log("${provider.name}: pagina di verifica aperta${if (automatic) " automaticamente." else "."}")
            if (automatic && !autoAnnounced) {
                autoAnnounced = true
                notify("Verifica automatica avviata. Completa eventuali richieste del server.")
            }

            val request = object : WebViewRequest.Headers<String> {
                override val initialUrl: NetworkRequest =
                    challengeUrl.toGetRequest(mapOf("User-Agent" to MODERN_CHROME_UA))
                override val interceptUrlRegex: Regex = CALLBACK_STOP_REGEX
                override val stopUrlRegex: Regex = CALLBACK_STOP_REGEX
                override val maxTimeout: Long = ttlSec * 1000L
                override val dontCache: Boolean = true

                override suspend fun onStop(requests: List<NetworkRequest>): String? {
                    return requests.lastOrNull { CALLBACK_STOP_REGEX.containsMatchIn(it.url) }?.url
                }
            }

            val webResult = try {
                webView.await(
                    showWebView = true,
                    reason = "Verifica ${provider.name}",
                    request = request
                )
            } catch (ce: CancellationException) {
                removeKey(PENDING_KEY)
                throw ce
            } catch (_: Throwable) {
                removeKey(PENDING_KEY)
                log("${provider.name}: pagina di verifica non disponibile.")
                if (!automatic) {
                    notify("Impossibile aprire la verifica di ${provider.name}. Chiudi altre pagine di accesso o aggiorna Android System WebView.")
                }
                return VerificationStatus.AUTH_UNAVAILABLE
            }

            // Ignore callback if user cleared sessions while page was open
            if (forgetGeneration.get() != generationAtStart || getPendingVerification() == null) {
                removeKey(PENDING_KEY)
                return VerificationStatus.FAILED
            }

            removeKey(PENDING_KEY)
            return finishVerification(provider, pending, webResult, generationAtStart)
        } finally {
            verifyMutex.unlock()
        }
    }

    private suspend fun finishVerification(
        provider: ProviderSpec,
        pending: PendingVerification,
        webResult: Result<String?>,
        generationAtStart: Long
    ): VerificationStatus {
        try {
            val callbackUrl = webResult.getOrNull()?.trim().orEmpty()
            if (webResult.isFailure || callbackUrl.isEmpty()) {
                throw IllegalStateException("CLOSED_OR_EXPIRED")
            }
            if (!callbackUrl.startsWith("$CALLBACK_URI?") && !callbackUrl.startsWith("$CALLBACK_URI#")) {
                throw IllegalStateException("INVALID_CALLBACK")
            }
            if (clock() > pending.until) {
                throw IllegalStateException("EXPIRED_CALLBACK")
            }
            val vals = ZarzCrypto.parseCallbackValues(callbackUrl)
            val grant = (vals["grant"] ?: vals["code"]).orEmpty().trim()
            if (grant.isEmpty() || grant.length > 8192 || vals["state"]?.trim() != pending.state) {
                throw IllegalStateException("INVALID_STATE")
            }

            val payload = JsonObject(
                mapOf(
                    "grant" to JsonPrimitive(grant),
                    "install_id" to JsonPrimitive(pending.installId),
                    "app_version" to JsonPrimitive(provider.app),
                    "platform" to JsonPrimitive("extension")
                )
            )
            val exchangeUrl = (gateBaseUrl.trimEnd('/') + "/session/exchange").toHttpUrl()
            val callClient = httpClient.newBuilder()
                .callTimeout(6500L, TimeUnit.MILLISECONDS)
                .connectTimeout(6500L, TimeUnit.MILLISECONDS)
                .readTimeout(6500L, TimeUnit.MILLISECONDS)
                .build()
            val req = Request.Builder()
                .url(exchangeUrl)
                .header("User-Agent", MODERN_CHROME_UA)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .post(payload.toString().toRequestBody("application/json".toMediaType()))
                .build()

            return callClient.newCall(req).await().use { r ->
                val raw = runCatching { r.body.string() }.getOrDefault("")
                val data = parseJsonObjectOrNull(raw)
                if (r.code == 429) {
                    rememberVerificationLimit(r, data, provider)
                    log("${provider.name}: creazione della sessione in pausa dopo HTTP 429.")
                    return@use VerificationStatus.RATE_LIMITED
                }
                if (forgetGeneration.get() != generationAtStart) {
                    return@use VerificationStatus.FAILED
                }
                if (!r.isSuccessful || !saveSession(provider, data, pending.installId, null)) {
                    log("${provider.name}: sessione non creata (HTTP ${r.code}).")
                    throw IllegalStateException("EXCHANGE_FAILED")
                }
                clearVerificationForce(provider)
                log("${provider.name}: sessione verificata.")
                if (!pending.automatic) notify("${provider.name} attivato.")
                VerificationStatus.COMPLETE
            }
        } catch (ce: CancellationException) {
            throw ce
        } catch (_: Throwable) {
            log("${provider.name}: verifica non completata o ritorno non valido.")
            if (automaticEnabled()) stopAutomatic(RENEW_INTERVAL_MS)
            else stopAutomatic(0L)
            if (pending.automatic) notice("Verifica automatica interrotta. Puoi riprovare dalle impostazioni.")
            else notify("Verifica non completata. Riprova dalla pagina dell’estensione.")
            return VerificationStatus.FAILED
        }
    }

    private fun getPendingVerification(): PendingVerification? {
        val raw = runCatching { settingsRef?.getString(PENDING_KEY) }.getOrNull() ?: return null
        return runCatching { json.decodeFromString<PendingVerification>(raw) }.getOrNull()
    }

    fun stopAutomatic(delayMs: Long) {
        autoRunning = false
        synchronized(autoQueue) { autoQueue.clear() }
        autoHadFailure = false
        if (delayMs > 0L) {
            putLong(AUTO_RETRY_KEY, clock() + delayMs)
        }
    }

    fun startAutomatic(scope: CoroutineScope, reset: Boolean, reason: String = ""): Boolean {
        if (!automaticEnabled()) {
            if (reset || reason == "avvio") {
                log("Verifica automatica disattivata: attiva l’interruttore nelle impostazioni.")
            }
            if (reset) notify("Attiva prima la verifica automatica nelle impostazioni.")
            return false
        }
        if (reset) {
            removeKey(AUTO_PAUSED_KEY)
            removeKey(AUTO_RETRY_KEY)
        }
        if (!automaticAllowed()) {
            if (reset || reason == "avvio") {
                log("Verifica automatica sospesa dopo la cancellazione delle sessioni: usa Riprova verifica automatica.")
            }
            return false
        }
        if (retryDelayed()) {
            if (reset || reason == "avvio") {
                log("Verifica automatica in pausa: ancora circa ${FlacBridgeDiagnostics.humanWait(waitUntil(AUTO_RETRY_KEY))}.")
                if (verificationWaitMs() > 0L) log(verificationWaitMessage())
            }
            return false
        }
        if (verificationWaitMs() > 0L) {
            if (reset) notify(verificationWaitMessage())
            log(verificationWaitMessage())
            return false
        }
        if (getLong(VERIFY_LIMIT_KEY) > 0L) removeKey(VERIFY_LIMIT_KEY)

        val forced = verificationForceKeys().toSet()
        val enabled = enabledProviderKeys()
        synchronized(autoQueue) {
            autoQueue.clear()
            ProviderSpec.ALL.filter { it.key in enabled }.forEach { p ->
                autoQueue.addLast(AutoQueueItem(p.key, p.key in forced))
            }
        }
        autoRunning = true
        autoHadFailure = false
        autoAnnounced = false

        when {
            reason == "avvio" -> log("Avvio: controllo automatico delle sorgenti programmato; attendo la rete.")
            reason == "periodico" -> log("Controllo periodico programmato; attendo la rete.")
            reset -> log("Controllo automatico richiesto; attendo la rete.")
        }
        return wakeAutomatic(scope)
    }

    fun scheduleRenewalIfDue(scope: CoroutineScope): Boolean {
        if (!automaticAllowed() || retryDelayed() || verificationWaitMs() > 0L || autoRunning || getPendingVerification() != null) {
            return false
        }
        val forced = verificationForceKeys().toSet()
        val enabled = enabledProviderKeys()
        for (p in ProviderSpec.ALL.filter { it.key in enabled }) {
            val s = getStoredSession(p)
            if (p.key in forced || s == null || (renewalNeeded(p, s) && renewalRetryWait(p) <= 0L)) {
                return startAutomatic(scope, reset = false, reason = "periodico")
            }
        }
        return false
    }

    fun queueAutomaticForProvider(scope: CoroutineScope, provider: ProviderSpec): Boolean {
        if (!automaticAllowed()) return false
        if (retryDelayed() || verificationWaitMs() > 0L) {
            markVerificationForce(provider)
            return false
        }
        val pending = getPendingVerification()
        if (pending?.provider == provider.key) return true
        synchronized(autoQueue) {
            if (!autoRunning) {
                autoQueue.clear()
                autoRunning = true
                autoHadFailure = false
                autoAnnounced = false
            }
            val existing = autoQueue.firstOrNull { it.key == provider.key }
            if (existing != null) existing.force = true
            else autoQueue.addLast(AutoQueueItem(provider.key, true))
        }
        if (pending == null) wakeAutomatic(scope)
        return true
    }

    fun wakeAutomatic(scope: CoroutineScope): Boolean {
        if (!autoRunning || !automaticAllowed()) return false
        if (!isOnline()) return true // Will automatically resume in updateNetworkConnection when online
        if (autoJob?.isActive == true) return true
        autoJob = scope.launch {
            runAutomaticQueue()
        }
        return true
    }

    internal suspend fun awaitAutomaticIdle() {
        autoJob?.join()
    }

    suspend fun runAutomaticQueue() {
        if (!autoMutex.tryLock()) return
        try {
            if (!autoRunning) return
            if (!automaticAllowed() || retryDelayed() || verificationWaitMs() > 0L) {
                stopAutomatic(0L)
                return
            }
            if (!isOnline()) return
            log("Controllo automatico: rete disponibile, elaborazione delle sorgenti.")
            if (getPendingVerification() != null) return

            while (autoRunning) {
                val item = synchronized(autoQueue) {
                    if (autoQueue.isEmpty()) null else autoQueue.removeFirst()
                } ?: break

                if (!isOnline()) {
                    synchronized(autoQueue) { autoQueue.addFirst(item) }
                    return
                }
                val provider = ProviderSpec.byKey(item.key) ?: continue
                var force = item.force
                val stepDeadline = clock() + 22_000L

                if (!force) {
                    var session = getStoredSession(provider)
                    if (session != null) {
                        var expires = session.expiresAtEpochMs()
                        if (renewalNeeded(provider, session) && renewalRetryWait(provider) <= 0L) {
                            try {
                                log("${provider.name}: rinnovo automatico della sessione richiesto.")
                                session = activeSession(provider, stepDeadline, scheduledRenewal = true)
                                expires = session?.expiresAtEpochMs()
                            } catch (ce: CancellationException) {
                                throw ce
                            } catch (err: Throwable) {
                                val reason = (err as? BridgeGatewayException)?.code.orEmpty()
                                when (reason) {
                                    "TIME_BUDGET" -> {
                                        synchronized(autoQueue) { autoQueue.addFirst(item) }
                                        continue
                                    }
                                    "RATE_LIMITED" -> {
                                        stopAutomatic(0L)
                                        return
                                    }
                                    "VERIFY_REQUIRED" -> {
                                        force = true
                                    }
                                    else -> {
                                        autoHadFailure = true
                                        log("${provider.name}: rinnovo non disponibile; sessione conservata e nuovo tentativo rinviato.")
                                        continue
                                    }
                                }
                            }
                        }
                        if (!force && session != null && session.isValid() && (expires == null || expires > clock())) {
                            log("${provider.name}: sessione locale conservata.")
                            continue
                        }
                        if (!force && session != null && session.isValid() && renewalRetryWait(provider) > 0L) {
                            autoHadFailure = true
                            log("${provider.name}: sessione scaduta; rinnovo rinviato per evitare richieste ripetute.")
                            continue
                        }
                    }
                }

                val status = beginVerification(provider, automatic = true, deadlineMs = clock() + 25_000L)
                when (status) {
                    VerificationStatus.COMPLETE -> Unit
                    VerificationStatus.DEFER -> {
                        synchronized(autoQueue) { autoQueue.addFirst(item) }
                    }
                    VerificationStatus.BUSY -> {
                        synchronized(autoQueue) { autoQueue.addFirst(item) }
                        return
                    }
                    VerificationStatus.RATE_LIMITED -> {
                        stopAutomatic(0L)
                        return
                    }
                    VerificationStatus.AUTH_UNAVAILABLE -> {
                        stopAutomatic(15L * 60L * 1000L)
                        notice("Verifica sospesa. Chiudi altre pagine di accesso o aggiorna Android System WebView.")
                        return
                    }
                    VerificationStatus.OPENED, VerificationStatus.FAILED -> {
                        if (!autoRunning) return // Stopped by finishVerification on cancel/close
                        autoHadFailure = true
                    }
                }
            }

            autoRunning = false
            if (autoHadFailure) {
                putLong(AUTO_RETRY_KEY, clock() + 15L * 60L * 1000L)
                log("Alcune verifiche automatiche non sono disponibili.")
                notice("Alcune verifiche non sono disponibili. Riprova dalle impostazioni.")
            } else {
                removeKey(AUTO_RETRY_KEY)
                log("Controllo automatico delle sorgenti completato.")
            }
            autoHadFailure = false
        } finally {
            autoMutex.unlock()
        }
    }

    fun forgetAllSessions() {
        forgetGeneration.incrementAndGet()
        stopAutomatic(0L)
        for (p in ProviderSpec.ALL) {
            removeKey(p.storage)
            removeKey(RENEW_CHECKED_PREFIX + p.key)
            removeKey(RENEW_ATTEMPT_PREFIX + p.key)
        }
        removeKey(INSTALL_KEY)
        removeKey(PENDING_KEY)
        removeKey(AUTO_RETRY_KEY)
        removeKey(VERIFY_FORCE_KEY)
        // Preserve server HTTP 429 cooldown (`VERIFY_LIMIT_KEY` & `VERIFY_SERVER_LIMIT_KEY`)
        settingsRef?.putBoolean(AUTO_PAUSED_KEY, true)
        lastNoticeMs = 0L
        log("Sessioni locali cancellate; verifica automatica sospesa.")
        notify("Sessioni cancellate. Verifica automatica sospesa: usa Riprova per riattivarla.")
    }

    private fun parseJsonObjectOrNull(raw: String): JsonObject? {
        if (raw.isBlank()) return null
        return runCatching { json.parseToJsonElement(raw).jsonObject }.getOrNull()
    }

    private fun parseJsonElementToObject(raw: String): JsonObject? {
        if (raw.isBlank()) return null
        return runCatching {
            val element: JsonElement = json.parseToJsonElement(raw)
            if (element is JsonObject) element
            else JsonObject(mapOf("_items" to element))
        }.getOrNull()
    }
}

internal fun JsonObject.stringField(name: String): String? =
    (this[name] as? JsonPrimitive)?.contentOrNull?.trim()

internal fun JsonObject.doubleField(name: String): Double? =
    (this[name] as? JsonPrimitive)?.doubleOrNull
        ?: (this[name] as? JsonPrimitive)?.contentOrNull?.trim()?.toDoubleOrNull()

internal fun JsonObject.booleanField(name: String): Boolean? {
    val raw = (this[name] as? JsonPrimitive)?.contentOrNull?.trim()?.lowercase(Locale.ROOT) ?: return null
    return when (raw) {
        "true" -> true
        "false" -> false
        else -> null
    }
}
