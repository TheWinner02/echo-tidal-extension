package dev.brahmkshatriya.echo.extension.tidal

import dev.brahmkshatriya.echo.common.helpers.ContinuationCallback.Companion.await
import dev.brahmkshatriya.echo.common.helpers.WebViewClient
import dev.brahmkshatriya.echo.common.helpers.WebViewRequest
import dev.brahmkshatriya.echo.common.models.NetworkConnection
import dev.brahmkshatriya.echo.common.models.NetworkRequest
import dev.brahmkshatriya.echo.common.models.NetworkRequest.Companion.toGetRequest
import dev.brahmkshatriya.echo.common.settings.Settings
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import okhttp3.Call
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.Interceptor
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import java.io.IOException
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.Locale
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class TidalSessionManager(
    baseHttpClient: OkHttpClient,
    internal val logs: DiagnosticLogBuffer,
    private val onNotify: (String) -> Unit = {},
    internal val gatewayBaseUrl: String = "https://api.zarz.moe/v2",
    private val clock: () -> Long = System::currentTimeMillis
) {
    val httpClient: OkHttpClient = withBrowserHeaders(baseHttpClient)

    private val sessionMutex = Mutex()
    private val autoMutex = Mutex()
    private val verifyMutex = Mutex()

    @Volatile
    private var settingsRef: Settings? = null

    @Volatile
    private var webViewClientRef: WebViewClient? = null

    @Volatile
    private var networkState: NetworkConnection = NetworkConnection.Unmetered

    private var autoJob: Job? = null
    private val autoStartedAt = AtomicBoolean(false)

    companion object {
        const val TIDAL_APP = "tidal-web@1.2.5"
        const val TIDAL_STORAGE = "tidal_session"
        const val TIDAL_TICKET_ALIAS = "tid"
        const val TIDAL_DL_PATH = "/dl/tid"

        const val KEY_AUTO_VERIFY = "tidal_auto_verify"
        const val KEY_PENDING = "tidal_pending"
        const val KEY_COOLDOWN = "tidal_cooldown"
        const val KEY_SERVER_WAIT = "tidal_server_wait"
        const val KEY_AUTO_429 = "tidal_auto_429"
        const val KEY_FORCE_GUARD = "tidal_force_guard"
        const val INSTALL_KEY = "installation_id"

        const val CALLBACK_URI = "spotiflac://session-grant"
        val CALLBACK_STOP_REGEX = Regex("^spotiflac://session-grant[?#].*", RegexOption.IGNORE_CASE)

        const val MODERN_CHROME_UA =
            "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"

        private const val RENEW_WINDOW_MS = 60L * 60L * 1000L
        private const val PERIODIC_RENEW_MS = 6L * 60L * 60L * 1000L

        val json = Json {
            ignoreUnknownKeys = true
            encodeDefaults = true
            isLenient = true
        }

        fun withBrowserHeaders(client: OkHttpClient): OkHttpClient = client.newBuilder()
            .addInterceptor(Interceptor { chain ->
                val original = chain.request()
                val ua = original.header("User-Agent")
                val updated = if (ua.isNullOrBlank() || ua.startsWith("okhttp/", ignoreCase = true)) {
                    original.newBuilder().header("User-Agent", MODERN_CHROME_UA).build()
                } else original
                chain.proceed(updated)
            })
            .build()
    }

    val autoRunning: Boolean get() = autoJob?.isActive == true

    fun attachSettings(settings: Settings) {
        settingsRef = settings
    }

    fun attachWebViewClient(client: WebViewClient) {
        webViewClientRef = client
    }

    fun log(msg: String) = logs.append(msg, clock())
    fun notify(msg: String) = onNotify(msg)

    fun notice(msg: String) {
        log(msg)
        notify(msg)
    }

    fun isOnline(): Boolean = networkState != NetworkConnection.NotConnected

    fun updateNetworkConnection(connection: NetworkConnection, scope: CoroutineScope?) {
        val wasOffline = !isOnline()
        networkState = connection
        if (wasOffline && isOnline() && scope != null) {
            log("Connessione di rete ripristinata.")
            val pending = getPendingVerification()
            if (pending != null && pending.automatic && pending.until > clock()) {
                scope.launch { startAutomatic(this, reset = false, reason = "ritorno-online") }
            } else {
                scheduleRenewalIfDue(scope)
            }
        }
    }

    fun installId(): String {
        val current = settingsRef?.getString(INSTALL_KEY)?.trim().orEmpty()
        if (StoredSession.HEX_32_REGEX.matches(current)) return current
        val generated = TidalCrypto.generateInstallId(clock())
        settingsRef?.putString(INSTALL_KEY, generated)
        return generated
    }

    fun getStoredSession(): StoredSession? {
        val raw = settingsRef?.getString(TIDAL_STORAGE) ?: return null
        return runCatching { json.decodeFromString<StoredSession>(raw) }.getOrNull()
            ?.takeIf { it.isValid() }
    }

    fun saveStoredSession(session: StoredSession) {
        settingsRef?.putString(TIDAL_STORAGE, json.encodeToString(session))
    }

    fun clearStoredSession() {
        removeKey(TIDAL_STORAGE)
    }

    private fun getPendingVerification(): PendingVerification? {
        val raw = settingsRef?.getString(KEY_PENDING) ?: return null
        return runCatching { json.decodeFromString<PendingVerification>(raw) }.getOrNull()
    }

    private fun savePendingVerification(pv: PendingVerification) {
        settingsRef?.putString(KEY_PENDING, json.encodeToString(pv))
    }

    private fun clearPendingVerification() {
        removeKey(KEY_PENDING)
    }

    private fun removeKey(key: String) {
        runCatching {
            settingsRef?.javaClass?.getMethod("remove", String::class.java)?.invoke(settingsRef, key)
        }.onFailure {
            settingsRef?.putString(key, "")
        }
    }

    private fun getWaitUntil(key: String): Long {
        return settingsRef?.getString(key)?.toLongOrNull() ?: 0L
    }

    private fun setWaitUntil(key: String, epochMs: Long) {
        settingsRef?.putString(key, epochMs.toString())
    }

    fun remainingCooldown(): Long = maxOf(0L, getWaitUntil(KEY_COOLDOWN) - clock())
    fun remainingServerWait(): Long = maxOf(0L, getWaitUntil(KEY_SERVER_WAIT) - clock())
    fun remainingAuto429(): Long = maxOf(0L, getWaitUntil(KEY_AUTO_429) - clock())

    fun automaticEnabled(): Boolean = settingsRef?.getBoolean(KEY_AUTO_VERIFY) ?: true

    fun automaticAllowed(): Boolean =
        automaticEnabled() && isOnline() && clock() >= getWaitUntil(KEY_AUTO_429)

    fun sessionSummaryForUi(): String {
        val stored = getStoredSession()
        val base = if (stored == null) "Nessuna sessione attiva"
        else {
            val exp = stored.expiresAtEpochMs()
            if (exp != null) {
                val diff = exp - clock()
                if (diff > 0L) "Attiva (scade tra ~${TidalDiagnostics.humanWait(diff)})"
                else "Scaduta"
            } else "Attiva"
        }
        val serverWait = remainingServerWait()
        return if (serverWait > 0L) "$base · Attesa server: ${TidalDiagnostics.humanWait(serverWait)}"
        else base
    }

    fun remainingTimeoutMs(deadlineMs: Long, floorMs: Long = 2000L): Long =
        maxOf(floorMs, deadlineMs - clock())

    suspend fun activeSession(deadlineMs: Long): StoredSession? = sessionMutex.withLock {
        val stored = getStoredSession() ?: return null
        if (!stored.isValid()) return null

        val expEpoch = stored.expiresAtEpochMs()
        val now = clock()
        val dueForRenew = expEpoch == null || (expEpoch - now) < RENEW_WINDOW_MS
        if (!dueForRenew) return stored

        val refreshed = tryRefreshSession(stored, deadlineMs)
        if (refreshed != null) {
            saveStoredSession(refreshed)
            log("Tidal: sessione rinnovata con successo.")
            return refreshed
        }
        if (expEpoch != null && expEpoch > now) {
            log("Tidal: rinnovo non riuscito, uso la sessione corrente valida.")
            return stored
        }
        null
    }

    fun scheduleRenewalIfDue(scope: CoroutineScope) {
        if (!isOnline()) return
        val stored = getStoredSession() ?: return
        val exp = stored.expiresAtEpochMs() ?: return
        val diff = exp - clock()
        if (diff in 0L..PERIODIC_RENEW_MS) {
            scope.launch {
                sessionMutex.withLock {
                    val fresh = getStoredSession() ?: return@withLock
                    val refreshed = tryRefreshSession(fresh, clock() + 10_000L)
                    if (refreshed != null) {
                        saveStoredSession(refreshed)
                        log("Tidal: rinnovo periodico completato.")
                    }
                }
            }
        }
    }

    suspend fun signedRequest(
        method: String,
        path: String,
        payload: JsonObject,
        extraHeaders: Map<String, String> = emptyMap(),
        deadlineMs: Long
    ): JsonObject = withContext(Dispatchers.IO) {
        val stored = getStoredSession()
            ?: throw TidalGatewayException("AUTH_REQUIRED")
        val bodyStr = payload.toString()
        val sigHeaders = TidalCrypto.signedHeaders(
            session = stored,
            method = method,
            path = path,
            body = bodyStr,
            nowMs = clock()
        )
        val url = gatewayBaseUrl.trimEnd('/') + path
        val timeoutMs = remainingTimeoutMs(deadlineMs, 2500L)
        val reqClient = httpClient.newBuilder()
            .callTimeout(timeoutMs, TimeUnit.MILLISECONDS)
            .connectTimeout(timeoutMs, TimeUnit.MILLISECONDS)
            .readTimeout(timeoutMs, TimeUnit.MILLISECONDS)
            .build()

        val reqBuilder = Request.Builder()
            .url(url)
            .method(method.uppercase(), bodyStr.toRequestBody("application/json".toMediaType()))
        sigHeaders.forEach { (k, v) -> reqBuilder.header(k, v) }
        extraHeaders.forEach { (k, v) -> reqBuilder.header(k, v) }

        val resp = reqClient.newCall(reqBuilder.build()).await()
        resp.use { r ->
            val respBodyStr = r.body.string()
            if (r.code == 429) {
                parseAndApplyRetryAfter(r, KEY_SERVER_WAIT)
                throw TidalGatewayException("RATE_LIMITED")
            }
            if (r.code == 401 || r.code == 403) {
                clearStoredSession()
                throw TidalGatewayException("AUTH_REQUIRED")
            }
            if (!r.isSuccessful) {
                throw TidalGatewayException("GATEWAY_HTTP_${r.code}")
            }
            runCatching { json.parseToJsonElement(respBodyStr).jsonObject }.getOrNull()
                ?: throw TidalGatewayException("INVALID_RESPONSE")
        }
    }

    suspend fun requestTicket(trackId: String, deadlineMs: Long): String =
        withContext(Dispatchers.IO) {
            val hash = TidalCrypto.sha256Hex("$TIDAL_TICKET_ALIAS:track:${trackId.trim().lowercase(Locale.ROOT)}")
            val payload = JsonObject(
                mapOf(
                    "capability" to JsonPrimitive("download_ticket"),
                    "provider" to JsonPrimitive(TIDAL_TICKET_ALIAS),
                    "resource_hash" to JsonPrimitive(hash)
                )
            )
            val resp = signedRequest("POST", "/tickets", payload, emptyMap(), deadlineMs)
            val ticket = resp.stringField("ticket_id")
                ?: resp.stringField("ticket")
                ?: (resp["data"] as? JsonObject)?.stringField("ticket_id")
                ?: (resp["data"] as? JsonObject)?.stringField("ticket")
                ?: throw TidalGatewayException("NO_TICKET")
            ticket
        }

    private suspend fun tryRefreshSession(session: StoredSession, deadlineMs: Long): StoredSession? =
        withContext(Dispatchers.IO) {
            val body = JsonObject(mapOf("install_id" to JsonPrimitive(session.installId)))
            val bodyStr = body.toString()
            val sigHeaders = TidalCrypto.signedHeaders(
                session = session,
                method = "POST",
                path = "/session/refresh",
                body = bodyStr,
                nowMs = clock()
            )
            val timeout = remainingTimeoutMs(deadlineMs, 2500L)
            val reqClient = httpClient.newBuilder()
                .callTimeout(timeout, TimeUnit.MILLISECONDS)
                .connectTimeout(timeout, TimeUnit.MILLISECONDS)
                .readTimeout(timeout, TimeUnit.MILLISECONDS)
                .build()

            val req = Request.Builder()
                .url("$gatewayBaseUrl/session/refresh")
                .post(bodyStr.toRequestBody("application/json".toMediaType()))
                .apply { sigHeaders.forEach { (k, v) -> header(k, v) } }
                .build()

            try {
                reqClient.newCall(req).await().use { r ->
                    if (r.code == 429) {
                        parseAndApplyRetryAfter(r, KEY_SERVER_WAIT)
                        return@withContext null
                    }
                    if (r.code == 401 || r.code == 403) return@withContext null
                    if (!r.isSuccessful) return@withContext null
                    val obj = runCatching { json.parseToJsonElement(r.body.string()).jsonObject }.getOrNull()
                        ?: return@withContext null
                    val expiresAt = obj["expires_at"]?.jsonPrimitive?.contentOrNull
                        ?: obj["expiresAt"]?.jsonPrimitive?.contentOrNull
                        ?: (obj["data"] as? JsonObject)?.get("expires_at")?.jsonPrimitive?.contentOrNull
                        ?: (obj["data"] as? JsonObject)?.get("expiresAt")?.jsonPrimitive?.contentOrNull
                        .orEmpty()
                    session.copy(expiresAt = expiresAt)
                }
            } catch (ce: CancellationException) {
                throw ce
            } catch (_: Throwable) {
                null
            }
        }

    suspend fun manualVerify(force: Boolean = false): VerificationStatus = autoMutex.withLock {
        beginVerification(automatic = false, forceManual = force)
    }

    suspend fun startAutomatic(scope: CoroutineScope, reset: Boolean = false, reason: String = ""): Boolean {
        if (!automaticAllowed()) return false
        if (autoJob?.isActive == true) return true
        autoJob = scope.launch {
            try {
                autoMutex.withLock {
                    if (reset) clearPendingVerification()
                    beginVerification(automatic = true)
                }
            } finally {
                autoJob = null
            }
        }
        return true
    }

    suspend fun beginVerification(
        automatic: Boolean,
        deadlineMs: Long = clock() + 25_000L,
        forceManual: Boolean = false
    ): VerificationStatus = withContext(Dispatchers.IO) {
        if (!verifyMutex.tryLock()) {
            if (!automatic) {
                log("Tidal: una verifica è già aperta; completala o chiudila prima di riprovare.")
                notify("Una verifica è già aperta. Completa o chiudi quella pagina.")
            }
            return@withContext VerificationStatus.BUSY
        }
        try {
            if (getPendingVerification() != null) {
                if (!automatic) {
                    log("Tidal: una verifica è già aperta; completala o chiudila prima di riprovare.")
                    notify("Una verifica è già aperta. Completa o chiudi quella pagina.")
                }
                return@withContext VerificationStatus.BUSY
            }

            if (forceManual) {
                val serverWait = remainingServerWait()
                val guardWait = maxOf(0L, getWaitUntil(KEY_FORCE_GUARD) - clock())
                if (serverWait > 0L) {
                    log("Tidal: verifica manuale in attesa del server; ancora circa ${TidalDiagnostics.humanWait(serverWait)}.")
                    notify("Il server richiede ancora ${TidalDiagnostics.humanWait(serverWait)} di attesa.")
                    return@withContext VerificationStatus.RATE_LIMITED
                }
                if (guardWait > 0L) {
                    log("Tidal: attendi ancora circa ${TidalDiagnostics.humanWait(guardWait)} prima di un altro tentativo manuale.")
                    notify("Attendi ${TidalDiagnostics.humanWait(guardWait)} prima di un altro tentativo manuale.")
                    return@withContext VerificationStatus.RATE_LIMITED
                }
                setWaitUntil(KEY_FORCE_GUARD, clock() + 30_000L)
            } else if (remainingCooldown() > 0L) {
                val wait = remainingCooldown()
                val msg = "Attendi ancora circa ${TidalDiagnostics.humanWait(wait)} prima di un altro tentativo."
                log("Tidal: $msg")
                if (!automatic) notify(msg)
                return@withContext VerificationStatus.RATE_LIMITED
            }

            val install = installId()
            log("Tidal: avvio verifica bootstrap (automatica=$automatic)...")

            val bootUrl = (gatewayBaseUrl.trimEnd('/') + "/bootstrap").toHttpUrl().newBuilder()
                .addQueryParameter("app_version", TIDAL_APP)
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

            val bootstrapJson = try {
                callClient.newCall(req).await().use { r ->
                    val raw = runCatching { r.body.string() }.getOrDefault("")
                    val parsed = parseJsonObjectOrNull(raw)
                    if (!r.isSuccessful) {
                        log("Tidal: avvio verifica non disponibile (HTTP ${r.code}).")
                        if (r.code == 429) {
                            parseAndApplyRetryAfter(r, KEY_SERVER_WAIT)
                            if (!automatic) notify("Tidal: server HTTP 429. Riprova più tardi.")
                            return@withContext VerificationStatus.RATE_LIMITED
                        }
                        if (!automatic) notify("Verifica Tidal non disponibile. Riprova più tardi.")
                        return@withContext VerificationStatus.FAILED
                    }
                    parsed
                }
            } catch (ce: CancellationException) {
                throw ce
            } catch (err: Throwable) {
                log("Tidal: gateway non raggiungibile: ${TidalDiagnostics.failureCategory(err)}.")
                if (!automatic) notify("Verifica Tidal non disponibile. Controlla la rete.")
                return@withContext VerificationStatus.FAILED
            } ?: run {
                log("Tidal: risposta bootstrap non valida.")
                if (!automatic) notify("Risposta server Tidal non riconosciuta.")
                return@withContext VerificationStatus.FAILED
            }

            if (saveSessionFromData(bootstrapJson, install, null)) {
                clearPendingVerification()
                log("Tidal: sessione verificata direttamente!")
                if (!automatic) notify("Tidal attivato con successo!")
                return@withContext VerificationStatus.COMPLETE
            }

            val challenge = bootstrapJson.stringField("challenge_id").orEmpty()
            val serverNonce = bootstrapJson.stringField("server_nonce").orEmpty().ifEmpty { challenge }
            if (challenge.isEmpty() || serverNonce.isEmpty()) {
                log("Tidal: risposta di verifica non valida.")
                if (!automatic) notify("Verifica Tidal non riconosciuta.")
                return@withContext VerificationStatus.FAILED
            }

            val webView = webViewClientRef ?: run {
                log("Tidal: WebView non disponibile.")
                if (!automatic) notify("Impossibile aprire la verifica Tidal. Aggiorna Android System WebView.")
                return@withContext VerificationStatus.AUTH_UNAVAILABLE
            }

            val state = TidalCrypto.generateState(serverNonce, install, clock())
            val callbackParam = "$CALLBACK_URI?cb_version=v2grant&state=" +
                URLEncoder.encode(state, StandardCharsets.UTF_8.name())
            val challengeUrl = (gatewayBaseUrl.trimEnd('/') + "/challenge").toHttpUrl().newBuilder()
                .addQueryParameter("id", challenge)
                .addQueryParameter("cb", callbackParam)
                .build()
                .toString()

            val ttlSec = (bootstrapJson.doubleField("expires_in")?.toLong() ?: 300L).coerceIn(15L, 300L)
            val pending = PendingVerification(
                state = state,
                installId = install,
                until = clock() + ttlSec * 1000L,
                automatic = automatic
            )
            savePendingVerification(pending)
            log("Tidal: pagina di verifica aperta${if (automatic) " automaticamente." else "."}")
            if (automatic) {
                notify("Verifica automatica avviata. Completa la verifica nella finestra.")
            }

            val wvRequest = object : WebViewRequest.Headers<String> {
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
                    reason = "Verifica Tidal",
                    request = wvRequest
                )
            } catch (ce: CancellationException) {
                clearPendingVerification()
                throw ce
            } catch (_: Throwable) {
                clearPendingVerification()
                log("Tidal: apertura WebView fallita.")
                if (!automatic) notify("Impossibile aprire la verifica Tidal. Aggiorna Android System WebView.")
                return@withContext VerificationStatus.AUTH_UNAVAILABLE
            }

            clearPendingVerification()
            return@withContext finishVerification(pending, webResult)
        } finally {
            verifyMutex.unlock()
        }
    }

    private suspend fun finishVerification(
        pending: PendingVerification,
        webResult: Result<String?>
    ): VerificationStatus = withContext(Dispatchers.IO) {
        try {
            val callbackUrl = webResult.getOrNull()?.trim().orEmpty()
            if (webResult.isFailure || callbackUrl.isEmpty()) {
                throw IllegalStateException("CLOSED_OR_EXPIRED")
            }
            if (!callbackUrl.startsWith("$CALLBACK_URI?", ignoreCase = true) &&
                !callbackUrl.startsWith("$CALLBACK_URI#", ignoreCase = true)
            ) {
                throw IllegalStateException("INVALID_CALLBACK")
            }
            if (clock() > pending.until) {
                throw IllegalStateException("EXPIRED_CALLBACK")
            }
            val vals = TidalCrypto.parseCallbackValues(callbackUrl)
            val grant = (vals["grant"] ?: vals["code"]).orEmpty().trim()
            if (grant.isEmpty() || grant.length > 8192 || vals["state"]?.trim() != pending.state) {
                throw IllegalStateException("INVALID_STATE")
            }

            val payload = JsonObject(
                mapOf(
                    "grant" to JsonPrimitive(grant),
                    "install_id" to JsonPrimitive(pending.installId),
                    "app_version" to JsonPrimitive(TIDAL_APP),
                    "platform" to JsonPrimitive("extension")
                )
            )
            val exchangeUrl = (gatewayBaseUrl.trimEnd('/') + "/session/exchange").toHttpUrl()
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

            callClient.newCall(req).await().use { r ->
                val raw = runCatching { r.body.string() }.getOrDefault("")
                val data = parseJsonObjectOrNull(raw)
                if (r.code == 429) {
                    parseAndApplyRetryAfter(r, KEY_SERVER_WAIT)
                    log("Tidal: creazione sessione in pausa dopo HTTP 429.")
                    return@use VerificationStatus.RATE_LIMITED
                }
                if (!r.isSuccessful || !saveSessionFromData(data, pending.installId, null)) {
                    log("Tidal: sessione non creata (HTTP ${r.code}).")
                    throw IllegalStateException("EXCHANGE_FAILED")
                }
                log("Tidal: sessione verificata con successo.")
                if (!pending.automatic) notify("Tidal verificato con successo!")
                VerificationStatus.COMPLETE
            }
        } catch (ce: CancellationException) {
            throw ce
        } catch (_: Throwable) {
            log("Tidal: verifica non completata o annullata.")
            if (!pending.automatic) notify("Verifica non completata. Riprova dalle impostazioni.")
            VerificationStatus.FAILED
        }
    }

    private fun saveSessionFromData(
        data: JsonObject?,
        installId: String,
        old: StoredSession?
    ): Boolean {
        val id = data?.stringField("session_id").orEmpty()
            .ifEmpty { data?.stringField("id").orEmpty() }
            .ifEmpty { old?.id.orEmpty() }
        val secret = data?.stringField("session_secret").orEmpty()
            .ifEmpty { data?.stringField("secret").orEmpty() }
            .ifEmpty { old?.secret.orEmpty() }
        val expiresAt = data?.stringField("expires_at").orEmpty()
            .ifEmpty { data?.stringField("expiresAt").orEmpty() }
            .ifEmpty { old?.expiresAt.orEmpty() }

        val candidate = StoredSession(
            id = id,
            secret = secret,
            expiresAt = expiresAt,
            installId = installId
        )
        if (!candidate.isValid()) return false
        saveStoredSession(candidate)
        return true
    }

    private fun parseJsonObjectOrNull(raw: String): JsonObject? {
        if (raw.isBlank()) return null
        return runCatching { json.parseToJsonElement(raw).jsonObject }.getOrNull()
    }

    private fun parseAndApplyRetryAfter(r: Response, key: String) {
        val retryHeader = r.header("Retry-After")?.toLongOrNull()
        val waitSeconds = retryHeader ?: 300L
        setWaitUntil(key, clock() + waitSeconds * 1000L)
    }

    fun clearStalePendingOnStartup() {
        val pending = getPendingVerification() ?: return
        if (pending.until <= clock()) {
            clearPendingVerification()
        }
    }
}

internal fun JsonObject.stringField(name: String): String? =
    (this[name] as? JsonPrimitive)?.contentOrNull?.trim()
        ?: (this["data"] as? JsonObject)?.stringField(name)
        ?: (this["session"] as? JsonObject)?.stringField(name)

internal fun JsonObject.doubleField(name: String): Double? =
    (this[name] as? JsonPrimitive)?.doubleOrNull
        ?: (this[name] as? JsonPrimitive)?.contentOrNull?.trim()?.toDoubleOrNull()
        ?: (this["data"] as? JsonObject)?.doubleField(name)

internal fun JsonObject.booleanField(name: String): Boolean? {
    val raw = (this[name] as? JsonPrimitive)?.contentOrNull?.trim()?.lowercase(Locale.ROOT)
        ?: return (this["data"] as? JsonObject)?.booleanField(name)
    return when (raw) {
        "true" -> true
        "false" -> false
        else -> null
    }
}
