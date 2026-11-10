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
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class TidalSessionManager(
    baseHttpClient: OkHttpClient,
    internal val logs: DiagnosticLogBuffer,
    private val onNotify: (String) -> Unit = {},
    internal val gatewayBaseUrl: String = "https://gateway.spotiflac.com",
    private val clock: () -> Long = System::currentTimeMillis
) {
    val httpClient: OkHttpClient = withBrowserHeaders(baseHttpClient)

    private val sessionMutex = Mutex()
    private val autoMutex = Mutex()

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

        const val CALLBACK_URI = "spotiflac://session-grant"
        val CALLBACK_STOP_REGEX = Regex("^spotiflac://session-grant", RegexOption.IGNORE_CASE)

        const val MODERN_CHROME_UA =
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
                "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

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
        val url = gatewayBaseUrl.trimEnd('/') + "/v2" + path
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
            val payload = JsonObject(
                mapOf(
                    "alias" to JsonPrimitive(TIDAL_TICKET_ALIAS),
                    "id" to JsonPrimitive(trackId)
                )
            )
            val resp = signedRequest("POST", "/ticket", payload, emptyMap(), deadlineMs)
            resp["ticket"]?.jsonPrimitive?.contentOrNull
                ?: (resp["data"] as? JsonObject)?.get("ticket")?.jsonPrimitive?.contentOrNull
                ?: throw TidalGatewayException("NO_TICKET")
        }

    private suspend fun tryRefreshSession(session: StoredSession, deadlineMs: Long): StoredSession? =
        withContext(Dispatchers.IO) {
            val body = JsonObject(mapOf("app" to JsonPrimitive(TIDAL_APP)))
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
                .url("$gatewayBaseUrl/v2/session/refresh")
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
                    val expiresAt = obj["expiresAt"]?.jsonPrimitive?.contentOrNull
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
        val serverWait = remainingServerWait()
        if (serverWait > 0L && !force) {
            val msg = "Il server richiede ancora ${TidalDiagnostics.humanWait(serverWait)} di attesa."
            log("Tidal: verifica manuale in attesa del server.")
            notify(msg)
            return VerificationStatus.RATE_LIMITED
        }
        val cooldown = remainingCooldown()
        if (cooldown > 0L && !force) {
            val msg = "Attendi ancora ${TidalDiagnostics.humanWait(cooldown)} prima di un altro tentativo."
            log("Tidal: cooldown manuale attivo.")
            notify(msg)
            return VerificationStatus.BUSY
        }
        setWaitUntil(KEY_COOLDOWN, clock() + 60_000L)
        executeVerification(automatic = false)
    }

    suspend fun startAutomatic(scope: CoroutineScope, reset: Boolean = false, reason: String = ""): Boolean {
        if (!automaticAllowed()) return false
        if (autoJob?.isActive == true) return true
        autoJob = scope.launch {
            try {
                autoMutex.withLock {
                    if (reset) clearPendingVerification()
                    executeVerification(automatic = true)
                }
            } finally {
                autoJob = null
            }
        }
        return true
    }

    private suspend fun executeVerification(automatic: Boolean): VerificationStatus = withContext(Dispatchers.IO) {
        val installId = getStoredSession()?.installId ?: TidalCrypto.generateInstallId()
        log("Tidal: avvio verifica bootstrap (automatica=$automatic)...")

        val bootstrapUrl = "$gatewayBaseUrl/v2/bootstrap"
        val payload = JsonObject(
            mapOf(
                "app" to JsonPrimitive(TIDAL_APP),
                "install_id" to JsonPrimitive(installId)
            )
        )
        val req = Request.Builder()
            .url(bootstrapUrl)
            .post(payload.toString().toRequestBody("application/json".toMediaType()))
            .build()

        val resp = try {
            httpClient.newCall(req).await()
        } catch (ce: CancellationException) {
            throw ce
        } catch (err: Throwable) {
            log("Tidal: gateway non raggiungibile: ${TidalDiagnostics.failureCategory(err)}.")
            return@withContext VerificationStatus.FAILED
        }

        resp.use { r ->
            if (r.code == 429) {
                parseAndApplyRetryAfter(r, KEY_SERVER_WAIT)
                log("Tidal: limite richieste raggiunto (HTTP 429).")
                return@withContext VerificationStatus.RATE_LIMITED
            }
            if (!r.isSuccessful) {
                log("Tidal: bootstrap fallito (HTTP ${r.code}).")
                return@withContext VerificationStatus.FAILED
            }
            val obj = runCatching { json.parseToJsonElement(r.body.string()).jsonObject }.getOrNull()
                ?: return@withContext VerificationStatus.FAILED

            val challenge = obj["challenge_id"]?.jsonPrimitive?.contentOrNull.orEmpty()
            val serverNonce = obj["server_nonce"]?.jsonPrimitive?.contentOrNull.orEmpty().ifEmpty { challenge }
            if (challenge.isEmpty() || serverNonce.isEmpty()) {
                val directId = obj["id"]?.jsonPrimitive?.contentOrNull
                val directSecret = obj["secret"]?.jsonPrimitive?.contentOrNull
                if (!directId.isNullOrEmpty() && !directSecret.isNullOrEmpty()) {
                    saveStoredSession(
                        StoredSession(
                            id = directId,
                            secret = directSecret,
                            expiresAt = obj["expiresAt"]?.jsonPrimitive?.contentOrNull.orEmpty(),
                            installId = installId
                        )
                    )
                    clearPendingVerification()
                    notice("Sessione Tidal verificata direttamente!")
                    return@withContext VerificationStatus.COMPLETE
                }
                log("Tidal: risposta bootstrap non valida.")
                return@withContext VerificationStatus.FAILED
            }

            val webView = webViewClientRef ?: run {
                log("Tidal: WebView non disponibile per la sfida.")
                return@withContext VerificationStatus.AUTH_UNAVAILABLE
            }

            val state = TidalCrypto.generateState(serverNonce, installId, clock())
            val callbackParam = "$CALLBACK_URI?cb_version=v2grant&state=" +
                URLEncoder.encode(state, StandardCharsets.UTF_8.name())
            val challengeUrl = (gatewayBaseUrl.trimEnd('/') + "/challenge").toHttpUrl().newBuilder()
                .addQueryParameter("id", challenge)
                .addQueryParameter("cb", callbackParam)
                .build()
                .toString()

            val ttlSec = (obj["expires_in"]?.jsonPrimitive?.doubleOrNull?.toLong() ?: 300L).coerceIn(15L, 300L)
            savePendingVerification(
                PendingVerification(
                    state = state,
                    installId = installId,
                    until = clock() + ttlSec * 1000L,
                    automatic = automatic
                )
            )

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

            log("Tidal: apertura verifica WebView...")
            val webResult = try {
                webView.await(
                    showWebView = true,
                    reason = "Verifica Tidal",
                    request = wvRequest
                ).getOrNull()
            } catch (ce: CancellationException) {
                clearPendingVerification()
                throw ce
            } catch (_: Throwable) {
                clearPendingVerification()
                log("Tidal: apertura WebView fallita.")
                return@withContext VerificationStatus.AUTH_UNAVAILABLE
            }

            val callbackUrl = webResult ?: return@withContext VerificationStatus.FAILED
            val parsed = TidalCrypto.parseCallback(callbackUrl)
            if (parsed != null && parsed.second == state) {
                exchangeGrant(parsed.first, installId)
                VerificationStatus.COMPLETE
            } else {
                VerificationStatus.FAILED
            }
        }
    }

    private fun exchangeGrant(grant: String, installId: String) {
        val payload = JsonObject(
            mapOf(
                "grant" to JsonPrimitive(grant),
                "installId" to JsonPrimitive(installId)
            )
        )
        val req = Request.Builder()
            .url("$gatewayBaseUrl/v2/session/exchange")
            .post(payload.toString().toRequestBody("application/json".toMediaType()))
            .build()

        httpClient.newCall(req).enqueue(object : okhttp3.Callback {
            override fun onFailure(call: Call, e: IOException) {
                log("Tidal: scambio grant fallito: ${e.message}")
            }

            override fun onResponse(call: Call, response: Response) {
                response.use { r ->
                    if (!r.isSuccessful) {
                        log("Tidal: scambio grant rifiutato (HTTP ${r.code})")
                        return
                    }
                    val body = runCatching { json.parseToJsonElement(r.body.string()).jsonObject }.getOrNull()
                    val id = body?.get("id")?.jsonPrimitive?.contentOrNull
                    val secret = body?.get("secret")?.jsonPrimitive?.contentOrNull
                    val exp = body?.get("expiresAt")?.jsonPrimitive?.contentOrNull.orEmpty()
                    if (!id.isNullOrEmpty() && !secret.isNullOrEmpty()) {
                        saveStoredSession(
                            StoredSession(
                                id = id,
                                secret = secret,
                                expiresAt = exp,
                                installId = installId
                            )
                        )
                        clearPendingVerification()
                        notice("Sessione Tidal verificata con successo!")
                    }
                }
            }
        })
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
