# FLAC Bridge Multi per Echo / Echo Fluid

Porting nativo in **Kotlin** dell'estensione FLAC Bridge Multi per **Echo / Echo Fluid / Echo Nightly** (`dev.brahmkshatriya.echo`, API `echo-common:1.0`).

- **Repository GitHub**: <https://github.com/TheWinner02/echo-flac-bridge-extension>
- **Endpoint aggiornamenti automatici in-app Echo**: `https://api.github.com/repos/TheWinner02/echo-flac-bridge-extension/releases`
- **Repository JSON Echo (opzionale)**: `https://raw.githubusercontent.com/TheWinner02/echo-flac-bridge-extension/master/echo_extensions.json`

Risolve flussi audio **FLAC lossless** reali tramite quattro sorgenti (**Tidal**, **Qobuz**, **Deezer**, **Amazon Music**), mantenendo il protocollo gateway `ZARZ-HMAC-V1`, la selezione manuale del server direttamente nel player, la verifica sessioni tramite WebView integrata, il rinnovo automatico ogni 6 ore e la gestione completa dei limiti `HTTP 429`.

---

## Caratteristiche principali

### 1. Doppia modalità operativa e Scelta Manuale Server nel Player
L'estensione implementa `MusicExtensionsProvider` e può lavorare in **due modi** (selezionabili dalle impostazioni):

- **Bridge Integrato (`Automatica` / `Solo Bridge`)** — *Comportamento integrato trasparente*:
  - Avvolge trasparentemente un'estensione metadati già installata in Echo (ad es. **Spotify**, YouTube Music, Deezer, ecc.).
  - Mantiene intatti **Home, Libreria, Playlist, Ricerca, Album, Artisti, Radio, Like/Salvataggi e video di sfondo (Spotify Canvas)** dell'estensione metadati.
  - Nel selettore **Server / Qualità** del player di Echo Fluid espone sia il chip **`Auto · <Provider>`** (preselezionato di default) sia i chip manuali **`Tidal · FLAC`**, **`Qobuz · FLAC`**, **`Deezer · FLAC`**, **`Amazon Music · FLAC`** e l'estensione metadati originale, risolvendo solo il server effettivamente selezionato.
- **Sorgente FLAC Autonoma (`Solo Sorgente FLAC autonoma` o abbinata a `Combine Extension`)**:
  - Funziona anche **senza alcuna altra estensione installata**, permettendo di cercare brani direttamente sui cataloghi lossless (Qobuz, Tidal, Deezer) e riprodurli in FLAC.
  - Compatibile con **Combine Extension** (`echo_combine`).

### 2. Protocollo, Verifica e Rinnovo Sessioni
- **Gateway e Firma Rolling**: bootstrap sessione (`/v2/bootstrap`), scambio grant (`/v2/session/exchange`), rinnovo (`/v2/session/refresh`) e richieste firmate **`ZARZ-HMAC-V1`** su finestra temporale di 5 minuti con segreto rolling.
- **Verifica WebView nativa**: quando il gateway richiede una sfida anti-bot (`/v2/challenge`), apre **una sola finestra WebView** integrata in Echo con User-Agent Chrome moderno e intercetta automaticamente il redirect `spotiflac://session-grant`.
- **Verifica automatica ordinata e Rinnovo ogni 6 ore**:
  - Controlla e rinnova silenziosamente le sessioni attive ogni **6 ore** o quando mancano **meno di 60 minuti** alla scadenza.
  - Intercetta il ritorno della connessione dati/Wi-Fi (`NetworkConnectionProvider`) per riprendere verifiche o rinnovi rimasti in sospeso.
- **Gestione rigorosa `HTTP 429` (`Retry-After`)**:
  - Conserva intatte le sessioni ancora valide se `/v2/session/refresh` restituisce `429`.
  - **Forza verifica manuale** consente un singolo tentativo anticipato oltre l'attesa server memorizzata.
  - **Mostra attese verifica** è garantita di **sola lettura** (zero richieste di rete).

### 3. Validazione reale dei flussi FLAC e Download
- **Controllo firma `fLaC` (`Range: bytes=0-3`, HTTP 206)** su domini CDN HTTPS autorizzati per Qobuz (`streaming-qobuz-std.akamaized.net`), Deezer (`dl.musicdl.me`, `*.dzcdn.net`) e Amazon Music (`*.cloudfront.net`).
- **Tidal FLAC diretto + DASH MPD non cifrato**:
  - Durante lo **streaming**, i manifesti MPD FLAC non cifrati (`*.audio.tidal.com`) vengono passati nativamente a ExoPlayer (`SourceType.DASH`).
  - Durante il **download offline** in Echo (`isDownload = true`), l'estensione privilegia gli URL `.flac` diretti (`Progressive`) e, se è disponibile solo Tidal DASH, concatena il segmento `initialization` + i segmenti fMP4 in un flusso continuo scaricabile.
- **Privacy e Log sicuri**: nessun token, segreto HMAC, grant, ticket o URL firmato viene mai registrato nei log diagnostici.

---

## Installazione in Echo Fluid

### Metodo A — Aggiornamento in-app e Repository JSON
In Echo Fluid -> **Estensioni** -> **Aggiungi repository**, incolla:
```text
https://raw.githubusercontent.com/TheWinner02/echo-flac-bridge-extension/master/echo_extensions.json
```
L'estensione apparirà nella lista con supporto agli aggiornamenti automatici in-app.

### Metodo B — File `.eapk` diretto
1. Scarica l'ultima release `.eapk` dalla sezione **Releases** su GitHub.
2. Apri **Echo Fluid** -> **Estensioni** -> **`+` (Aggiungi estensione)** -> **Installa da file** e seleziona il file `.eapk`.

---

## Logica di Aggiornamento In-App e Versioning

Il sistema di aggiornamento è allineato allo standard di **Echo Fluid** e alle estensioni ufficiali:
- **`gradle.properties`**: definisce `extVersionCode`, `extVersionName`, `extRepoUrl` e `extUpdateUrl` (`https://api.github.com/repos/TheWinner02/echo-flac-bridge-extension/releases`).
- **`AndroidManifest.xml`**: espone i metadati `version`, `version_code`, `repo_url` e `update_url` letti dall'`ExtensionParser` di Echo.
- **GitHub Actions (`nightly.yml`)**: compila e firma l'estensione (`.eapk`), pubblicando la release su GitHub affinché l'`AppUpdater` di Echo rilevi automaticamente le nuove versioni in-app tramite il confronto semver.

---

## Struttura del progetto

```text
echo-flac-bridge-extension/
├── app/                                  # Modulo Android APK / .eapk (AndroidManifest + R8 + icone)
├── ext/                                  # Modulo Kotlin dell'estensione Echo (echo-common:1.0)
│   ├── src/main/java/dev/brahmkshatriya/echo/extension/
│   │   ├── FlacBridgeExtension.kt        # Client Echo completo (Bridge + Sorgente Autonoma + Impostazioni)
│   │   └── bridge/
│   │       ├── FlacBridgeModels.kt       # Modelli provider, sessioni, stream risolti e buffer log sicuro
│   │       ├── ZarzCrypto.kt             # Crittografia SHA-256 / Rolling HMAC ZARZ-HMAC-V1 / Callback parser
│   │       ├── TrackMatcher.kt           # Normalizzazione titoli/artisti, ISRC, Spotify URI, Songlink ASIN
│   │       ├── ZarzSessionManager.kt     # Bootstrap, WebView challenge, rinnovo 6h e gestione HTTP 429
│   │       └── FlacStreamResolver.kt     # Ricerca cataloghi e validazione FLAC / MPD su Tidal, Qobuz, Deezer, Amazon
│   └── src/test/java/dev/brahmkshatriya/echo/extension/
│       ├── FlacBridgeExtensionTest.kt    # Suite test end-to-end JUnit + Coroutines
│       └── MockedSettings.kt             # Settings in-memory per i test
├── echo_extensions.json                  # Indice repository JSON compatibile con Echo Fluid
├── .github/workflows/build.yml           # Pipeline GitHub Actions (build + package + release in-app)
└── release/                              # Pacchetti pronti (.eapk, .apk, .jar)
```

## Compilazione manuale da sorgente

Requisiti: **JDK 21** e **Android SDK** (`compileSdk = 36`).

```bash
./gradlew :ext:check :ext:shadowJar :app:assembleRelease
```
