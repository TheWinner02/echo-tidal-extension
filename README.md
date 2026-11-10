# FLAC Bridge Multi per Echo / Echo Fluid

Porting nativo in **Kotlin** dell'estensione [FLAC-Bridge-Multi-Myspoti](https://gitlab.com/solidsnakeater/flac-bridge-myspoti) per **Echo / Echo Fluid / Echo Nightly** (`dev.brahmkshatriya.echo`, API `echo-common:1.0`).

- **Repository GitLab**: <https://gitlab.com/solidsnakeater/echo-flac-bridge>
- **Endpoint aggiornamenti automatici Echo**: `https://gitlab.com/api/v4/projects/solidsnakeater%2Fecho-flac-bridge/releases/permalink/latest`
- **Repository JSON Echo (opzionale)**: `https://gitlab.com/solidsnakeater/echo-flac-bridge/-/raw/main/echo_extensions.json`

Risolve flussi audio **FLAC lossless** reali tramite quattro sorgenti (**Tidal**, **Qobuz**, **Deezer**, **Amazon Music**), mantenendo il protocollo gateway `ZARZ-HMAC-V1`, la selezione manuale del server direttamente nel player, la verifica sessioni tramite WebView integrata, il rinnovo automatico ogni 6 ore e la gestione completa dei limiti `HTTP 429`.

---

## Caratteristiche principali

### 1. Doppia modalità operativa e Scelta Manuale Server nel Player
L'estensione implementa `MusicExtensionsProvider` e può lavorare in **due modi** (selezionabili dalle impostazioni):

- **Bridge Integrato (`Automatica` / `Solo Bridge`)** — *Comportamento identico a Myspoti*:
  - Avvolge trasparentemente un'estensione metadati già installata in Echo (ad es. **Spotify**, YouTube Music, Deezer, ecc.).
  - Mantiene intatti **Home, Libreria, Playlist, Ricerca, Album, Artisti, Radio, Like/Salvataggi e video di sfondo (Spotify Canvas)** dell'estensione metadati.
  - Nel selettore **Server / Qualità** del player di Echo Fluid espone sia il chip **`Auto · <Provider>`** (preselezionato di default) sia i chip manuali **`Tidal · FLAC`**, **`Qobuz · FLAC`**, **`Deezer · FLAC`**, **`Amazon Music · FLAC`** e **`Spotify`** (audio originale), risolvendo solo il server effettivamente selezionato.
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

### Metodo A — File `.eapk` diretto
1. Trasferisci **`echo_flac_bridge-v2.3.2.eapk`** sul telefono Android.
2. Apri **Echo Fluid** -> **Estensioni** -> **`+` (Aggiungi estensione)** -> **Installa da file** e seleziona `echo_flac_bridge-v2.3.2.eapk`.

### Metodo B — Repository JSON da GitLab
In Echo Fluid -> **Estensioni** -> **Aggiungi repository**, incolla:
```text
https://gitlab.com/solidsnakeater/echo-flac-bridge/-/raw/main/echo_extensions.json
```

---

## Aggiornamenti Automatici tramite GitLab CI/CD (`.gitlab-ci.yml`)

Il file `.gitlab-ci.yml` compila automaticamente i test e il pacchetto `.eapk`, lo carica nel **GitLab Generic Package Registry** tramite `$CI_JOB_TOKEN` (senza bisogno di creare token manuali) e pubblica una **GitLab Release** rilevata da Echo Fluid tramite `extUpdateUrl`.

### Firma persistente Android (consigliata per aggiornamenti in-place)
In **GitLab -> Settings -> CI/CD -> Variables**, aggiungi:
- `SIGNING_KEYSTORE`: contenuto in Base64 del keystore `.jks` (opzione *Masked* attiva, *Protect variable* disattivata se vuoi usarla anche su branch/tag non protetti)
- `SIGNING_STORE_PASSWORD`: password del keystore (*Masked* attiva)

Per rilasciare una nuova versione basta fare `git push` su `main` (genera `v<extBaseVersion>-<commit>`) oppure creare un tag come `v2.3.3` (aggiornando `extBaseVersion` in `gradle.properties`).

---

## Struttura del progetto

```text
echo-flac-bridge/
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
├── .gitlab-ci.yml                        # Pipeline GitLab CI/CD automatica (build + package + release)
└── release/                              # Pacchetti pronti (.eapk, .apk, .jar)
```

## Compilazione manuale da sorgente

Requisiti: **JDK 21** e **Android SDK** (`compileSdk = 36`, `build-tools = 35.0.0`).

```bash
./gradlew :ext:check :ext:shadowJar :app:assembleRelease
```

---

## Licenza

Distribuito sotto licenza **GNU General Public License v3.0 (GPL-3.0)**, in continuità con il progetto originale [FLAC-Bridge-Multi-Myspoti](https://gitlab.com/solidsnakeater/flac-bridge-myspoti).
