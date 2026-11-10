# Tidal Extension per Echo / Echo Fluid

Estensione nativa standalone in **Kotlin** per **Tidal** su **Echo / Echo Fluid / Echo Nightly** (`dev.brahmkshatriya.echo`, API `echo-common:1.0`).

- **Repository GitHub**: <https://github.com/TheWinner02/echo-tidal-extension>
- **Endpoint aggiornamenti in-app Echo**: `https://api.github.com/repos/TheWinner02/echo-tidal-extension/releases`
- **Repository JSON Echo**: `https://raw.githubusercontent.com/TheWinner02/echo-tidal-extension/master/echo_extensions.json`

---

## Caratteristiche principali

### 1. Estensione Nativa Standalone
- **Nessuna logica Bridge**: Navigazione diretta del catalogo Tidal senza intermediari.
- **Home Feed personalizzato**: Nuove uscite (Album e Singoli), Brani più ascoltati e Playlist consigliate.
- **Ricerca Completa a Tab**: Brani, Album, Artisti e Playlist con filtro paese configurabile.
- **Pagine dedicate**:
  - **Album**: tracciamento dettagliato e lista tracce.
  - **Artisti**: panoramica, brani popolari e discografia.
  - **Playlist**: visualizzazione completa con metadati e tracce.
  - **Radio**: generazione dinamica della traccia radio.

### 2. Riproduzione e Download FLAC Lossless
- **Qualità FLAC (Lossless / Hi-Res Lossless)**: Risoluzione di stream audio FLAC autentici.
- **Streaming Diretto & DASH**: Supporto nativo per URL diretti FLAC o manifesti DASH MPD non protetti da DRM.
- **Supporto Download Offline**: Generazione di stream continui fMP4 segmentati per il download offline in Echo.

### 3. Sessione, Verifica WebView e Rinnovo Silenzioso
- **Protocollo Gateway ZARZ-HMAC-V1**: Firma crittografica rolling HMAC con finestra di validità a 5 minuti per token e ticket sicuri.
- **Verifica WebView Integrata**: Apertura controllata per superare le sfide anti-bot quando richiesto.
- **Rinnovo Automatico ogni 6 ore**: Mantiene la sessione sempre fresca in background.
- **Gestione `HTTP 429` (Rate Limiting)**: Rispetto degli header `Retry-After` e backoff automatico.
- **Network Awareness**: Riconnessione e ripresa automatica delle verifiche al ripristino della connettività di rete.

---

## Logica di Aggiornamento In-App e Versioning

Il sistema di build e aggiornamento è allineato allo standard di **Echo**:
- **Workflow GitHub Actions (`.github/workflows/build.yml`)**:
  - Compilazione automatica tramite Gradle.
  - Firma e impacchettamento del file `.eapk` e `.jar` con `release-signer`.
  - Pubblicazione automatica della release con tag progressivo `tidal`.
- **In-App Updater**: Echo rileva automaticamente le nuove release grazie a `extUpdateUrl` (`https://api.github.com/repos/TheWinner02/echo-tidal-extension/releases`).

---

## Struttura del progetto

```text
echo-tidal-extension/
├── app/                                  # Modulo Android (.eapk con AndroidManifest + R8 + icone)
├── ext/                                  # Modulo Kotlin dell'estensione Echo (echo-common:1.0)
│   ├── src/main/java/dev/brahmkshatriya/echo/extension/
│   │   ├── TidalExtension.kt             # Client Echo principale (Home, Search, Track, Album, Artist, Playlist, Radio)
│   │   └── tidal/
│   │       ├── TidalApi.kt               # Client API REST Tidal per catalogo e metadati
│   │       ├── TidalConverter.kt         # Mappatura modelli Tidal -> Echo Common Models
│   │       ├── TidalCrypto.kt            # Crittografia ZARZ-HMAC-V1, installId, parsing callback
│   │       ├── TidalModels.kt            # Modelli dati interni (sessione, stream, diagnostica)
│   │       ├── TidalSessionManager.kt    # Gestione sessione, WebView, rinnovo 6h e rate limiting
│   │       └── TidalStreamResolver.kt    # Risoluzione stream FLAC (Progressive / DASH) e download provider
│   └── src/test/java/dev/brahmkshatriya/echo/extension/
│       ├── TidalExtensionTest.kt         # Test unitari per Crypto, API, Stream Resolver e Settings
│       └── MockedSettings.kt             # Settings in-memory per test
├── echo_extensions.json                  # Indice repository JSON per Echo
├── .github/workflows/build.yml           # Workflow di build e rilascio automatico
└── gradle.properties                     # Configurazione estensione e versioning
```

---

## Compilazione da sorgente

```bash
./gradlew :ext:check :ext:shadowJar :ext:test
```
