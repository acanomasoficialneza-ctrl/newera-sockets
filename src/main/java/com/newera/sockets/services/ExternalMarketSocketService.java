package com.newera.sockets.services;

import com.newera.sockets.models.ApuestaCliente;
import com.newera.sockets.models.PriceDto;
import com.newera.sockets.repositories.ApuestaClienteRepository;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Servicio reactivo para cotizaciones de Posiciones Abiertas.
 * 
 * Reglas:
 * 1. Solo se conecta a los activos que tienen posiciones abiertas en BD ("ABIERTO").
 * 2. Si hay 0 posiciones abiertas, 0 conexiones externas.
 * 3. Segmentación en chunks de hasta 10 símbolos por WebSocket (idéntico al frontend).
 * 4. Mapeo puro 1:1 de precios Ask/Bid sin valores ficticios.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ExternalMarketSocketService {

    private final SseService sseService;
    private final ApuestaClienteRepository apuestaRepository;
    private final HttpClient httpClient = HttpClient.newHttpClient();

    public static final Set<String> KNOWN_ACCIONES = Set.of(
        "AMZN", "TSLA", "MSFT", "NVDA", "AAPL", "GOOG", "META", "LLY", "JNJ", "ORCL",
        "ADBE", "UBER", "SBUX", "MCD", "KO", "WMT", "PFE", "AZN", "BABA", "PEP",
        "BBVA", "MA", "INTC", "ERII", "BE", "CARR", "CMI", "NFLX",
        "DIS", "TSM", "SQM", "ALB", "CVX", "XOM", "SPCE", "V", "MELI", "GM",
        "AXP", "AAL", "BAC", "F", "JPM", "IBM", "WFC", "C", "T", "MMM", "IBEX"
    );

    // Sockets y símbolos activos por tipo de mercado
    private WebSocket binanceSocket;
    private final Set<String> subscribedBinanceSymbols = ConcurrentHashMap.newKeySet();

    private final List<WebSocket> forexSockets = new CopyOnWriteArrayList<>();
    private final Set<String> subscribedForexSymbols = ConcurrentHashMap.newKeySet();

    private final List<WebSocket> materiasSockets = new CopyOnWriteArrayList<>();
    private final Set<String> subscribedMateriasSymbols = ConcurrentHashMap.newKeySet();

    private final List<WebSocket> accionesSockets = new CopyOnWriteArrayList<>();
    private final Set<String> subscribedAccionesSymbols = ConcurrentHashMap.newKeySet();

    @Value("${app.sockets.binance-url:wss://stream.binance.com:9443/ws}")
    private String binanceUrl;

    @Value("${app.sockets.eod-forex-url:wss://ws.eodhistoricaldata.com/ws/forex}")
    private String eodForexUrl;

    @Value("${app.sockets.eod-us-url:wss://ws.eodhistoricaldata.com/ws/us-quote}")
    private String eodUsUrl;

    @Value("${app.sockets.eod-token:68b09a16609891.62026589}")
    private String eodToken;

    @Value("${app.sockets.binance-enabled:true}")
    private boolean binanceEnabled;

    @Value("${app.sockets.eod-enabled:true}")
    private boolean eodEnabled;

    private boolean wasEmptyLogged = false;

    /**
     * Tarea periódica cada 2 segundos:
     * Consulta las posiciones abiertas en BD y sincroniza los sockets externos bajo demanda.
     */
    @Scheduled(fixedRate = 2000)
    public void syncWithOpenPositions() {
        if (!binanceEnabled && !eodEnabled) return;

        try {
            List<ApuestaCliente> abiertas = apuestaRepository.findByEstatusCompra("ABIERTO");

            if (abiertas == null || abiertas.isEmpty()) {
                if (!wasEmptyLogged && (!subscribedBinanceSymbols.isEmpty() || !subscribedForexSymbols.isEmpty() || !subscribedMateriasSymbols.isEmpty() || !subscribedAccionesSymbols.isEmpty())) {
                    log.info("ℹ️ [Backend Sockets] No hay posiciones abiertas en el sistema. Liberando todos los sockets externos.");
                    closeAllSockets();
                    wasEmptyLogged = true;
                }
                return;
            }

            wasEmptyLogged = false;

            // Agrupar activos de posiciones abiertas por tipo
            Set<String> targetCrypto = new HashSet<>();
            Set<String> targetForex = new HashSet<>();
            Set<String> targetMaterias = new HashSet<>();
            Set<String> targetAcciones = new HashSet<>();

            for (ApuestaCliente pos : abiertas) {
                if (pos.getCompra() == null || pos.getCompra().trim().isEmpty()) continue;
                String raw = pos.getCompra().trim().toUpperCase();
                String clean = raw.replace("/USD", "").replace("/USDT", "").replace("/", "");
                String cat = pos.getCategoria() != null ? pos.getCategoria().trim().toUpperCase() : "";

                if ("ACCIONES".equals(cat) || KNOWN_ACCIONES.contains(clean)) {
                    targetAcciones.add(clean);
                } else if ("DIVISA".equals(cat)) {
                    targetForex.add(clean);
                } else if ("MATERIAS".equals(cat)) {
                    targetMaterias.add(clean);
                } else {
                    targetCrypto.add(clean);
                }
            }

            // Sincronizar cada tipo de mercado
            if (binanceEnabled) syncBinance(targetCrypto);
            if (eodEnabled) syncEodCategory("DIVISA", eodForexUrl, targetForex, forexSockets, subscribedForexSymbols, false);
            if (eodEnabled) syncEodCategory("MATERIAS", eodForexUrl, targetMaterias, materiasSockets, subscribedMateriasSymbols, false);
            if (eodEnabled) syncEodCategory("ACCIONES", eodUsUrl, targetAcciones, accionesSockets, subscribedAccionesSymbols, true);

        } catch (Exception e) {
            log.error("Error sincronizando posiciones abiertas con sockets", e);
        }
    }

    // =========================================================================
    // EODHD: CHUNKING DE HASTA 10 SÍMBOLOS POR SOCKET (DIVISAS, MATERIAS, ACCIONES)
    // =========================================================================
    private synchronized void syncEodCategory(
            String categoryName,
            String baseUrl,
            Set<String> targetSymbols,
            List<WebSocket> activeSockets,
            Set<String> currentSubscribed,
            boolean isAcciones
    ) {
        // Si no hubo cambios en los símbolos abiertos, mantener sockets actuales
        if (targetSymbols.equals(currentSubscribed)) {
            return;
        }

        log.info("🔌 [Backend {}] Actualizando sockets para posiciones abiertas: {}", categoryName, targetSymbols);

        // 1. Cerrar sockets viejos de esta categoría
        closeSocketList(activeSockets);
        currentSubscribed.clear();

        if (targetSymbols.isEmpty()) {
            return;
        }

        // 2. Precarga instantánea por REST para cotizaciones inmediatas en memoria
        if (isAcciones) {
            preloadAccionesRest(targetSymbols);
        }

        // 3. Dividir en chunks de hasta 10 símbolos por conexión (Regla del Front)
        List<String> symbolsList = new ArrayList<>(targetSymbols);
        int chunkSize = 10;
        List<List<String>> chunks = new ArrayList<>();
        for (int i = 0; i < symbolsList.size(); i += chunkSize) {
            chunks.add(symbolsList.subList(i, Math.min(i + chunkSize, symbolsList.size())));
        }

        currentSubscribed.addAll(targetSymbols);

        for (int i = 0; i < chunks.size(); i++) {
            List<String> chunk = chunks.get(i);
            int chunkNum = i + 1;
            int totalChunks = chunks.size();
            connectEodChunkSocket(categoryName, baseUrl, chunk, chunkNum, totalChunks, activeSockets, isAcciones);
        }
    }

    private void connectEodChunkSocket(
            String categoryName,
            String baseUrl,
            List<String> chunk,
            int chunkNum,
            int totalChunks,
            List<WebSocket> activeSockets,
            boolean isAcciones
    ) {
        try {
            String url = baseUrl + "?api_token=" + eodToken;
            httpClient.newWebSocketBuilder()
                    .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
                    .buildAsync(URI.create(url), new WebSocket.Listener() {
                        private final StringBuilder textBuilder = new StringBuilder();

                        @Override
                        public void onOpen(WebSocket webSocket) {
                            log.info("✅ [Backend {} ({}/{})] Conectado a EOD", categoryName, chunkNum, totalChunks);
                            activeSockets.add(webSocket);
                            WebSocket.Listener.super.onOpen(webSocket);
                        }

                        @Override
                        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                            textBuilder.append(data);
                            if (last) {
                                String msg = textBuilder.toString();
                                textBuilder.setLength(0);
                                handleEodMessage(msg, webSocket, categoryName, chunk, chunkNum, totalChunks, isAcciones);
                            }
                            return WebSocket.Listener.super.onText(webSocket, data, last);
                        }

                        @Override
                        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
                            activeSockets.remove(webSocket);
                            return WebSocket.Listener.super.onClose(webSocket, statusCode, reason);
                        }

                        @Override
                        public void onError(WebSocket webSocket, Throwable error) {
                            log.error("❌ [Backend {} ({}/{})] Error en socket EOD: {}", categoryName, chunkNum, totalChunks, error.getMessage());
                            activeSockets.remove(webSocket);
                        }
                    });
        } catch (Exception e) {
            log.error("Error conectando socket EOD {} ({}/{})", categoryName, chunkNum, totalChunks, e);
        }
    }

    private void handleEodMessage(
            String msg,
            WebSocket webSocket,
            String categoryName,
            List<String> chunk,
            int chunkNum,
            int totalChunks,
            boolean isAcciones
    ) {
        try {
            // 1. Autorización exitosa -> Suscribir
            if (msg.contains("\"status_code\":200") || msg.contains("\"status\":200")) {
                if (isAcciones) {
                    List<String> wsChunk = chunk.stream().filter(s -> !s.equals("IBEX") && !s.contains(".")).toList();
                    // Acciones en ws/us-quote: enviar en sub-lotes de 5 para respetar límite por mensaje
                    for (int i = 0; i < wsChunk.size(); i += 5) {
                        List<String> sub = wsChunk.subList(i, Math.min(i + 5, wsChunk.size()));
                        String symbolsStr = String.join(",", sub);
                        log.info("📡 [Backend ACCIONES ({}/{})] Suscribiendo {} acciones: {}", chunkNum, totalChunks, sub.size(), symbolsStr);
                        webSocket.sendText(String.format("{\"action\": \"subscribe\", \"symbols\": \"%s\"}", symbolsStr), true);
                    }
                } else {
                    String symbolsStr = String.join(",", chunk);
                    log.info("📡 [Backend {} ({}/{})] Suscribiendo {} activos: {}", categoryName, chunkNum, totalChunks, chunk.size(), symbolsStr);
                    webSocket.sendText(String.format("{\"action\": \"subscribe\", \"symbols\": \"%s\"}", symbolsStr), true);
                }
                return;
            }

            // 2. Manejo de límite por mensaje si ocurre 422
            if (msg.contains("\"status_code\":422")) {
                log.warn("⚠️ [Backend {} ({}/{})] Aviso de límite en suscripción EODHD. Aplicando sub-lotes unitarios.", categoryName, chunkNum, totalChunks);
                for (String sym : chunk) {
                    webSocket.sendText(String.format("{\"action\": \"subscribe\", \"symbols\": \"%s\"}", sym), true);
                }
                return;
            }

            // 3. Procesar Ticks de Precios
            String symbol = extractJsonString(msg, "s");
            if (symbol != null) {
                String cleanSymbol = symbol.replace("/USD", "").replace("/USDT", "").replace("/", "").toUpperCase();

                if (isAcciones) {
                    // Acciones: ap/ask = Venta, bp/bid = Compra
                    String askStr = extractJsonString(msg, "ap");
                    if (askStr == null) askStr = extractJsonString(msg, "ask");
                    String bidStr = extractJsonString(msg, "bp");
                    if (bidStr == null) bidStr = extractJsonString(msg, "bid");
                    String priceStr = extractJsonString(msg, "p");

                    double pAsk = askStr != null ? Double.parseDouble(askStr) : 0.0;
                    double pBid = bidStr != null ? Double.parseDouble(bidStr) : 0.0;
                    double pActual = priceStr != null ? Double.parseDouble(priceStr) : ((pAsk + pBid) > 0 ? (pAsk + pBid) / 2 : (pAsk > 0 ? pAsk : pBid));

                    if (pActual > 0 || pAsk > 0 || pBid > 0) {
                        updatePrices(cleanSymbol, pActual, pBid, pAsk, categoryName);
                    }
                } else {
                    // Forex y Materias: a = Compra, b = Venta
                    String askStr = extractJsonString(msg, "a");
                    String bidStr = extractJsonString(msg, "b");
                    String priceStr = extractJsonString(msg, "p");

                    double pAsk = askStr != null ? Double.parseDouble(askStr) : 0.0;
                    double pBid = bidStr != null ? Double.parseDouble(bidStr) : 0.0;
                    double pActual = priceStr != null ? Double.parseDouble(priceStr) : (pAsk > 0 ? pAsk : pBid);

                    if (pActual > 0 || pAsk > 0 || pBid > 0) {
                        updatePrices(cleanSymbol, pActual, pAsk, pBid, categoryName);
                    }
                }
            }
        } catch (Exception e) {
            log.trace("Error procesando mensaje EOD {}: {}", categoryName, e.getMessage());
        }
    }

    // =========================================================================
    // BINANCE: CRIPTO Y FONDOS EXCLUSIVOS PARA POSICIONES ABIERTAS
    // =========================================================================
    private synchronized void syncBinance(Set<String> targetCrypto) {
        if (targetCrypto.equals(subscribedBinanceSymbols)) {
            return;
        }

        if (binanceSocket != null) {
            try {
                binanceSocket.sendClose(WebSocket.NORMAL_CLOSURE, "Actualizando activos");
            } catch (Exception e) {}
            binanceSocket = null;
        }
        subscribedBinanceSymbols.clear();

        if (targetCrypto.isEmpty()) {
            return;
        }

        log.info("🔌 [Backend Binance] Conectando para cripto/fondos abiertos: {}", targetCrypto);
        subscribedBinanceSymbols.addAll(targetCrypto);

        try {
            httpClient.newWebSocketBuilder()
                    .buildAsync(URI.create(binanceUrl), new WebSocket.Listener() {
                        private final StringBuilder textBuilder = new StringBuilder();

                        @Override
                        public void onOpen(WebSocket webSocket) {
                            binanceSocket = webSocket;
                            log.info("✅ [Backend Binance] Conectado");

                            // Suscribir solo los activos abiertos
                            List<String> streams = new ArrayList<>();
                            for (String sym : targetCrypto) {
                                streams.add("\"" + sym.toLowerCase() + "usdt@ticker\"");
                            }
                            String payload = String.format("{\"method\": \"SUBSCRIBE\", \"params\": [%s], \"id\": 1}", String.join(",", streams));
                            webSocket.sendText(payload, true);
                            WebSocket.Listener.super.onOpen(webSocket);
                        }

                        @Override
                        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                            textBuilder.append(data);
                            if (last) {
                                String msg = textBuilder.toString();
                                textBuilder.setLength(0);
                                handleBinanceMessage(msg);
                            }
                            return WebSocket.Listener.super.onText(webSocket, data, last);
                        }

                        @Override
                        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
                            binanceSocket = null;
                            return WebSocket.Listener.super.onClose(webSocket, statusCode, reason);
                        }

                        @Override
                        public void onError(WebSocket webSocket, Throwable error) {
                            log.error("❌ [Backend Binance] Error: {}", error.getMessage());
                            binanceSocket = null;
                        }
                    });
        } catch (Exception e) {
            log.error("Error conectando a Binance WebSocket", e);
        }
    }

    private void handleBinanceMessage(String message) {
        try {
            String symbol = extractJsonString(message, "s");
            String priceStr = extractJsonString(message, "c"); // Close/Last price -> Compra
            String vwapStr = extractJsonString(message, "w");  // Weighted avg -> Venta

            if (symbol != null && priceStr != null) {
                String cleanSymbol = symbol.replace("USDT", "").toUpperCase();
                double pActual = Double.parseDouble(priceStr);
                double pCompra = pActual;
                double pVenta = vwapStr != null ? Double.parseDouble(vwapStr) : pActual;

                updatePrices(cleanSymbol, pActual, pCompra, pVenta, "CRIPTO");
            }
        } catch (Exception e) {}
    }

    // =========================================================================
    // INYECCIÓN DE PRECIOS PUROS A MEMORIA PARA EL MOTOR DE PnL
    // =========================================================================
    private void updatePrices(String cleanSymbol, double actual, double compra, double venta, String categoria) {
        PriceDto dto = new PriceDto(
                cleanSymbol,
                BigDecimal.valueOf(actual).setScale(6, RoundingMode.HALF_UP),
                BigDecimal.valueOf(compra).setScale(6, RoundingMode.HALF_UP),
                BigDecimal.valueOf(venta).setScale(6, RoundingMode.HALF_UP),
                BigDecimal.ZERO,
                LocalDateTime.now(),
                categoria
        );

        // Guardamos tanto con el símbolo limpio como con "/USD" para asegurar coincidencia exacta con pos.getCompra()
        sseService.updateMarketPrice(cleanSymbol, dto);
        sseService.updateMarketPrice(cleanSymbol + "/USD", dto);
    }

    private void closeSocketList(List<WebSocket> sockets) {
        for (WebSocket ws : sockets) {
            try {
                ws.sendClose(WebSocket.NORMAL_CLOSURE, "Cerrando");
            } catch (Exception e) {}
        }
        sockets.clear();
    }

    private void closeAllSockets() {
        if (binanceSocket != null) {
            try { binanceSocket.sendClose(WebSocket.NORMAL_CLOSURE, "Cerrando todo"); } catch (Exception e) {}
            binanceSocket = null;
        }
        subscribedBinanceSymbols.clear();

        closeSocketList(forexSockets);
        subscribedForexSymbols.clear();

        closeSocketList(materiasSockets);
        subscribedMateriasSymbols.clear();

        closeSocketList(accionesSockets);
        subscribedAccionesSymbols.clear();
    }

    @PreDestroy
    public void cleanup() {
        closeAllSockets();
    }

    // =========================================================================
    // PRECARGA INSTANTÁNEA REST PARA COTIZACIONES DE ACCIONES ABIERTAS
    // =========================================================================
    private void preloadAccionesRest(Set<String> symbols) {
        if (symbols == null || symbols.isEmpty()) return;
        try {
            List<String> list = new ArrayList<>(symbols);
            String first = "IBEX".equals(list.get(0)) ? "IBEX.INDX" : list.get(0);
            List<String> tail = new ArrayList<>();
            for (int i = 1; i < list.size(); i++) {
                tail.add("IBEX".equals(list.get(i)) ? "IBEX.INDX" : list.get(i));
            }
            String rest = tail.isEmpty() ? "" : "&s=" + String.join(",", tail);
            String url = String.format("https://eodhistoricaldata.com/api/real-time/%s?api_token=%s&fmt=json%s",
                    first, eodToken, rest);

            java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .build();

            httpClient.sendAsync(request, java.net.http.HttpResponse.BodyHandlers.ofString())
                    .thenAccept(response -> {
                        if (response.statusCode() == 200) {
                            parseAndInjectRestQuotes(response.body());
                        }
                    })
                    .exceptionally(ex -> {
                        log.trace("Error en preloadAccionesRest: {}", ex.getMessage());
                        return null;
                    });
        } catch (Exception e) {
            log.trace("Error construyendo request preloadAccionesRest: {}", e.getMessage());
        }
    }

    private void parseAndInjectRestQuotes(String body) {
        if (body == null || body.isBlank()) return;
        try {
            // EODHD real-time devuelve un array JSON de objetos o un objeto único
            java.util.regex.Matcher m = java.util.regex.Pattern.compile("\\{[^{}]+?\\}").matcher(body);
            while (m.find()) {
                String obj = m.group();
                String code = extractJsonString(obj, "code");
                if (code == null) continue;
                String clean = code.replace(".US", "").replace(".INDX", "").trim().toUpperCase();
                String closeStr = extractJsonString(obj, "close");
                if (closeStr == null || "null".equalsIgnoreCase(closeStr) || "0".equals(closeStr)) {
                    closeStr = extractJsonString(obj, "previousClose");
                }
                if (closeStr != null) {
                    try {
                        double close = Double.parseDouble(closeStr);
                        if (close > 0) {
                            updatePrices(clean, close, close, close, "ACCIONES");
                        }
                    } catch (NumberFormatException ignored) {}
                }
            }
        } catch (Exception e) {
            log.trace("Error parseando REST quotes: {}", e.getMessage());
        }
    }

    private String extractJsonString(String json, String key) {
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("\"" + key + "\"\\s*:\\s*(?:\"([^\"]+)\"|([^,}]+))").matcher(json);
        if (m.find()) {
            return m.group(1) != null ? m.group(1) : m.group(2).trim();
        }
        return null;
    }
}
