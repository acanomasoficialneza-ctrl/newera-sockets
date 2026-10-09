package com.newera.sockets.services;

import com.newera.sockets.models.PriceDto;
import jakarta.annotation.PostConstruct;
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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;

@Slf4j
@Service
@RequiredArgsConstructor
public class ExternalMarketSocketService {

    private final SseService sseService;
    private final HttpClient httpClient = HttpClient.newHttpClient();

    private WebSocket binanceSocket;
    private WebSocket eodForexSocket;
    private WebSocket eodMateriasSocket;
    private WebSocket eodUsSocket;
    private final Set<String> subscribedBinanceSymbols = new HashSet<>();
    private final Set<String> subscribedEodForexSymbols = new HashSet<>();
    private final Set<String> subscribedEodMateriasSymbols = new HashSet<>();
    private final Set<String> subscribedEodUsSymbols = new HashSet<>();
    private volatile int lastEodStatus = 0;

    @Value("${app.sockets.binance-url:wss://stream.binance.us:9443/ws}")
    private String binanceUrl;

    @Value("${app.sockets.eod-forex-url:wss://ws.eodhistoricaldata.com/ws/forex}")
    private String eodForexUrl;

    @Value("${app.sockets.eod-us-url:wss://ws.eodhistoricaldata.com/ws/us-quote}")
    private String eodUsUrl;

    @Value("${app.sockets.eod-token:667d8404377b62.46044727}")
    private String eodToken;

    @Value("${app.sockets.binance-enabled:true}")
    private boolean binanceEnabled;

    @Value("${app.sockets.eod-enabled:true}")
    private boolean eodEnabled;

    @PostConstruct
    public void init() {
        if (binanceEnabled) {
            connectBinance();
        } else {
            log.info("Conexión a Binance deshabilitada por configuración");
        }
        
        if (eodEnabled) {
            connectEodForex();
            connectEodMaterias();
            connectEodUs();
        } else {
            log.info("Conexión a EOD deshabilitada por configuración");
        }
    }

    private void connectBinance() {
        try {
            httpClient.newWebSocketBuilder()
                    .buildAsync(URI.create(binanceUrl), new WebSocket.Listener() {
                        StringBuilder textBuilder = new StringBuilder();

                        @Override
                        public void onOpen(WebSocket webSocket) {
                            log.info("Conectado a Binance WebSocket");
                            binanceSocket = webSocket;
                            subscribedBinanceSymbols.clear();
                            WebSocket.Listener.super.onOpen(webSocket);
                        }

                        @Override
                        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                            textBuilder.append(data);
                            if (last) {
                                processBinanceMessage(textBuilder.toString());
                                textBuilder.setLength(0);
                            }
                            return WebSocket.Listener.super.onText(webSocket, data, last);
                        }

                        @Override
                        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
                            log.warn("Binance WebSocket cerrado. Reconectando...");
                            binanceSocket = null;
                            connectBinance();
                            return WebSocket.Listener.super.onClose(webSocket, statusCode, reason);
                        }

                        @Override
                        public void onError(WebSocket webSocket, Throwable error) {
                            log.error("Error en Binance WebSocket", error);
                            binanceSocket = null;
                        }
                    }).join();
        } catch (Exception e) {
            log.error("Fallo al conectar con Binance", e);
        }
    }

    private void connectEodForex() {
        try {
            httpClient.newWebSocketBuilder()
                    .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
                    .buildAsync(URI.create(eodForexUrl + "?api_token=" + eodToken), new WebSocket.Listener() {
                        StringBuilder textBuilder = new StringBuilder();

                        @Override
                        public void onOpen(WebSocket webSocket) {
                            log.info("Conectado a EOD Forex WebSocket");
                            eodForexSocket = webSocket;
                            subscribedEodForexSymbols.clear();
                            WebSocket.Listener.super.onOpen(webSocket);
                        }

                        @Override
                        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                            textBuilder.append(data);
                            if (last) {
                                String msg = textBuilder.toString();
                                if (msg.contains("\"status_code\"") || msg.contains("\"status\"") || msg.contains("\"message\"")) {
                                    log.info("📡 EOD Forex Respuesta: {}", msg);
                                }
                                processEodMessage(msg);
                                textBuilder.setLength(0);
                            }
                            return WebSocket.Listener.super.onText(webSocket, data, last);
                        }

                        @Override
                        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
                            log.warn("EOD Forex WebSocket cerrado. Reconectando...");
                            eodForexSocket = null;
                            connectEodForex();
                            return WebSocket.Listener.super.onClose(webSocket, statusCode, reason);
                        }

                        @Override
                        public void onError(WebSocket webSocket, Throwable error) {
                            log.error("Error en EOD Forex WebSocket", error);
                            eodForexSocket = null;
                        }
                    }).join();
        } catch (Exception e) {
            log.error("Fallo al conectar con EOD Forex", e);
        }
    }

    private void connectEodMaterias() {
        try {
            httpClient.newWebSocketBuilder()
                    .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
                    .buildAsync(URI.create(eodForexUrl + "?api_token=" + eodToken), new WebSocket.Listener() {
                        StringBuilder textBuilder = new StringBuilder();

                        @Override
                        public void onOpen(WebSocket webSocket) {
                            log.info("Conectado a EOD Materias WebSocket");
                            eodMateriasSocket = webSocket;
                            subscribedEodMateriasSymbols.clear();
                            WebSocket.Listener.super.onOpen(webSocket);
                        }

                        @Override
                        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                            textBuilder.append(data);
                            if (last) {
                                String msg = textBuilder.toString();
                                if (msg.contains("\"status_code\"") || msg.contains("\"status\"") || msg.contains("\"message\"")) {
                                    log.info("📡 EOD Materias Respuesta: {}", msg);
                                }
                                processEodMessage(msg);
                                textBuilder.setLength(0);
                            }
                            return WebSocket.Listener.super.onText(webSocket, data, last);
                        }

                        @Override
                        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
                            log.warn("EOD Materias WebSocket cerrado. Reconectando...");
                            eodMateriasSocket = null;
                            connectEodMaterias();
                            return WebSocket.Listener.super.onClose(webSocket, statusCode, reason);
                        }

                        @Override
                        public void onError(WebSocket webSocket, Throwable error) {
                            log.error("Error en EOD Materias WebSocket", error);
                            eodMateriasSocket = null;
                        }
                    }).join();
        } catch (Exception e) {
            log.error("Fallo al conectar con EOD Materias", e);
        }
    }

    private void connectEodUs() {
        try {
            httpClient.newWebSocketBuilder()
                    .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
                    .buildAsync(URI.create(eodUsUrl + "?api_token=" + eodToken), new WebSocket.Listener() {
                        StringBuilder textBuilder = new StringBuilder();

                        @Override
                        public void onOpen(WebSocket webSocket) {
                            log.info("Conectado a EOD US Quotes WebSocket");
                            eodUsSocket = webSocket;
                            subscribedEodUsSymbols.clear();
                            WebSocket.Listener.super.onOpen(webSocket);
                        }

                        @Override
                        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                            textBuilder.append(data);
                            if (last) {
                                String msg = textBuilder.toString();
                                if (msg.contains("\"status_code\"") || msg.contains("\"status\"") || msg.contains("\"message\"")) {
                                    log.info("📡 EOD US Quotes Respuesta: {}", msg);
                                }
                                processEodMessage(msg);
                                textBuilder.setLength(0);
                            }
                            return WebSocket.Listener.super.onText(webSocket, data, last);
                        }

                        @Override
                        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
                            log.warn("EOD US WebSocket cerrado. Reconectando...");
                            eodUsSocket = null;
                            connectEodUs();
                            return WebSocket.Listener.super.onClose(webSocket, statusCode, reason);
                        }

                        @Override
                        public void onError(WebSocket webSocket, Throwable error) {
                            log.error("Error en EOD US WebSocket", error);
                            eodUsSocket = null;
                        }
                    }).join();
        } catch (Exception e) {
            log.error("Fallo al conectar con EOD US", e);
        }
    }

    private void processBinanceMessage(String message) {
        try {
            String symbol = extractJsonString(message, "s");
            String priceStr = extractJsonString(message, "c"); // Last price
            String askStr = extractJsonString(message, "a");   // Best ask price
            String bidStr = extractJsonString(message, "b");   // Best bid price
            String percentStr = extractJsonString(message, "P");

            if (symbol != null && priceStr != null) {
                symbol = symbol.replace("USDT", "").toUpperCase();
                double currentPrice = Double.parseDouble(priceStr);
                double priceChangePercent = percentStr != null ? Double.parseDouble(percentStr) : 0.0;
                
                double askPrice = askStr != null ? Double.parseDouble(askStr) : currentPrice;
                double bidPrice = bidStr != null ? Double.parseDouble(bidStr) : currentPrice;

                PriceDto existing = sseService.getMarketPrices().get(symbol);
                if (existing != null) {
                    existing.setPrecioActual(BigDecimal.valueOf(currentPrice).setScale(6, RoundingMode.HALF_UP));
                    existing.setPrecioCompra(BigDecimal.valueOf(askPrice).setScale(6, RoundingMode.HALF_UP));
                    existing.setPrecioVenta(BigDecimal.valueOf(bidPrice).setScale(6, RoundingMode.HALF_UP));
                    existing.setVariacionPorcentaje(BigDecimal.valueOf(priceChangePercent).setScale(6, RoundingMode.HALF_UP));
                    existing.setTimestamp(LocalDateTime.now());
                }
            }
        } catch (Exception e) {
            log.trace("Error parseando mensaje Binance: {}", message);
        }
    }

    private void processEodMessage(String message) {
        try {
            if (message.contains("\"status_code\":422")) {
                lastEodStatus = 422;
            }

            if (message.contains("\"status\":401") || message.contains("\"status\":403") || 
                message.contains("\"status_code\":401") || message.contains("\"status_code\":403")) {
                
                log.error("⚠️ EOD API TOKEN EXPIRADO O RECHAZADO: {}", message);
                
                try {
                    String jsonPayload = "{\"nivel\": \"CRITICAL\", \"mensaje\": \"INFRA: API de Mercados (EOD) rechazada (Error 401/403). Límite mensual excedido o token inválido. Requiere pago/renovación inmediata.\", \"ip\": \"Sockets\"}";
                    java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder()
                            .uri(java.net.URI.create("http://localhost:8080/api/v1/auditoria"))
                            .header("Content-Type", "application/json")
                            .POST(java.net.http.HttpRequest.BodyPublishers.ofString(jsonPayload))
                            .build();
                    httpClient.sendAsync(request, java.net.http.HttpResponse.BodyHandlers.discarding());
                } catch (Exception ex) {
                    log.error("No se pudo enviar la alerta crítica a negocio-service", ex);
                }
                return;
            }

            String symbol = extractJsonString(message, "s");
            String priceStr = extractJsonString(message, "p");
            String askStr = extractJsonString(message, "a");
            String bidStr = extractJsonString(message, "b");
            
            // ACCIONES usan 'ap', 'bp', 'ask', 'bid'
            if (askStr == null) askStr = extractJsonString(message, "ap");
            if (askStr == null) askStr = extractJsonString(message, "ask");
            if (bidStr == null) bidStr = extractJsonString(message, "bp");
            if (bidStr == null) bidStr = extractJsonString(message, "bid");

            if (symbol != null) {
                String cleanSymbol = symbol.split("\\.")[0].toUpperCase();
                
                // Si el precio principal ('p') no viene, intentamos sacarlo de 'a' o 'b'
                if (priceStr == null && askStr != null) priceStr = askStr;
                if (priceStr == null && bidStr != null) priceStr = bidStr;
                
                if (priceStr != null) {
                    double currentPrice = Double.parseDouble(priceStr);
                    double askPrice = askStr != null ? Double.parseDouble(askStr) : currentPrice;
                    double bidPrice = bidStr != null ? Double.parseDouble(bidStr) : currentPrice;
                    
                    PriceDto existing = sseService.getMarketPrices().get(cleanSymbol);
                    if (existing != null) {
                        existing.setPrecioActual(BigDecimal.valueOf(currentPrice).setScale(6, RoundingMode.HALF_UP));
                        existing.setPrecioCompra(BigDecimal.valueOf(askPrice).setScale(6, RoundingMode.HALF_UP));
                        existing.setPrecioVenta(BigDecimal.valueOf(bidPrice).setScale(6, RoundingMode.HALF_UP));
                        existing.setTimestamp(LocalDateTime.now());
                    }
                }
            }
        } catch (Exception e) {
            log.trace("Error parseando mensaje EOD: {}", message);
        }
    }

    private String extractJsonString(String json, String key) {
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("\"" + key + "\"\\s*:\\s*(?:\"([^\"]+)\"|([^,}]+))").matcher(json);
        if (m.find()) {
            return m.group(1) != null ? m.group(1) : m.group(2).trim();
        }
        return null;
    }

    private volatile boolean forexLimitReached = false;
    private volatile boolean materiasLimitReached = false;
    private volatile boolean usLimitReached = false;

    @Scheduled(fixedRate = 25000)
    public void rotateConnections() {
        if (forexLimitReached) {
            log.info("🔄 Rotando conexion EOD Forex para dar turno a activos faltantes...");
            if (eodForexSocket != null) eodForexSocket.sendClose(1000, "Rotacion Forex");
        }
        if (materiasLimitReached) {
            log.info("🔄 Rotando conexion EOD Materias para dar turno a activos faltantes...");
            if (eodMateriasSocket != null) eodMateriasSocket.sendClose(1000, "Rotacion Materias");
        }
        if (usLimitReached) {
            log.info("🔄 Rotando conexion EOD US Quotes para dar turno a activos faltantes...");
            if (eodUsSocket != null) eodUsSocket.sendClose(1000, "Rotacion US Quotes");
        }
    }

    @Scheduled(fixedRate = 5000)
    public void syncSubscriptions() {
        Map<String, PriceDto> currentPrices = sseService.getMarketPrices();

        Set<String> newBinanceSymbols = new HashSet<>();
        Set<String> newEodForexSymbols = new HashSet<>();
        Set<String> newEodMateriasSymbols = new HashSet<>();
        Set<String> newEodUsSymbols = new HashSet<>();

        for (Map.Entry<String, PriceDto> entry : currentPrices.entrySet()) {
            String symbol = entry.getKey();
            String cat = entry.getValue().getCategoria();
            if (cat == null) cat = "CRIPTO";

            if ("CRIPTO".equals(cat) || "FONDOS".equals(cat)) {
                String binanceSymbol = symbol.toLowerCase() + "usdt@ticker";
                if (!subscribedBinanceSymbols.contains(binanceSymbol)) {
                    newBinanceSymbols.add(binanceSymbol);
                    subscribedBinanceSymbols.add(binanceSymbol);
                }
            } else if ("ACCIONES".equals(cat)) {
                if (!subscribedEodUsSymbols.contains(symbol)) {
                    newEodUsSymbols.add(symbol);
                    subscribedEodUsSymbols.add(symbol);
                }
            } else if ("MATERIAS".equals(cat)) { 
                if (!subscribedEodMateriasSymbols.contains(symbol)) {
                    newEodMateriasSymbols.add(symbol);
                    subscribedEodMateriasSymbols.add(symbol);
                }
            } else { // DIVISA
                if (!subscribedEodForexSymbols.contains(symbol)) {
                    newEodForexSymbols.add(symbol);
                    subscribedEodForexSymbols.add(symbol);
                }
            }
        }

        if (binanceSocket != null && !newBinanceSymbols.isEmpty()) {
            try {
                StringBuilder params = new StringBuilder("[");
                int i = 0;
                for (String s : newBinanceSymbols) {
                    params.append("\"").append(s).append("\"");
                    if (++i < newBinanceSymbols.size()) params.append(",");
                }
                params.append("]");
                String subscribeMsg = String.format("{\"method\": \"SUBSCRIBE\", \"params\": %s, \"id\": 1}", params.toString());
                binanceSocket.sendText(subscribeMsg, true);
                log.info("Binance suscrito a: {}", newBinanceSymbols);
            } catch (Exception e) {
                log.error("Error enviando suscripcion a Binance", e);
            }
        }

        if (eodForexSocket != null && !newEodForexSymbols.isEmpty()) {
            try {
                java.util.List<String> list = new java.util.ArrayList<>(newEodForexSymbols);
                java.util.Collections.shuffle(list);
                forexLimitReached = sendInBatches(eodForexSocket, list, "EOD Forex");
            } catch (Exception e) {
                log.error("Error enviando suscripcion a EOD Forex", e);
            }
        }
        
        if (eodMateriasSocket != null && !newEodMateriasSymbols.isEmpty()) {
            try {
                java.util.List<String> list = new java.util.ArrayList<>(newEodMateriasSymbols);
                java.util.Collections.shuffle(list);
                materiasLimitReached = sendInBatches(eodMateriasSocket, list, "EOD Materias");
            } catch (Exception e) {
                log.error("Error enviando suscripcion a EOD Materias", e);
            }
        }

        if (eodUsSocket != null && !newEodUsSymbols.isEmpty()) {
            try {
                java.util.List<String> list = new java.util.ArrayList<>(newEodUsSymbols);
                java.util.Collections.shuffle(list);
                usLimitReached = sendInBatches(eodUsSocket, list, "EOD US Quote");
            } catch (Exception e) {
                log.error("Error enviando suscripcion a EOD US", e);
            }
        }
    }

    private boolean sendInBatches(WebSocket socket, java.util.List<String> symbols, String name) {
        int batchSize = 5;
        int maxSymbols = symbols.size(); // Intentamos mandar TODOS hasta que EODHD nos detenga
        int i = 0;
        
        while (i < maxSymbols) {
            java.util.List<String> batch = symbols.subList(i, Math.min(i + batchSize, maxSymbols));
            String symbolsStr = String.join(",", batch);
            String subscribeMsg = String.format("{\"action\": \"subscribe\", \"symbols\": \"%s\"}", symbolsStr);
            
            lastEodStatus = 0; // reset
            socket.sendText(subscribeMsg, true);
            log.info("{} probando lote de {}: {}", name, batch.size(), batch);
            
            // Esperamos hasta 600ms a ver si EODHD escupe un 422
            long start = System.currentTimeMillis();
            while (lastEodStatus == 0 && (System.currentTimeMillis() - start) < 600) {
                try { Thread.sleep(50); } catch (InterruptedException e) {}
            }
            
            if (lastEodStatus == 422) {
                if (batchSize > 1) {
                    batchSize = 1;
                    log.warn("{} limite 422 detectado con lote, reintentando de 1 en 1 (Estilo Alpha Edge)", name);
                } else {
                    log.warn("{} limite ABSOLUTO de token alcanzado en simbolo {}. Deteniendo suscripciones de este mercado.", name, batch);
                    return true; // Se acabo el token para este mercado
                }
            } else {
                log.info("{} lote aceptado exitosamente", name);
                i += batch.size();
                try { Thread.sleep(200); } catch (InterruptedException e) {}
            }
        }
        return false; // Se suscribió a todo con éxito sin alcanzar el límite
    }
}
