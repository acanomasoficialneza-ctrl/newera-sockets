package com.newera.sockets.services;

import jakarta.annotation.PostConstruct;

import com.newera.sockets.models.ApuestaCliente;
import com.newera.sockets.models.PositionUpdateDto;
import com.newera.sockets.models.PriceDto;
import com.newera.sockets.models.UserDashboardDto;
import com.newera.sockets.models.UsuarioParcial;
import com.newera.sockets.models.BalanceDto;
import com.newera.sockets.repositories.ApuestaClienteRepository;
import com.newera.sockets.repositories.UsuarioParcialRepository;
import lombok.RequiredArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

@Slf4j
@Service
@RequiredArgsConstructor
public class SseService {

    // Emitters (Legado eliminado)

    // Nuevos Emitters Fase 1 y 2 y 3
    private final Map<Integer, SseEmitter> balanceEmitters = new ConcurrentHashMap<>();
    private final Map<Integer, Set<SseEmitter>> positionsEmitters = new ConcurrentHashMap<>();
    
    // RAM Caches (Evita colapsar la BD)
    private final Map<Integer, List<ApuestaCliente>> userPositionsCache = new ConcurrentHashMap<>();
    private final Map<Integer, UsuarioParcial> userCache = new ConcurrentHashMap<>();
    
    private final Random random = new Random();
    
    private final UsuarioParcialRepository usuarioRepository;
    private final ApuestaClienteRepository apuestaRepository;

    @Data
    @AllArgsConstructor
    public static class LeverageRule {
        private double gana1;
        private double gana2;
        private double pierde1;
        private double pierde2;
    }

    // Reglas de Apalancamiento Duras (Sin BD)
    private final Map<String, LeverageRule> leverageRules = Map.of(
        "CRIPTO", new LeverageRule(0.2, 0.2, 3.0, 3.0),
        "MATERIAS", new LeverageRule(0.8, 0.8, 3.0, 3.0),
        "ACCIONES", new LeverageRule(0.8, 0.8, 3.0, 3.0),
        "FONDOS", new LeverageRule(0.8, 0.8, 3.0, 3.0),
        "DIVISA", new LeverageRule(1.2, 0.8, 1.2, 0.8)
    );

    // Memoria de Precios Globales (Exclusivamente alimentada por sockets de posiciones abiertas)
    private final Map<String, PriceDto> marketPrices = new ConcurrentHashMap<>();





    // Suscripción de Balance
    public SseEmitter subscribeBalance(Integer idUsuario) {
        SseEmitter emitter = new SseEmitter(3600000L); // 1 hora timeout
        balanceEmitters.put(idUsuario, emitter);
        forceSyncUserFromDB(idUsuario);
        
        emitter.onCompletion(() -> balanceEmitters.remove(idUsuario));
        emitter.onTimeout(() -> balanceEmitters.remove(idUsuario));
        emitter.onError((e) -> balanceEmitters.remove(idUsuario));
        
        return emitter;
    }

    // FASE 3: Suscripción a Posiciones de un Usuario Específico
    public SseEmitter subscribePositions(Integer idUsuario) {
        SseEmitter emitter = new SseEmitter(3600000L);
        positionsEmitters.computeIfAbsent(idUsuario, k -> new CopyOnWriteArraySet<>()).add(emitter);
        forceSyncUserFromDB(idUsuario);
        
        Runnable cleanup = () -> {
            Set<SseEmitter> set = positionsEmitters.get(idUsuario);
            if (set != null) {
                set.remove(emitter);
                if (set.isEmpty()) positionsEmitters.remove(idUsuario);
            }
        };

        emitter.onCompletion(cleanup);
        emitter.onTimeout(cleanup);
        emitter.onError(e -> cleanup.run());
        
        return emitter;
    }

    private void cleanupUser(Integer idUsuario) {
        if (!balanceEmitters.containsKey(idUsuario) && !positionsEmitters.containsKey(idUsuario)) {
            userPositionsCache.remove(idUsuario);
            userCache.remove(idUsuario);
        }
    }

    private void forceSyncUserFromDB(Integer idUsuario) {
        Optional<UsuarioParcial> optUsuario = usuarioRepository.findById(idUsuario);
        if (optUsuario.isPresent()) {
            userCache.put(idUsuario, optUsuario.get());
            List<ApuestaCliente> posicionesAbiertas = apuestaRepository.findByIdUsuarioAndEstatusCompra(idUsuario, "ABIERTO");
            userPositionsCache.put(idUsuario, posicionesAbiertas);
        }
    }

    // Exponer precios al servicio externo
    public Map<String, PriceDto> getMarketPrices() {
        return marketPrices;
    }

    // Método para que el servicio externo inyecte el precio real
    public void updateMarketPrice(String symbol, PriceDto realPrice) {
        marketPrices.put(symbol, realPrice);
    }

    // Sincroniza desde la BD a la RAM CADA 1 SEGUNDO (Solo usuarios siendo observados)
    // Para entregar la última información de la BD al socket de balance
    @Scheduled(fixedRate = 1000)
    public void syncActiveUsersFromDB() {
        Set<Integer> activeUsers = new HashSet<>();
        activeUsers.addAll(balanceEmitters.keySet());
        activeUsers.addAll(positionsEmitters.keySet());
        
        for (Integer idUsuario : activeUsers) {
            forceSyncUserFromDB(idUsuario);
        }
    }

    // Variable para controlar el guardado en BD y no saturarla cada segundo
    private int tickCount = 0;

    // Tarea (1 segundo): Emite el P&L en Memoria y Balance crudo
    @Scheduled(fixedRate = 1000)
    public void emitirDatosEnVivo() {
        tickCount++;
        boolean shouldSaveToDb = (tickCount % 10 == 0); // Guarda en BD cada 5 segundos (10 * 500ms)

        if (balanceEmitters.isEmpty() && positionsEmitters.isEmpty()) return;

        // --- BROADCAST BALANCE ---
        for (Map.Entry<Integer, SseEmitter> entry : balanceEmitters.entrySet()) {
            Integer idUsuario = entry.getKey();
            SseEmitter emitter = entry.getValue();
            
            UsuarioParcial usuario = userCache.get(idUsuario);
            if (usuario != null) {
                // Se envía el balance puramente de la base de datos
                BalanceDto balance = new BalanceDto(usuario.getTotalDinero(), usuario.getMargenLibre(), usuario.getMargen());
                
                try {
                    emitter.send(SseEmitter.event().name("balance-update").data(balance));
                } catch (IOException e) {
                    balanceEmitters.remove(idUsuario);
                }
            }
        }

        // --- BROADCAST POSICIONES (Solo para los que las tienen abiertas en pantalla) ---
        for (Map.Entry<Integer, Set<SseEmitter>> entry : positionsEmitters.entrySet()) {
            Integer idUsuario = entry.getKey();
            Set<SseEmitter> emitters = entry.getValue();
            
            List<ApuestaCliente> posiciones = userPositionsCache.get(idUsuario);
            if (posiciones != null && !emitters.isEmpty()) {
                // Hacemos el cálculo de memoria para enviar la foto instantánea fresca SÓLO DE LO ESENCIAL (Optimizacion Visor)
                List<PositionUpdateDto> actualizaciones = new ArrayList<>();
                for (ApuestaCliente pos : posiciones) {
                    double pnl = calcularPnL(pos);
                    pos.setGananciaPerdida(BigDecimal.valueOf(pnl).setScale(2, RoundingMode.HALF_UP));
                    actualizaciones.add(new PositionUpdateDto(pos.getIdApuestaCliente(), pos.getGananciaPerdida()));
                }
                
                List<SseEmitter> deadEmitters = new ArrayList<>();
                for (SseEmitter emitter : emitters) {
                    try {
                        emitter.send(SseEmitter.event().name("positions-update").data(actualizaciones));
                    } catch (IOException e) {
                        deadEmitters.add(emitter);
                    }
                }
                emitters.removeAll(deadEmitters);
                if (emitters.isEmpty()) positionsEmitters.remove(idUsuario);
            }
        }
    }

    private int alphaTickCount = 0;

    // ============================================
    // FASE 3: MOTOR CENTRAL ALPHA (Corre cada 3 segundos)
    // ============================================
    @Scheduled(fixedRate = 3000)
    public void motorAlphaGlobal() {
        try {
            // Obtenemos TODAS las posiciones ABIERTAS del sistema, sin importar si el usuario está conectado
            List<ApuestaCliente> posicionesAbiertas = apuestaRepository.findByEstatusCompra("ABIERTO");
            
            for (ApuestaCliente pos : posicionesAbiertas) {
                // Calcula el PnL con el último precio conocido en memoria
                double pnl = calcularPnL(pos);
                pos.setGananciaPerdida(BigDecimal.valueOf(pnl).setScale(2, RoundingMode.HALF_UP));
                
                // Actualiza el registro en Base de Datos (Mantiene la BD fresca)
                apuestaRepository.save(pos);
                
                // Además, si el usuario está en RAM, actualizamos su posición en memoria para que no tenga que esperar a la sincronización de 5 seg
                if (pos.getIdUsuario() != null) {
                    List<ApuestaCliente> memPos = userPositionsCache.get(pos.getIdUsuario());
                    if (memPos != null) {
                        for (int i = 0; i < memPos.size(); i++) {
                            if (memPos.get(i).getIdApuestaCliente().equals(pos.getIdApuestaCliente())) {
                                memPos.set(i, pos);
                                break;
                            }
                        }
                    }
                }
            }
        } catch(Exception e) {
            log.error("Error en Motor Central Alpha", e);
        }
    }

    // Motor Matemático P&L en Memoria (Puro 1:1, sin números inventados)
    private double calcularPnL(ApuestaCliente pos) {
        String rawSimbolo = pos.getCompra() != null ? pos.getCompra().trim().toUpperCase() : "UNKNOWN"; 
        String cleanSimbolo = rawSimbolo.replace("/USD", "").replace("/USDT", "").replace("/", "");
        
        PriceDto price = marketPrices.get(cleanSimbolo);
        if (price == null) {
            price = marketPrices.get(rawSimbolo);
        }

        // Si aún no ha llegado el primer tick del socket para este activo, usamos su valorUnidad de entrada (PnL = 0)
        double openPrice = pos.getValorUnidad() != null && pos.getValorUnidad().doubleValue() > 0 
            ? pos.getValorUnidad().doubleValue() 
            : 0.0;

        if (price == null) {
            return 0.0;
        }
        
        // Evaluamos usando precioCompra para apuestas a la COMPRA, y precioVenta para apuestas a la VENTA
        double pCompra = price.getPrecioCompra() != null ? price.getPrecioCompra().doubleValue() : (price.getPrecioActual() != null ? price.getPrecioActual().doubleValue() : openPrice);
        double pVenta = price.getPrecioVenta() != null ? price.getPrecioVenta().doubleValue() : (price.getPrecioActual() != null ? price.getPrecioActual().doubleValue() : openPrice);
        
        if (openPrice == 0) return 0.0;

        double investment = pos.getMontoApuesta() != null ? pos.getMontoApuesta().doubleValue() : 0.0;
        String categoria = pos.getCategoria() != null ? pos.getCategoria().toUpperCase() : "CRIPTO";
        String tipoCompra = pos.getTipoCompra(); 

        LeverageRule rule = leverageRules.getOrDefault(categoria, new LeverageRule(1.0, 1.0, 1.0, 1.0));

        double calculo1 = 0;
        double calculo2 = 0;
        double calculo3 = 0;
        double calculo4 = 0;

        if ("COMPRA".equalsIgnoreCase(tipoCompra) || "COMPRAR".equalsIgnoreCase(tipoCompra)) {
            if (pCompra > openPrice) {
                // GANA
                calculo1 = investment * rule.getGana1();
                calculo2 = calculo1 / openPrice;
                calculo3 = pCompra - openPrice;
                
                if ("DIVISA".equalsIgnoreCase(categoria) && (rawSimbolo.contains("USD"))) {
                    calculo4 = (calculo3 * calculo2) * rule.getGana2();
                } else {
                    calculo4 = calculo3 * calculo2;
                }
            } else {
                // PIERDE
                calculo1 = investment * rule.getPierde1();
                calculo2 = calculo1 / openPrice;
                calculo3 = pCompra - openPrice; // Valor Negativo natural
                
                if ("DIVISA".equalsIgnoreCase(categoria) && (rawSimbolo.contains("USD"))) {
                    calculo4 = (calculo3 * calculo2) * rule.getPierde2();
                } else {
                    calculo4 = calculo3 * calculo2;
                }
            }
        } else if ("VENTA".equalsIgnoreCase(tipoCompra) || "VENDER".equalsIgnoreCase(tipoCompra)) {
            if (pVenta < openPrice) {
                // GANA (venta en corto)
                calculo1 = investment * rule.getGana1();
                calculo2 = calculo1 / openPrice;
                calculo3 = pVenta - openPrice; 
                calculo4 = -(calculo3 * calculo2); 
                
                if ("DIVISA".equalsIgnoreCase(categoria) && (rawSimbolo.contains("USD"))) {
                    calculo4 = -(calculo3 * calculo2) * rule.getGana2();
                } 
            } else {
                // PIERDE (venta en corto)
                calculo1 = investment * rule.getPierde1();
                calculo2 = calculo1 / openPrice;
                calculo3 = pVenta - openPrice; 
                calculo4 = -(calculo3 * calculo2);
                
                if ("DIVISA".equalsIgnoreCase(categoria) && (rawSimbolo.contains("USD"))) {
                    calculo4 = -(calculo3 * calculo2) * rule.getPierde2();
                } 
            }
        }

        return calculo4;
    }
}
