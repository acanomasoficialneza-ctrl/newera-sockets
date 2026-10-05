package com.newera.sockets.controllers;

import com.newera.sockets.services.SseService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
@RequestMapping("/api/v1/stream")
@RequiredArgsConstructor
public class SseController {

    private final SseService sseService;



    // FASE 1: Endpoint exclusivo para Balance
    @GetMapping(value = "/balance/{idUsuario}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamBalance(@PathVariable Integer idUsuario) {
        return sseService.subscribeBalance(idUsuario);
    }

    // FASE 1: Endpoint exclusivo para Mercado (Global)
    @GetMapping(value = "/mercados", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamMercados() {
        return sseService.subscribeMarket();
    }

    // FASE 3: Endpoint exclusivo para Posiciones de un cliente
    @GetMapping(value = "/posiciones/{idUsuario}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamPosiciones(@PathVariable Integer idUsuario) {
        return sseService.subscribePositions(idUsuario);
    }
}
