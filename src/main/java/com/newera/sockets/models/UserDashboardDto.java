package com.newera.sockets.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserDashboardDto {
    private BigDecimal totalDinero;
    private BigDecimal margenLibre;
    private BigDecimal margen;
    private List<ApuestaCliente> posicionesAbiertas;
    private List<PriceDto> preciosMercado; // Mantiene el feed global también
}
