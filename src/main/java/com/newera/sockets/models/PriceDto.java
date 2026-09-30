package com.newera.sockets.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PriceDto {
    private String simbolo;
    private BigDecimal precioActual;
    private BigDecimal precioCompra;
    private BigDecimal precioVenta;
    private BigDecimal variacionPorcentaje;
    private LocalDateTime timestamp;
    private String categoria;
}
