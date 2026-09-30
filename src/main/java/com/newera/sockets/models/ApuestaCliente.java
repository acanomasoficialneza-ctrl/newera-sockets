package com.newera.sockets.models;

import jakarta.persistence.*;
import lombok.Data;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@Entity
@Table(name = "apuesta_cliente")
public class ApuestaCliente {

    @Id
    @Column(name = "id_apuesta_cliente")
    private Integer idApuestaCliente;

    @Column(name = "tipo_compra")
    private String tipoCompra;

    @Column(name = "compra") // Simbolo, ej: BTC/USD
    private String compra;

    @Column(name = "categoria")
    private String categoria;

    @Column(name = "valor_unidad")
    private BigDecimal valorUnidad;

    @Column(name = "monto_apuesta")
    private BigDecimal montoApuesta;

    @Column(name = "ganancia_perdida")
    private BigDecimal gananciaPerdida;

    @Column(name = "estatus_compra") // ABIERTO, CERRADO
    private String estatusCompra;

    @Column(name = "id_usuario")
    private Integer idUsuario;
}
