package com.newera.sockets.models;

import jakarta.persistence.*;
import lombok.Data;
import java.math.BigDecimal;

@Data
@Entity
@Table(name = "perfil_cliente")
public class UsuarioParcial {

    @Id
    @Column(name = "id_usuario")
    private Integer idUsuario;

    @Column(name = "balance")
    private BigDecimal totalDinero;

    @Column(name = "margen_libre")
    private BigDecimal margenLibre;

    @Column(name = "margen_utilizado")
    private BigDecimal margen;
}
