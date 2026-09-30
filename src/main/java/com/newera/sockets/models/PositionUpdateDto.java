package com.newera.sockets.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PositionUpdateDto {
    private Integer id;
    private BigDecimal pnl;
}
