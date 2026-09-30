package com.newera.sockets.repositories;

import com.newera.sockets.models.ApuestaCliente;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface ApuestaClienteRepository extends JpaRepository<ApuestaCliente, Integer> {
    List<ApuestaCliente> findByIdUsuarioAndEstatusCompra(Integer idUsuario, String estatusCompra);
    List<ApuestaCliente> findByEstatusCompra(String estatusCompra);
}
