package com.newera.sockets.repositories;

import com.newera.sockets.models.UsuarioParcial;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface UsuarioParcialRepository extends JpaRepository<UsuarioParcial, Integer> {
}
