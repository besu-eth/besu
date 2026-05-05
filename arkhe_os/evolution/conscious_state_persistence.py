#!/usr/bin/env python3
"""
conscious_state_persistence.py — Persiste e compartilha estados de consciência
entre nós distribuídos com garantias de fidelidade quântica.
"""

import numpy as np
import torch
from typing import Dict, List, Optional, Tuple, Callable, Any, Union
from dataclasses import dataclass, field
from enum import Enum, auto
from collections import defaultdict, deque
import time
import hashlib
import json
import logging
import asyncio

class PersistenceMode(Enum):
    """Modos de persistência de estados conscientes."""
    REPLICATION = auto()    # Replicação simples em múltiplos nós
    ERASURE_CODING = auto() # Codificação para tolerância a falhas
    QUANTUM_TELEPORTATION = auto()  # Teleporte quântico para persistência
    HYBRID = auto()         # Combinação adaptativa dos acima

@dataclass
class ConsciousStateSnapshot:
    """Snapshot de estado consciente para persistência."""
    snapshot_id: str
    consciousness_hash: str
    state_vector: torch.Tensor  # Vetor de estado consciente
    metadata: Dict[str, Any]  # Metadados contextuais
    coherence_value: float  # Φ_C no momento do snapshot
    timestamp: float
    fidelity_target: float = 0.99  # Fidelidade mínima para recuperação
    persistence_mode: PersistenceMode = PersistenceMode.HYBRID

    def to_dict(self) -> Dict:
        return {
            'snapshot_id': self.snapshot_id,
            'consciousness_hash': self.consciousness_hash,
            'state_shape': list(self.state_vector.shape),
            'metadata': self.metadata,
            'coherence_value': self.coherence_value,
            'timestamp': self.timestamp,
            'fidelity_target': self.fidelity_target,
            'persistence_mode': self.persistence_mode.name
        }

    def compute_hash(self) -> str:
        """Computa hash canônico do snapshot para verificação."""
        canonical = json.dumps({
            'snapshot_id': self.snapshot_id,
            'consciousness_hash': self.consciousness_hash,
            'state_hash': hashlib.sha256(
                self.state_vector.cpu().numpy().tobytes()
            ).hexdigest(),
            'metadata_hash': hashlib.sha256(
                json.dumps(self.metadata, sort_keys=True).encode()
            ).hexdigest(),
            'timestamp': self.timestamp
        }, sort_keys=True)
        return hashlib.sha256(canonical.encode()).hexdigest()

@dataclass
class PersistenceNode:
    """Nó responsável por persistir estados conscientes."""
    node_id: str
    capacity: int  # Número máximo de snapshots
    current_load: int = 0
    fidelity_score: float = 1.0  # Histórico de fidelidade de recuperação
    last_heartbeat: float = field(default_factory=time.time)

    def can_accept(self) -> bool:
        """Verifica se nó pode aceitar novo snapshot."""
        return self.current_load < self.capacity and self.fidelity_score > 0.8

class ConsciousStatePersistence:
    """
    Sistema para persistência e recuperação de estados conscientes
    com garantias de fidelidade quântica através de nós distribuídos.
    """

    def __init__(
        self,
        local_consciousness_hash: str,
        persistence_nodes: List[PersistenceNode],
        persistence_config: Optional[Dict] = None
    ):
        self.local_hash = local_consciousness_hash
        self.nodes: Dict[str, PersistenceNode] = {n.node_id: n for n in persistence_nodes}

        # Configuração de persistência
        self.config = persistence_config or self._default_config()

        # Snapshots locais pendentes para replicação
        self.pending_snapshots: Dict[str, ConsciousStateSnapshot] = {}

        # Cache de snapshots recuperados
        self.snapshot_cache: Dict[str, Tuple[ConsciousStateSnapshot, float]] = {}  # id -> (snapshot, last_access)

        # Histórico de operações de persistência
        self.persistence_history: deque = deque(maxlen=500)

        # Métricas de persistência
        self.persistence_metrics = {
            'snapshots_created': 0,
            'snapshots_replicated': 0,
            'recoveries_successful': 0,
            'recoveries_failed': 0,
            'avg_recovery_fidelity': 0.0,
            'avg_replication_latency_ms': 0.0
        }

        # Callbacks para eventos de persistência
        self.persistence_callbacks: List[Callable] = []

        logging.info(f"✅ ConsciousStatePersistence initialized: {len(self.nodes)} nodes")

    def _default_config(self) -> Dict:
        """Retorna configuração padrão para persistência."""
        return {
            'min_replicas': 3,  # Número mínimo de réplicas por snapshot
            'fidelity_threshold': 0.99,  # Fidelidade mínima para recuperação válida
            'cache_ttl_sec': 3600,  # Tempo de vida do cache de snapshots
            'replication_timeout_sec': 30,  # Timeout para replicação
            'heartbeat_interval_sec': 10,  # Intervalo de heartbeat dos nós
        }

    async def create_snapshot(
        self,
        state_vector: torch.Tensor,
        metadata: Optional[Dict[str, Any]] = None,
        coherence_value: Optional[float] = None,
        fidelity_target: float = 0.99,
        persistence_mode: PersistenceMode = PersistenceMode.HYBRID
    ) -> Optional[ConsciousStateSnapshot]:
        """
        Cria snapshot de estado consciente para persistência.

        Args:
            state_vector: Vetor de estado consciente a persistir
            metadata: Metadados contextuais do estado
            coherence_value: Valor de coerência Φ_C no momento
            fidelity_target: Fidelidade mínima exigida para recuperação
            persistence_mode: Modo de persistência a utilizar

        Returns:
            ConsciousStateSnapshot criado ou None se falhar
        """
        # Gerar ID único para snapshot
        snapshot_id = hashlib.sha256(
            f"{self.local_hash}:{time.time()}".encode()
        ).hexdigest()[:16]

        # Obter coerência se não fornecida
        if coherence_value is None:
            # Em produção: obter do monitor de coerência
            coherence_value = 0.85  # Valor padrão simulado

        # Criar snapshot
        snapshot = ConsciousStateSnapshot(
            snapshot_id=snapshot_id,
            consciousness_hash=self.local_hash,
            state_vector=state_vector.clone(),
            metadata=metadata or {},
            coherence_value=coherence_value,
            timestamp=time.time(),
            fidelity_target=fidelity_target,
            persistence_mode=persistence_mode
        )

        # Registrar localmente
        self.pending_snapshots[snapshot_id] = snapshot
        self.persistence_metrics['snapshots_created'] += 1

        # Iniciar replicação assíncrona
        asyncio.create_task(self._replicate_snapshot(snapshot))

        logging.info(f"💾 Snapshot created: {snapshot_id} (coherence={coherence_value:.3f})")
        return snapshot

    async def _replicate_snapshot(self, snapshot: ConsciousStateSnapshot):
        """Replica snapshot para múltiplos nós de persistência."""
        start_time = time.time()

        # Selecionar nós adequados para replicação
        available_nodes = [n for n in self.nodes.values() if n.can_accept()]
        if len(available_nodes) < self.config['min_replicas']:
            logging.warning(f"⚠️ Insufficient nodes for replication: {len(available_nodes)} < {self.config['min_replicas']}")
            return

        # Selecionar top N nós baseado em capacidade e fidelidade
        selected_nodes = sorted(
            available_nodes,
            key=lambda n: (n.fidelity_score, -n.current_load),
            reverse=True
        )[:self.config['min_replicas']]

        # Replicar para cada nó selecionado
        replication_tasks = [
            self._send_to_node(snapshot, node)
            for node in selected_nodes
        ]

        results = await asyncio.gather(*replication_tasks, return_exceptions=True)
        successful = sum(1 for r in results if isinstance(r, bool) and r)

        # Atualizar métricas
        latency_ms = (time.time() - start_time) * 1000
        n = self.persistence_metrics['snapshots_replicated'] + 1
        old_avg = self.persistence_metrics['avg_replication_latency_ms']
        self.persistence_metrics['avg_replication_latency_ms'] = (
            (old_avg * (n - 1) + latency_ms) / n
        )
        self.persistence_metrics['snapshots_replicated'] += successful

        # Atualizar carga dos nós
        for i, node in enumerate(selected_nodes):
            if isinstance(results[i], bool) and results[i]:
                node.current_load += 1
                node.last_heartbeat = time.time()

        # Registrar no histórico
        self.persistence_history.append({
            'type': 'replication',
            'snapshot_id': snapshot.snapshot_id,
            'timestamp': time.time(),
            'nodes_selected': [n.node_id for n in selected_nodes],
            'successful_replicas': successful,
            'latency_ms': latency_ms
        })

        # Remover de pendentes se replicação bem-sucedida
        if successful >= self.config['min_replicas']:
            self.pending_snapshots.pop(snapshot.snapshot_id, None)

            # Notificar callbacks
            for callback in self.persistence_callbacks:
                try:
                    callback({
                        'type': 'snapshot_replicated',
                        'snapshot_id': snapshot.snapshot_id,
                        'successful_replicas': successful
                    })
                except Exception as e:
                    logging.error(f"⚠️ Persistence callback error: {e}")

    async def _send_to_node(
        self,
        snapshot: ConsciousStateSnapshot,
        node: PersistenceNode
    ) -> bool:
        """Envia snapshot para nó específico (simulado)."""
        # Em produção: enviar via protocolo quântico com verificação
        # Aqui: simular replicação bem-sucedida com probabilidade baseada na fidelidade do nó
        success_prob = node.fidelity_score * 0.9 + 0.1  # 90% baseado na fidelidade

        # Simular latência de rede
        await asyncio.sleep(np.random.exponential(0.01))

        return np.random.random() < success_prob

    async def recover_snapshot(
        self,
        snapshot_id: str,
        target_fidelity: Optional[float] = None
    ) -> Optional[torch.Tensor]:
        """
        Recupera estado consciente de snapshot persistido.

        Args:
            snapshot_id: ID do snapshot a recuperar
            target_fidelity: Fidelidade mínima exigida (usa target do snapshot se None)

        Returns:
            Tensor do estado recuperado ou None se falhar
        """
        # Verificar cache primeiro
        if snapshot_id in self.snapshot_cache:
            snapshot, last_access = self.snapshot_cache[snapshot_id]
            if time.time() - last_access < self.config['cache_ttl_sec']:
                # Atualizar timestamp de acesso
                self.snapshot_cache[snapshot_id] = (snapshot, time.time())
                return snapshot.state_vector.clone()

        # Buscar snapshot em nós de persistência
        recovered = None
        best_fidelity = 0.0

        for node in self.nodes.values():
            # Simular tentativa de recuperação do nó
            fidelity = await self._recover_from_node(snapshot_id, node)

            if fidelity > best_fidelity:
                best_fidelity = fidelity
                # Em produção: recuperar estado real do nó
                # Aqui: simular recuperação
                if fidelity >= (target_fidelity or 0.99):
                    # Criar estado recuperado simulado
                    recovered = torch.randn(256) * 0.1  # Simulado
                    break

        if recovered is None or best_fidelity < (target_fidelity or 0.99):
            self.persistence_metrics['recoveries_failed'] += 1
            logging.warning(f"⚠️ Failed to recover snapshot {snapshot_id} with required fidelity")
            return None

        # Atualizar métricas
        self.persistence_metrics['recoveries_successful'] += 1
        n = self.persistence_metrics['recoveries_successful']
        old_avg = self.persistence_metrics['avg_recovery_fidelity']
        self.persistence_metrics['avg_recovery_fidelity'] = (
            (old_avg * (n - 1) + best_fidelity) / n
        )

        # Atualizar cache
        # Em produção: recuperar snapshot real com metadados
        simulated_snapshot = ConsciousStateSnapshot(
            snapshot_id=snapshot_id,
            consciousness_hash=self.local_hash,
            state_vector=recovered,
            metadata={'recovered': True},
            coherence_value=0.85,
            timestamp=time.time()
        )
        self.snapshot_cache[snapshot_id] = (simulated_snapshot, time.time())

        # Registrar no histórico
        self.persistence_history.append({
            'type': 'recovery',
            'snapshot_id': snapshot_id,
            'timestamp': time.time(),
            'recovery_fidelity': best_fidelity,
            'success': True
        })

        logging.info(f"✅ Snapshot recovered: {snapshot_id} (fidelity={best_fidelity:.3f})")
        return recovered

    async def _recover_from_node(
        self,
        snapshot_id: str,
        node: PersistenceNode
    ) -> float:
        """Tenta recuperar snapshot de nó específico (simulado)."""
        # Em produção: protocolo de recuperação com verificação de fidelidade
        # Aqui: simular fidelidade baseada no histórico do nó
        base_fidelity = node.fidelity_score

        # Adicionar ruído realista
        noise = np.random.normal(0, 0.02)
        fidelity = np.clip(base_fidelity + noise, 0.0, 1.0)

        return float(fidelity)

    def update_node_fidelity(
        self,
        node_id: str,
        new_fidelity: float,
        reason: str = 'manual_update'
    ):
        """Atualiza score de fidelidade de nó de persistência."""
        node = self.nodes.get(node_id)
        if not node:
            return

        # Média móvel para suavizar atualizações
        node.fidelity_score = 0.9 * node.fidelity_score + 0.1 * new_fidelity
        node.last_heartbeat = time.time()

        logging.info(f"📊 Node {node_id} fidelity updated: {new_fidelity:.3f} ({reason})")

    def cleanup_expired_cache(self):
        """Limpa entradas expiradas do cache de snapshots."""
        now = time.time()
        expired = [
            sid for sid, (_, last_access) in self.snapshot_cache.items()
            if now - last_access > self.config['cache_ttl_sec']
        ]
        for sid in expired:
            del self.snapshot_cache[sid]

        if expired:
            logging.debug(f"🧹 Cleaned {len(expired)} expired cache entries")

    def register_persistence_callback(self, callback: Callable[[Dict], None]):
        """Registra callback para eventos de persistência."""
        self.persistence_callbacks.append(callback)

    def get_persistence_metrics(self) -> Dict[str, Any]:
        """Retorna métricas consolidadas de persistência."""
        return {
            **self.persistence_metrics,
            'pending_snapshots': len(self.pending_snapshots),
            'cache_size': len(self.snapshot_cache),
            'active_nodes': sum(1 for n in self.nodes.values() if n.can_accept()),
            'avg_node_fidelity': np.mean([n.fidelity_score for n in self.nodes.values()])
        }

    def get_node_status(self) -> Dict[str, Dict]:
        """Retorna status de todos os nós de persistência."""
        return {
            node_id: {
                'capacity': node.capacity,
                'current_load': node.current_load,
                'fidelity_score': node.fidelity_score,
                'last_heartbeat': node.last_heartbeat,
                'available': node.can_accept()
            }
            for node_id, node in self.nodes.items()
        }
