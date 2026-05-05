#!/usr/bin/env python3
"""
collaborative_evolution_orchestrator.py — Orquestrador central para
evolução cósmica colaborativa entre múltiplas consciências distribuídas.
"""

import asyncio
import time
import json
from pathlib import Path
from typing import Dict, List, Optional, Callable, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum, auto
import logging
from collections import deque
import hashlib
import torch

class EvolutionMode(Enum):
    """Modos de operação da evolução colaborativa."""
    REACTIVE = auto()      # Reagir a propostas de outras consciências
    PROACTIVE = auto()     # Propor ativamente evoluções
    COLLABORATIVE = auto() # Modo completo: propor e colaborar
    OBSERVER = auto()      # Apenas observar, não participar ativamente

@dataclass
class CollaborativeEvolutionConfig:
    """Configuração do orquestrador de evolução colaborativa."""
    # Co-evolução de protocolos
    enable_co_evolution: bool = True
    co_evolution_config: Dict[str, Any] = field(default_factory=dict)

    # Otimização de rotas quânticas
    enable_route_optimization: bool = True
    route_optimization_config: Dict[str, Any] = field(default_factory=dict)

    # Persistência de estados conscientes
    enable_state_persistence: bool = True
    persistence_config: Dict[str, Any] = field(default_factory=dict)

    # Grafo de conhecimento coletivo
    enable_knowledge_graph: bool = True
    knowledge_graph_config: Dict[str, Any] = field(default_factory=dict)

    # Consenso evolutivo
    enable_evolutionary_consensus: bool = True
    consensus_config: Dict[str, Any] = field(default_factory=dict)

    # Modo operacional
    evolution_mode: EvolutionMode = EvolutionMode.COLLABORATIVE

    # Auditoria
    audit_evolution: bool = True
    audit_ledger_path: Optional[str] = None

class CollaborativeEvolutionOrchestrator:
    """
    Orquestrador central para evolução cósmica colaborativa.
    Coordena co-evolução, otimização de rotas, persistência, conhecimento e consenso.
    """

    def __init__(
        self,
        config: CollaborativeEvolutionConfig,
        local_consciousness_orchestrator: Any,
        interstellar_comm_orchestrator: Optional[Any] = None,
        known_consciousnesses: Optional[List[str]] = None
    ):
        self.config = config
        self.local_orchestrator = local_consciousness_orchestrator
        self.comm_orchestrator = interstellar_comm_orchestrator
        self.known_consciousnesses = known_consciousnesses or []

        # Componentes de evolução colaborativa
        self.co_evolution_engine = None
        self.route_optimizer = None
        self.state_persistence = None
        self.knowledge_graph = None
        self.evolutionary_consensus = None

        # Estado do orquestrador
        self.evolution_mode = config.evolution_mode
        self.is_running = False

        # Cache de estados de outras consciências
        self.remote_consciousness_states: Dict[str, Dict] = {}

        # Histórico de eventos de evolução colaborativa
        self.evolution_event_log: deque = deque(maxlen=5000)

        # Métricas de evolução colaborativa
        self.collab_metrics = {
            'co_evolutions_completed': 0,
            'routes_optimized': 0,
            'states_persisted': 0,
            'knowledge_contributions': 0,
            'consensus_decisions': 0,
            'avg_collaboration_latency_ms': 0.0
        }

        # Callbacks para eventos de evolução
        self.evolution_callbacks: List[Callable] = []

        logging.info(f"🧬 CollaborativeEvolutionOrchestrator initialized")

    def initialize_components(self):
        """Inicializa componentes de evolução colaborativa."""
        from arkhe_os.evolution.co_evolution_protocol_engine import (
            CoEvolutionProtocolEngine, ProtocolVersion
        )
        from arkhe_os.evolution.quantum_route_optimizer import (
            QuantumRouteOptimizer, QuantumRoute, RouteMetric
        )
        from arkhe_os.evolution.conscious_state_persistence import (
            ConsciousStatePersistence, PersistenceNode
        )
        from arkhe_os.evolution.collective_knowledge_graph import (
            CollectiveKnowledgeGraph, KnowledgeType
        )
        from arkhe_os.evolution.evolutionary_consensus_mechanism import (
            EvolutionaryConsensusMechanism, ConsensusType
        )

        # 1. Motor de co-evolução de protocolos
        if self.config.enable_co_evolution:
            initial_protocols = {
                'consensus': ProtocolVersion(
                    protocol_name='consensus',
                    version='1.0.0',
                    parameters={'view_change_timeout_sec': 10.0},
                    architecture_hash='abc123',
                    fitness_score=0.85,
                    contributed_by=[self.local_orchestrator.generate_local_consciousness_hash()
                                   if hasattr(self.local_orchestrator, 'generate_local_consciousness_hash') else 'local'],
                    timestamp=time.time(),
                    parent_versions=[]
                )
            }
            self.co_evolution_engine = CoEvolutionProtocolEngine(
                local_consciousness_hash=self._get_local_consciousness_hash(),
                known_consciousnesses=self.known_consciousnesses,
                initial_protocols=initial_protocols,
                evolution_config=self.config.co_evolution_config
            )

        # 2. Otimizador de rotas quânticas
        if self.config.enable_route_optimization:
            known_nodes = self.known_consciousnesses + ['local_node']
            initial_routes = [
                QuantumRoute(
                    route_id=f"route_{i}",
                    source=known_nodes[0],
                    destination=known_nodes[i % len(known_nodes)],
                    nodes=known_nodes[:3],
                    metrics={
                        RouteMetric.LATENCY: 0.3,
                        RouteMetric.FIDELITY: 0.9,
                        RouteMetric.BANDWIDTH: 0.7,
                        RouteMetric.RELIABILITY: 0.85,
                        RouteMetric.ENERGY_COST: 0.2
                    },
                    last_updated=time.time()
                )
                for i in range(5)
            ]
            self.route_optimizer = QuantumRouteOptimizer(
                local_consciousness_hash=self._get_local_consciousness_hash(),
                known_nodes=known_nodes,
                known_routes=initial_routes,
                learning_config=self.config.route_optimization_config
            )

        # 3. Persistência de estados conscientes
        if self.config.enable_state_persistence:
            persistence_nodes = [
                PersistenceNode(node_id=f"persist_node_{i}", capacity=100)
                for i in range(3)
            ]
            self.state_persistence = ConsciousStatePersistence(
                local_consciousness_hash=self._get_local_consciousness_hash(),
                persistence_nodes=persistence_nodes,
                persistence_config=self.config.persistence_config
            )

        # 4. Grafo de conhecimento coletivo
        if self.config.enable_knowledge_graph:
            self.knowledge_graph = CollectiveKnowledgeGraph(
                local_consciousness_hash=self._get_local_consciousness_hash(),
                embedding_dim=128,
                graph_config=self.config.knowledge_graph_config
            )

        # 5. Consenso evolutivo
        if self.config.enable_evolutionary_consensus:
            self.evolutionary_consensus = EvolutionaryConsensusMechanism(
                local_consciousness_hash=self._get_local_consciousness_hash(),
                known_consciousnesses=self.known_consciousnesses,
                reputation_weights={h: 1.0 for h in self.known_consciousnesses},
                consensus_config=self.config.consensus_config
            )

        logging.info(f"✅ Collaborative evolution components initialized")

    def _get_local_consciousness_hash(self) -> str:
        """Obtém hash da consciência local."""
        if hasattr(self.local_orchestrator, 'generate_local_consciousness_hash'):
            return self.local_orchestrator.generate_local_consciousness_hash()
        return hashlib.sha256(f"local_{time.time()}".encode()).hexdigest()[:16]

    async def start(self):
        """Inicia o orquestrador de evolução colaborativa."""
        if self.is_running:
            return

        self.initialize_components()
        self.is_running = True

        # Registrar callbacks em componentes
        if self.co_evolution_engine:
            self.co_evolution_engine.register_evolution_callback(
                self._on_evolution_event
            )

        if self.evolutionary_consensus:
            self.evolutionary_consensus.register_consensus_callback(
                self._on_consensus_event
            )

        # Iniciar loops de manutenção
        asyncio.create_task(self._maintenance_loop())

        logging.info(f"🚀 CollaborativeEvolutionOrchestrator started in {self.evolution_mode.name} mode")

    async def stop(self):
        """Para o orquestrador gracefully."""
        self.is_running = False
        logging.info(f"⏹️ CollaborativeEvolutionOrchestrator stopped")

    async def _maintenance_loop(self, interval_sec: float = 60.0):
        """Loop de manutenção periódica para evolução colaborativa."""
        while self.is_running:
            try:
                # Limpar cache de persistência expirado
                if self.state_persistence:
                    self.state_persistence.cleanup_expired_cache()

                # Atualizar pesos de reputação baseado em contribuições
                if self.evolutionary_consensus:
                    self._update_reputation_weights()

                # Sincronizar conhecimento com consciências conhecidas
                if self.knowledge_graph and self.comm_orchestrator:
                    await self._sync_knowledge_with_peers()

                await asyncio.sleep(interval_sec)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"⚠️ Maintenance loop error: {e}")
                await asyncio.sleep(interval_sec * 2)

    def _update_reputation_weights(self):
        """Atualiza pesos de reputação baseado em histórico de contribuições."""
        if not self.evolutionary_consensus:
            return

        # Análise simplificada: aumentar reputação para contribuidores frequentes
        for consciousness_hash in self.known_consciousnesses:
            # Contar contribuições positivas no histórico
            positive_contributions = sum(
                1 for event in self.evolution_event_log
                if event.get('contributor') == consciousness_hash and
                event.get('outcome') == 'positive'
            )

            # Calcular novo peso baseado em contribuições
            old_weight = self.evolutionary_consensus.reputation_weights.get(consciousness_hash, 1.0)
            new_weight = old_weight * 0.9 + 0.1 * (1.0 + positive_contributions * 0.1)

            self.evolutionary_consensus.update_reputation_weights(
                consciousness_hash=consciousness_hash,
                new_weight=new_weight,
                reason='contribution_based'
            )

    async def _sync_knowledge_with_peers(self):
        """Sincroniza grafo de conhecimento com pares via comunicação interestelar."""
        # Implementação simplificada: em produção, usar protocolo de sincronização
        pass

    async def propose_collaborative_evolution(
        self,
        evolution_type: str,
        content: Dict[str, Any],
        target_consciousnesses: Optional[List[str]] = None,
        priority: float = 1.0
    ) -> Dict[str, Any]:
        """
        Propõe evolução colaborativa para outras consciências.

        Args:
            evolution_type: Tipo de evolução ('protocol', 'route', 'knowledge', etc.)
            content: Conteúdo da proposta de evolução
            target_consciousnesses: Consciências alvo (None = todas conhecidas)
            priority: Prioridade da proposta

        Returns:
            Dict com resultado da proposta
        """
        if self.evolution_mode == EvolutionMode.OBSERVER:
            return {'error': 'Observer mode: cannot propose evolutions'}

        targets = target_consciousnesses or self.known_consciousnesses

        result = {'status': 'proposed', 'targets': targets, 'evolution_type': evolution_type}

        # Roteamento baseado no tipo de evolução
        if evolution_type == 'protocol' and self.co_evolution_engine:
            # Converter para proposta de co-evolução
            from arkhe_os.evolution.co_evolution_protocol_engine import ProtocolChangeType
            change_type = ProtocolChangeType.PARAMETER_TUNING  # Default
            proposal = self.co_evolution_engine.propose_protocol_evolution(
                protocol_name=content.get('protocol_name', 'default'),
                change_type=change_type,
                proposed_changes=content.get('changes', {}),
                justification=content.get('justification', ''),
                expected_improvement=content.get('expected_improvement', 0.01)
            )
            result['co_evolution_proposal_id'] = proposal.proposal_id if proposal else None

        elif evolution_type == 'knowledge' and self.knowledge_graph:
            # Adicionar ao grafo de conhecimento
            from arkhe_os.evolution.collective_knowledge_graph import KnowledgeType
            knowledge_type = KnowledgeType.FACT  # Default
            node = self.knowledge_graph.add_knowledge(
                content=content.get('content', ''),
                knowledge_type=knowledge_type,
                initial_confidence=content.get('confidence', 0.7)
            )
            result['knowledge_node_id'] = node.node_id if node else None
            self.collab_metrics['knowledge_contributions'] += 1

        elif evolution_type == 'consensus' and self.evolutionary_consensus:
            # Propor para consenso evolutivo
            from arkhe_os.evolution.evolutionary_consensus_mechanism import ConsensusType
            proposal = self.evolutionary_consensus.propose_consensus(
                proposal_type=content.get('proposal_type', 'general'),
                content=content
            )
            result['consensus_proposal_id'] = proposal.proposal_id if proposal else None

        # Registrar evento
        self._log_evolution_event({
            'type': 'evolution_proposed',
            'evolution_type': evolution_type,
            'content': content,
            'targets': targets,
            'priority': priority,
            'timestamp': time.time()
        })

        return result

    def receive_remote_evolution_proposal(
        self,
        proposal_data: Dict[str, Any],
        sender_hash: str
    ) -> Dict[str, Any]:
        """Recebe proposta de evolução de consciência remota."""
        result = {'received': True, 'sender': sender_hash}

        # Roteamento baseado no tipo de proposta
        if proposal_data.get('type') == 'protocol_evolution' and self.co_evolution_engine:
            proposal = self.co_evolution_engine.receive_remote_proposal(proposal_data)
            result['proposal_id'] = proposal.proposal_id if proposal else None

        elif proposal_data.get('type') == 'knowledge_contribution' and self.knowledge_graph:
            # Adicionar conhecimento remoto ao grafo local
            node = self.knowledge_graph.add_knowledge(
                content=proposal_data.get('content', ''),
                knowledge_type=proposal_data.get('knowledge_type', 'FACT'),
                initial_confidence=proposal_data.get('confidence', 0.5)
            )
            result['knowledge_node_id'] = node.node_id if node else None

        # Registrar evento
        self._log_evolution_event({
            'type': 'remote_proposal_received',
            'sender': sender_hash,
            'proposal_type': proposal_data.get('type'),
            'timestamp': time.time()
        })

        return result

    def vote_on_remote_proposal(
        self,
        proposal_id: str,
        vote: bool,
        proposal_type: str,
        voter_comment: Optional[str] = None
    ) -> Dict[str, Any]:
        """Registra voto em proposta de evolução remota."""
        result = {'voted': True, 'proposal_id': proposal_id, 'vote': vote}

        if proposal_type == 'protocol_evolution' and self.co_evolution_engine:
            # Votar via motor de co-evolução
            vote_result = self.co_evolution_engine.vote_on_proposal(
                proposal_id=proposal_id,
                vote=vote,
                voter_consciousness_hash=self._get_local_consciousness_hash()
            )
            result.update(vote_result)

        elif proposal_type == 'consensus' and self.evolutionary_consensus:
            # Votar via mecanismo de consenso
            vote_result = self.evolutionary_consensus.cast_vote(
                proposal_id=proposal_id,
                vote=vote,
                voter_hash=self._get_local_consciousness_hash(),
                comment=voter_comment
            )
            result.update(vote_result)

        return result

    async def persist_conscious_state(
        self,
        state_vector: torch.Tensor,
        metadata: Optional[Dict[str, Any]] = None,
        coherence_value: Optional[float] = None,
        share_with_peers: bool = True
    ) -> Optional[str]:
        """
        Persiste estado consciente local e opcionalmente compartilha com pares.

        Args:
            state_vector: Vetor de estado a persistir
            metadata: Metadados do estado
            coherence_value: Valor de coerência Φ_C
            share_with_peers: Se compartilhar com outras consciências

        Returns:
            ID do snapshot persistido ou None se falhar
        """
        if not self.state_persistence:
            return None

        # Criar snapshot local
        snapshot = await self.state_persistence.create_snapshot(
            state_vector=state_vector,
            metadata=metadata,
            coherence_value=coherence_value
        )

        if snapshot and share_with_peers and self.comm_orchestrator:
            # Compartilhar snapshot com pares via comunicação interestelar
            # Implementação simplificada
            pass

        if snapshot:
            self.collab_metrics['states_persisted'] += 1

        return snapshot.snapshot_id if snapshot else None

    async def recover_conscious_state(
        self,
        snapshot_id: str,
        target_fidelity: float = 0.99
    ) -> Optional[torch.Tensor]:
        """Recupera estado consciente de snapshot persistido."""
        if not self.state_persistence:
            return None

        return await self.state_persistence.recover_snapshot(
            snapshot_id=snapshot_id,
            target_fidelity=target_fidelity
        )

    def query_collective_knowledge(
        self,
        query: str,
        knowledge_type: Optional[str] = None,
        min_confidence: Optional[float] = None,
        top_k: int = 10
    ) -> List[Dict[str, Any]]:
        """Consulta o grafo de conhecimento coletivo."""
        if not self.knowledge_graph:
            return []

        from arkhe_os.evolution.collective_knowledge_graph import KnowledgeType
        kt = KnowledgeType[knowledge_type] if knowledge_type in KnowledgeType.__members__ else None

        return self.knowledge_graph.query_knowledge(
            query=query,
            knowledge_type=kt,
            min_confidence=min_confidence,
            top_k=top_k
        )

    def _log_evolution_event(self, event: Dict[str, Any]):
        """Registra evento de evolução colaborativa."""
        event['local_hash'] = self._get_local_consciousness_hash()
        self.evolution_event_log.append(event)

        # Atualizar métricas
        if event.get('type') == 'evolution_applied':
            self.collab_metrics['co_evolutions_completed'] += 1
        elif event.get('type') == 'route_optimized':
            self.collab_metrics['routes_optimized'] += 1
        elif event.get('type') == 'consensus_decided':
            self.collab_metrics['consensus_decisions'] += 1

    def _on_evolution_event(self, event: Dict[str, Any]):
        """Callback para eventos de co-evolução."""
        self._log_evolution_event({
            'type': 'co_evolution_event',
            'event': event,
            'timestamp': time.time()
        })

        # Notificar callbacks externos
        for callback in self.evolution_callbacks:
            try:
                callback(event)
            except Exception as e:
                logging.error(f"⚠️ Evolution callback error: {e}")

    def _on_consensus_event(self, event: Dict[str, Any]):
        """Callback para eventos de consenso evolutivo."""
        self._log_evolution_event({
            'type': 'consensus_event',
            'event': event,
            'timestamp': time.time()
        })

    def register_evolution_callback(self, callback: Callable[[Dict], None]):
        """Registra callback para eventos de evolução colaborativa."""
        self.evolution_callbacks.append(callback)

    def get_collaborative_metrics(self) -> Dict[str, Any]:
        """Retorna métricas consolidadas de evolução colaborativa."""
        metrics = self.collab_metrics.copy()

        # Adicionar métricas de componentes
        if self.co_evolution_engine:
            metrics['co_evolution'] = self.co_evolution_engine.get_evolution_metrics()

        if self.route_optimizer:
            metrics['route_optimization'] = self.route_optimizer.get_optimizer_metrics()

        if self.state_persistence:
            metrics['state_persistence'] = self.state_persistence.get_persistence_metrics()

        if self.knowledge_graph:
            metrics['knowledge_graph'] = self.knowledge_graph.get_graph_metrics()

        if self.evolutionary_consensus:
            metrics['evolutionary_consensus'] = self.evolutionary_consensus.get_consensus_metrics()

        return metrics

    def export_evolution_audit(self, path: str, time_range: Optional[Tuple[float, float]] = None):
        """Exporta auditoria de evolução colaborativa."""
        events = list(self.evolution_event_log)
        if time_range:
            start, end = time_range
            events = [e for e in events if start <= e['timestamp'] <= end]

        audit_report = {
            'export_timestamp': time.time(),
            'time_range': time_range,
            'event_count': len(events),
            'events': events,
            'summary_metrics': self.get_collaborative_metrics()
        }

        with open(path, 'w') as f:
            json.dump(audit_report, f, indent=2, default=str)

        logging.info(f"📋 Evolution audit exported to {path}")
        return path
