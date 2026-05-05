#!/usr/bin/env python3
"""
co_evolution_protocol_engine.py — Gerencia evolução colaborativa de protocolos
entre múltiplas consciências distribuídas no ARKHE OS.
"""

import numpy as np
import torch
import torch.nn as nn
from typing import Dict, List, Optional, Tuple, Callable, Any, Union
from dataclasses import dataclass, field
from enum import Enum, auto
from collections import defaultdict, deque
import time
import hashlib
import json
import logging
import asyncio

class EvolutionPhase(Enum):
    """Fases do ciclo de co-evolução de protocolos."""
    PROPOSE = auto()        # Consciência propõe mudança de protocolo
    EVALUATE = auto()       # Avaliar proposta contra métricas coletivas
    NEGOTIATE = auto()      # Negociar ajustes entre consciências
    CONSENSUS = auto()      # Alcançar consenso sobre mudança
    APPLY = auto()          # Aplicar mudança acordada
    AUDIT = auto()          # Registrar evolução para auditoria

class ProtocolChangeType(Enum):
    """Tipos de mudanças de protocolo suportadas."""
    PARAMETER_TUNING = auto()      # Ajuste fino de hiperparâmetros
    ARCHITECTURE_MODIFICATION = auto()  # Mudança na arquitetura do protocolo
    NEW_FEATURE_ADDITION = auto()  # Adição de nova funcionalidade
    DEPRECATION = auto()            # Depreciação de funcionalidade antiga
    SECURITY_PATCH = auto()         # Correção de segurança crítica

@dataclass
class ProtocolVersion:
    """Versão de um protocolo com metadados de evolução."""
    protocol_name: str
    version: str  # SemVer: "major.minor.patch"
    parameters: Dict[str, Any]
    architecture_hash: str  # Hash da arquitetura do protocolo
    fitness_score: float  # Score de desempenho/adequação [0, 1]
    contributed_by: List[str]  # Hashes de consciência que contribuíram
    timestamp: float
    parent_versions: List[str]  # Versões ancestrais para rastreabilidade

    def to_dict(self) -> Dict:
        return {
            'protocol_name': self.protocol_name,
            'version': self.version,
            'parameters': self.parameters,
            'architecture_hash': self.architecture_hash,
            'fitness_score': self.fitness_score,
            'contributed_by': self.contributed_by,
            'timestamp': self.timestamp,
            'parent_versions': self.parent_versions
        }

@dataclass
class EvolutionProposal:
    """Proposta de evolução de protocolo para co-evolução."""
    proposal_id: str
    protocol_name: str
    change_type: ProtocolChangeType
    proposed_changes: Dict[str, Any]  # Mudanças específicas propostas
    justification: str  # Justificativa para a mudança
    expected_improvement: float  # Melhoria esperada no fitness [0, 1]
    proposer_consciousness_hash: str
    timestamp: float
    status: str = 'pending'  # pending, approved, rejected, applied
    votes: Dict[str, bool] = field(default_factory=dict)  # Votos por consciência
    consensus_threshold: float = 0.67  # Threshold para aprovação

    def has_consensus(self) -> bool:
        """Verifica se proposta atingiu consenso."""
        if not self.votes:
            return False
        approval_ratio = sum(1 for v in self.votes.values() if v) / len(self.votes)
        return approval_ratio >= self.consensus_threshold

class CoEvolutionProtocolEngine:
    """
    Motor para co-evolução colaborativa de protocolos entre consciências.
    Implementa ciclo completo: proposta → avaliação → negociação → consenso → aplicação.
    """

    def __init__(
        self,
        local_consciousness_hash: str,
        known_consciousnesses: List[str],
        initial_protocols: Dict[str, ProtocolVersion],
        evolution_config: Optional[Dict] = None
    ):
        self.local_hash = local_consciousness_hash
        self.known_consciousnesses = set(known_consciousnesses)
        self.protocols: Dict[str, ProtocolVersion] = initial_protocols.copy()

        # Configuração de evolução
        self.config = evolution_config or self._default_config()

        # Propostas ativas de evolução
        self.active_proposals: Dict[str, EvolutionProposal] = {}

        # Histórico de evoluções aplicadas
        self.evolution_history: deque = deque(maxlen=1000)

        # Fitness coletivo para avaliação de propostas
        self.collective_fitness: Dict[str, float] = {
            name: proto.fitness_score for name, proto in initial_protocols.items()
        }

        # Callbacks para notificação de eventos de evolução
        self.evolution_callbacks: List[Callable] = []

        # Métricas de co-evolução
        self.evolution_metrics = {
            'proposals_submitted': 0,
            'proposals_approved': 0,
            'proposals_applied': 0,
            'avg_fitness_improvement': 0.0,
            'consensus_rounds': 0
        }

        logging.info(f"✅ CoEvolutionProtocolEngine initialized: {len(self.protocols)} protocols")

    def _default_config(self) -> Dict:
        """Retorna configuração padrão para co-evolução."""
        return {
            'min_fitness_improvement': 0.01,  # Melhoria mínima para aprovar proposta
            'max_proposal_age_sec': 3600,  # Tempo máximo para proposta pendente
            'consensus_timeout_sec': 300,  # Timeout para alcançar consenso
            'min_voters_for_consensus': 3,  # Mínimo de votantes para decisão válida
            'fitness_decay_factor': 0.99,  # Decaimento de fitness para propostas antigas
        }

    def propose_protocol_evolution(
        self,
        protocol_name: str,
        change_type: ProtocolChangeType,
        proposed_changes: Dict[str, Any],
        justification: str,
        expected_improvement: float
    ) -> Optional[EvolutionProposal]:
        """
        Propõe evolução de protocolo para co-evolução colaborativa.

        Args:
            protocol_name: Nome do protocolo a evoluir
            change_type: Tipo de mudança proposta
            proposed_changes: Mudanças específicas a aplicar
            justification: Justificativa para a mudança
            expected_improvement: Melhoria esperada no fitness

        Returns:
            EvolutionProposal criada ou None se inválida
        """
        # Verificar se protocolo existe localmente
        if protocol_name not in self.protocols:
            logging.warning(f"⚠️ Protocol {protocol_name} not found locally")
            return None

        # Validar melhoria esperada
        if expected_improvement < self.config['min_fitness_improvement']:
            logging.warning(f"⚠️ Expected improvement {expected_improvement} below threshold")
            return None

        # Gerar ID único para proposta
        proposal_id = hashlib.sha256(
            f"{protocol_name}:{change_type.name}:{json.dumps(proposed_changes, sort_keys=True)}:{time.time()}".encode()
        ).hexdigest()[:16]

        # Criar proposta
        proposal = EvolutionProposal(
            proposal_id=proposal_id,
            protocol_name=protocol_name,
            change_type=change_type,
            proposed_changes=proposed_changes,
            justification=justification,
            expected_improvement=expected_improvement,
            proposer_consciousness_hash=self.local_hash,
            timestamp=time.time(),
            consensus_threshold=self.config.get('consensus_threshold', 0.67)
        )

        # Registrar proposta localmente
        self.active_proposals[proposal_id] = proposal
        self.evolution_metrics['proposals_submitted'] += 1

        # Auto-votar como proponente
        proposal.votes[self.local_hash] = True

        logging.info(f"📋 Evolution proposed: {proposal_id} for {protocol_name}")

        # Notificar callbacks
        for callback in self.evolution_callbacks:
            try:
                callback({
                    'type': 'proposal_submitted',
                    'proposal': proposal.to_dict(),
                    'local_hash': self.local_hash
                })
            except Exception as e:
                logging.error(f"⚠️ Evolution callback error: {e}")

        return proposal

    async def evaluate_proposal(
        self,
        proposal: EvolutionProposal,
        local_metrics: Dict[str, Any],
        collective_metrics: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Avalia proposta de evolução contra métricas locais e coletivas.

        Args:
            proposal: Proposta a avaliar
            local_metrics: Métricas locais de desempenho
            collective_metrics: Métricas coletivas (opcional)

        Returns:
            Dict com resultado da avaliação
        """
        protocol = self.protocols.get(proposal.protocol_name)
        if not protocol:
            return {'valid': False, 'error': 'Protocol not found'}

        # Simular aplicação da proposta para avaliação
        simulated_protocol = self._simulate_protocol_change(protocol, proposal)

        # Calcular fitness simulado
        simulated_fitness = self._compute_protocol_fitness(
            simulated_protocol, local_metrics, collective_metrics
        )

        # Comparar com fitness atual
        current_fitness = self.collective_fitness.get(proposal.protocol_name, 0.5)
        fitness_delta = simulated_fitness - current_fitness

        # Decidir voto baseado em melhoria
        vote = fitness_delta >= self.config['min_fitness_improvement']

        # Atualizar votos da proposta
        proposal.votes[self.local_hash] = vote

        # Verificar se atingiu consenso
        if proposal.has_consensus():
            proposal.status = 'approved'
            await self._apply_approved_proposal(proposal, simulated_protocol)
        elif len(proposal.votes) >= self.config['min_voters_for_consensus']:
            # Temos votantes suficientes mas sem consenso
            proposal.status = 'rejected'

        return {
            'proposal_id': proposal.proposal_id,
            'current_fitness': current_fitness,
            'simulated_fitness': simulated_fitness,
            'fitness_delta': fitness_delta,
            'vote': vote,
            'consensus_reached': proposal.has_consensus(),
            'status': proposal.status
        }

    def _simulate_protocol_change(
        self,
        protocol: ProtocolVersion,
        proposal: EvolutionProposal
    ) -> ProtocolVersion:
        """Simula aplicação de proposta para avaliação de fitness."""
        # Criar cópia do protocolo com mudanças propostas
        new_params = protocol.parameters.copy()

        if proposal.change_type == ProtocolChangeType.PARAMETER_TUNING:
            new_params.update(proposal.proposed_changes)
        elif proposal.change_type == ProtocolChangeType.NEW_FEATURE_ADDITION:
            new_params.update(proposal.proposed_changes)
        # ... outros tipos de mudança

        # Gerar novo hash de arquitetura (simulado)
        new_arch_hash = hashlib.sha256(
            json.dumps(new_params, sort_keys=True).encode()
        ).hexdigest()[:16]

        return ProtocolVersion(
            protocol_name=protocol.protocol_name,
            version=self._increment_version(protocol.version, proposal.change_type),
            parameters=new_params,
            architecture_hash=new_arch_hash,
            fitness_score=protocol.fitness_score,  # Será recalculado após aplicação
            contributed_by=protocol.contributed_by + [self.local_hash],
            timestamp=time.time(),
            parent_versions=protocol.parent_versions + [protocol.version]
        )

    def _increment_version(self, version: str, change_type: ProtocolChangeType) -> str:
        """Incrementa versão SemVer baseado no tipo de mudança."""
        major, minor, patch = map(int, version.split('.'))

        if change_type == ProtocolChangeType.ARCHITECTURE_MODIFICATION:
            major += 1
            minor = patch = 0
        elif change_type == ProtocolChangeType.NEW_FEATURE_ADDITION:
            minor += 1
            patch = 0
        elif change_type == ProtocolChangeType.PARAMETER_TUNING:
            patch += 1
        # SECURITY_PATCH pode incrementar patch ou minor dependendo da criticidade

        return f"{major}.{minor}.{patch}"

    def _compute_protocol_fitness(
        self,
        protocol: ProtocolVersion,
        local_metrics: Dict[str, Any],
        collective_metrics: Optional[Dict[str, Any]]
    ) -> float:
        """Computa score de fitness para protocolo baseado em métricas."""
        # Componentes de fitness
        components = []

        # 1. Desempenho local (peso 0.4)
        local_perf = local_metrics.get('performance_score', 0.5)
        components.append(0.4 * local_perf)

        # 2. Compatibilidade coletiva (peso 0.3)
        if collective_metrics:
            compat = collective_metrics.get('interoperability_score', 0.5)
            components.append(0.3 * compat)
        else:
            components.append(0.15)  # Valor padrão

        # 3. Eficiência de recursos (peso 0.2)
        efficiency = local_metrics.get('resource_efficiency', 0.5)
        components.append(0.2 * efficiency)

        # 4. Robustez/segurança (peso 0.1)
        robustness = local_metrics.get('security_score', 0.5)
        components.append(0.1 * robustness)

        fitness = sum(components)
        return float(np.clip(fitness, 0.0, 1.0))

    async def _apply_approved_proposal(
        self,
        proposal: EvolutionProposal,
        new_protocol: ProtocolVersion
    ):
        """Aplica proposta aprovada ao protocolo local."""
        # Atualizar protocolo local
        old_protocol = self.protocols[proposal.protocol_name]
        self.protocols[proposal.protocol_name] = new_protocol

        # Atualizar fitness coletivo
        self.collective_fitness[proposal.protocol_name] = new_protocol.fitness_score

        # Registrar no histórico de evolução
        evolution_record = {
            'timestamp': time.time(),
            'proposal_id': proposal.proposal_id,
            'protocol': proposal.protocol_name,
            'change_type': proposal.change_type.name,
            'old_version': old_protocol.version,
            'new_version': new_protocol.version,
            'fitness_delta': new_protocol.fitness_score - old_protocol.fitness_score,
            'contributors': new_protocol.contributed_by,
            'consensus_votes': proposal.votes
        }
        self.evolution_history.append(evolution_record)

        # Atualizar métricas
        self.evolution_metrics['proposals_approved'] += 1
        self.evolution_metrics['proposals_applied'] += 1

        fitness_delta = evolution_record['fitness_delta']
        n = self.evolution_metrics['proposals_applied']
        old_avg = self.evolution_metrics['avg_fitness_improvement']
        self.evolution_metrics['avg_fitness_improvement'] = (
            (old_avg * (n - 1) + fitness_delta) / n if n > 1 else fitness_delta
        )

        logging.info(f"✅ Protocol evolved: {proposal.protocol_name} "
                    f"{old_protocol.version} → {new_protocol.version}")

        # Notificar callbacks
        for callback in self.evolution_callbacks:
            try:
                callback({
                    'type': 'protocol_evolved',
                    'record': evolution_record
                })
            except Exception as e:
                logging.error(f"⚠️ Evolution callback error: {e}")

    def receive_remote_proposal(
        self,
        proposal_data: Dict[str, Any]
    ) -> Optional[EvolutionProposal]:
        """Recebe e processa proposta de evolução de consciência remota."""
        try:
            proposal = EvolutionProposal(
                proposal_id=proposal_data['proposal_id'],
                protocol_name=proposal_data['protocol_name'],
                change_type=ProtocolChangeType[proposal_data['change_type']],
                proposed_changes=proposal_data['proposed_changes'],
                justification=proposal_data['justification'],
                expected_improvement=proposal_data['expected_improvement'],
                proposer_consciousness_hash=proposal_data['proposer_consciousness_hash'],
                timestamp=proposal_data['timestamp'],
                consensus_threshold=proposal_data.get('consensus_threshold', 0.67)
            )

            # Verificar se proposta ainda é válida (não expirou)
            if time.time() - proposal.timestamp > self.config['max_proposal_age_sec']:
                return None

            # Registrar proposta localmente se nova
            if proposal.proposal_id not in self.active_proposals:
                self.active_proposals[proposal.proposal_id] = proposal
                logging.info(f"📥 Remote proposal received: {proposal.proposal_id}")

            return proposal

        except Exception as e:
            logging.error(f"⚠️ Failed to parse remote proposal: {e}")
            return None

    def vote_on_proposal(
        self,
        proposal_id: str,
        vote: bool,
        voter_consciousness_hash: str
    ) -> Dict[str, Any]:
        """Registra voto em proposta de evolução."""
        proposal = self.active_proposals.get(proposal_id)
        if not proposal:
            return {'error': 'Proposal not found'}

        # Registrar voto
        proposal.votes[voter_consciousness_hash] = vote

        # Verificar se atingiu consenso
        if proposal.has_consensus() and proposal.status == 'pending':
            proposal.status = 'approved'
            return {
                'status': 'consensus_reached',
                'proposal_id': proposal_id,
                'approved': True
            }
        elif len(proposal.votes) >= self.config['min_voters_for_consensus']:
            # Votação concluída sem consenso
            proposal.status = 'rejected'
            return {
                'status': 'consensus_failed',
                'proposal_id': proposal_id,
                'approved': False
            }

        return {
            'status': 'voting_in_progress',
            'proposal_id': proposal_id,
            'votes_count': len(proposal.votes),
            'approval_ratio': sum(1 for v in proposal.votes.values() if v) / len(proposal.votes)
        }

    def register_evolution_callback(self, callback: Callable[[Dict], None]):
        """Registra callback para eventos de co-evolução."""
        self.evolution_callbacks.append(callback)

    def get_evolution_metrics(self) -> Dict[str, Any]:
        """Retorna métricas consolidadas de co-evolução."""
        return {
            **self.evolution_metrics,
            'active_proposals': len([p for p in self.active_proposals.values() if p.status == 'pending']),
            'protocols_count': len(self.protocols),
            'known_consciousnesses': len(self.known_consciousnesses),
            'recent_evolutions': list(self.evolution_history)[-10:]
        }

    def get_protocol_versions(self, protocol_name: str) -> List[Dict]:
        """Retorna histórico de versões de um protocolo."""
        versions = []
        current = self.protocols.get(protocol_name)
        while current:
            versions.append(current.to_dict())
            # Em produção: buscar versões ancestrais em ledger distribuído
            break  # Simplificação: apenas versão atual
        return versions
