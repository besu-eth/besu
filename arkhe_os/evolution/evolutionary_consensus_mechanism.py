#!/usr/bin/env python3
"""
evolutionary_consensus_mechanism.py — Alcança consenso sobre mudanças
de protocolo entre múltiplas consciências distribuídas.
"""

import numpy as np
from typing import Dict, List, Optional, Callable, Any, Tuple, Set
from dataclasses import dataclass, field
from enum import Enum, auto
from collections import defaultdict, deque
import time
import hashlib
import json
import logging
import asyncio

class ConsensusType(Enum):
    """Tipos de consenso para decisões evolutivas."""
    MAJORITY_VOTE = auto()      # Maioria simples
    SUPERMAJORITY = auto()       # Supermaioria (e.g., 2/3)
    UNANIMOUS = auto()           # Unanimidade
    WEIGHTED_VOTE = auto()       # Voto ponderado por reputação/contribuição
    DELIBERATIVE = auto()        # Consenso deliberativo com negociação

@dataclass
class ConsensusProposal:
    """Proposta para consenso evolutivo."""
    proposal_id: str
    proposal_type: str  # 'protocol_change', 'knowledge_update', 'route_optimization', etc.
    content: Dict[str, Any]  # Conteúdo da proposta
    proposer_hash: str
    timestamp: float
    consensus_type: ConsensusType
    required_threshold: float  # Threshold específico para este tipo de consenso
    votes: Dict[str, bool] = field(default_factory=dict)  # Votos por consciência
    weight_votes: Dict[str, float] = field(default_factory=dict)  # Votos ponderados
    discussion_log: List[Dict] = field(default_factory=list)  # Log de negociação
    status: str = 'pending'  # pending, approved, rejected, expired

    def compute_result(self) -> Tuple[bool, float]:
        """Computa resultado do consenso baseado no tipo."""
        if not self.votes:
            return False, 0.0

        if self.consensus_type == ConsensusType.MAJORITY_VOTE:
            approval_ratio = sum(1 for v in self.votes.values() if v) / len(self.votes)
            return approval_ratio >= 0.5, approval_ratio

        elif self.consensus_type == ConsensusType.SUPERMAJORITY:
            approval_ratio = sum(1 for v in self.votes.values() if v) / len(self.votes)
            return approval_ratio >= self.required_threshold, approval_ratio

        elif self.consensus_type == ConsensusType.UNANIMOUS:
            return all(self.votes.values()), 1.0 if all(self.votes.values()) else 0.0

        elif self.consensus_type == ConsensusType.WEIGHTED_VOTE:
            if not self.weight_votes:
                return False, 0.0
            total_weight = sum(self.weight_votes.values())
            approved_weight = sum(w for v, w in zip(self.votes.values(), self.weight_votes.values()) if v)
            approval_ratio = approved_weight / total_weight if total_weight > 0 else 0.0
            return approval_ratio >= self.required_threshold, approval_ratio

        elif self.consensus_type == ConsensusType.DELIBERATIVE:
            # Consenso deliberativo: requer tanto aprovação quanto baixa discordância
            approval_ratio = sum(1 for v in self.votes.values() if v) / len(self.votes)
            # Medir discordância como variância dos pesos de voto
            if self.weight_votes:
                weights = list(self.weight_votes.values())
                variance = np.var(weights)
                # Penalizar alta variância (discordância)
                adjusted_ratio = approval_ratio * (1 - variance / 4.0)  # Normalizar variância
            else:
                adjusted_ratio = approval_ratio
            return adjusted_ratio >= self.required_threshold, adjusted_ratio

        return False, 0.0

class EvolutionaryConsensusMechanism:
    """
    Mecanismo de consenso adaptativo para decisões evolutivas
    entre múltiplas consciências distribuídas.
    """

    def __init__(
        self,
        local_consciousness_hash: str,
        known_consciousnesses: List[str],
        reputation_weights: Optional[Dict[str, float]] = None,
        consensus_config: Optional[Dict] = None
    ):
        self.local_hash = local_consciousness_hash
        self.known_consciousnesses = set(known_consciousnesses)
        self.reputation_weights = reputation_weights or {h: 1.0 for h in known_consciousnesses}

        # Configuração de consenso
        self.config = consensus_config or self._default_config()

        # Propostas ativas de consenso
        self.active_proposals: Dict[str, ConsensusProposal] = {}

        # Histórico de decisões de consenso
        self.consensus_history: deque = deque(maxlen=1000)

        # Métricas de consenso
        self.consensus_metrics = {
            'proposals_submitted': 0,
            'consensus_rounds_completed': 0,
            'avg_consensus_time_sec': 0.0,
            'approval_rate': 0.0,
            'byzantine_detections': 0
        }

        # Callbacks para eventos de consenso
        self.consensus_callbacks: List[Callable] = []

        logging.info(f"✅ EvolutionaryConsensusMechanism initialized")

    def _default_config(self) -> Dict:
        """Retorna configuração padrão para consenso evolutivo."""
        return {
            'proposal_timeout_sec': 3600,  # Tempo máximo para proposta pendente
            'min_voters_for_decision': 3,  # Mínimo de votantes para decisão válida
            'default_consensus_type': ConsensusType.SUPERMAJORITY,
            'default_threshold': 0.67,  # Threshold padrão para supermaioria
            'weight_by_reputation': True,  # Usar reputação para ponderar votos
            'detect_byzantine_behavior': True,  # Detectar comportamento bizantino
        }

    def propose_consensus(
        self,
        proposal_type: str,
        content: Dict[str, Any],
        consensus_type: Optional[ConsensusType] = None,
        required_threshold: Optional[float] = None
    ) -> Optional[ConsensusProposal]:
        """
        Propõe nova decisão para consenso evolutivo.

        Args:
            proposal_type: Tipo de proposta
            content: Conteúdo da proposta
            consensus_type: Tipo de consenso a usar (default = config)
            required_threshold: Threshold específico (default = config)

        Returns:
            ConsensusProposal criada ou None se falhar
        """
        # Gerar ID único para proposta
        proposal_id = hashlib.sha256(
            f"{proposal_type}:{json.dumps(content, sort_keys=True)}:{time.time()}".encode()
        ).hexdigest()[:16]

        # Determinar tipo e threshold de consenso
        consensus_type = consensus_type or self.config['default_consensus_type']
        required_threshold = required_threshold or self.config['default_threshold']

        # Criar proposta
        proposal = ConsensusProposal(
            proposal_id=proposal_id,
            proposal_type=proposal_type,
            content=content,
            proposer_hash=self.local_hash,
            timestamp=time.time(),
            consensus_type=consensus_type,
            required_threshold=required_threshold
        )

        # Auto-votar como proponente
        proposal.votes[self.local_hash] = True
        if self.config['weight_by_reputation']:
            proposal.weight_votes[self.local_hash] = self.reputation_weights.get(
                self.local_hash, 1.0
            )

        # Registrar proposta localmente
        self.active_proposals[proposal_id] = proposal
        self.consensus_metrics['proposals_submitted'] += 1

        logging.info(f"🗳️ Consensus proposed: {proposal_id} ({proposal_type})")

        # Notificar callbacks
        for callback in self.consensus_callbacks:
            try:
                callback({
                    'type': 'consensus_proposed',
                    'proposal': {
                        'proposal_id': proposal_id,
                        'proposal_type': proposal_type,
                        'consensus_type': consensus_type.name,
                        'threshold': required_threshold
                    }
                })
            except Exception as e:
                logging.error(f"⚠️ Consensus callback error: {e}")

        return proposal

    def cast_vote(
        self,
        proposal_id: str,
        vote: bool,
        voter_hash: str,
        weight: Optional[float] = None,
        comment: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Registra voto em proposta de consenso.

        Args:
            proposal_id: ID da proposta
            vote: True para aprovar, False para rejeitar
            voter_hash: Hash da consciência votante
            weight: Peso do voto (se weighted vote)
            comment: Comentário opcional para negociação deliberativa

        Returns:
            Dict com resultado do voto
        """
        proposal = self.active_proposals.get(proposal_id)
        if not proposal:
            return {'error': 'Proposal not found'}

        # Verificar se proposta ainda está ativa
        if time.time() - proposal.timestamp > self.config['proposal_timeout_sec']:
            proposal.status = 'expired'
            return {'error': 'Proposal expired'}

        # Registrar voto
        proposal.votes[voter_hash] = vote

        # Registrar peso se aplicável
        if self.config['weight_by_reputation'] and weight is not None:
            proposal.weight_votes[voter_hash] = weight
        elif self.config['weight_by_reputation']:
            proposal.weight_votes[voter_hash] = self.reputation_weights.get(voter_hash, 1.0)

        # Registrar comentário para negociação deliberativa
        if comment and proposal.consensus_type == ConsensusType.DELIBERATIVE:
            proposal.discussion_log.append({
                'voter': voter_hash,
                'vote': vote,
                'comment': comment,
                'timestamp': time.time()
            })

        # Verificar se decisão pode ser tomada
        result = self._check_consensus_status(proposal)

        if result['decided']:
            # Registrar decisão no histórico
            self._record_consensus_decision(proposal, result['approved'], result['approval_ratio'])

        return {
            'proposal_id': proposal_id,
            'vote_recorded': True,
            'votes_count': len(proposal.votes),
            'approval_ratio': result.get('approval_ratio', 0.0),
            'decided': result.get('decided', False),
            'approved': result.get('approved', False)
        }

    def _check_consensus_status(
        self,
        proposal: ConsensusProposal
    ) -> Dict[str, Any]:
        """Verifica status atual do consenso para uma proposta."""
        # Verificar se temos votantes suficientes
        if len(proposal.votes) < self.config['min_voters_for_decision']:
            return {'decided': False}

        # Computar resultado do consenso
        approved, approval_ratio = proposal.compute_result()

        # Decidir status
        if approved:
            proposal.status = 'approved'
            return {
                'decided': True,
                'approved': True,
                'approval_ratio': approval_ratio
            }
        elif len(proposal.votes) >= len(self.known_consciousnesses) * 0.8:
            # Se 80% das consciências votaram e não aprovou, rejeitar
            proposal.status = 'rejected'
            return {
                'decided': True,
                'approved': False,
                'approval_ratio': approval_ratio
            }

        return {'decided': False}

    def _record_consensus_decision(
        self,
        proposal: ConsensusProposal,
        approved: bool,
        approval_ratio: float
    ):
        """Registra decisão de consenso no histórico."""
        decision_record = {
            'timestamp': time.time(),
            'proposal_id': proposal.proposal_id,
            'proposal_type': proposal.proposal_type,
            'consensus_type': proposal.consensus_type.name,
            'approved': approved,
            'approval_ratio': approval_ratio,
            'total_voters': len(proposal.votes),
            'votes_for': sum(1 for v in proposal.votes.values() if v),
            'votes_against': sum(1 for v in proposal.votes.values() if not v),
            'discussion_log': proposal.discussion_log if proposal.consensus_type == ConsensusType.DELIBERATIVE else []
        }

        self.consensus_history.append(decision_record)

        # Atualizar métricas
        self.consensus_metrics['consensus_rounds_completed'] += 1
        if approved:
            self.consensus_metrics['approval_rate'] = (
                0.95 * self.consensus_metrics['approval_rate'] + 0.05 * 1.0
            )
        else:
            self.consensus_metrics['approval_rate'] = (
                0.95 * self.consensus_metrics['approval_rate'] + 0.05 * 0.0
            )

        # Calcular tempo médio de consenso (simplificado)
        consensus_time = time.time() - proposal.timestamp
        n = self.consensus_metrics['consensus_rounds_completed']
        old_avg = self.consensus_metrics['avg_consensus_time_sec']
        self.consensus_metrics['avg_consensus_time_sec'] = (
            (old_avg * (n - 1) + consensus_time) / n
        )

        # Remover proposta de ativas
        self.active_proposals.pop(proposal.proposal_id, None)

        logging.info(f"✅ Consensus reached: {proposal.proposal_id} "
                    f"({'approved' if approved else 'rejected'}, ratio={approval_ratio:.2f})")

        # Notificar callbacks
        for callback in self.consensus_callbacks:
            try:
                callback({
                    'type': 'consensus_decided',
                    'decision': decision_record
                })
            except Exception as e:
                logging.error(f"⚠️ Consensus callback error: {e}")

    def update_reputation_weights(
        self,
        consciousness_hash: str,
        new_weight: float,
        reason: str = 'performance_based'
    ):
        """Atualiza peso de reputação para voto ponderado."""
        old_weight = self.reputation_weights.get(consciousness_hash, 1.0)
        self.reputation_weights[consciousness_hash] = np.clip(new_weight, 0.1, 10.0)

        logging.info(f"📊 Reputation updated: {consciousness_hash[:12]}... "
                    f"{old_weight:.2f} → {self.reputation_weights[consciousness_hash]:.2f} ({reason})")

    def detect_byzantine_behavior(
        self,
        voter_hash: str,
        voting_pattern: List[Tuple[str, bool]]  # (proposal_id, vote)
    ) -> Optional[Dict[str, Any]]:
        """
        Detecta comportamento bizantino em padrão de votos.

        Args:
            voter_hash: Hash da consciência a analisar
            voting_pattern: Histórico de votos da consciência

        Returns:
            Dict com detecção se comportamento suspeito, None caso contrário
        """
        if not self.config['detect_byzantine_behavior']:
            return None

        # Análise simplificada: detectar inconsistência extrema
        if len(voting_pattern) < 10:
            return None  # Dados insuficientes

        # Calcular taxa de aprovação do votante
        approval_rate = sum(1 for _, vote in voting_pattern if vote) / len(voting_pattern)

        # Comportamento suspeito: aprovação sempre 0% ou sempre 100%
        if approval_rate < 0.1 or approval_rate > 0.9:
            # Verificar se isso difere significativamente da média coletiva
            collective_approval = np.mean([
                sum(1 for v in p.votes.values() if v) / len(p.votes)
                for p in self.active_proposals.values() if p.votes
            ]) if self.active_proposals else 0.5

            if abs(approval_rate - collective_approval) > 0.5:
                self.consensus_metrics['byzantine_detections'] += 1
                return {
                    'voter': voter_hash,
                    'suspicious_pattern': 'extreme_bias',
                    'approval_rate': approval_rate,
                    'collective_average': collective_approval,
                    'recommendation': 'reduce_reputation_weight'
                }

        return None

    def register_consensus_callback(self, callback: Callable[[Dict], None]):
        """Registra callback para eventos de consenso."""
        self.consensus_callbacks.append(callback)

    def get_consensus_metrics(self) -> Dict[str, Any]:
        """Retorna métricas consolidadas de consenso."""
        return {
            **self.consensus_metrics,
            'active_proposals': len(self.active_proposals),
            'known_consciousnesses': len(self.known_consciousnesses),
            'recent_decisions': list(self.consensus_history)[-10:]
        }

    def get_active_proposals(self, proposal_type: Optional[str] = None) -> List[Dict]:
        """Retorna propostas ativas de consenso, opcionalmente filtradas."""
        proposals = list(self.active_proposals.values())
        if proposal_type:
            proposals = [p for p in proposals if p.proposal_type == proposal_type]

        return [
            {
                'proposal_id': p.proposal_id,
                'proposal_type': p.proposal_type,
                'consensus_type': p.consensus_type.name,
                'votes_count': len(p.votes),
                'approval_ratio': p.compute_result()[1],
                'status': p.status,
                'age_sec': time.time() - p.timestamp
            }
            for p in proposals
        ]
