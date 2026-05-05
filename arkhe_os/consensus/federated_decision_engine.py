#!/usr/bin/env python3
"""
federated_decision_engine.py — Motor de decisão baseado em consenso federado
sobre métricas agregadas da Wheeler Mesh.
"""

import asyncio
import time
import json
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass, field
from enum import Enum, auto
import logging

class DecisionOutcome(Enum):
    """Resultados possíveis de uma decisão federada."""
    APPROVED = auto()      # Decisão aprovada e executável
    REJECTED = auto()      # Decisão rejeitada
    DEFERRED = auto()      # Decisão adiada para mais dados
    EMERGENCY = auto()     # Decisão de emergência com quórum reduzido
    INVALID = auto()       # Decisão inválida (métricas inconsistentes)

@dataclass
class FederatedDecision:
    """Decisão alcançada via consenso federado."""
    decision_id: str
    decision_type: str
    metric_basis: Dict[str, Any]  # Métricas que fundamentaram a decisão
    action_spec: Dict[str, Any]   # Especificação da ação a executar
    outcome: DecisionOutcome
    consensus_votes: Dict[str, bool]  # Votos por nó validador
    timestamp: float
    execution_deadline: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return {
            'decision_id': self.decision_id,
            'decision_type': self.decision_type,
            'metric_basis': self.metric_basis,
            'action_spec': self.action_spec,
            'outcome': self.outcome.name,
            'consensus_votes': self.consensus_votes,
            'timestamp': self.timestamp,
            'execution_deadline': self.execution_deadline,
            'metadata': self.metadata
        }

class FederatedDecisionEngine:
    """
    Motor de decisão que traduz consenso federado em ações executáveis.
    Características:
    - Thresholds adaptativos baseados em criticidade da missão
    - Fallback para modo de emergência com quórum reduzido
    - Integração com sistemas de execução de ações
    - Auditoria de todas as decisões tomadas
    """

    def __init__(
        self,
        consensus_protocol: Any,
        metric_aggregator: Any,
        execution_callback: Optional[Callable[[FederatedDecision], bool]] = None,
        decision_thresholds: Optional[Dict[str, Dict]] = None
    ):
        self.consensus = consensus_protocol
        self.aggregator = metric_aggregator
        self.execution_callback = execution_callback

        # Thresholds de decisão por tipo
        self.thresholds = decision_thresholds or self._default_thresholds()

        # Decisões pendentes e executadas
        self.pending_decisions: Dict[str, FederatedDecision] = {}
        self.executed_decisions: List[FederatedDecision] = []

        # Callbacks para notificação de decisões
        self.decision_callbacks: List[Callable] = []

        # Métricas do motor de decisão
        self.decision_metrics = {
            'decisions_proposed': 0,
            'decisions_approved': 0,
            'decisions_rejected': 0,
            'emergency_decisions': 0,
            'avg_execution_time_sec': 0.0
        }

        # Registrar callback no consenso para novas decisões
        self.consensus.register_consensus_callback(self._on_consensus_decision)

        logging.info(f"✅ FederatedDecisionEngine initialized")

    def _default_thresholds(self) -> Dict[str, Dict]:
        """Retorna thresholds padrão para tipos de decisão."""
        return {
            'resource_allocation': {
                'min_consensus_votes': 0.67,  # 2/3 dos validadores
                'min_metric_confidence': 0.8,
                'emergency_quorum': 0.51,  # Maioria simples em emergência
                'max_execution_delay_sec': 300
            },
            'mission_priority': {
                'min_consensus_votes': 0.75,  # 3/4 dos validadores
                'min_metric_confidence': 0.9,
                'emergency_quorum': 0.6,
                'max_execution_delay_sec': 60
            },
            'security_response': {
                'min_consensus_votes': 0.8,  # 4/5 dos validadores
                'min_metric_confidence': 0.95,
                'emergency_quorum': 0.67,
                'max_execution_delay_sec': 10
            },
            'default': {
                'min_consensus_votes': 0.67,
                'min_metric_confidence': 0.8,
                'emergency_quorum': 0.51,
                'max_execution_delay_sec': 300
            }
        }

    async def propose_decision(
        self,
        decision_type: str,
        metric_aggregate: Dict[str, Any],
        proposed_action: Dict[str, Any],
        priority: float = 1.0,
        emergency_mode: bool = False
    ) -> Optional[FederatedDecision]:
        """
        Propõe nova decisão para consenso federado.

        Args:
            decision_type: Tipo de decisão ('resource_allocation', etc.)
            metric_aggregate: Métricas agregadas que fundamentam a decisão
            proposed_action: Ação proposta baseada nas métricas
            priority: Prioridade da decisão (para scheduling)
            emergency_mode: Se habilitar modo de emergência com quórum reduzido

        Returns:
            FederatedDecision ou None se proposta inválida
        """
        # Validar proposta contra thresholds
        thresholds = self.thresholds.get(decision_type, self.thresholds['default'])

        # Verificar confiança mínima das métricas
        metric_confidence = metric_aggregate.get('confidence', 1.0)
        if metric_confidence < thresholds['min_metric_confidence']:
            logging.warning(f"⚠️ Metric confidence {metric_confidence} below threshold {thresholds['min_metric_confidence']}")
            return None

        # Criar decisão pendente
        decision = FederatedDecision(
            decision_id=hashlib.sha256(
                f"{decision_type}:{json.dumps(metric_aggregate, sort_keys=True)}:{time.time()}".encode()
            ).hexdigest()[:16],
            decision_type=decision_type,
            metric_basis=metric_aggregate,
            action_spec=proposed_action,
            outcome=DecisionOutcome.APPROVED if emergency_mode else DecisionOutcome.DEFERRED,
            consensus_votes={},
            timestamp=time.time(),
            execution_deadline=time.time() + thresholds['max_execution_delay_sec'] if not emergency_mode else time.time() + 10,
            metadata={'emergency_mode': emergency_mode, 'priority': priority}
        )

        self.pending_decisions[decision.decision_id] = decision
        self.decision_metrics['decisions_proposed'] += 1

        # Submeter para consenso (se não for emergência imediata)
        if not emergency_mode:
            await self.consensus.propose_decision(
                decision_type=decision_type,
                metric_aggregate=metric_aggregate,
                proposed_action=proposed_action,
                priority=priority
            )
        else:
            # Modo emergência: aprovar com quórum reduzido
            decision.outcome = DecisionOutcome.EMERGENCY
            decision.consensus_votes = {self.consensus.node_id: True}  # Auto-aprovação inicial
            await self._execute_decision(decision)

        return decision

    def _on_consensus_decision(self, event: Dict):
        """Callback para decisões alcançadas via consenso."""
        if event.get('type') != 'consensus_decided':
            return

        proposal = event.get('proposal')
        if not proposal:
            return

        # Encontrar decisão pendente correspondente
        decision = None
        for dec_id, dec in self.pending_decisions.items():
            if dec.decision_type == proposal['decision_type']:
                # Verificar se métricas correspondem (simplificação)
                if dec.metric_basis == proposal['metric_aggregate']:
                    decision = dec
                    break

        if not decision:
            return

        # Atualizar decisão com resultado do consenso
        decision.outcome = DecisionOutcome.APPROVED
        decision.consensus_votes = proposal.get('consensus_votes', {})

        # Remover de pendentes e adicionar a executadas
        self.pending_decisions.pop(decision.decision_id, None)
        self.executed_decisions.append(decision)
        self.decision_metrics['decisions_approved'] += 1

        # Executar ação se aprovada
        if decision.outcome == DecisionOutcome.APPROVED:
            asyncio.create_task(self._execute_decision(decision))

        # Notificar callbacks
        for callback in self.decision_callbacks:
            try:
                callback(decision.to_dict())
            except Exception as e:
                logging.error(f"⚠️ Decision callback error: {e}")

    async def _execute_decision(self, decision: FederatedDecision) -> bool:
        """Executa ação especificada em decisão aprovada."""
        start_time = time.time()

        try:
            # Validar deadline de execução
            if decision.execution_deadline and time.time() > decision.execution_deadline:
                logging.warning(f"⚠️ Decision {decision.decision_id} execution deadline passed")
                decision.outcome = DecisionOutcome.INVALID
                return False

            # Chamar callback de execução (em produção: integrar com sistema de ações)
            if self.execution_callback:
                success = self.execution_callback(decision)
                if not success:
                    decision.outcome = DecisionOutcome.REJECTED
                    return False

            # Registrar execução bem-sucedida
            decision.outcome = DecisionOutcome.APPROVED
            logging.info(f"✅ Decision {decision.decision_id} executed successfully")

            # Atualizar métricas
            execution_time = time.time() - start_time
            old_avg = self.decision_metrics['avg_execution_time_sec']
            n = self.decision_metrics['decisions_approved']
            self.decision_metrics['avg_execution_time_sec'] = (
                (old_avg * (n - 1) + execution_time) / n if n > 1 else execution_time
            )

            return True

        except Exception as e:
            logging.error(f"❌ Decision execution failed: {e}")
            decision.outcome = DecisionOutcome.REJECTED
            return False

    def register_decision_callback(self, callback: Callable[[Dict], None]):
        """Registra callback para notificação de decisões."""
        self.decision_callbacks.append(callback)

    def get_pending_decisions(self, decision_type: Optional[str] = None) -> List[Dict]:
        """Retorna decisões pendentes, opcionalmente filtradas por tipo."""
        decisions = list(self.pending_decisions.values())
        if decision_type:
            decisions = [d for d in decisions if d.decision_type == decision_type]
        return [d.to_dict() for d in sorted(decisions, key=lambda d: d.timestamp, reverse=True)]

    def get_executed_decisions(
        self,
        decision_type: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict]:
        """Retorna decisões executadas, opcionalmente filtradas."""
        decisions = self.executed_decisions
        if decision_type:
            decisions = [d for d in decisions if d.decision_type == decision_type]
        return [d.to_dict() for d in sorted(decisions, key=lambda d: d.timestamp, reverse=True)[:limit]]

    def get_decision_metrics(self) -> Dict[str, Any]:
        """Retorna métricas consolidadas do motor de decisão."""
        return {
            **self.decision_metrics,
            'pending_decisions_count': len(self.pending_decisions),
            'executed_decisions_count': len(self.executed_decisions),
            'approval_rate': (
                self.decision_metrics['decisions_approved'] /
                max(1, self.decision_metrics['decisions_proposed'])
            ) * 100
        }
