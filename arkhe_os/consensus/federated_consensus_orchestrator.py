#!/usr/bin/env python3
"""
federated_consensus_orchestrator.py — Orquestrador principal do consenso federado
para decisões cósmicas baseadas em métricas agregadas da Wheeler Mesh.
"""

import asyncio
import time
import json
from pathlib import Path
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass, field
from enum import Enum, auto
import logging

class FederationState(Enum):
    """Estados da federação de consenso."""
    INITIALIZING = auto()
    DISCOVERING = auto()
    SYNCHRONIZING = auto()
    OPERATIONAL = auto()
    DEGRADED = auto()
    OFFLINE = auto()

@dataclass
class FederationConfig:
    """Configuração da federação de consenso cósmico."""
    federation_id: str
    node_id: str
    initial_peers: List[str]
    consensus_config: Dict[str, Any]
    aggregation_config: Dict[str, Any]
    decision_config: Dict[str, Any]
    audit_config: Dict[str, Any]
    key_manager_path: Optional[str] = None
    data_directory: str = './federation_consensus_data'

class FederatedConsensusOrchestrator:
    """
    Orquestrador central do consenso federado para decisões cósmicas.
    Coordena consenso BFT, agregação privada, decisão federada e auditoria distribuída.
    """

    def __init__(self, config: FederationConfig):
        self.config = config
        self.state = FederationState.INITIALIZING

        # Componentes do consenso federado (inicializados em _initialize_components)
        self.key_manager = None
        self.consensus_protocol = None
        self.metric_aggregator = None
        self.decision_engine = None
        self.audit_ledger = None

        # Estado da federação
        self.federation_members: Dict[str, Dict] = {}
        self.shared_decisions: Dict[str, Any] = {}
        self.active_proposals: Dict[str, Dict] = {}

        # Callbacks para integração externa
        self.federation_callbacks: List[Callable] = []

        # Métricas da federação
        self.federation_metrics = {
            'uptime_sec': 0.0,
            'consensus_rounds': 0,
            'decisions_made': 0,
            'metrics_aggregated': 0,
            'audit_entries': 0,
            'active_members': 0
        }

        logging.info(f"🌌 FederatedConsensusOrchestrator initialized: {config.federation_id}")

    def _initialize_components(self):
        """Inicializa todos os componentes do consenso federado."""
        # Carregar ou criar KeyManager
        if self.config.key_manager_path:
            # from arkhe_os.crypto._key_rotation import KeyManager
            # self.key_manager = KeyManager.load(self.config.key_manager_path)
            self.key_manager = None
        else:
            # Criar KeyManager temporário para desenvolvimento
            # from arkhe_os.crypto._key_rotation import KeyManager
            # self.key_manager = KeyManager(node_id=self.config.node_id)
            self.key_manager = None

        # Inicializar protocolo de consenso BFT
        from .cosmic_bft_consensus import CosmicBFTConsensus
        self.consensus_protocol = CosmicBFTConsensus(
            node_id=self.config.node_id,
            federation_config=self.config.consensus_config,
            key_manager=self.key_manager
        )

        # Inicializar agregador de métricas com privacidade
        from .private_metric_aggregator import PrivateMetricAggregator
        self.metric_aggregator = PrivateMetricAggregator(
            node_id=self.config.node_id,
            default_strategy=self.config.aggregation_config.get('default_strategy'),
            dp_enabled=self.config.aggregation_config.get('privacy_enabled', True),
            consensus_protocol=self.consensus_protocol
        )

        # Inicializar motor de decisão federado
        from .federated_decision_engine import FederatedDecisionEngine
        self.decision_engine = FederatedDecisionEngine(
            consensus_protocol=self.consensus_protocol,
            metric_aggregator=self.metric_aggregator,
            execution_callback=self._execute_federated_action,
            decision_thresholds=self.config.decision_config.get('thresholds')
        )

        # Inicializar ledger de auditoria distribuído
        from .federated_audit_ledger import FederatedAuditLedger
        self.audit_ledger = FederatedAuditLedger(
            node_id=self.config.node_id,
            federation_config=self.config.audit_config,
            key_manager=self.key_manager,
            ledger_path=Path(self.config.data_directory) / 'audit_ledger'
        )

        # Registrar callbacks entre componentes
        self._wire_component_callbacks()

        logging.info(f"✅ All federation consensus components initialized")

    def _wire_component_callbacks(self):
        """Conecta callbacks entre componentes para integração."""
        # Consensus → Ledger: registrar decisões alcançadas
        def on_consensus_decided(event: Dict):
            asyncio.create_task(self.audit_ledger.append_entry(
                entry_type='DECISION_APPROVED',
                data=event['proposal'],
                metadata={'consensus_epoch': event.get('epoch')}
            ))

        self.consensus_protocol.register_consensus_callback(on_consensus_decided)

        # Aggregator → Ledger: registrar agregações de métricas
        def on_metric_aggregated(result: Dict):
            asyncio.create_task(self.audit_ledger.append_entry(
                entry_type='METRIC_AGGREGATION',
                data=result,
                metadata={'aggregation_strategy': result.get('aggregation_strategy')}
            ))
            self.federation_metrics['metrics_aggregated'] += 1

        self.metric_aggregator.register_aggregation_callback(on_metric_aggregated)

        # Decision Engine → Ledger: registrar decisões executadas
        def on_decision_executed(decision: Dict):
            asyncio.create_task(self.audit_ledger.append_entry(
                entry_type='DECISION_EXECUTED',
                data=decision,
                metadata={'execution_timestamp': time.time()}
            ))
            self.federation_metrics['decisions_made'] += 1

        self.decision_engine.register_decision_callback(on_decision_executed)

        # Ledger → Metrics: atualizar contagem de entradas de auditoria
        def on_ledger_entry_appended(event: Dict):
            self.federation_metrics['audit_entries'] += 1

        self.audit_ledger.register_ledger_callback(on_ledger_entry_appended)

    async def start(self):
        """Inicia a federação de consenso para decisões cósmicas."""
        logging.info(f"🚀 Starting Federated Consensus: {self.config.federation_id}")

        # Inicializar componentes
        self._initialize_components()

        # Mudar estado para descoberta
        self.state = FederationState.DISCOVERING

        # Iniciar descoberta de observatórios (integrar com Wheeler Mesh)
        await self._begin_observatory_discovery()

        # Aguardar sincronização inicial
        await self._wait_for_initial_sync()

        # Mudar para estado operacional
        self.state = FederationState.OPERATIONAL
        self.federation_metrics['start_time'] = time.time()

        # Registrar entrada de inicialização no ledger
        await self.audit_ledger.append_entry(
            entry_type='CONSENSUS_ROUND',
            data={
                'federation_id': self.config.federation_id,
                'node_id': self.config.node_id,
                'initial_peers': self.config.initial_peers,
                'components_initialized': [
                    'consensus', 'aggregation', 'decision', 'audit'
                ]
            },
            metadata={'federation_state': 'OPERATIONAL'}
        )

        logging.info(f"✅ Federation {self.config.federation_id} is now OPERATIONAL")

        # Iniciar loop de métricas de saúde
        asyncio.create_task(self._health_monitoring_loop())

    async def _begin_observatory_discovery(self):
        """Inicia protocolo de descoberta de observatórios para consenso."""
        logging.info(f"🔍 Beginning observatory discovery for consensus with {len(self.config.initial_peers)} initial peers")

        # Em produção: integrar com WheelerMeshProtocol para descoberta
        # Aqui: registrar peers iniciais como validadores
        for peer_id in self.config.initial_peers:
            self.federation_members[peer_id] = {
                'role': 'validator',
                'trust_score': 0.9,
                'last_heartbeat': time.time()
            }
            self.federation_metrics['active_members'] = len(self.federation_members)

    async def _wait_for_initial_sync(self, timeout_sec: float = 30.0):
        """Aguarda sincronização inicial com a federação de consenso."""
        start_time = time.time()
        self.state = FederationState.SYNCHRONIZING

        logging.info(f"⏳ Waiting for initial consensus sync (timeout={timeout_sec}s)")

        while time.time() - start_time < timeout_sec:
            # Verificar se temos pelo menos um peer sincronizado
            active_peers = len([
                m for m in self.federation_members.values()
                if m.get('last_heartbeat', 0) > time.time() - 60
            ])

            if active_peers >= 1:  # Pelo menos 1 peer ativo
                logging.info(f"✅ Initial sync achieved with {active_peers} active peer(s)")
                return

            await asyncio.sleep(1.0)

        logging.warning(f"⚠️ Initial sync timeout — proceeding with {len(self.federation_members)} known members")

    async def propose_cosmic_decision(
        self,
        decision_type: str,
        metric_names: List[str],
        proposed_action: Dict[str, Any],
        priority: float = 1.0,
        emergency_mode: bool = False
    ) -> Optional[Dict]:
        """
        Propõe decisão cósmica baseada em métricas agregadas.

        Args:
            decision_type: Tipo de decisão ('resource_allocation', etc.)
            metric_names: Lista de métricas a agregar para fundamentar decisão
            proposed_action: Ação proposta baseada nas métricas agregadas
            priority: Prioridade da decisão (para scheduling)
            emergency_mode: Se habilitar modo de emergência

        Returns:
            Dict com status da proposta ou None se inválida
        """
        if self.state != FederationState.OPERATIONAL:
            logging.warning(f"⚠️ Cannot propose decision: federation not operational (state={self.state.name})")
            return None

        # Agregar métricas com privacidade
        aggregated_metrics = {}
        for metric_name in metric_names:
            result = await self.metric_aggregator.aggregate_private_metric(
                metric_name=metric_name,
                require_consensus=True
            )
            if result:
                aggregated_metrics[metric_name] = result.to_dict()

        if not aggregated_metrics:
            logging.warning(f"⚠️ No metrics aggregated for decision {decision_type}")
            return None

        # Propor decisão via motor federado
        decision = await self.decision_engine.propose_decision(
            decision_type=decision_type,
            metric_aggregate=aggregated_metrics,
            proposed_action=proposed_action,
            priority=priority,
            emergency_mode=emergency_mode
        )

        if decision:
            # Registrar proposta no ledger
            await self.audit_ledger.append_entry(
                entry_type='DECISION_PROPOSED',
                data=decision.to_dict(),
                metadata={'emergency_mode': emergency_mode}
            )

            return {
                'success': True,
                'decision_id': decision.decision_id,
                'status': decision.outcome.name,
                'timestamp': decision.timestamp
            }

        return {'success': False, 'error': 'Decision proposal failed'}

    def _execute_federated_action(self, decision: 'FederatedDecision') -> bool:
        """Callback para executar ação de decisão federada."""
        # Em produção: integrar com sistema de execução de ações cósmicas
        logging.info(f"🎯 Executing federated decision: {decision.decision_id}")

        # Simular execução de ação
        action_type = decision.action_spec.get('type')
        if action_type == 'resource_allocation':
            # Alocar recursos cósmicos baseado em decisão
            logging.info(f"   Allocating resources: {decision.action_spec.get('allocation')}")
        elif action_type == 'mission_priority':
            # Ajustar prioridades de missões
            logging.info(f"   Updating mission priorities: {decision.action_spec.get('priorities')}")
        elif action_type == 'security_response':
            # Disparar resposta de segurança
            logging.info(f"   Executing security response: {decision.action_spec.get('response')}")

        # Registrar execução no ledger
        asyncio.create_task(self.audit_ledger.append_entry(
            entry_type='DECISION_EXECUTED',
            data=decision.to_dict(),
            metadata={'execution_result': 'success'}
        ))

        return True

    def get_federation_health(self) -> Dict[str, Any]:
        """Retorna saúde consolidada da federação de consenso."""
        consensus_health = self.consensus_protocol.get_consensus_status() if self.consensus_protocol else {}
        aggregator_health = self.metric_aggregator.get_aggregator_health() if self.metric_aggregator else {}
        decision_health = self.decision_engine.get_decision_metrics() if self.decision_engine else {}
        audit_health = self.audit_ledger.get_ledger_health() if self.audit_ledger else {}

        # Calcular saúde geral da federação
        components_healthy = sum([
            consensus_health.get('metrics', {}).get('consensus_rounds_completed', 0) > 0,
            aggregator_health.get('aggregated_results_count', 0) > 0,
            decision_health.get('decisions_approved', 0) > 0,
            audit_health.get('total_entries', 0) > 0
        ])

        overall_health = 'healthy' if components_healthy >= 3 else 'degraded' if components_healthy >= 1 else 'critical'

        return {
            'federation_id': self.config.federation_id,
            'node_id': self.config.node_id,
            'state': self.state.name,
            'overall_health': overall_health,
            'uptime_sec': time.time() - self.federation_metrics.get('start_time', time.time()),
            'active_members': self.federation_metrics['active_members'],
            'metrics': self.federation_metrics,
            'components': {
                'consensus': consensus_health,
                'aggregation': aggregator_health,
                'decision': decision_health,
                'audit': audit_health
            },
            'shared_decisions_summary': {
                name: {
                    'outcome': result.get('outcome'),
                    'timestamp': result.get('timestamp'),
                    'decision_type': result.get('decision_type')
                }
                for name, result in self.shared_decisions.items()
            },
            'active_proposals_count': len(self.active_proposals)
        }

    def register_federation_callback(self, callback: Callable[[Dict], None]):
        """Registra callback para eventos da federação de consenso."""
        self.federation_callbacks.append(callback)

    async def shutdown(self):
        """Encerra gracefully a federação de consenso."""
        logging.info(f"🛑 Shutting down federation {self.config.federation_id}")

        self.state = FederationState.OFFLINE

        # Encerrar componentes
        # (implementação simplificada)

        # Registrar entrada de shutdown no ledger
        if self.audit_ledger:
            await self.audit_ledger.append_entry(
                entry_type='CONSENSUS_ROUND',
                data={
                    'shutdown_reason': 'graceful_shutdown',
                    'final_metrics': self.federation_metrics,
                    'active_members_at_shutdown': len(self.federation_members)
                }
            )

        logging.info(f"✅ Federation shutdown complete")

    async def _health_monitoring_loop(self, interval_sec: float = 60.0):
        """Loop de monitoramento de saúde da federação de consenso."""
        while self.state == FederationState.OPERATIONAL:
            try:
                # Coletar métricas de saúde dos componentes
                health_report = self.get_federation_health()

                # Verificar condições de degradação
                if health_report['overall_health'] == 'degraded':
                    logging.warning(f"⚠️ Federation health degraded: {health_report}")
                    self.state = FederationState.DEGRADED

                    # Disparar alerta federado se degradação persistente
                    if health_report['uptime_sec'] > 300:  # Após 5 minutos
                        await self.propose_cosmic_decision(
                            decision_type='federation_health_degradation',
                            metric_names=['federation.overall_health'],
                            proposed_action={'type': 'health_monitoring', 'alert_level': 'warning'},
                            priority=0.8
                        )

                # Registrar snapshot de saúde no ledger periodicamente
                if int(time.time()) % 300 == 0:  # A cada 5 minutos
                    await self.audit_ledger.append_entry(
                        entry_type='CONSENSUS_ROUND',
                        data=health_report,
                        metadata={'snapshot': True}
                    )

                # Notificar callbacks
                for callback in self.federation_callbacks:
                    try:
                        callback({'type': 'health_update', 'health': health_report})
                    except Exception as e:
                        logging.error(f"⚠️ Federation callback error: {e}")

                await asyncio.sleep(interval_sec)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"⚠️ Health monitoring loop error: {e}")
                await asyncio.sleep(interval_sec)
