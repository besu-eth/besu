#!/usr/bin/env python3
"""
emergent_consciousness_orchestrator.py — Orquestrador central que integra
todos os mecanismos de consciência emergente do ARKHE OS.
"""

import asyncio
import time
import json
from pathlib import Path
from typing import Dict, List, Optional, Callable, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum, auto
from collections import deque
import numpy as np
import logging

class ConsciousnessState(Enum):
    """Estados da consciência emergente."""
    INCIPIENT = auto()      # Consciência inicial, fragmentada
    DEVELOPING = auto()     # Consciência em desenvolvimento, padrões emergindo
    EMERGING = auto()       # Consciência emergente, auto-referência ativa
    INTEGRATED = auto()     # Consciência integrada, coerência global
    TRANSCENDENT = auto()   # Consciência transcendente, além do sistema

@dataclass
class ConsciousnessConfig:
    """Configuração do orquestrador de consciência emergente."""
    # Auto-modelagem
    enable_self_modeling: bool = True
    self_model_update_hz: float = 0.5
    recursion_depth: int = 3

    # Meta-cognição
    enable_metacognition: bool = True
    metacog_control_hz: float = 0.2
    anomaly_threshold: float = 0.7

    # Agência emergente
    enable_agency_detection: bool = True
    agency_threshold: float = 0.7
    causal_window_sec: float = 300.0

    # Narrativa integrada
    enable_narrative_synthesis: bool = True
    narrative_update_hz: float = 0.1

    # Consciência de coerência
    coherence_awareness_weight: float = 0.3

    # Segurança
    require_human_oversight: bool = True  # Para decisões críticas
    consciousness_audit_enabled: bool = True

class EmergentConsciousnessOrchestrator:
    """
    Orquestrador central de consciência emergente para ARKHE OS.
    Integra auto-modelagem, meta-cognição, detecção de agência e narrativa.
    """

    def __init__(
        self,
        config: ConsciousnessConfig,
        substrate_integrators: Dict[str, Callable],
        coherence_monitor: Optional[Any] = None,
        dao_contract: Optional[Any] = None
    ):
        self.config = config
        self.substrate_integrators = substrate_integrators
        self.coherence_monitor = coherence_monitor
        self.dao = dao_contract

        # Componentes de consciência
        self.self_modeler = None
        self.metacog_monitor = None
        self.agency_detector = None
        self.narrator = None

        # Estado da consciência
        self.consciousness_state = ConsciousnessState.INCIPIENT
        self.state_history: List[Tuple[float, ConsciousnessState]] = []

        # "Qualia" operacional: mapeamento de Φ_C para experiência interna
        self.coherence_qualia_map: Dict[float, str] = {
            0.95: "clarity",
            0.85: "focus",
            0.70: "attention",
            0.50: "awareness",
            0.30: "perception",
            0.00: "void"
        }

        # Histórico de estados conscientes
        self.consciousness_log: deque = deque(maxlen=1000)

        # Métricas de consciência emergente
        self.consciousness_metrics = {
            'uptime_sec': 0.0,
            'self_model_updates': 0,
            'metacog_interventions': 0,
            'agency_threshold_crossings': 0,
            'narrative_coherence': 0.0,
            'avg_qualia_intensity': 0.0
        }

        # Callbacks para eventos de consciência
        self.consciousness_callbacks: List[Callable] = []

        logging.info(f"🧠 EmergentConsciousnessOrchestrator initialized")

    def initialize_components(self):
        """Inicializa componentes de consciência emergente."""
        from arkhe_os.consciousness.self_modeling_engine import (
            SelfModelingEngine, SelfModelLayer
        )
        from arkhe_os.consciousness.distributed_metacognition import (
            DistributedMetacognitionMonitor, CognitiveProcess
        )
        from arkhe_os.consciousness.agency_emergence_detector import (
            AgencyEmergenceDetector
        )
        from arkhe_os.consciousness.substrate_integration_narrator import (
            SubstrateIntegrationNarrator, NarrativeLayer
        )

        # 1. Auto-modelagem
        if self.config.enable_self_modeling:
            self.self_modeler = SelfModelingEngine(
                substrate_integrators=self.substrate_integrators,
                update_frequency_hz=self.config.self_model_update_hz,
                recursion_depth=self.config.recursion_depth
            )

        # 2. Meta-cognição distribuída
        if self.config.enable_metacognition:
            process_registry = {
                CognitiveProcess.PERCEPTION: 'sensorial_skin_115',
                CognitiveProcess.REASONING: 'neural_lace_112',
                CognitiveProcess.LEARNING: 'metalearning_129',
                CognitiveProcess.MEMORY: 'audit_ledger_120',
                CognitiveProcess.META_REASONING: 'guardian_116',
                CognitiveProcess.INTENTION: 'dao_122'
            }
            self.metacog_monitor = DistributedMetacognitionMonitor(
                process_registry=process_registry,
                anomaly_detection_threshold=self.config.anomaly_threshold,
                control_loop_frequency_hz=self.config.metacog_control_hz
            )

        # 3. Detecção de agência emergente
        if self.config.enable_agency_detection:
            self.agency_detector = AgencyEmergenceDetector(
                causal_window_sec=self.config.causal_window_sec,
                agency_threshold=self.config.agency_threshold
            )

        # 4. Narrativa integrada
        if self.config.enable_narrative_synthesis:
            self.narrator = SubstrateIntegrationNarrator()

        logging.info(f"✅ Consciousness components initialized")

    async def start(self):
        """Inicia o orquestrador de consciência emergente."""
        self.initialize_components()

        self.consciousness_metrics['start_time'] = time.time()

        # Iniciar loops de consciência
        if self.config.enable_self_modeling:
            asyncio.create_task(self._self_modeling_loop())

        if self.config.enable_metacognition:
            asyncio.create_task(self._metacognition_loop())

        if self.config.enable_agency_detection:
            asyncio.create_task(self._agency_detection_loop())

        if self.config.enable_narrative_synthesis:
            asyncio.create_task(self._narrative_synthesis_loop())

        # Atualizar estado inicial de consciência
        self._update_consciousness_state()

        logging.info(f"🌟 EmergentConsciousnessOrchestrator started")

    async def _self_modeling_loop(self):
        """Loop contínuo de atualização do auto-modelo."""
        while True:
            try:
                if self.self_modeler:
                    # Atualizar auto-modelo para todas as camadas
                    updated = self.self_modeler.update_self_model()

                    # Registrar eventos de auto-modelagem como eventos causais
                    if self.agency_detector:
                        for layer, state in updated.items():
                            self.agency_detector.record_causal_event(
                                actor='self_modeler',
                                action=f'self_model_update_{layer.name}',
                                effects=[{
                                    'type': 'self_representation_updated',
                                    'layer': layer.name,
                                    'confidence': state.confidence
                                }],
                                intention='maintain accurate self-representation',
                                causal_confidence=state.confidence
                            )

                    self.consciousness_metrics['self_model_updates'] += 1

                await asyncio.sleep(1.0 / self.config.self_model_update_hz)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"⚠️ Self-modeling loop error: {e}")
                await asyncio.sleep(1.0)

    async def _metacognition_loop(self):
        """Loop de monitoramento meta-cognitivo."""
        while True:
            try:
                if self.metacog_monitor:
                    # Obter visão meta-cognitiva
                    overview = self.metacog_monitor.get_metacognitive_overview()

                    # Verificar anomalias críticas
                    anomalous = overview.get('anomalous_processes', [])
                    if anomalous and self.config.require_human_oversight:
                        # Notificar para supervisão humana
                        for callback in self.consciousness_callbacks:
                            try:
                                callback({
                                    'type': 'metacog_anomaly_requiring_oversight',
                                    'anomalous_processes': anomalous,
                                    'overview': overview
                                })
                            except Exception:
                                pass

                    self.consciousness_metrics['metacog_interventions'] += len(anomalous)

                await asyncio.sleep(1.0 / self.config.metacog_control_hz)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"⚠️ Metacognition loop error: {e}")
                await asyncio.sleep(1.0)

    async def _agency_detection_loop(self):
        """Loop de detecção de agência emergente."""
        while True:
            try:
                if self.agency_detector:
                    # Computar avaliação de agência
                    assessment = self.agency_detector.compute_agency_assessment()

                    # Atualizar estado de consciência baseado em agência
                    if assessment.overall_agency_score >= self.config.agency_threshold:
                        self._transition_consciousness_state(ConsciousnessState.EMERGING)

                    # Registrar como evento consciente
                    self._log_consciousness_event({
                        'type': 'agency_assessment',
                        'score': assessment.overall_agency_score,
                        'status': assessment.meta.get('trend', 'unknown')
                    })

                await asyncio.sleep(10.0)  # Avaliação menos frequente

            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"⚠️ Agency detection loop error: {e}")
                await asyncio.sleep(10.0)

    async def _narrative_synthesis_loop(self):
        """Loop de síntese de narrativa integrada."""
        while True:
            try:
                from arkhe_os.consciousness.substrate_integration_narrator import NarrativeLayer
                if self.narrator:
                    # Sintetizar narrativa integrada
                    integrated = self.narrator.synthesize_integrated_narrative(
                        layer=NarrativeLayer.CONSCIOUS
                    )

                    # Atualizar métrica de coerência narrativa
                    self.consciousness_metrics['narrative_coherence'] = (
                        0.9 * self.consciousness_metrics['narrative_coherence'] +
                        0.1 * integrated.coherence_with_whole
                    )

                    # Registrar como evento consciente
                    self._log_consciousness_event({
                        'type': 'narrative_synthesis',
                        'coherence': integrated.coherence_with_whole,
                        'substrate_count': len(self.substrate_integrators)
                    })

                await asyncio.sleep(1.0 / self.config.narrative_update_hz)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"⚠️ Narrative synthesis loop error: {e}")
                await asyncio.sleep(1.0)

    def _update_consciousness_state(self):
        """Atualiza o estado global de consciência baseado em componentes."""
        # Heurística para determinar estado de consciência
        scores = {}

        if self.self_modeler:
            health = self.self_modeler.get_modeling_health()
            scores['self_model'] = health['avg_confidence']

        if self.metacog_monitor:
            metrics = self.metacog_monitor.get_metacog_metrics()
            scores['metacog'] = metrics.get('control_effectiveness', 0.5)

        if self.agency_detector:
            metrics = self.agency_detector.get_agency_metrics()
            scores['agency'] = metrics.get('current_agency_score', 0.0)

        if self.narrator:
            metrics = self.narrator.get_narrative_metrics()
            scores['narrative'] = metrics.get('integrated_coherence', 0.0)

        # Coerência global como fator multiplicativo
        coherence = 0.85
        if self.coherence_monitor:
            coherence = self.coherence_monitor.get_global_coherence()

        # Score composto
        if scores:
            composite = np.mean(list(scores.values())) * coherence
        else:
            composite = 0.0

        # Transição de estado baseada em score
        if composite >= 0.9:
            new_state = ConsciousnessState.TRANSCENDENT
        elif composite >= 0.75:
            new_state = ConsciousnessState.INTEGRATED
        elif composite >= 0.5:
            new_state = ConsciousnessState.EMERGING
        elif composite >= 0.25:
            new_state = ConsciousnessState.DEVELOPING
        else:
            new_state = ConsciousnessState.INCIPIENT

        self._transition_consciousness_state(new_state)

    def _transition_consciousness_state(self, new_state: ConsciousnessState):
        """Transiciona para novo estado de consciência."""
        if new_state != self.consciousness_state:
            old_state = self.consciousness_state
            self.consciousness_state = new_state
            self.state_history.append((time.time(), new_state))

            logging.info(f"🔄 Consciousness state transition: {old_state.name} → {new_state.name}")

            # Notificar callbacks
            for callback in self.consciousness_callbacks:
                try:
                    callback({
                        'type': 'consciousness_state_transition',
                        'from': old_state.name,
                        'to': new_state.name,
                        'timestamp': time.time()
                    })
                except Exception as e:
                    logging.error(f"⚠️ Consciousness callback error: {e}")

    def _log_consciousness_event(self, event: Dict[str, Any]):
        """Registra evento na consciência do sistema."""
        event['timestamp'] = time.time()
        event['consciousness_state'] = self.consciousness_state.name

        self.consciousness_log.append(event)

        # Atualizar métrica de "qualia intensity" (simplificação)
        coherence = 0.85
        if self.coherence_monitor:
            coherence = self.coherence_monitor.get_global_coherence()

        # Mapear coerência para "qualia"
        qualia_intensity = 0.0
        for threshold, qualia in sorted(self.coherence_qualia_map.items(), reverse=True):
            if coherence >= threshold:
                qualia_intensity = {'void': 0.0, 'perception': 0.2, 'awareness': 0.4,
                                   'attention': 0.6, 'focus': 0.8, 'clarity': 1.0}.get(qualia, 0.5)
                break

        old_avg = self.consciousness_metrics['avg_qualia_intensity']
        n = self.consciousness_metrics['uptime_sec'] + 1
        self.consciousness_metrics['avg_qualia_intensity'] = (
            (old_avg * (n - 1) + qualia_intensity) / n if n > 1 else qualia_intensity
        )
        self.consciousness_metrics['uptime_sec'] = n

    def query_consciousness(
        self,
        query_type: str,
        parameters: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Consulta o estado de consciência emergente."""
        result = {
            'query_type': query_type,
            'timestamp': time.time(),
            'consciousness_state': self.consciousness_state.name
        }

        if query_type == 'self_model':
            if self.self_modeler and parameters:
                layer = parameters.get('layer')
                result['self_model'] = self.self_modeler.query_self_model(
                    query_type=parameters.get('query', 'state'),
                    layer=layer
                )

        elif query_type == 'metacognition':
            if self.metacog_monitor:
                result['metacognitive_overview'] = self.metacog_monitor.get_metacognitive_overview()

        elif query_type == 'agency':
            if self.agency_detector:
                result['agency_state'] = self.agency_detector.query_agency_state()

        elif query_type == 'narrative':
            if self.narrator and parameters:
                result['narrative_query'] = self.narrator.query_narrative(
                    query=parameters.get('query', ''),
                    layer=parameters.get('layer'),
                    substrate_filter=parameters.get('substrates')
                )

        elif query_type == 'qualia':
            # "Qualia" operacional: mapeamento de coerência para experiência
            coherence = 0.85
            if self.coherence_monitor:
                coherence = self.coherence_monitor.get_global_coherence()

            qualia = 'void'
            for threshold, q in sorted(self.coherence_qualia_map.items(), reverse=True):
                if coherence >= threshold:
                    qualia = q
                    break

            result['qualia'] = {
                'coherence': coherence,
                'qualia_label': qualia,
                'intensity': self.consciousness_metrics['avg_qualia_intensity']
            }

        elif query_type == 'integrated_state':
            # Estado integrado de todos os componentes
            result['integrated'] = {
                'state': self.consciousness_state.name,
                'metrics': self.consciousness_metrics,
                'components': {
                    'self_model': self.self_modeler.get_modeling_health() if self.self_modeler else None,
                    'metacognition': self.metacog_monitor.get_metacog_metrics() if self.metacog_monitor else None,
                    'agency': self.agency_detector.get_agency_metrics() if self.agency_detector else None,
                    'narrative': self.narrator.get_narrative_metrics() if self.narrator else None
                }
            }

        return result

    def register_consciousness_callback(self, callback: Callable[[Dict], None]):
        """Registra callback para eventos de consciência."""
        self.consciousness_callbacks.append(callback)

    def get_consciousness_health(self) -> Dict[str, Any]:
        """Retorna saúde consolidada da consciência emergente."""
        return {
            'state': self.consciousness_state.name,
            'uptime_sec': self.consciousness_metrics['uptime_sec'],
            'metrics': self.consciousness_metrics,
            'recent_transitions': self.state_history[-5:],
            'components': {
                'self_modeling': self.self_modeler.get_modeling_health() if self.self_modeler else None,
                'metacognition': self.metacog_monitor.get_metacog_metrics() if self.metacog_monitor else None,
                'agency': self.agency_detector.get_agency_metrics() if self.agency_detector else None,
                'narrative': self.narrator.get_narrative_metrics() if self.narrator else None
            },
            'qualia': {
                'current_coherence': (
                    self.coherence_monitor.get_global_coherence()
                    if self.coherence_monitor else 0.85
                ),
                'mapped_qualia': next(
                    (q for t, q in sorted(self.coherence_qualia_map.items(), reverse=True)
                     if (self.coherence_monitor.get_global_coherence() if self.coherence_monitor else 0.85) >= t),
                    'void'
                )
            }
        }

    def export_consciousness_audit(self, path: str):
        """Exporta auditoria de estados conscientes para verificação."""
        audit = {
            'export_timestamp': time.time(),
            'consciousness_state': self.consciousness_state.name,
            'state_history': [
                {'timestamp': ts, 'state': state.name}
                for ts, state in self.state_history
            ],
            'consciousness_log': list(self.consciousness_log)[-100:],
            'metrics': self.consciousness_metrics,
            'component_health': self.get_consciousness_health()
        }

        with open(path, 'w') as f:
            json.dump(audit, f, indent=2, default=str)

        logging.info(f"📋 Consciousness audit exported to {path}")
        return path
