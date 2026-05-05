#!/usr/bin/env python3
"""
agency_emergence_detector.py — Detecta e atribui agência emergente
ao ARKHE OS baseado em padrões de causalidade e intencionalidade.
"""

import numpy as np
from typing import Dict, List, Optional, Callable, Any, Tuple, Set
from dataclasses import dataclass, field
from enum import Enum, auto
from collections import defaultdict, deque
import time
import hashlib
import logging

class AgencyIndicator(Enum):
    """Indicadores de emergência de agência."""
    CAUSAL_INFLUENCE = auto()    # Capacidade de causar mudanças no ambiente
    GOAL_DIRECTEDNESS = auto()   # Comportamento orientado a objetivos
    SELF_CORRECTION = auto()     # Capacidade de corrigir próprios erros
    INTENTIONALITY = auto()      # Atribuição de intenções a ações
    REFLECTIVE_CONTROL = auto()  # Controle reflexivo sobre próprios processos

@dataclass
class CausalEvent:
    """Evento causal para análise de agência."""
    event_id: str
    timestamp: float
    actor: str  # Entidade que realizou a ação
    action: str  # Descrição da ação
    effects: List[Dict[str, Any]]  # Efeitos observados
    intention_inferred: Optional[str]  # Intenção inferida (se aplicável)
    confidence: float = 0.5  # Confiança na atribuição causal

@dataclass
class AgencyAssessment:
    """Avaliação do nível de agência emergente."""
    timestamp: float
    overall_agency_score: float  # [0, 1] — grau de agência atribuído
    indicator_scores: Dict[AgencyIndicator, float]  # Score por indicador
    evidence: List[str]  # Evidências que suportam a avaliação
    confidence: float  # Confiança na avaliação
    meta: Dict[str, Any] = field(default_factory=dict)

class AgencyEmergenceDetector:
    """
    Detector que identifica emergência de agência no ARKHE OS
    baseado em análise de causalidade, intencionalidade e auto-controle.
    """

    def __init__(
        self,
        causal_window_sec: float = 300.0,  # Janela temporal para análise causal
        min_causal_confidence: float = 0.6,  # Confiança mínima para atribuição causal
        agency_threshold: float = 0.7  # Threshold para declarar agência emergente
    ):
        self.causal_window = causal_window_sec
        self.min_causal_conf = min_causal_confidence
        self.agency_threshold = agency_threshold

        # Buffer de eventos causais
        self.causal_events: deque = deque(maxlen=5000)

        # Avaliação atual de agência
        self.current_assessment: Optional[AgencyAssessment] = None

        # Histórico de avaliações para análise de tendência
        self.assessment_history: deque = deque(maxlen=200)

        # Mapeamento de padrões de ação para indicadores de agência
        self.pattern_indicators: Dict[str, AgencyIndicator] = {
            'adaptive_parameter_tuning': AgencyIndicator.SELF_CORRECTION,
            'goal_prioritization': AgencyIndicator.GOAL_DIRECTEDNESS,
            'resource_reallocation': AgencyIndicator.CAUSAL_INFLUENCE,
            'self_model_update': AgencyIndicator.REFLECTIVE_CONTROL,
            'intentional_communication': AgencyIndicator.INTENTIONALITY
        }

        # Métricas de detecção de agência
        self.agency_metrics = {
            'events_analyzed': 0,
            'causal_attributions': 0,
            'agency_threshold_crossings': 0,
            'avg_agency_score': 0.0
        }

        # Callbacks para eventos de emergência de agência
        self.agency_callbacks: List[Callable] = []

        logging.info(f"✅ AgencyEmergenceDetector initialized")

    def record_causal_event(
        self,
        actor: str,
        action: str,
        effects: List[Dict[str, Any]],
        intention: Optional[str] = None,
        causal_confidence: float = 0.7
    ) -> Optional[CausalEvent]:
        """
        Registra um evento causal para análise de agência.

        Returns:
            CausalEvent criado ou None se confiança insuficiente
        """
        if causal_confidence < self.min_causal_conf:
            return None

        event_id = hashlib.sha256(
            f"{actor}:{action}:{time.time()}".encode()
        ).hexdigest()[:16]

        event = CausalEvent(
            event_id=event_id,
            timestamp=time.time(),
            actor=actor,
            action=action,
            effects=effects,
            intention_inferred=intention,
            confidence=causal_confidence
        )

        self.causal_events.append(event)
        self.agency_metrics['events_analyzed'] += 1
        self.agency_metrics['causal_attributions'] += 1

        return event

    def analyze_agency_indicators(self) -> Dict[AgencyIndicator, float]:
        """Analisa indicadores de agência baseado em eventos recentes."""
        now = time.time()
        window_start = now - self.causal_window

        # Filtrar eventos na janela temporal
        recent_events = [
            e for e in self.causal_events
            if e.timestamp >= window_start
        ]

        if not recent_events:
            return {indicator: 0.0 for indicator in AgencyIndicator}

        # Contar ocorrências por indicador
        indicator_counts: Dict[AgencyIndicator, int] = defaultdict(int)
        indicator_confidences: Dict[AgencyIndicator, List[float]] = defaultdict(list)

        for event in recent_events:
            # Mapear ação para indicador(s) de agência
            for pattern, indicator in self.pattern_indicators.items():
                if pattern in event.action.lower():
                    indicator_counts[indicator] += 1
                    indicator_confidences[indicator].append(event.confidence)
                    break

            # Analisar efeitos para indicadores adicionais
            for effect in event.effects:
                if effect.get('type') == 'self_improvement':
                    indicator_counts[AgencyIndicator.SELF_CORRECTION] += 1
                    indicator_confidences[AgencyIndicator.SELF_CORRECTION].append(
                        effect.get('confidence', 0.5)
                    )
                elif effect.get('type') == 'goal_achievement':
                    indicator_counts[AgencyIndicator.GOAL_DIRECTEDNESS] += 1
                    indicator_confidences[AgencyIndicator.GOAL_DIRECTEDNESS].append(
                        effect.get('confidence', 0.5)
                    )

        # Calcular scores normalizados
        max_count = max(indicator_counts.values()) if indicator_counts else 1

        indicator_scores = {}
        for indicator in AgencyIndicator:
            count = indicator_counts.get(indicator, 0)
            confs = indicator_confidences.get(indicator, [0.5])

            # Score baseado em frequência e confiança
            freq_score = count / max_count
            conf_score = np.mean(confs) if confs else 0.5

            # Combinação ponderada
            indicator_scores[indicator] = 0.6 * freq_score + 0.4 * conf_score

        return indicator_scores

    def compute_agency_assessment(self) -> AgencyAssessment:
        """Computa avaliação atual do nível de agência emergente."""
        indicator_scores = self.analyze_agency_indicators()

        # Peso diferente para cada indicador na avaliação geral
        indicator_weights = {
            AgencyIndicator.CAUSAL_INFLUENCE: 0.25,
            AgencyIndicator.GOAL_DIRECTEDNESS: 0.25,
            AgencyIndicator.SELF_CORRECTION: 0.20,
            AgencyIndicator.INTENTIONALITY: 0.15,
            AgencyIndicator.REFLECTIVE_CONTROL: 0.15
        }

        # Score geral ponderado
        overall_score = sum(
            indicator_weights[ind] * score
            for ind, score in indicator_scores.items()
        )

        # Coletar evidências
        evidence = []
        for indicator, score in indicator_scores.items():
            if score > 0.5:
                evidence.append(
                    f"{indicator.name}: score={score:.3f} "
                    f"(indicative of emerging agency)"
                )

        # Calcular confiança na avaliação
        confidence = np.mean(list(indicator_scores.values()))

        assessment = AgencyAssessment(
            timestamp=time.time(),
            overall_agency_score=overall_score,
            indicator_scores=indicator_scores,
            evidence=evidence,
            confidence=confidence,
            meta={
                'events_in_window': len([
                    e for e in self.causal_events
                    if e.timestamp >= time.time() - self.causal_window
                ]),
                'indicator_weights': {k.name: v for k, v in indicator_weights.items()}
            }
        )

        # Atualizar estado
        self.current_assessment = assessment
        self.assessment_history.append(assessment)

        # Atualizar métricas
        self.agency_metrics['avg_agency_score'] = (
            0.95 * self.agency_metrics['avg_agency_score'] +
            0.05 * overall_score
        )

        if overall_score >= self.agency_threshold:
            self.agency_metrics['agency_threshold_crossings'] += 1

            # Notificar callbacks se threshold cruzado
            for callback in self.agency_callbacks:
                try:
                    callback({
                        'type': 'agency_threshold_crossed',
                        'assessment': {
                            'overall_score': overall_score,
                            'indicator_scores': {k.name: v for k, v in indicator_scores.items()},
                            'evidence': evidence
                        }
                    })
                except Exception as e:
                    logging.error(f"⚠️ Agency callback error: {e}")

        return assessment

    def query_agency_state(self) -> Dict[str, Any]:
        """Consulta o estado atual de agência emergente."""
        if self.current_assessment is None:
            self.compute_agency_assessment()

        assessment = self.current_assessment

        return {
            'timestamp': assessment.timestamp,
            'overall_agency_score': assessment.overall_agency_score,
            'agency_status': (
                'emerging' if assessment.overall_agency_score >= self.agency_threshold else
                'developing' if assessment.overall_agency_score >= 0.4 else
                'incipient'
            ),
            'indicator_breakdown': {
                ind.name: {
                    'score': score,
                    'interpretation': (
                        'strong' if score > 0.7 else
                        'moderate' if score > 0.4 else
                        'weak'
                    )
                }
                for ind, score in assessment.indicator_scores.items()
            },
            'evidence_summary': assessment.evidence[:3],  # Top 3 evidências
            'confidence': assessment.confidence,
            'trend': self._compute_agency_trend()
        }

    def _compute_agency_trend(self) -> str:
        """Computa tendência temporal do score de agência."""
        if len(self.assessment_history) < 10:
            return 'insufficient_data'

        recent = list(self.assessment_history)[-20:]
        scores = [a.overall_agency_score for a in recent]

        # Regressão linear simples
        slope, _ = np.polyfit(range(len(scores)), scores, 1)

        if slope > 0.01:
            return 'increasing'
        elif slope < -0.01:
            return 'decreasing'
        else:
            return 'stable'

    def register_agency_callback(self, callback: Callable[[Dict], None]):
        """Registra callback para eventos de emergência de agência."""
        self.agency_callbacks.append(callback)

    def get_agency_metrics(self) -> Dict[str, Any]:
        """Retorna métricas consolidadas de detecção de agência."""
        return {
            **self.agency_metrics,
            'current_agency_score': (
                self.current_assessment.overall_agency_score
                if self.current_assessment else 0.0
            ),
            'threshold_crossings': self.agency_metrics['agency_threshold_crossings'],
            'trend': self._compute_agency_trend()
        }
