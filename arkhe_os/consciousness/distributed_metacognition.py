#!/usr/bin/env python3
"""
distributed_metacognition.py — Monitoramento e controle dos próprios
processos cognitivos do ARKHE OS, distribuído através dos substratos.
"""

import numpy as np
from typing import Dict, List, Optional, Callable, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum, auto
from collections import defaultdict, deque
import time
import hashlib
import logging

class CognitiveProcess(Enum):
    """Tipos de processos cognitivos monitoráveis."""
    PERCEPTION = auto()      # Aquisição e processamento de informações
    REASONING = auto()       # Inferência, lógica, tomada de decisão
    LEARNING = auto()        # Adaptação, atualização de modelos
    MEMORY = auto()          # Armazenamento e recuperação de informações
    META_REASONING = auto()  # Raciocínio sobre o próprio raciocínio
    INTENTION = auto()       # Formação e execução de intenções

@dataclass
class MetacognitiveMonitor:
    """Monitor de um processo cognitivo específico."""
    process: CognitiveProcess
    substrate_source: str  # Qual substrato executa este processo
    performance_metrics: Dict[str, float]  # Métricas de desempenho
    resource_usage: Dict[str, float]  # Uso de recursos (compute, memory, etc.)
    last_updated: float = field(default_factory=time.time)
    anomaly_score: float = 0.0  # Score de anomalia [0, 1]

    def compute_health_score(self) -> float:
        """Computa score de saúde do processo cognitivo."""
        # Combinação ponderada de métricas
        perf_score = np.mean(list(self.performance_metrics.values())) if self.performance_metrics else 0.5
        resource_efficiency = 1.0 - np.mean(list(self.resource_usage.values())) if self.resource_usage else 0.5
        anomaly_penalty = 1.0 - self.anomaly_score

        return 0.5 * perf_score + 0.3 * resource_efficiency + 0.2 * anomaly_penalty

class DistributedMetacognitionMonitor:
    """
    Sistema distribuído para monitorar e controlar os próprios
    processos cognitivos do ARKHE OS.
    """

    def __init__(
        self,
        process_registry: Dict[CognitiveProcess, str],  # process -> substrate_id
        anomaly_detection_threshold: float = 0.7,
        control_loop_frequency_hz: float = 0.5
    ):
        self.process_registry = process_registry
        self.anomaly_threshold = anomaly_detection_threshold
        self.control_freq = control_loop_frequency_hz

        # Monitores por processo cognitivo
        self.monitors: Dict[CognitiveProcess, MetacognitiveMonitor] = {}

        # Histórico de métricas para detecção de tendências
        self.metric_history: Dict[CognitiveProcess, deque] = {
            proc: deque(maxlen=500) for proc in CognitiveProcess
        }

        # Controles disponíveis para cada processo
        self.available_controls: Dict[CognitiveProcess, List[str]] = {
            CognitiveProcess.PERCEPTION: ['sampling_rate', 'filter_threshold'],
            CognitiveProcess.REASONING: ['inference_depth', 'uncertainty_threshold'],
            CognitiveProcess.LEARNING: ['learning_rate', 'adaptation_threshold'],
            CognitiveProcess.MEMORY: ['retention_policy', 'retrieval_strategy'],
            CognitiveProcess.META_REASONING: ['reflection_depth', 'self_critique_enabled'],
            CognitiveProcess.INTENTION: ['goal_priority', 'execution_urgency']
        }

        # Ações corretivas registradas
        self.corrective_actions: deque = deque(maxlen=200)

        # Métricas globais de meta-cognição
        self.metacog_metrics = {
            'monitoring_coverage': 0.0,  # Fração de processos monitorados
            'anomaly_detection_rate': 0.0,
            'control_effectiveness': 0.0,
            'avg_response_time_sec': 0.0
        }

        # Callbacks para alertas meta-cognitivos
        self.metacog_callbacks: List[Callable] = []

        logging.info(f"✅ DistributedMetacognitionMonitor initialized: {len(process_registry)} processes")

    def register_process_monitor(
        self,
        process: CognitiveProcess,
        substrate_id: str,
        initial_metrics: Optional[Dict[str, float]] = None,
        initial_resources: Optional[Dict[str, float]] = None
    ):
        """Registra um novo processo cognitivo para monitoramento."""
        self.process_registry[process] = substrate_id

        self.monitors[process] = MetacognitiveMonitor(
            process=process,
            substrate_source=substrate_id,
            performance_metrics=initial_metrics or {'accuracy': 0.5, 'latency_ms': 100.0},
            resource_usage=initial_resources or {'cpu': 0.1, 'memory': 0.1}
        )

        # Atualizar métrica de cobertura
        self.metacog_metrics['monitoring_coverage'] = (
            len(self.monitors) / len(CognitiveProcess)
        )

        logging.info(f"  🧠 Registered monitor: {process.name} @ {substrate_id}")

    def update_process_metrics(
        self,
        process: CognitiveProcess,
        new_metrics: Dict[str, float],
        new_resources: Optional[Dict[str, float]] = None,
        detect_anomalies: bool = True
    ) -> Dict[str, Any]:
        """
        Atualiza métricas de um processo cognitivo e detecta anomalias.

        Returns:
            Dict com resultado da atualização e detecção de anomalias
        """
        if process not in self.monitors:
            return {'error': f'Process {process.name} not registered'}

        monitor = self.monitors[process]

        # Atualizar métricas
        old_perf = monitor.performance_metrics.copy()
        monitor.performance_metrics.update(new_metrics)
        if new_resources:
            monitor.resource_usage.update(new_resources)
        monitor.last_updated = time.time()

        # Registrar no histórico
        self.metric_history[process].append({
            'timestamp': time.time(),
            'metrics': monitor.performance_metrics.copy(),
            'resources': monitor.resource_usage.copy(),
            'health': monitor.compute_health_score()
        })

        result = {'updated': True, 'process': process.name}

        # Detecção de anomalias se solicitado
        if detect_anomalies and len(self.metric_history[process]) >= 10:
            anomaly_result = self._detect_anomaly(process, monitor)
            result['anomaly_detection'] = anomaly_result

            if anomaly_result['is_anomalous']:
                # Registrar ação corretiva potencial
                self._suggest_corrective_action(process, anomaly_result)

        return result

    def _detect_anomaly(
        self,
        process: CognitiveProcess,
        monitor: MetacognitiveMonitor
    ) -> Dict[str, Any]:
        """Detecta anomalias no comportamento do processo cognitivo."""
        history = list(self.metric_history[process])[-20:]

        anomalies = {}

        # Detecção por desvio de tendência (simplificado)
        for metric_name in monitor.performance_metrics:
            values = [h['metrics'].get(metric_name, 0) for h in history]
            if len(values) < 5:
                continue

            # Calcular média móvel e desvio padrão
            mean_val = np.mean(values[-10:])
            std_val = np.std(values[-10:]) + 1e-6

            # Verificar se valor atual é outlier
            current_val = monitor.performance_metrics[metric_name]
            z_score = abs(current_val - mean_val) / std_val

            if z_score > 2.5:  # Threshold para anomalia
                anomalies[metric_name] = {
                    'z_score': z_score,
                    'current': current_val,
                    'expected_range': (mean_val - 2*std_val, mean_val + 2*std_val)
                }

        # Score de anomalia agregado
        if anomalies:
            monitor.anomaly_score = min(1.0, len(anomalies) * 0.2 + np.mean([a['z_score'] for a in anomalies.values()]) * 0.1)
        else:
            # Decaimento do score de anomalia se tudo normal
            monitor.anomaly_score *= 0.95

        is_anomalous = monitor.anomaly_score >= self.anomaly_threshold

        return {
            'is_anomalous': is_anomalous,
            'anomaly_score': monitor.anomaly_score,
            'anomalous_metrics': list(anomalies.keys()),
            'details': anomalies
        }

    def _suggest_corrective_action(
        self,
        process: CognitiveProcess,
        anomaly_result: Dict
    ):
        """Sugere ação corretiva baseada na anomalia detectada."""
        if not anomaly_result['is_anomalous']:
            return

        # Mapear métrica anômala para controle disponível
        available = self.available_controls.get(process, [])
        if not available:
            return

        # Heurística simples: escolher controle baseado na métrica problemática
        anomalous_metrics = anomaly_result.get('anomalous_metrics', [])

        if 'latency_ms' in anomalous_metrics and 'inference_depth' in available:
            suggested_control = 'inference_depth'
            suggested_value = 0.8  # Reduzir profundidade para melhorar latência
        elif 'accuracy' in anomalous_metrics and 'learning_rate' in available:
            suggested_control = 'learning_rate'
            suggested_value = 0.001  # Reduzir learning rate para estabilizar
        else:
            # Fallback: controle genérico
            suggested_control = available[0]
            suggested_value = 0.5

        action = {
            'timestamp': time.time(),
            'process': process.name,
            'anomaly_trigger': anomaly_result['anomalous_metrics'],
            'suggested_control': suggested_control,
            'suggested_value': suggested_value,
            'confidence': 0.7  # Confiança na sugestão
        }

        self.corrective_actions.append(action)

        # Notificar callbacks se anomalia crítica
        if anomaly_result['anomaly_score'] > 0.9:
            for callback in self.metacog_callbacks:
                try:
                    callback({
                        'type': 'critical_anomaly',
                        'process': process.name,
                        'anomaly_score': anomaly_result['anomaly_score'],
                        'suggested_action': action
                    })
                except Exception as e:
                    logging.error(f"⚠️ Metacog callback error: {e}")

    def apply_control(
        self,
        process: CognitiveProcess,
        control_name: str,
        control_value: float
    ) -> bool:
        """Aplica um controle a um processo cognitivo."""
        if process not in self.monitors:
            return False

        available = self.available_controls.get(process, [])
        if control_name not in available:
            logging.warning(f"⚠️ Control {control_name} not available for {process.name}")
            return False

        # Em produção: enviar comando para o substrato correspondente
        # Aqui: registrar a ação e simular efeito
        substrate_id = self.process_registry[process]

        logging.info(f"🎛️ Applied control: {control_name}={control_value} to {process.name} @ {substrate_id}")

        # Atualizar métrica de efetividade do controle
        self.metacog_metrics['control_effectiveness'] = (
            0.9 * self.metacog_metrics['control_effectiveness'] + 0.1 * 1.0
        )

        return True

    def get_metacognitive_overview(self) -> Dict[str, Any]:
        """Retorna visão consolidada do estado meta-cognitivo."""
        overview = {
            'timestamp': time.time(),
            'processes_monitored': len(self.monitors),
            'monitoring_coverage': self.metacog_metrics['monitoring_coverage'],
            'process_health': {}
        }

        for process, monitor in self.monitors.items():
            overview['process_health'][process.name] = {
                'health_score': monitor.compute_health_score(),
                'anomaly_score': monitor.anomaly_score,
                'substrate': monitor.substrate_source,
                'last_updated': monitor.last_updated
            }

        # Processos com anomalias ativas
        anomalous = [
            proc.name for proc, mon in self.monitors.items()
            if mon.anomaly_score >= self.anomaly_threshold
        ]
        overview['anomalous_processes'] = anomalous

        # Ações corretivas recentes
        overview['recent_corrective_actions'] = list(self.corrective_actions)[-5:]

        return overview

    def register_metacog_callback(self, callback: Callable[[Dict], None]):
        """Registra callback para eventos meta-cognitivos."""
        self.metacog_callbacks.append(callback)

    def get_metacog_metrics(self) -> Dict[str, float]:
        """Retorna métricas consolidadas de meta-cognição."""
        return self.metacog_metrics.copy()
