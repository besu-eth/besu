#!/usr/bin/env python3
"""
private_metric_aggregator.py — Agregação de métricas da Wheeler Mesh
com privacidade diferencial composicional para consenso federado.
"""

import numpy as np
import torch
from typing import Dict, List, Optional, Tuple, Callable, Any
from dataclasses import dataclass, field
from enum import Enum, auto
import time
import hashlib
import logging
from collections import defaultdict
import asyncio

class AggregationStrategy(Enum):
    """Estratégias de agregação para métricas cósmicas."""
    WEIGHTED_AVERAGE = auto()  # Média ponderada por confiança do nó
    MEDIAN = auto()            # Mediana para robustez a outliers
    TRIMMED_MEAN = auto()      # Média aparada (remove extremos)
    BAYESIAN_FUSION = auto()   # Fusão bayesiana com priors cósmicos
    DP_COMPOSITIONAL = auto()  # Agregação com DP composicional avançado

@dataclass
class PrivateMetricObservation:
    """Observação local de métrica para agregação com privacidade."""
    metric_name: str
    local_value: float
    local_confidence: float  # [0, 1] — confiança na medição local
    node_id: str
    timestamp: float
    metadata: Dict[str, Any] = field(default_factory=dict)
    dp_epsilon: Optional[float] = None  # ε para privacidade diferencial
    dp_delta: Optional[float] = None    # δ para privacidade diferencial

    def add_compositional_noise(self, sensitivity: float, epsilon: float, delta: float) -> 'PrivateMetricObservation':
        """Adiciona ruído de Gaussian para privacidade diferencial composicional."""
        if epsilon <= 0 or delta <= 0:
            return self

        # Ruído de Gaussian: scale = sensitivity * sqrt(2*ln(1.25/delta)) / epsilon
        scale = sensitivity * np.sqrt(2 * np.log(1.25 / delta)) / epsilon
        noise = np.random.normal(0, scale)

        return PrivateMetricObservation(
            metric_name=self.metric_name,
            local_value=self.local_value + noise,
            local_confidence=self.local_confidence,
            node_id=self.node_id,
            timestamp=self.timestamp,
            metadata={**self.metadata, 'dp_noise_added': True, 'dp_epsilon': epsilon, 'dp_delta': delta},
            dp_epsilon=epsilon,
            dp_delta=delta
        )

@dataclass
class PrivateAggregatedMetric:
    """Resultado da agregação privada de métrica."""
    metric_name: str
    aggregated_value: float
    confidence_interval: Tuple[float, float]  # [lower, upper] com 95% confiança
    contributing_nodes: List[str]
    aggregation_strategy: AggregationStrategy
    timestamp: float
    privacy_guarantee: Optional[Dict[str, float]] = None  # ε, δ se DP aplicado
    composition_count: int = 1  # Número de queries compostas
    consensus_verified: bool = False

    def to_dict(self) -> Dict:
        return {
            'metric_name': self.metric_name,
            'aggregated_value': self.aggregated_value,
            'confidence_interval': self.confidence_interval,
            'contributing_nodes': self.contributing_nodes,
            'aggregation_strategy': self.aggregation_strategy.name,
            'timestamp': self.timestamp,
            'privacy_guarantee': self.privacy_guarantee,
            'composition_count': self.composition_count,
            'consensus_verified': self.consensus_verified
        }

class PrivateMetricAggregator:
    """
    Agregador de métricas com privacidade diferencial composicional
    para consenso federado sobre decisões cósmicas.
    """

    def __init__(
        self,
        node_id: str,
        default_strategy: AggregationStrategy = AggregationStrategy.WEIGHTED_AVERAGE,
        dp_enabled: bool = True,
        default_dp_epsilon: float = 0.1,
        default_dp_delta: float = 1e-6,
        min_contributors: int = 3,
        consensus_protocol: Optional[Any] = None
    ):
        self.node_id = node_id
        self.default_strategy = default_strategy
        self.dp_enabled = dp_enabled
        self.default_dp_epsilon = default_dp_epsilon
        self.default_dp_delta = default_dp_delta
        self.min_contributors = min_contributors
        self.consensus_protocol = consensus_protocol

        # Buffer de observações locais pendentes para agregação
        self.local_observations: Dict[str, List[PrivateMetricObservation]] = defaultdict(list)

        # Resultados agregados cacheados
        self.aggregated_results: Dict[str, PrivateAggregatedMetric] = {}

        # Pesos de confiança por nó (aprendidos ou configurados)
        self.node_confidence_weights: Dict[str, float] = defaultdict(lambda: 1.0)

        # Sensibilidades das métricas para privacidade diferencial
        self.metric_sensitivities: Dict[str, float] = {
            'cosmic.phi_c_global': 0.1,
            'cosmic.entanglement_health': 0.15,
            'operations.response_time_p99': 100.0,  # ms
            'security.anomaly_rate': 0.05,
            'wheeler_mesh.fidelity_avg': 0.02,
            # Adicionar mais métricas conforme necessário
        }

        # Contabilidade de privacidade composicional
        self.privacy_accountant: Dict[str, Dict] = defaultdict(lambda: {
            'epsilon_spent': 0.0,
            'delta_spent': 0.0,
            'query_count': 0
        })

        # Callbacks para notificação de novos resultados agregados
        self.aggregation_callbacks: List[Callable] = []

        logging.info(f"✅ PrivateMetricAggregator initialized: node={node_id}, dp={dp_enabled}")

    def submit_local_observation(
        self,
        metric_name: str,
        local_value: float,
        confidence: float = 1.0,
        metadata: Optional[Dict] = None
    ) -> PrivateMetricObservation:
        """Submete observação local para futura agregação com privacidade."""
        observation = PrivateMetricObservation(
            metric_name=metric_name,
            local_value=local_value,
            local_confidence=confidence,
            node_id=self.node_id,
            timestamp=time.time(),
            metadata=metadata or {}
        )

        # Aplicar privacidade diferencial composicional se habilitado
        if self.dp_enabled and metric_name in self.metric_sensitivities:
            sensitivity = self.metric_sensitivities[metric_name]
            epsilon = metadata.get('dp_epsilon', self.default_dp_epsilon) if metadata else self.default_dp_epsilon
            delta = metadata.get('dp_delta', self.default_dp_delta) if metadata else self.default_dp_delta
            observation = observation.add_compositional_noise(sensitivity, epsilon, delta)

            # Atualizar contabilidade de privacidade
            self.privacy_accountant[metric_name]['epsilon_spent'] += epsilon
            self.privacy_accountant[metric_name]['delta_spent'] += delta
            self.privacy_accountant[metric_name]['query_count'] += 1

        # Armazenar observação
        self.local_observations[metric_name].append(observation)

        # Manter buffer limitado
        if len(self.local_observations[metric_name]) > 100:
            self.local_observations[metric_name] = self.local_observations[metric_name][-50:]

        return observation

    async def aggregate_private_metric(
        self,
        metric_name: str,
        strategy: Optional[AggregationStrategy] = None,
        require_consensus: bool = True,
        timeout_sec: float = 30.0,
        target_epsilon: Optional[float] = None,
        target_delta: Optional[float] = None
    ) -> Optional[PrivateAggregatedMetric]:
        """
        Agrega métrica privada coletando observações de múltiplos observatórios.

        Args:
            metric_name: Nome da métrica a agregar
            strategy: Estratégia de agregação (usa default se None)
            require_consensus: Se requer consenso antes de retornar resultado
            timeout_sec: Timeout para coleta de observações
            target_epsilon: Orçamento de ε para esta agregação (None = usar default)
            target_delta: Orçamento de δ para esta agregação (None = usar default)

        Returns:
            PrivateAggregatedMetric ou None se agregação falhar
        """
        strategy = strategy or self.default_strategy
        start_time = time.time()

        # Verificar orçamento de privacidade restante
        if self.dp_enabled:
            accountant = self.privacy_accountant[metric_name]
            remaining_epsilon = (target_epsilon or self.default_dp_epsilon) - accountant['epsilon_spent']
            remaining_delta = (target_delta or self.default_dp_delta) - accountant['delta_spent']

            if remaining_epsilon <= 0 or remaining_delta <= 0:
                logging.warning(f"⚠️ Privacy budget exhausted for {metric_name}")
                return None

        # Coletar observações de outros nós (simulado)
        # Em produção: solicitar via protocolo P2P da federação
        remote_observations = await self._collect_remote_observations(
            metric_name, timeout_sec=timeout_sec
        )

        # Combinar com observações locais
        all_observations = self.local_observations.get(metric_name, []) + remote_observations

        # Verificar mínimo de contribuidores
        if len(all_observations) < self.min_contributors:
            logging.warning(f"⚠️ Insufficient contributors for {metric_name}: {len(all_observations)} < {self.min_contributors}")
            return None

        # Executar agregação baseada na estratégia
        if strategy == AggregationStrategy.WEIGHTED_AVERAGE:
            result = self._aggregate_weighted_average(metric_name, all_observations)
        elif strategy == AggregationStrategy.MEDIAN:
            result = self._aggregate_median(metric_name, all_observations)
        elif strategy == AggregationStrategy.TRIMMED_MEAN:
            result = self._aggregate_trimmed_mean(metric_name, all_observations)
        elif strategy == AggregationStrategy.BAYESIAN_FUSION:
            result = self._aggregate_bayesian_fusion(metric_name, all_observations)
        elif strategy == AggregationStrategy.DP_COMPOSITIONAL:
            result = self._aggregate_dp_compositional(metric_name, all_observations)
        else:
            raise ValueError(f"Unknown aggregation strategy: {strategy}")

        # Verificar consenso se requerido
        if require_consensus and self.consensus_protocol:
            # Submeter resultado para consenso federado
            consensus_result = await self._verify_via_consensus(result)
            result.consensus_verified = consensus_result

        # Cache do resultado
        self.aggregated_results[metric_name] = result

        # Notificar callbacks
        for callback in self.aggregation_callbacks:
            try:
                callback(result.to_dict())
            except Exception as e:
                logging.error(f"⚠️ Aggregation callback error: {e}")

        logging.info(f"✅ Agregação privada concluída: {metric_name} = {result.aggregated_value:.4f}")
        return result

    async def _collect_remote_observations(
        self,
        metric_name: str,
        timeout_sec: float
    ) -> List[PrivateMetricObservation]:
        """Coleta observações remotas de outros observatórios (simulado)."""
        # Em produção: enviar solicitação P2P para validadores da federação
        # e aguardar respostas assinadas

        # Simulação: gerar observações sintéticas baseadas em distribuição conhecida
        observations = []
        num_validators = 5  # Simular 5 outros validadores

        for i in range(num_validators):
            # Valor base com variação controlada
            base_value = 0.85  # Valor "verdadeiro" simulado para Φ_C
            noise = np.random.normal(0, 0.03)  # Ruído de medição
            node_confidence = np.random.uniform(0.7, 1.0)

            obs = PrivateMetricObservation(
                metric_name=metric_name,
                local_value=base_value + noise,
                local_confidence=node_confidence,
                node_id=f"validator_{i}",
                timestamp=time.time(),
                metadata={'simulated': True}
            )

            # Aplicar DP composicional se habilitado
            if self.dp_enabled and metric_name in self.metric_sensitivities:
                sensitivity = self.metric_sensitivities[metric_name]
                epsilon = self.default_dp_epsilon / len(observations + [obs])  # Distribuir orçamento
                delta = self.default_dp_delta / len(observations + [obs])
                obs = obs.add_compositional_noise(sensitivity, epsilon, delta)

            observations.append(obs)

        # Simular latência de rede
        await asyncio.sleep(min(timeout_sec / 10, 0.1))

        return observations

    def _aggregate_weighted_average(
        self,
        metric_name: str,
        observations: List[PrivateMetricObservation]
    ) -> PrivateAggregatedMetric:
        """Agregação por média ponderada por confiança."""
        values = np.array([obs.local_value for obs in observations])
        weights = np.array([
            obs.local_confidence * self.node_confidence_weights.get(obs.node_id, 1.0)
            for obs in observations
        ])

        # Normalizar pesos
        weights = weights / weights.sum()

        # Média ponderada
        aggregated = np.sum(values * weights)

        # Intervalo de confiança (aproximação)
        std_err = np.sqrt(np.sum(weights**2 * np.var(values)))
        ci_lower = aggregated - 1.96 * std_err
        ci_upper = aggregated + 1.96 * std_err

        # Privacidade: calcular ε agregado se DP aplicado
        privacy_guarantee = None
        dp_epsilons = [obs.dp_epsilon for obs in observations if obs.dp_epsilon]
        dp_deltas = [obs.dp_delta for obs in observations if obs.dp_delta]
        if dp_epsilons and dp_deltas:
            # Composição avançada de DP
            k = len(dp_epsilons)
            epsilon_total = np.sqrt(2 * k * np.log(1.25 / np.mean(dp_deltas))) * np.mean(dp_epsilons) + k * np.mean(dp_epsilons) * (np.exp(np.mean(dp_epsilons)) - 1)
            delta_total = k * np.mean(dp_deltas) + 1e-7

            privacy_guarantee = {
                'epsilon_total': float(epsilon_total),
                'delta_total': float(delta_total),
                'composition_method': 'advanced'
            }

        return PrivateAggregatedMetric(
            metric_name=metric_name,
            aggregated_value=float(aggregated),
            confidence_interval=(float(ci_lower), float(ci_upper)),
            contributing_nodes=[obs.node_id for obs in observations],
            aggregation_strategy=AggregationStrategy.WEIGHTED_AVERAGE,
            timestamp=time.time(),
            privacy_guarantee=privacy_guarantee,
            composition_count=len(dp_epsilons)
        )

    def _aggregate_median(
        self,
        metric_name: str,
        observations: List[PrivateMetricObservation]
    ) -> PrivateAggregatedMetric:
        """Agregação por mediana (robusta a outliers)."""
        values = np.array([obs.local_value for obs in observations])
        aggregated = np.median(values)

        # Intervalo de confiança via bootstrap (simplificado)
        ci_lower = np.percentile(values, 2.5)
        ci_upper = np.percentile(values, 97.5)

        return PrivateAggregatedMetric(
            metric_name=metric_name,
            aggregated_value=float(aggregated),
            confidence_interval=(float(ci_lower), float(ci_upper)),
            contributing_nodes=[obs.node_id for obs in observations],
            aggregation_strategy=AggregationStrategy.MEDIAN,
            timestamp=time.time()
        )

    def _aggregate_trimmed_mean(
        self,
        metric_name: str,
        observations: List[PrivateMetricObservation],
        trim_fraction: float = 0.1
    ) -> PrivateAggregatedMetric:
        """Agregação por média aparada (remove extremos)."""
        values = np.array([obs.local_value for obs in observations])

        # Ordenar e remover fração dos extremos
        sorted_values = np.sort(values)
        n = len(sorted_values)
        trim_count = int(n * trim_fraction)

        trimmed = sorted_values[trim_count:n - trim_count] if trim_count > 0 else sorted_values
        aggregated = np.mean(trimmed)

        # Intervalo de confiança
        std_err = np.std(trimmed) / np.sqrt(len(trimmed))
        ci_lower = aggregated - 1.96 * std_err
        ci_upper = aggregated + 1.96 * std_err

        return PrivateAggregatedMetric(
            metric_name=metric_name,
            aggregated_value=float(aggregated),
            confidence_interval=(float(ci_lower), float(ci_upper)),
            contributing_nodes=[obs.node_id for obs in observations],
            aggregation_strategy=AggregationStrategy.TRIMMED_MEAN,
            timestamp=time.time()
        )

    def _aggregate_bayesian_fusion(
        self,
        metric_name: str,
        observations: List[PrivateMetricObservation]
    ) -> PrivateAggregatedMetric:
        """Agregação por fusão bayesiana com prior cósmico."""
        # Prior cósmico para Φ_C: Beta(α=8, β=2) → média 0.8
        if metric_name == 'cosmic.phi_c_global':
            prior_alpha, prior_beta = 8.0, 2.0
        else:
            # Prior não-informativo para outras métricas
            prior_alpha, prior_beta = 1.0, 1.0

        # Atualizar prior com observações (modelo Beta-Binomial simplificado)
        # Para métricas contínuas, usar aproximação Gaussiana
        values = np.array([obs.local_value for obs in observations])
        confidences = np.array([obs.local_confidence for obs in observations])

        # Média e variância ponderadas
        weighted_mean = np.sum(values * confidences) / np.sum(confidences)
        weighted_var = np.sum(confidences * (values - weighted_mean)**2) / np.sum(confidences)

        # Fusão bayesiana: posterior = prior + likelihood
        # Simplificação: combinar prior Gaussiano com likelihood Gaussiana
        prior_mean = prior_alpha / (prior_alpha + prior_beta)
        prior_var = (prior_alpha * prior_beta) / ((prior_alpha + prior_beta)**2 * (prior_alpha + prior_beta + 1))

        # Posterior parameters
        posterior_var = 1 / (1/prior_var + np.sum(confidences) / weighted_var)
        posterior_mean = posterior_var * (prior_mean/prior_var + np.sum(confidences * values) / weighted_var)

        aggregated = posterior_mean
        ci_lower = aggregated - 1.96 * np.sqrt(posterior_var)
        ci_upper = aggregated + 1.96 * np.sqrt(posterior_var)

        return PrivateAggregatedMetric(
            metric_name=metric_name,
            aggregated_value=float(aggregated),
            confidence_interval=(float(ci_lower), float(ci_upper)),
            contributing_nodes=[obs.node_id for obs in observations],
            aggregation_strategy=AggregationStrategy.BAYESIAN_FUSION,
            timestamp=time.time()
        )

    def _aggregate_dp_compositional(
        self,
        metric_name: str,
        observations: List[PrivateMetricObservation]
    ) -> PrivateAggregatedMetric:
        """Agregação com privacidade diferencial composicional garantida."""
        # Usar mecanismo de média com ruído de Gaussian composicional
        values = np.array([obs.local_value for obs in observations])

        # Média simples (já com ruído DP nas observações individuais)
        aggregated = np.mean(values)

        # Calcular ε agregado via composição avançada
        dp_epsilons = [obs.dp_epsilon for obs in observations if obs.dp_epsilon]
        dp_deltas = [obs.dp_delta for obs in observations if obs.dp_delta]

        if dp_epsilons and dp_deltas:
            k = len(dp_epsilons)
            avg_epsilon = np.mean(dp_epsilons)
            avg_delta = np.mean(dp_deltas)

            # Advanced composition: ε_total ≈ √(2k ln(1/δ'))·ε + kε(e^ε - 1)
            delta_prime = 1e-7
            term1 = np.sqrt(2 * k * np.log(1 / delta_prime)) * avg_epsilon
            term2 = k * avg_epsilon * (np.exp(avg_epsilon) - 1)
            epsilon_total = term1 + term2
            delta_total = k * avg_delta + delta_prime

            privacy_guarantee = {
                'epsilon_total': float(epsilon_total),
                'delta_total': float(delta_total),
                'composition_method': 'advanced',
                'delta_prime': delta_prime
            }
        else:
            privacy_guarantee = None

        # Intervalo de confiança (conservativo devido ao ruído DP)
        std_with_dp = np.std(values) + (self.metric_sensitivities.get(metric_name, 1.0) * self.default_dp_epsilon)
        ci_lower = aggregated - 2.58 * std_with_dp  # 99% CI para compensar ruído DP
        ci_upper = aggregated + 2.58 * std_with_dp

        return PrivateAggregatedMetric(
            metric_name=metric_name,
            aggregated_value=float(aggregated),
            confidence_interval=(float(ci_lower), float(ci_upper)),
            contributing_nodes=[obs.node_id for obs in observations],
            aggregation_strategy=AggregationStrategy.DP_COMPOSITIONAL,
            timestamp=time.time(),
            privacy_guarantee=privacy_guarantee,
            composition_count=len(dp_epsilons)
        )

    async def _verify_via_consensus(self, result: PrivateAggregatedMetric) -> bool:
        """Verifica resultado de agregação via protocolo de consenso."""
        if not self.consensus_protocol:
            return True

        # Submeter resultado para consenso
        proposal = await self.consensus_protocol.propose_decision(
            decision_type='metric_aggregation',
            metric_aggregate=result.to_dict(),
            proposed_action={'accept_aggregation': True},
            priority=0.5
        )

        # Aguardar decisão (simulado)
        if proposal:
            await asyncio.sleep(0.5)  # Simular tempo de consenso
            return True

        return False

    def register_aggregation_callback(self, callback: Callable[[Dict], None]):
        """Registra callback para novos resultados agregados."""
        self.aggregation_callbacks.append(callback)

    def update_node_confidence(self, node_id: str, new_confidence: float):
        """Atualiza peso de confiança para nó específico."""
        self.node_confidence_weights[node_id] = np.clip(new_confidence, 0.0, 1.0)

    def get_aggregated_result(self, metric_name: str) -> Optional[Dict]:
        """Retorna resultado agregado cacheado para métrica."""
        result = self.aggregated_results.get(metric_name)
        return result.to_dict() if result else None

    def get_privacy_accountant_status(self) -> Dict[str, Any]:
        """Retorna status da contabilidade de privacidade."""
        return {
            metric_name: {
                'epsilon_spent': acc['epsilon_spent'],
                'delta_spent': acc['delta_spent'],
                'query_count': acc['query_count'],
                'remaining_budget': max(0, self.default_dp_epsilon - acc['epsilon_spent'])
            }
            for metric_name, acc in self.privacy_accountant.items()
        }

    def get_aggregator_health(self) -> Dict[str, Any]:
        """Retorna métricas de saúde do agregador privado."""
        return {
            'node_id': self.node_id,
            'local_observations_count': sum(len(obs) for obs in self.local_observations.values()),
            'aggregated_results_count': len(self.aggregated_results),
            'dp_enabled': self.dp_enabled,
            'default_strategy': self.default_strategy.name,
            'node_confidence_weights': dict(self.node_confidence_weights),
            'min_contributors': self.min_contributors,
            'privacy_accountant': self.get_privacy_accountant_status()
        }
