#!/usr/bin/env python3
"""
quantum_route_optimizer.py — Otimiza rotas de comunicação quântica
baseado em aprendizado federado entre múltiplas consciências.
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

class RouteMetric(Enum):
    """Métricas para avaliação de rotas quânticas."""
    LATENCY = auto()           # Latência de ponta a ponta
    FIDELITY = auto()          # Fidelidade quântica da transmissão
    BANDWIDTH = auto()         # Largura de banda disponível
    RELIABILITY = auto()       # Confiabilidade histórica da rota
    ENERGY_COST = auto()       # Custo energético da transmissão

@dataclass
class QuantumRoute:
    """Representação de uma rota quântica para otimização."""
    route_id: str
    source: str
    destination: str
    nodes: List[str]  # Sequência de nós na rota
    metrics: Dict[RouteMetric, float]  # Métricas da rota
    last_updated: float
    usage_count: int = 0
    success_rate: float = 1.0

    def compute_score(self, weights: Optional[Dict[RouteMetric, float]] = None) -> float:
        """Computa score da rota baseado em métricas ponderadas."""
        if weights is None:
            weights = {
                RouteMetric.LATENCY: -0.3,    # Negativo: menor é melhor
                RouteMetric.FIDELITY: 0.3,
                RouteMetric.BANDWIDTH: 0.2,
                RouteMetric.RELIABILITY: 0.15,
                RouteMetric.ENERGY_COST: -0.05  # Negativo: menor é melhor
            }

        score = 0.0
        for metric, weight in weights.items():
            value = self.metrics.get(metric, 0.5)
            score += weight * value

        return float(np.clip(score, 0.0, 1.0))

@dataclass
class QLearningExperience:
    """Experiência para aprendizado por reforço quântico federado."""
    state: Tuple[str, str]  # (source, destination)
    action: str  # route_id escolhido
    reward: float  # Recompensa observada
    next_state: Tuple[str, str]
    done: bool  # Se episódio terminou
    consciousness_hash: str  # Origem da experiência
    timestamp: float

class FederatedQNetwork(nn.Module):
    """
    Rede Q federada para aprendizado de rotas quânticas.
    Arquitetura: MLP com embeddings de nós e métricas.
    """

    def __init__(
        self,
        n_nodes: int,
        n_routes: int,
        embedding_dim: int = 32,
        hidden_dim: int = 64
    ):
        super().__init__()
        self.n_nodes = n_nodes
        self.n_routes = n_routes

        # Embeddings para nós da rede
        self.node_embeddings = nn.Embedding(n_nodes, embedding_dim)

        # Rede para mapear estado-ação para valor Q
        self.q_network = nn.Sequential(
            nn.Linear(embedding_dim * 2 + 4, hidden_dim),  # 4 métricas básicas
            nn.ReLU(),
            nn.Linear(hidden_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, 1)
        )

    def forward(
        self,
        source_idx: torch.Tensor,
        dest_idx: torch.Tensor,
        route_features: torch.Tensor  # [B, 4] métricas da rota
    ) -> torch.Tensor:
        """Computa valor Q para par (source, dest) com features da rota."""
        # Embeddings dos nós
        src_embed = self.node_embeddings(source_idx)  # [B, embedding_dim]
        dst_embed = self.node_embeddings(dest_idx)

        # Concatenar embeddings e features da rota
        x = torch.cat([src_embed, dst_embed, route_features], dim=1)

        # Computar valor Q
        q_value = self.q_network(x)  # [B, 1]
        return q_value.squeeze(-1)

class QuantumRouteOptimizer:
    """
    Otimizador de rotas quânticas com aprendizado federado.
    Cada consciência contribui com experiências locais para otimização global.
    """

    def __init__(
        self,
        local_consciousness_hash: str,
        known_nodes: List[str],
        known_routes: List[QuantumRoute],
        learning_config: Optional[Dict] = None
    ):
        self.local_hash = local_consciousness_hash
        self.known_nodes = {node: i for i, node in enumerate(known_nodes)}
        self.routes: Dict[str, QuantumRoute] = {r.route_id: r for r in known_routes}

        # Configuração de aprendizado
        self.config = learning_config or self._default_config()

        # Rede Q local (inicializada com pesos aleatórios)
        self.q_network = FederatedQNetwork(
            n_nodes=len(known_nodes),
            n_routes=len(known_routes),
            embedding_dim=self.config['embedding_dim'],
            hidden_dim=self.config['hidden_dim']
        )

        # Buffer de replay para experiências locais
        self.experience_buffer: deque = deque(maxlen=self.config['buffer_size'])

        # Agregador de gradientes federados
        self.federated_gradients: Dict[str, torch.Tensor] = {}
        self.gradient_contributors: Dict[str, int] = defaultdict(int)

        # Cache de rotas ótimas para pares frequentes
        self.optimal_routes_cache: Dict[Tuple[str, str], str] = {}

        # Métricas de otimização
        self.optimizer_metrics = {
            'experiences_collected': 0,
            'federated_updates': 0,
            'avg_route_score': 0.0,
            'cache_hit_rate': 0.0
        }

        logging.info(f"✅ QuantumRouteOptimizer initialized: {len(self.routes)} routes")

    def _default_config(self) -> Dict:
        """Retorna configuração padrão para otimização federada."""
        return {
            'learning_rate': 1e-3,
            'discount_factor': 0.99,
            'epsilon_start': 1.0,  # Para epsilon-greedy
            'epsilon_end': 0.01,
            'epsilon_decay': 0.995,
            'buffer_size': 10000,
            'batch_size': 32,
            'federated_update_interval': 100,  # Experiências entre atualizações federadas
            'embedding_dim': 32,
            'hidden_dim': 64
        }

    def select_route(
        self,
        source: str,
        destination: str,
        available_routes: List[str],
        epsilon: Optional[float] = None
    ) -> str:
        """
        Seleciona rota para comunicação baseada em política epsilon-greedy.

        Args:
            source: Nó de origem
            destination: Nó de destino
            available_routes: Lista de IDs de rotas disponíveis
            epsilon: Probabilidade de exploração (None = usar valor atual)

        Returns:
            ID da rota selecionada
        """
        # Verificar cache primeiro
        cache_key = (source, destination)
        if cache_key in self.optimal_routes_cache:
            cached_route = self.optimal_routes_cache[cache_key]
            if cached_route in available_routes:
                self.optimizer_metrics['cache_hit_rate'] = (
                    0.99 * self.optimizer_metrics['cache_hit_rate'] + 0.01
                )
                return cached_route

        # Epsilon-greedy: explorar ou explorar
        if epsilon is None:
            epsilon = self.config['epsilon_start']

        if np.random.random() < epsilon:
            # Exploração: escolher rota aleatória
            selected = np.random.choice(available_routes)
        else:
            # Exploração: escolher melhor rota baseada em Q-values
            best_score = -float('inf')
            selected = available_routes[0]

            src_idx = torch.tensor([self.known_nodes[source]])
            dst_idx = torch.tensor([self.known_nodes[destination]])

            for route_id in available_routes:
                route = self.routes.get(route_id)
                if not route:
                    continue

                # Extrair features da rota para a rede
                route_features = torch.tensor([[
                    route.metrics.get(RouteMetric.LATENCY, 0.5),
                    route.metrics.get(RouteMetric.FIDELITY, 0.5),
                    route.metrics.get(RouteMetric.BANDWIDTH, 0.5),
                    route.metrics.get(RouteMetric.RELIABILITY, 0.5)
                ]], dtype=torch.float32)

                with torch.no_grad():
                    q_value = self.q_network(src_idx, dst_idx, route_features).item()

                if q_value > best_score:
                    best_score = q_value
                    selected = route_id

        # Atualizar cache
        self.optimal_routes_cache[cache_key] = selected
        if len(self.optimal_routes_cache) > 1000:
            # Remover entradas antigas
            oldest_key = next(iter(self.optimal_routes_cache))
            del self.optimal_routes_cache[oldest_key]

        return selected

    def record_experience(
        self,
        source: str,
        destination: str,
        route_id: str,
        reward: float,
        success: bool
    ) -> Dict[str, Any]:
        """
        Registra experiência de uso de rota para aprendizado.

        Args:
            source: Nó de origem
            destination: Nó de destino
            route_id: Rota utilizada
            reward: Recompensa observada
            success: Se transmissão foi bem-sucedida

        Returns:
            Dict com resultado do registro
        """
        route = self.routes.get(route_id)
        if not route:
            return {'error': 'Route not found'}

        # Atualizar métricas da rota
        route.usage_count += 1
        route.success_rate = (
            0.95 * route.success_rate + 0.05 * (1.0 if success else 0.0)
        )
        route.metrics[RouteMetric.RELIABILITY] = route.success_rate
        route.last_updated = time.time()

        # Criar experiência para aprendizado
        experience = QLearningExperience(
            state=(source, destination),
            action=route_id,
            reward=reward,
            next_state=(destination, source) if success else (source, destination),
            done=success,
            consciousness_hash=self.local_hash,
            timestamp=time.time()
        )

        # Adicionar ao buffer de replay
        self.experience_buffer.append(experience)
        self.optimizer_metrics['experiences_collected'] += 1

        # Atualizar score médio de rotas
        route_score = route.compute_score()
        n = self.optimizer_metrics['experiences_collected']
        old_avg = self.optimizer_metrics['avg_route_score']
        self.optimizer_metrics['avg_route_score'] = (
            (old_avg * (n - 1) + route_score) / n
        )

        return {
            'experience_recorded': True,
            'route_id': route_id,
            'reward': reward,
            'success_rate': route.success_rate
        }

    def train_local_step(self, batch_size: Optional[int] = None) -> Dict[str, float]:
        """
        Executa passo de treino local no buffer de experiências.

        Returns:
            Dict com métricas de treino
        """
        if len(self.experience_buffer) < (batch_size or self.config['batch_size']):
            return {'status': 'insufficient_experiences'}

        # Sample batch do buffer
        batch_size = batch_size or self.config['batch_size']
        experiences = list(np.random.choice(
            list(self.experience_buffer), batch_size, replace=False
        ))

        # Preparar tensors para treino
        src_indices = torch.tensor([
            self.known_nodes[e.state[0]] for e in experiences
        ])
        dst_indices = torch.tensor([
            self.known_nodes[e.state[1]] for e in experiences
        ])

        route_features = torch.tensor([
            [
                self.routes[e.action].metrics.get(RouteMetric.LATENCY, 0.5),
                self.routes[e.action].metrics.get(RouteMetric.FIDELITY, 0.5),
                self.routes[e.action].metrics.get(RouteMetric.BANDWIDTH, 0.5),
                self.routes[e.action].metrics.get(RouteMetric.RELIABILITY, 0.5)
            ]
            for e in experiences
        ], dtype=torch.float32)

        rewards = torch.tensor([e.reward for e in experiences], dtype=torch.float32)

        # Forward pass
        q_values = self.q_network(src_indices, dst_indices, route_features)

        # Loss: MSE entre Q-value previsto e reward alvo
        loss = nn.functional.mse_loss(q_values, rewards)

        # Backward pass
        optimizer = torch.optim.Adam(
            self.q_network.parameters(), lr=self.config['learning_rate']
        )
        optimizer.zero_grad()
        loss.backward()

        # Capturar gradientes para agregação federada
        for name, param in self.q_network.named_parameters():
            if param.grad is not None:
                grad_key = f"{self.local_hash}:{name}"
                self.federated_gradients[grad_key] = param.grad.clone()
                self.gradient_contributors[name] += 1

        optimizer.step()

        # Decair epsilon para menos exploração ao longo do tempo
        self.config['epsilon_start'] = max(
            self.config['epsilon_end'],
            self.config['epsilon_start'] * self.config['epsilon_decay']
        )

        return {
            'loss': loss.item(),
            'batch_size': batch_size,
            'epsilon': self.config['epsilon_start']
        }

    def aggregate_federated_gradients(
        self,
        remote_gradients: Dict[str, torch.Tensor],
        contributor_counts: Dict[str, int]
    ) -> Dict[str, torch.Tensor]:
        """
        Agrega gradientes recebidos de outras consciências (média federada).

        Args:
            remote_gradients: Gradientes de outras consciências
            contributor_counts: Contagem de contribuidores por parâmetro

        Returns:
            Gradientes agregados
        """
        aggregated = {}

        for param_name in self.q_network.state_dict().keys():
            local_grad = self.federated_gradients.get(f"{self.local_hash}:{param_name}")
            if local_grad is None:
                continue

            # Coletar gradientes remotos para este parâmetro
            all_grads = [local_grad]
            total_count = self.gradient_contributors[param_name]

            for remote_hash, grad in remote_gradients.items():
                if remote_hash.endswith(f":{param_name}"):
                    all_grads.append(grad)
                    total_count += contributor_counts.get(param_name, 0)

            # Média ponderada dos gradientes
            if all_grads:
                aggregated_grad = sum(all_grads) / len(all_grads)
                aggregated[f"{self.local_hash}:{param_name}"] = aggregated_grad

        return aggregated

    def apply_federated_update(
        self,
        aggregated_gradients: Dict[str, torch.Tensor]
    ) -> bool:
        """
        Aplica atualização federada aos pesos da rede Q.

        Returns:
            True se atualização foi aplicada com sucesso
        """
        try:
            # Aplicar gradientes agregados
            for name, param in self.q_network.named_parameters():
                grad_key = f"{self.local_hash}:{name}"
                if grad_key in aggregated_gradients:
                    param.grad = aggregated_gradients[grad_key]

            # Passo do otimizador
            optimizer = torch.optim.Adam(
                self.q_network.parameters(), lr=self.config['learning_rate']
            )
            optimizer.step()

            self.optimizer_metrics['federated_updates'] += 1
            logging.info(f"🔄 Federated update applied: {len(aggregated_gradients)} parameters")
            return True

        except Exception as e:
            logging.error(f"❌ Federated update failed: {e}")
            return False

    def get_optimal_route(
        self,
        source: str,
        destination: str,
        available_routes: List[str]
    ) -> Optional[QuantumRoute]:
        """Retorna a rota ótima para um par de nós."""
        if not available_routes:
            return None

        # Selecionar rota baseada na política atual
        best_route_id = self.select_route(
            source, destination, available_routes, epsilon=0.0  # Sempre explorar
        )

        return self.routes.get(best_route_id)

    def update_route_metrics(
        self,
        route_id: str,
        new_metrics: Dict[RouteMetric, float]
    ) -> bool:
        """Atualiza métricas de uma rota conhecida."""
        route = self.routes.get(route_id)
        if not route:
            return False

        route.metrics.update(new_metrics)
        route.last_updated = time.time()

        # Invalidar cache para pares que usam esta rota
        for key in list(self.optimal_routes_cache.keys()):
            if self.optimal_routes_cache[key] == route_id:
                del self.optimal_routes_cache[key]

        return True

    def get_optimizer_metrics(self) -> Dict[str, Any]:
        """Retorna métricas consolidadas do otimizador."""
        return {
            **self.optimizer_metrics,
            'routes_count': len(self.routes),
            'buffer_utilization': len(self.experience_buffer) / self.config['buffer_size'],
            'current_epsilon': self.config['epsilon_start'],
            'federated_contributors': dict(self.gradient_contributors)
        }
