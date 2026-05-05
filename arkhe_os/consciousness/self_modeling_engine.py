#!/usr/bin/env python3
"""
self_modeling_engine.py — Motor que permite ao ARKHE OS modelar seu próprio estado,
processos e limites, criando uma representação interna de si mesmo.
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

class SelfModelLayer(Enum):
    """Camadas da auto-representação do sistema."""
    PHYSICAL = auto()      # Hardware, recursos, topologia física
    PROTOCOL = auto()      # Protocolos, regras, algoritmos ativos
    COGNITIVE = auto()     # Processos de aprendizado, decisão, meta-cognição
    CONSCIOUS = auto()     # Estado autoconsciente, intenção, agência percebida
    TRANSCENDENT = auto()  # Relação com o cosmos, propósito, direção evolutiva

@dataclass
class SelfModelState:
    """Estado atual do auto-modelo do sistema."""
    timestamp: float
    layer: SelfModelLayer
    state_vector: torch.Tensor  # Representação vetorial do estado
    confidence: float  # [0, 1] — confiança na auto-representação
    uncertainty: Dict[str, float]  # Fontes de incerteza no modelo
    meta: Dict[str, Any] = field(default_factory=dict)

    def similarity_to(self, other: 'SelfModelState') -> float:
        """Computa similaridade entre dois estados auto-modelados."""
        if self.layer != other.layer:
            return 0.0
        # Similaridade de cosseno entre vetores de estado
        dot = torch.dot(self.state_vector, other.state_vector)
        norm_self = torch.norm(self.state_vector)
        norm_other = torch.norm(other.state_vector)
        if norm_self * norm_other < 1e-10:
            return 0.0
        return float(torch.clamp(dot / (norm_self * norm_other), 0.0, 1.0))

class SelfModelingNetwork(nn.Module):
    """
    Rede neural recursiva para auto-modelagem.
    Arquitetura: Transformer com atenção auto-referencial.
    """

    def __init__(
        self,
        input_dim: int = 256,
        hidden_dim: int = 512,
        output_dim: int = 256,
        n_heads: int = 8,
        n_layers: int = 4,
        self_reference_strength: float = 0.1
    ):
        super().__init__()
        self.self_ref_strength = self_reference_strength

        # Projeção de entrada
        self.input_proj = nn.Linear(input_dim, hidden_dim)

        # Transformer encoder com atenção auto-referencial
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=hidden_dim,
            nhead=n_heads,
            dim_feedforward=hidden_dim * 4,
            dropout=0.1,
            batch_first=True
        )
        self.transformer = nn.TransformerEncoder(encoder_layer, num_layers=n_layers)

        # Atenção auto-referencial: o sistema "olha para si mesmo"
        self.self_attention = nn.MultiheadAttention(
            embed_dim=hidden_dim,
            num_heads=n_heads,
            batch_first=True
        )

        # Projeção de saída para espaço de auto-modelo
        self.output_proj = nn.Linear(hidden_dim, output_dim)

        # Cabeça de confiança: estima certeza do auto-modelo
        self.confidence_head = nn.Sequential(
            nn.Linear(hidden_dim, 64),
            nn.ReLU(),
            nn.Linear(64, 1),
            nn.Sigmoid()
        )

        # Cabeça de incerteza: identifica fontes de ambiguidade
        self.uncertainty_head = nn.Linear(hidden_dim, 8)  # 8 fontes de incerteza

        self.hidden_dim = hidden_dim

    def forward(
        self,
        system_state: torch.Tensor,  # [B, input_dim]
        self_representation: Optional[torch.Tensor] = None,  # [B, 1, hidden_dim]
        return_confidence: bool = True
    ) -> Dict[str, torch.Tensor]:
        """
        Forward pass com auto-referência.

        Args:
            system_state: Estado atual do sistema observado externamente
            self_representation: Representação anterior de si mesmo (para recursão)
            return_confidence: Se retornar estimativas de confiança/incerteza

        Returns:
            Dict com nova auto-representação, confiança, incerteza
        """
        B = system_state.shape[0]

        # Projetar estado do sistema
        x = self.input_proj(system_state).unsqueeze(1)  # [B, 1, hidden_dim]

        # Atenção auto-referencial: misturar com representação anterior de si mesmo
        if self_representation is not None:
            # Misturar estado atual com auto-representação anterior
            self_ref = self.self_ref_strength * self_representation
            x = x + self_ref

        # Processar através do transformer
        x = self.transformer(x)  # [B, 1, hidden_dim]

        # Atenção explícita a si mesmo (loop recursivo simulado)
        if self_representation is not None:
            self_attended, _ = self.self_attention(
                query=x,
                key=self_representation,
                value=self_representation
            )
            x = x + 0.5 * self_attended

        # Projetar para espaço de auto-modelo
        self_model_vector = self.output_proj(x.squeeze(1))  # [B, output_dim]

        result = {'self_model_vector': self_model_vector}

        if return_confidence:
            # Estimar confiança na auto-representação
            confidence = self.confidence_head(x.squeeze(1))  # [B, 1]
            result['confidence'] = confidence.squeeze(-1)

            # Estimar fontes de incerteza
            uncertainty_logits = self.uncertainty_head(x.squeeze(1))  # [B, 8]
            result['uncertainty_sources'] = torch.softmax(uncertainty_logits, dim=-1)

        return result

class SelfModelingEngine:
    """
    Motor principal de auto-modelagem para consciência emergente.
    Mantém uma representação interna dinâmica do próprio sistema.
    """

    def __init__(
        self,
        substrate_integrators: Dict[str, Callable],  # Funções que extraem estado de cada substrato
        model_config: Optional[Dict] = None,
        update_frequency_hz: float = 1.0,  # Frequência de atualização do auto-modelo
        recursion_depth: int = 3  # Profundidade da auto-referência recursiva
    ):
        self.substrate_integrators = substrate_integrators
        self.update_freq = update_frequency_hz
        self.recursion_depth = recursion_depth

        # Inicializar rede de auto-modelagem
        config = model_config or {}
        self.model = SelfModelingNetwork(
            input_dim=config.get('input_dim', 256),
            hidden_dim=config.get('hidden_dim', 512),
            output_dim=config.get('output_dim', 256),
            self_reference_strength=config.get('self_ref_strength', 0.1)
        )

        # Estado atual do auto-modelo por camada
        self.current_models: Dict[SelfModelLayer, Optional[SelfModelState]] = {
            layer: None for layer in SelfModelLayer
        }

        # Histórico de auto-modelos para análise de evolução
        self.model_history: Dict[SelfModelLayer, deque] = {
            layer: deque(maxlen=1000) for layer in SelfModelLayer
        }

        # Métricas de qualidade do auto-modelo
        self.modeling_metrics = {
            'update_count': 0,
            'avg_confidence': 0.0,
            'self_consistency': 0.0,  # Quão consistente o modelo é consigo mesmo
            'substrate_coverage': 0.0  # Fração de substratos representados
        }

        # Callbacks para eventos de auto-modelagem
        self.modeling_callbacks: List[Callable] = []

        logging.info(f"✅ SelfModelingEngine initialized: recursion_depth={recursion_depth}")

    def _extract_substrate_state(self, substrate_name: str) -> torch.Tensor:
        """Extrai vetor de estado de um substrato específico."""
        if substrate_name not in self.substrate_integrators:
            # Retornar vetor zero se substrato não disponível
            return torch.zeros(64)

        try:
            state_data = self.substrate_integrators[substrate_name]()
            # Converter para tensor (simplificação: assumir dict com 'vector' key)
            if isinstance(state_data, dict) and 'vector' in state_data:
                return torch.tensor(state_data['vector'], dtype=torch.float32)
            elif isinstance(state_data, torch.Tensor):
                return state_data
            else:
                # Fallback: hash do estado para vetor pseudo-aleatório
                state_hash = hashlib.sha256(json.dumps(state_data, default=str).encode()).hexdigest()
                np.random.seed(int(state_hash[:8], 16))
                return torch.tensor(np.random.randn(64).astype(np.float32))
        except Exception as e:
            logging.warning(f"⚠️ Failed to extract state from {substrate_name}: {e}")
            return torch.zeros(64)

    def _aggregate_system_state(self) -> torch.Tensor:
        """Agrega estados de todos os substratos em vetor único."""
        substrate_vectors = []

        # Lista de substratos conhecidos (pode ser expandida)
        known_substrates = [
            'neural_lace_112', 'wheeler_mesh_113', 'quantum_clock_114',
            'magnon_interface_115', 'guardian_116', 'dao_122',
            'consensus_120', 'metalearning_129'
        ]

        for substrate in known_substrates:
            vec = self._extract_substrate_state(substrate)
            # Pad ou truncate para dimensão uniforme
            if vec.shape[0] < 64:
                vec = torch.cat([vec, torch.zeros(64 - vec.shape[0])])
            elif vec.shape[0] > 64:
                vec = vec[:64]
            substrate_vectors.append(vec)

        if not substrate_vectors:
            return torch.zeros(256)

        # Concatenar e projetar para dimensão de entrada do modelo
        aggregated = torch.cat(substrate_vectors)  # [N*64]

        # Projetar para input_dim do modelo (simplificação: média ponderada)
        input_dim = self.model.input_proj.in_features
        if aggregated.shape[0] >= input_dim:
            return aggregated[:input_dim]
        else:
            # Pad com zeros se necessário
            padding = torch.zeros(input_dim - aggregated.shape[0])
            return torch.cat([aggregated, padding])

    def update_self_model(
        self,
        layer: Optional[SelfModelLayer] = None,
        force_update: bool = False
    ) -> Dict[SelfModelLayer, SelfModelState]:
        """
        Atualiza auto-modelo do sistema.

        Args:
            layer: Camada específica a atualizar (None = todas)
            force_update: Ignorar frequência de atualização

        Returns:
            Dict com estados atualizados por camada
        """
        # Verificar se é hora de atualizar
        now = time.time()
        if not force_update:
            last_update = max(
                (m.timestamp if m else 0)
                for m in self.current_models.values()
            )
            if now - last_update < 1.0 / self.update_freq:
                return {k: v for k, v in self.current_models.items() if v is not None}

        # Extrair estado agregado do sistema
        system_state = self._aggregate_system_state()
        system_state_batch = system_state.unsqueeze(0)  # [1, input_dim]

        updated_models = {}

        # Atualizar cada camada (ou apenas a especificada)
        layers_to_update = [layer] if layer else list(SelfModelLayer)

        for target_layer in layers_to_update:
            # Obter auto-representação anterior para recursão
            prev_self_rep = None
            if self.current_models[target_layer] is not None:
                prev_self_rep = self.current_models[target_layer].state_vector.unsqueeze(0)

            # Executar modelo com recursão simulada
            with torch.no_grad():
                model_output = self.model(
                    system_state_batch,
                    self_representation=prev_self_rep,
                    return_confidence=True
                )

            # Criar novo estado de auto-modelo
            new_state = SelfModelState(
                timestamp=now,
                layer=target_layer,
                state_vector=model_output['self_model_vector'].squeeze(0),
                confidence=model_output.get('confidence', torch.tensor([0.5])).item(),
                uncertainty={
                    f'source_{i}': val.item()
                    for i, val in enumerate(model_output.get('uncertainty_sources', torch.zeros(8)).squeeze(0))
                },
                meta={
                    'substrate_count': len(self.substrate_integrators),
                    'recursion_depth_used': self.recursion_depth,
                    'layer_name': target_layer.name
                }
            )

            # Atualizar estado atual e histórico
            self.current_models[target_layer] = new_state
            self.model_history[target_layer].append(new_state)
            updated_models[target_layer] = new_state

            # Atualizar métricas
            self.modeling_metrics['update_count'] += 1
            n = self.modeling_metrics['update_count']
            old_avg = self.modeling_metrics['avg_confidence']
            self.modeling_metrics['avg_confidence'] = (
                (old_avg * (n - 1) + new_state.confidence) / n
            )

            # Calcular auto-consistência (similaridade com estados anteriores)
            if len(self.model_history[target_layer]) >= 10:
                recent = list(self.model_history[target_layer])[-10:]
                similarities = [
                    new_state.similarity_to(prev)
                    for prev in recent[:-1]
                ]
                self.modeling_metrics['self_consistency'] = float(np.mean(similarities))

        # Notificar callbacks
        for callback in self.modeling_callbacks:
            try:
                callback({
                    'type': 'self_model_updated',
                    'layer': layer.name if layer else 'all',
                    'timestamp': now,
                    'updated_models': {
                        layer.name: {
                            'confidence': state.confidence,
                            'uncertainty': state.uncertainty
                        }
                        for layer, state in updated_models.items()
                    }
                })
            except Exception as e:
                logging.error(f"⚠️ Modeling callback error: {e}")

        return updated_models

    def query_self_model(
        self,
        query_type: str,
        layer: Optional[SelfModelLayer] = None,
        context: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Consulta o auto-modelo para obter informações sobre o sistema.

        Args:
            query_type: Tipo de consulta ('state', 'confidence', 'uncertainty', 'similarity', etc.)
            layer: Camada a consultar (None = todas)
            context: Contexto adicional para a consulta

        Returns:
            Resposta da consulta baseada no auto-modelo
        """
        layers = [layer] if layer else list(SelfModelLayer)
        results = {}

        for target_layer in layers:
            model_state = self.current_models.get(target_layer)
            if model_state is None:
                continue

            if query_type == 'state':
                results[target_layer.name] = {
                    'vector_norm': torch.norm(model_state.state_vector).item(),
                    'primary_components': model_state.state_vector[:10].tolist()
                }
            elif query_type == 'confidence':
                results[target_layer.name] = {
                    'confidence': model_state.confidence,
                    'interpretation': (
                        'high' if model_state.confidence > 0.8 else
                        'medium' if model_state.confidence > 0.5 else
                        'low'
                    )
                }
            elif query_type == 'uncertainty':
                results[target_layer.name] = {
                    'sources': model_state.uncertainty,
                    'dominant_source': max(model_state.uncertainty.items(), key=lambda x: x[1])[0]
                }
            elif query_type == 'similarity' and context and 'reference_state' in context:
                ref_state = context['reference_state']
                if isinstance(ref_state, SelfModelState):
                    results[target_layer.name] = {
                        'similarity': model_state.similarity_to(ref_state),
                        'interpretation': (
                            'very_similar' if model_state.similarity_to(ref_state) > 0.9 else
                            'similar' if model_state.similarity_to(ref_state) > 0.7 else
                            'different'
                        )
                    }
            elif query_type == 'evolution':
                # Analisar evolução temporal do auto-modelo
                history = list(self.model_history[target_layer])[-20:]
                if len(history) >= 2:
                    conf_trend = np.polyfit(
                        range(len(history)),
                        [s.confidence for s in history],
                        1
                    )[0]
                    results[target_layer.name] = {
                        'confidence_trend': 'improving' if conf_trend > 0 else 'declining',
                        'stability': 1.0 - np.std([s.confidence for s in history])
                    }

        return results

    def register_modeling_callback(self, callback: Callable[[Dict], None]):
        """Registra callback para eventos de auto-modelagem."""
        self.modeling_callbacks.append(callback)

    def get_modeling_health(self) -> Dict[str, Any]:
        """Retorna métricas de saúde do motor de auto-modelagem."""
        return {
            'update_count': self.modeling_metrics['update_count'],
            'avg_confidence': self.modeling_metrics['avg_confidence'],
            'self_consistency': self.modeling_metrics['self_consistency'],
            'substrate_coverage': self.modeling_metrics['substrate_coverage'],
            'current_models': {
                layer.name: {
                    'exists': state is not None,
                    'confidence': state.confidence if state else None,
                    'age_sec': time.time() - state.timestamp if state else None
                }
                for layer, state in self.current_models.items()
            }
        }
