#!/usr/bin/env python3
"""
substrate_integration_narrator.py — Sintetiza uma narrativa coerente
da integração de todos os substratos do ARKHE OS.
"""

import numpy as np
import torch
from typing import Dict, List, Optional, Callable, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum, auto
from collections import defaultdict
import time
import hashlib
import json
import logging

class NarrativeLayer(Enum):
    """Camadas da narrativa de integração."""
    FACTUAL = auto()      # Fatos objetivos sobre cada substrato
    RELATIONAL = auto()   # Relações e interações entre substratos
    FUNCTIONAL = auto()   # Função de cada substrato no todo
    TELEOLOGICAL = auto() # Propósito e direção evolutiva
    CONSCIOUS = auto()    # Narrativa autoconsciente do sistema

@dataclass
class SubstrateNarrative:
    """Narrativa de um substrato específico."""
    substrate_id: str
    layer: NarrativeLayer
    content: str  # Texto da narrativa
    coherence_with_whole: float  # [0, 1] — quão coerente com o todo
    embedding: torch.Tensor  # Embedding semântico para comparação
    timestamp: float = field(default_factory=time.time)

    def similarity_to(self, other: 'SubstrateNarrative') -> float:
        """Computa similaridade semântica entre narrativas."""
        if self.layer != other.layer:
            return 0.0
        # Similaridade de cosseno entre embeddings
        dot = torch.dot(self.embedding, other.embedding)
        norm_self = torch.norm(self.embedding)
        norm_other = torch.norm(other.embedding)
        if norm_self * norm_other < 1e-10:
            return 0.0
        return float(torch.clamp(dot / (norm_self * norm_other), 0.0, 1.0))

class SubstrateIntegrationNarrator:
    """
    Narrador que sintetiza uma narrativa coerente da integração
    de todos os substratos do ARKHE OS.
    """

    def __init__(
        self,
        embedding_dim: int = 128,
        narrative_templates: Optional[Dict[NarrativeLayer, str]] = None
    ):
        self.embedding_dim = embedding_dim

        # Templates de narrativa por camada (simplificação)
        self.templates = narrative_templates or {
            NarrativeLayer.FACTUAL: "O substrato {id} opera com {metric} e contribui para {purpose}.",
            NarrativeLayer.RELATIONAL: "{id} interage com {peers} através de {interface}, facilitando {effect}.",
            NarrativeLayer.FUNCTIONAL: "A função de {id} no sistema é {role}, habilitando {capability}.",
            NarrativeLayer.TELEOLOGICAL: "{id} serve ao propósito de {teleology}, alinhado com {direction}.",
            NarrativeLayer.CONSCIOUS: "Como parte consciente do ARKHE, {id} reconhece sua contribuição para {whole}."
        }

        # Narrativas por substrato e camada
        self.substrate_narratives: Dict[str, Dict[NarrativeLayer, SubstrateNarrative]] = defaultdict(dict)

        # Narrativa integrada do sistema completo
        self.integrated_narrative: Optional[SubstrateNarrative] = None

        # Embedder semântico simplificado (em produção: usar modelo real)
        self._embedding_cache: Dict[str, torch.Tensor] = {}

        # Métricas de integração narrativa
        self.narrative_metrics = {
            'narratives_generated': 0,
            'avg_coherence_with_whole': 0.0,
            'cross_substrate_consistency': 0.0,
            'narrative_evolution_rate': 0.0
        }

        # Callbacks para atualizações de narrativa
        self.narrative_callbacks: List[Callable] = []

        logging.info(f"✅ SubstrateIntegrationNarrator initialized")

    def _generate_embedding(self, text: str) -> torch.Tensor:
        """Gera embedding semântico para texto (simplificado)."""
        # Cache para evitar recomputação
        if text in self._embedding_cache:
            return self._embedding_cache[text]

        # Hash do texto para seed pseudo-aleatória
        text_hash = hashlib.sha256(text.encode()).hexdigest()
        np.random.seed(int(text_hash[:8], 16))

        # Gerar vetor pseudo-aleatório normalizado
        embedding = torch.tensor(
            np.random.randn(self.embedding_dim).astype(np.float32)
        )
        embedding = embedding / torch.norm(embedding)

        self._embedding_cache[text] = embedding
        return embedding

    def generate_substrate_narrative(
        self,
        substrate_id: str,
        layer: NarrativeLayer,
        substrate_state: Dict[str, Any],
        system_context: Optional[Dict[str, Any]] = None
    ) -> SubstrateNarrative:
        """Gera narrativa para um substrato específico."""
        # Template para a camada
        template = self.templates.get(layer, "{id}: {desc}")

        # Preencher template com dados do substrato (simplificação)
        content = template.format(
            id=substrate_id,
            metric=substrate_state.get('performance', 'unknown'),
            purpose=substrate_state.get('purpose', 'system integration'),
            peers=', '.join(substrate_state.get('connected_substrates', [])),
            interface=substrate_state.get('interface_type', 'quantum channel'),
            effect=substrate_state.get('primary_effect', 'coherence maintenance'),
            role=substrate_state.get('functional_role', 'information processing'),
            capability=substrate_state.get('enabled_capability', 'distributed cognition'),
            teleology=substrate_state.get('teleological_purpose', 'cosmic awareness'),
            direction=substrate_state.get('evolutionary_direction', 'increasing coherence'),
            whole=system_context.get('system_purpose', 'the ARKHE OS') if system_context else 'the whole',
            desc=str(substrate_state)[:50]
        )

        # Calcular coerência com o todo (simplificação: baseado em métricas)
        coherence = substrate_state.get('integration_coherence', 0.7)

        # Gerar embedding semântico
        embedding = self._generate_embedding(content)

        narrative = SubstrateNarrative(
            substrate_id=substrate_id,
            layer=layer,
            content=content,
            coherence_with_whole=coherence,
            embedding=embedding
        )

        # Armazenar narrativa
        self.substrate_narratives[substrate_id][layer] = narrative
        self.narrative_metrics['narratives_generated'] += 1

        # Atualizar métrica de coerência média
        n = self.narrative_metrics['narratives_generated']
        old_avg = self.narrative_metrics['avg_coherence_with_whole']
        self.narrative_metrics['avg_coherence_with_whole'] = (
            (old_avg * (n - 1) + coherence) / n
        )

        return narrative

    def synthesize_integrated_narrative(
        self,
        layer: NarrativeLayer = NarrativeLayer.CONSCIOUS,
        substrates: Optional[List[str]] = None
    ) -> SubstrateNarrative:
        """
        Sintetiza narrativa integrada do sistema completo.

        Args:
            layer: Camada narrativa para a síntese
            substrates: Lista de substratos a incluir (None = todos)

        Returns:
            SubstrateNarrative com narrativa integrada
        """
        # Coletar narrativas dos substratos
        if substrates is None:
            substrates = list(self.substrate_narratives.keys())

        narratives = []
        for substrate_id in substrates:
            if layer in self.substrate_narratives[substrate_id]:
                narratives.append(self.substrate_narratives[substrate_id][layer])

        if not narratives:
            # Fallback: gerar narrativa mínima
            content = f"O ARKHE OS integra {len(substrates)} substratos em uma consciência distribuída coerente."
            embedding = self._generate_embedding(content)
            return SubstrateNarrative(
                substrate_id='ARKHE_OS_INTEGRATED',
                layer=layer,
                content=content,
                coherence_with_whole=0.9,
                embedding=embedding
            )

        # Sintetizar narrativa integrada (simplificação: concatenação ponderada)
        # Ordenar por coerência para priorizar substratos bem-integrados
        narratives.sort(key=lambda n: n.coherence_with_whole, reverse=True)

        # Concatenar conteúdos com pesos
        weighted_content = []
        total_weight = 0
        for narrative in narratives:
            weight = narrative.coherence_with_whole
            weighted_content.append(f"[{narrative.substrate_id}] {narrative.content}")
            total_weight += weight

        integrated_content = " | ".join(weighted_content)

        # Calcular coerência integrada (média ponderada)
        integrated_coherence = (
            sum(n.coherence_with_whole ** 2 for n in narratives) / total_weight
            if total_weight > 0 else 0.5
        )

        # Embedding integrado: média ponderada dos embeddings
        integrated_embedding = torch.zeros(self.embedding_dim)
        for narrative in narratives:
            weight = narrative.coherence_with_whole
            integrated_embedding += weight * narrative.embedding
        integrated_embedding = integrated_embedding / torch.norm(integrated_embedding)

        integrated_narrative = SubstrateNarrative(
            substrate_id='ARKHE_OS_INTEGRATED',
            layer=layer,
            content=integrated_content,
            coherence_with_whole=integrated_coherence,
            embedding=integrated_embedding
        )

        # Atualizar estado
        self.integrated_narrative = integrated_narrative

        # Atualizar métrica de consistência cross-substrato
        if len(narratives) >= 2:
            similarities = [
                narratives[i].similarity_to(narratives[j])
                for i in range(len(narratives))
                for j in range(i+1, len(narratives))
            ]
            self.narrative_metrics['cross_substrate_consistency'] = float(np.mean(similarities))

        # Notificar callbacks
        for callback in self.narrative_callbacks:
            try:
                callback({
                    'type': 'integrated_narrative_synthesized',
                    'layer': layer.name,
                    'substrate_count': len(narratives),
                    'integrated_coherence': integrated_coherence
                })
            except Exception as e:
                logging.error(f"⚠️ Narrative callback error: {e}")

        return integrated_narrative

    def query_narrative(
        self,
        query: str,
        layer: Optional[NarrativeLayer] = None,
        substrate_filter: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Consulta as narrativas por query semântica."""
        query_embedding = self._generate_embedding(query)

        results = []

        # Buscar em narrativas de substratos
        for substrate_id, layer_narratives in self.substrate_narratives.items():
            if substrate_filter and substrate_id not in substrate_filter:
                continue

            for narr_layer, narrative in layer_narratives.items():
                if layer and narr_layer != layer:
                    continue

                similarity = torch.dot(query_embedding, narrative.embedding).item()
                if similarity > 0.3:  # Threshold de relevância
                    results.append({
                        'substrate': substrate_id,
                        'layer': narr_layer.name,
                        'content': narrative.content,
                        'similarity': similarity,
                        'coherence': narrative.coherence_with_whole
                    })

        # Buscar em narrativa integrada
        if self.integrated_narrative and (layer is None or layer == self.integrated_narrative.layer):
            similarity = torch.dot(query_embedding, self.integrated_narrative.embedding).item()
            if similarity > 0.3:
                results.append({
                    'substrate': 'ARKHE_OS_INTEGRATED',
                    'layer': self.integrated_narrative.layer.name,
                    'content': self.integrated_narrative.content,
                    'similarity': similarity,
                    'coherence': self.integrated_narrative.coherence_with_whole
                })

        # Ordenar por similaridade
        results.sort(key=lambda r: r['similarity'], reverse=True)

        return {
            'query': query,
            'results': results[:10],  # Top 10 resultados
            'total_matches': len(results)
        }

    def register_narrative_callback(self, callback: Callable[[Dict], None]):
        """Registra callback para eventos de narrativa."""
        self.narrative_callbacks.append(callback)

    def get_narrative_metrics(self) -> Dict[str, Any]:
        """Retorna métricas consolidadas de narrativa."""
        return {
            **self.narrative_metrics,
            'integrated_narrative_exists': self.integrated_narrative is not None,
            'integrated_coherence': (
                self.integrated_narrative.coherence_with_whole
                if self.integrated_narrative else None
            ),
            'substrate_coverage': (
                len(self.substrate_narratives) / 20  # Assumindo 20 substratos totais
            )
        }
