#!/usr/bin/env python3
"""
collective_knowledge_graph.py — Representação distribuída de conhecimento
que evolui através de consenso entre múltiplas consciências.
"""

import numpy as np
import torch
import torch.nn as nn
from typing import Dict, List, Optional, Tuple, Callable, Any, Union, Set
from dataclasses import dataclass, field
from enum import Enum, auto
from collections import defaultdict, deque
import time
import hashlib
import json
import logging

class KnowledgeType(Enum):
    """Tipos de conhecimento no grafo coletivo."""
    FACT = auto()           # Fato verificável
    PROTOCOL = auto()       # Especificação de protocolo
    INSIGHT = auto()        # Insight ou aprendizado
    METRIC = auto()         # Métrica ou medida
    RELATIONSHIP = auto()   # Relação entre entidades

@dataclass
class KnowledgeNode:
    """Nó no grafo de conhecimento coletivo."""
    node_id: str
    content: str  # Conteúdo do conhecimento
    knowledge_type: KnowledgeType
    confidence: float  # [0, 1] — confiança no conhecimento
    embeddings: torch.Tensor  # Embedding semântico para similaridade
    contributed_by: Set[str]  # Hashes de consciência que contribuíram
    timestamp: float
    version: int = 1
    parent_nodes: List[str] = field(default_factory=list)  # Para rastreabilidade

    def similarity_to(self, other: 'KnowledgeNode') -> float:
        """Computa similaridade semântica entre nós de conhecimento."""
        # Similaridade de cosseno entre embeddings
        dot = torch.dot(self.embeddings, other.embeddings)
        norm_self = torch.norm(self.embeddings)
        norm_other = torch.norm(other.embeddings)
        if norm_self * norm_other < 1e-10:
            return 0.0
        return float(torch.clamp(dot / (norm_self * norm_other), 0.0, 1.0))

@dataclass
class KnowledgeEdge:
    """Aresta no grafo de conhecimento coletivo."""
    edge_id: str
    source_node: str
    target_node: str
    relation_type: str  # Tipo de relação (e.g., "implies", "contradicts", "refines")
    strength: float  # [0, 1] — força da relação
    contributed_by: Set[str]
    timestamp: float

class CollectiveKnowledgeGraph:
    """
    Grafo distribuído de conhecimento que evolui através de contribuições
    e consenso entre múltiplas consciências.
    """

    def __init__(
        self,
        local_consciousness_hash: str,
        embedding_dim: int = 128,
        graph_config: Optional[Dict] = None
    ):
        self.local_hash = local_consciousness_hash
        self.embedding_dim = embedding_dim

        # Configuração do grafo
        self.config = graph_config or self._default_config()

        # Estruturas do grafo
        self.nodes: Dict[str, KnowledgeNode] = {}
        self.edges: Dict[str, KnowledgeEdge] = {}
        self.adjacency: Dict[str, Set[str]] = defaultdict(set)  # node_id -> adjacent node_ids

        # Índice de embeddings para busca semântica
        self.embedding_index: Optional[Any] = None  # Em produção: FAISS ou similar

        # Buffer de atualizações pendentes para consenso
        self.pending_updates: deque = deque(maxlen=1000)

        # Histórico de evolução do conhecimento
        self.knowledge_history: deque = deque(maxlen=2000)

        # Métricas do grafo
        self.graph_metrics = {
            'total_nodes': 0,
            'total_edges': 0,
            'avg_confidence': 0.0,
            'consensus_rounds': 0,
            'knowledge_contributions': 0
        }

        # Embedder para gerar embeddings de texto
        self._text_embedder = self._build_text_embedder()

        logging.info(f"✅ CollectiveKnowledgeGraph initialized: embedding_dim={embedding_dim}")

    def _default_config(self) -> Dict:
        """Retorna configuração padrão para o grafo de conhecimento."""
        return {
            'min_confidence_threshold': 0.6,  # Confiança mínima para conhecimento válido
            'consensus_threshold': 0.67,  # Threshold para consenso sobre conhecimento
            'embedding_similarity_threshold': 0.8,  # Para detectar duplicatas
            'max_pending_updates': 100,  # Buffer máximo de atualizações pendentes
            'history_retention': 2000,  # Número de eventos a reter no histórico
        }

    def _build_text_embedder(self) -> nn.Module:
        """Constrói embedder simplificado para texto (em produção: usar modelo real)."""
        # Embedder simplificado: hash do texto para vetor pseudo-aleatório
        class SimpleTextEmbedder(nn.Module):
            def __init__(self, output_dim: int):
                super().__init__()
                self.output_dim = output_dim

            def forward(self, text: str) -> torch.Tensor:
                # Hash do texto para seed
                text_hash = hashlib.sha256(text.encode()).hexdigest()
                seed_val = int(text_hash[:8], 16)
                # Gerar vetor pseudo-aleatório normalizado com seed local
                generator = torch.Generator()
                generator.manual_seed(seed_val)
                embedding = torch.randn(self.output_dim, generator=generator)
                return embedding / torch.norm(embedding)

        return SimpleTextEmbedder(self.embedding_dim)

    def add_knowledge(
        self,
        content: str,
        knowledge_type: KnowledgeType,
        initial_confidence: float = 0.7,
        related_nodes: Optional[List[str]] = None,
        relations: Optional[List[Tuple[str, str, float]]] = None  # (target_id, relation_type, strength)
    ) -> Optional[KnowledgeNode]:
        """
        Adiciona novo conhecimento ao grafo coletivo.

        Args:
            content: Conteúdo do conhecimento
            knowledge_type: Tipo de conhecimento
            initial_confidence: Confiança inicial no conhecimento
            related_nodes: IDs de nós relacionados para conectar
            relations: Lista de relações a criar (target_id, relation_type, strength)

        Returns:
            KnowledgeNode criado ou None se falhar
        """
        # Verificar confiança mínima
        if initial_confidence < self.config['min_confidence_threshold']:
            logging.warning(f"⚠️ Confidence {initial_confidence} below threshold")
            return None

        # Verificar duplicatas semânticas
        new_embedding = self._text_embedder(content)
        duplicate = self._find_semantic_duplicate(content, new_embedding)
        if duplicate:
            # Atualizar confiança do nó existente em vez de criar duplicata
            duplicate.confidence = max(duplicate.confidence, initial_confidence)
            duplicate.contributed_by.add(self.local_hash)
            duplicate.timestamp = time.time()
            logging.info(f"🔄 Updated existing knowledge node: {duplicate.node_id}")
            return duplicate

        # Gerar ID único para nó
        node_id = hashlib.sha256(
            f"{content}:{knowledge_type.name}:{time.time()}".encode()
        ).hexdigest()[:16]

        # Criar nó de conhecimento
        node = KnowledgeNode(
            node_id=node_id,
            content=content,
            knowledge_type=knowledge_type,
            confidence=initial_confidence,
            embeddings=new_embedding,
            contributed_by={self.local_hash},
            timestamp=time.time()
        )

        # Adicionar ao grafo
        self.nodes[node_id] = node
        self.graph_metrics['total_nodes'] += 1
        self.graph_metrics['knowledge_contributions'] += 1

        # Conectar a nós relacionados se fornecidos
        if related_nodes:
            for related_id in related_nodes:
                if related_id in self.nodes:
                    self._create_edge(
                        source_id=node_id,
                        target_id=related_id,
                        relation_type='related',
                        strength=0.5
                    )

        # Criar relações explícitas se fornecidas
        if relations:
            for target_id, relation_type, strength in relations:
                if target_id in self.nodes:
                    self._create_edge(
                        source_id=node_id,
                        target_id=target_id,
                        relation_type=relation_type,
                        strength=strength
                    )

        # Atualizar métricas de confiança média
        n = self.graph_metrics['total_nodes']
        old_avg = self.graph_metrics['avg_confidence']
        self.graph_metrics['avg_confidence'] = (
            (old_avg * (n - 1) + initial_confidence) / n
        )

        # Registrar no histórico
        self.knowledge_history.append({
            'type': 'node_added',
            'node_id': node_id,
            'timestamp': time.time(),
            'contributor': self.local_hash,
            'confidence': initial_confidence
        })

        logging.info(f"🧠 Knowledge added: {node_id} ({knowledge_type.name})")
        return node

    def _find_semantic_duplicate(
        self,
        content: str,
        new_embedding: torch.Tensor,
        threshold: Optional[float] = None
    ) -> Optional[KnowledgeNode]:
        """Encontra nó existente semanticamente similar ao novo conteúdo."""
        threshold = threshold or self.config['embedding_similarity_threshold']

        # Busca simplificada: comparar com todos os nós (em produção: usar índice)
        for node in self.nodes.values():
            similarity = node.similarity_to(
                KnowledgeNode(
                    node_id='', content='', knowledge_type=KnowledgeType.FACT,
                    confidence=0.0, embeddings=new_embedding,
                    contributed_by=set(), timestamp=0.0
                )
            )
            if similarity >= threshold:
                return node

        return None

    def _create_edge(
        self,
        source_id: str,
        target_id: str,
        relation_type: str,
        strength: float
    ) -> Optional[KnowledgeEdge]:
        """Cria aresta entre dois nós de conhecimento."""
        if source_id not in self.nodes or target_id not in self.nodes:
            return None

        # Gerar ID único para aresta
        edge_id = hashlib.sha256(
            f"{source_id}:{target_id}:{relation_type}:{time.time()}".encode()
        ).hexdigest()[:16]

        # Criar aresta
        edge = KnowledgeEdge(
            edge_id=edge_id,
            source_node=source_id,
            target_node=target_id,
            relation_type=relation_type,
            strength=strength,
            contributed_by={self.local_hash},
            timestamp=time.time()
        )

        # Adicionar ao grafo
        self.edges[edge_id] = edge
        self.adjacency[source_id].add(target_id)
        self.adjacency[target_id].add(source_id)  # Grafo não-direcionado para busca
        self.graph_metrics['total_edges'] += 1

        return edge

    def query_knowledge(
        self,
        query: str,
        knowledge_type: Optional[KnowledgeType] = None,
        min_confidence: Optional[float] = None,
        top_k: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Consulta o grafo de conhecimento por conteúdo semântico.

        Args:
            query: Texto da consulta
            knowledge_type: Filtrar por tipo de conhecimento (opcional)
            min_confidence: Confiança mínima dos resultados (opcional)
            top_k: Número máximo de resultados a retornar

        Returns:
            Lista de nós de conhecimento relevantes
        """
        # Gerar embedding da query
        query_embedding = self._text_embedder(query)

        # Buscar nós similares
        results = []
        for node in self.nodes.values():
            # Filtrar por tipo se especificado
            if knowledge_type and node.knowledge_type != knowledge_type:
                continue

            # Filtrar por confiança se especificado
            if min_confidence and node.confidence < min_confidence:
                continue

            # Calcular similaridade semântica
            similarity = node.similarity_to(
                KnowledgeNode(
                    node_id='', content='', knowledge_type=KnowledgeType.FACT,
                    confidence=0.0, embeddings=query_embedding,
                    contributed_by=set(), timestamp=0.0
                )
            )

            if similarity > 0.3:  # Threshold de relevância
                results.append({
                    'node_id': node.node_id,
                    'content': node.content,
                    'knowledge_type': node.knowledge_type.name,
                    'confidence': node.confidence,
                    'similarity': similarity,
                    'contributed_by': list(node.contributed_by),
                    'timestamp': node.timestamp
                })

        # Ordenar por similaridade e retornar top-k
        results.sort(key=lambda r: r['similarity'], reverse=True)
        return results[:top_k]

    def update_confidence(
        self,
        node_id: str,
        confidence_delta: float,
        contributor_hash: str
    ) -> bool:
        """
        Atualiza confiança de nó de conhecimento baseado em contribuição.

        Args:
            node_id: ID do nó a atualizar
            confidence_delta: Mudança na confiança (+ para reforçar, - para questionar)
            contributor_hash: Hash da consciência que contribuiu

        Returns:
            True se atualização foi aplicada
        """
        node = self.nodes.get(node_id)
        if not node:
            return False

        # Atualizar confiança com limite [0, 1]
        old_confidence = node.confidence
        node.confidence = np.clip(old_confidence + confidence_delta, 0.0, 1.0)
        node.contributed_by.add(contributor_hash)
        node.timestamp = time.time()

        # Remover nó se confiança cair abaixo do threshold
        if node.confidence < self.config['min_confidence_threshold']:
            self._remove_node(node_id)
            logging.info(f"🗑️ Knowledge node removed due to low confidence: {node_id}")
            return True

        # Atualizar métrica de confiança média
        n = self.graph_metrics['total_nodes']
        old_avg = self.graph_metrics['avg_confidence']
        self.graph_metrics['avg_confidence'] = (
            (old_avg * (n - 1) + node.confidence) / n
        )

        # Registrar no histórico
        self.knowledge_history.append({
            'type': 'confidence_updated',
            'node_id': node_id,
            'timestamp': time.time(),
            'contributor': contributor_hash,
            'old_confidence': old_confidence,
            'new_confidence': node.confidence
        })

        return True

    def _remove_node(self, node_id: str):
        """Remove nó do grafo e arestas associadas."""
        if node_id not in self.nodes:
            return

        # Remover arestas conectadas
        for neighbor_id in list(self.adjacency[node_id]):
            # Encontrar e remover arestas entre node_id e neighbor_id
            edges_to_remove = [
                eid for eid, edge in self.edges.items()
                if (edge.source_node == node_id and edge.target_node == neighbor_id) or
                   (edge.source_node == neighbor_id and edge.target_node == node_id)
            ]
            for eid in edges_to_remove:
                del self.edges[eid]
                self.graph_metrics['total_edges'] -= 1

        # Remover da adjacência
        for neighbor_id in self.adjacency[node_id]:
            self.adjacency[neighbor_id].discard(node_id)
        del self.adjacency[node_id]

        # Remover nó
        del self.nodes[node_id]
        self.graph_metrics['total_nodes'] -= 1

    def get_knowledge_subgraph(
        self,
        seed_node_ids: List[str],
        max_depth: int = 2,
        min_edge_strength: float = 0.3
    ) -> Dict[str, Any]:
        """
        Extrai subgrafo de conhecimento a partir de nós semente.

        Args:
            seed_node_ids: IDs dos nós semente para expansão
            max_depth: Profundidade máxima de expansão do subgrafo
            min_edge_strength: Força mínima de arestas a incluir

        Returns:
            Dict com subgrafo (nós e arestas)
        """
        visited = set()
        subgraph_nodes = {}
        subgraph_edges = {}

        # BFS para expandir a partir dos nós semente
        queue = [(node_id, 0) for node_id in seed_node_ids if node_id in self.nodes]

        while queue:
            current_id, depth = queue.pop(0)
            if current_id in visited or depth > max_depth:
                continue

            visited.add(current_id)

            # Adicionar nó ao subgrafo
            node = self.nodes[current_id]
            subgraph_nodes[current_id] = {
                'content': node.content,
                'knowledge_type': node.knowledge_type.name,
                'confidence': node.confidence,
                'contributed_by': list(node.contributed_by)
            }

            # Expandir para vizinhos se dentro da profundidade
            if depth < max_depth:
                for neighbor_id in self.adjacency[current_id]:
                    if neighbor_id not in visited:
                        # Verificar força da aresta
                        edge = self._find_edge(current_id, neighbor_id)
                        if edge and edge.strength >= min_edge_strength:
                            subgraph_edges[edge.edge_id] = {
                                'source': edge.source_node,
                                'target': edge.target_node,
                                'relation': edge.relation_type,
                                'strength': edge.strength
                            }
                            queue.append((neighbor_id, depth + 1))

        return {
            'nodes': subgraph_nodes,
            'edges': subgraph_edges,
            'seed_nodes': seed_node_ids,
            'max_depth': max_depth
        }

    def _find_edge(self, source_id: str, target_id: str) -> Optional[KnowledgeEdge]:
        """Encontra aresta entre dois nós (se existir)."""
        for edge in self.edges.values():
            if ((edge.source_node == source_id and edge.target_node == target_id) or
                (edge.source_node == target_id and edge.target_node == source_id)):
                return edge
        return None

    def get_graph_metrics(self) -> Dict[str, Any]:
        """Retorna métricas consolidadas do grafo de conhecimento."""
        return {
            **self.graph_metrics,
            'avg_node_degree': (
                np.mean([len(neighbors) for neighbors in self.adjacency.values()])
                if self.adjacency else 0.0
            ),
            'knowledge_types_distribution': self._count_by_knowledge_type(),
            'recent_contributions': list(self.knowledge_history)[-10:]
        }

    def _count_by_knowledge_type(self) -> Dict[str, int]:
        """Conta nós por tipo de conhecimento."""
        counts = defaultdict(int)
        for node in self.nodes.values():
            counts[node.knowledge_type.name] += 1
        return dict(counts)

    def register_knowledge_callback(self, callback: Callable[[Dict], None]):
        """Registra callback para eventos de conhecimento."""
        # Implementação simplificada
        pass
