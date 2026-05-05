#!/usr/bin/env python3
"""
federated_audit_ledger.py — Ledger de auditoria distribuído e imutável
para registrar decisões de consenso federado.
"""

import asyncio
import time
import hashlib
import json
from pathlib import Path
from typing import Dict, List, Optional, Set, Callable, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum, auto
from collections import defaultdict
import logging
import numpy as np

class LedgerEntryType(Enum):
    """Tipos de entradas no ledger de auditoria federado."""
    DECISION_PROPOSED = auto()    # Decisão proposta para consenso
    DECISION_APPROVED = auto()    # Decisão aprovada via consenso
    DECISION_EXECUTED = auto()    # Decisão executada com sucesso
    CONSENSUS_ROUND = auto()      # Round de consenso completado
    METRIC_AGGREGATION = auto()   # Agregação de métricas com privacidade
    VALIDATOR_VOTE = auto()       # Voto de validador registrado
    EMERGENCY_OVERRIDE = auto()   # Override de emergência registrado

@dataclass
class FederatedLedgerEntry:
    """Entrada individual no ledger de auditoria federado."""
    entry_id: str
    entry_type: LedgerEntryType
    data: Dict[str, Any]
    node_id: str
    timestamp: float
    epoch: int
    previous_hash: str
    merkle_root: str  # Root da Merkle Tree deste bloco
    signature: str  # Assinatura do nó que criou a entrada

    def to_dict(self) -> Dict:
        return {
            'entry_id': self.entry_id,
            'entry_type': self.entry_type.name,
            'data': self.data,
            'node_id': self.node_id,
            'timestamp': self.timestamp,
            'epoch': self.epoch,
            'previous_hash': self.previous_hash,
            'merkle_root': self.merkle_root,
            'signature': self.signature
        }

    def compute_hash(self) -> str:
        """Computa hash da entrada para cadeia imutável."""
        canonical = json.dumps({
            'entry_id': self.entry_id,
            'entry_type': self.entry_type.name,
            'data': self.data,
            'node_id': self.node_id,
            'timestamp': self.timestamp,
            'epoch': self.epoch,
            'previous_hash': self.previous_hash,
            'merkle_root': self.merkle_root
        }, sort_keys=True)
        return hashlib.sha256(canonical.encode()).hexdigest()

class FederatedAuditLedger:
    """
    Ledger de auditoria distribuído e imutável para decisões de consenso federado.
    Características:
    - Cadeia de hashes encadeados para imutabilidade
    - Merkle Trees para verificação eficiente de subconjuntos
    - Replicação assíncrona entre observatórios federados
    - Verificação criptográfica de integridade
    """

    def __init__(
        self,
        node_id: str,
        federation_config: Dict[str, Any],
        key_manager: Optional[Any] = None,
        ledger_path: Optional[str] = None
    ):
        self.node_id = node_id
        self.config = federation_config
        self.key_manager = key_manager
        self.ledger_path = Path(ledger_path) if ledger_path else Path('ledger')
        self.ledger_path.mkdir(parents=True, exist_ok=True)

        # Estado do ledger local
        self.entries: List[FederatedLedgerEntry] = []
        self.entry_index: Dict[str, FederatedLedgerEntry] = {}  # entry_id -> entry
        self.current_epoch = 0
        self.last_entry_hash: Optional[str] = None

        # Merkle tree para verificação rápida
        self.merkle_tree: Optional[Any] = None  # Implementação simplificada

        # Buffer para replicação assíncrona
        self.replication_queue: asyncio.Queue = asyncio.Queue()

        # Cache de hashes de outros nós para verificação cruzada
        self.remote_hash_cache: Dict[str, Dict[str, str]] = defaultdict(dict)

        # Métricas de auditoria
        self.audit_metrics = {
            'entries_created': 0,
            'entries_replicated': 0,
            'integrity_verifications': 0,
            'integrity_failures': 0,
            'avg_replication_latency_ms': 0.0
        }

        # Callbacks para notificação de novas entradas
        self.ledger_callbacks: List[Callable] = []

        # Carregar ledger existente se disponível
        self._load_existing_ledger()

        logging.info(f"✅ FederatedAuditLedger initialized: node={node_id}, entries={len(self.entries)}")

    def _load_existing_ledger(self):
        """Carrega ledger existente do filesystem."""
        ledger_file = self.ledger_path / f"ledger_{self.node_id}.jsonl"
        if ledger_file.exists():
            with open(ledger_file, 'r') as f:
                for line in f:
                    try:
                        entry_data = json.loads(line.strip())
                        entry_data['entry_type'] = LedgerEntryType[entry_data['entry_type']]
                        entry = FederatedLedgerEntry(**entry_data)
                        self.entries.append(entry)
                        self.entry_index[entry.entry_id] = entry
                        if entry.entry_type == LedgerEntryType.CONSENSUS_ROUND:
                            self.current_epoch = max(self.current_epoch, entry.epoch)
                    except Exception as e:
                        logging.warning(f"⚠️ Failed to load ledger entry: {e}")

            # Reconstruir Merkle tree se houver entradas
            if self.entries:
                self._rebuild_merkle_tree()
                self.last_entry_hash = self.entries[-1].compute_hash()

            logging.info(f"📚 Loaded {len(self.entries)} entries from existing ledger")

    def _rebuild_merkle_tree(self):
        """Reconstrói Merkle tree a partir das entradas atuais."""
        # Implementação simplificada: hash concatenado
        if not self.entries:
            self.merkle_tree = None
            return

        # Extrair hashes das entradas
        entry_hashes = [entry.compute_hash() for entry in self.entries]
        # Merkle root simplificado: hash da concatenação ordenada
        self.merkle_tree = hashlib.sha256(
            ''.join(sorted(entry_hashes)).encode()
        ).hexdigest()

    async def append_entry(
        self,
        entry_type: Any,
        data: Dict[str, Any],
        metadata: Optional[Dict] = None
    ) -> FederatedLedgerEntry:
        """
        Adiciona nova entrada ao ledger local e inicia replicação.

        Returns:
            FederatedLedgerEntry criada
        """
        if isinstance(entry_type, str):
            entry_type = LedgerEntryType[entry_type]

        # Gerar ID único para entrada
        entry_id = hashlib.sha256(
            f"{entry_type.name}:{self.node_id}:{time.time()}".encode()
        ).hexdigest()[:16]

        # Criar entrada
        entry = FederatedLedgerEntry(
            entry_id=entry_id,
            entry_type=entry_type,
            data=data,
            node_id=self.node_id,
            timestamp=time.time(),
            epoch=self.current_epoch,
            previous_hash=self.last_entry_hash or 'genesis',
            merkle_root='',  # Será atualizado após adicionar
            signature=''
        )

        # Atualizar Merkle root temporário
        temp_hashes = [e.compute_hash() for e in self.entries] + [entry.compute_hash()]
        # Merkle root simplificado
        entry.merkle_root = hashlib.sha256(
            ''.join(sorted(temp_hashes)).encode()
        ).hexdigest()

        # Assinar entrada
        if self.key_manager:
            entry.signature = self.key_manager.sign_content(
                json.dumps(entry.to_dict(), sort_keys=True)
            )

        # Adicionar ao ledger local
        self.entries.append(entry)
        self.entry_index[entry_id] = entry
        self.last_entry_hash = entry.compute_hash()

        # Atualizar Merkle tree real
        self._rebuild_merkle_tree()

        # Atualizar métricas
        self.audit_metrics['entries_created'] += 1

        # Persistir em disco (assíncrono)
        asyncio.create_task(self._persist_entry_async(entry))

        # Enfileirar para replicação
        await self.replication_queue.put(entry)

        # Iniciar task de replicação se necessário
        if not hasattr(self, '_replication_task') or self._replication_task.done():
            self._replication_task = asyncio.create_task(self._replication_loop())

        # Notificar callbacks
        for callback in self.ledger_callbacks:
            try:
                callback({
                    'type': 'entry_appended',
                    'entry': entry.to_dict(),
                    'node_id': self.node_id
                })
            except Exception as e:
                logging.error(f"⚠️ Ledger callback error: {e}")

        logging.debug(f"📝 Ledger entry appended: {entry_id} ({entry_type.name})")
        return entry

    async def _persist_entry_async(self, entry: FederatedLedgerEntry):
        """Persiste entrada em disco de forma assíncrona."""
        ledger_file = self.ledger_path / f"ledger_{self.node_id}.jsonl"

        try:
            with open(ledger_file, 'a') as f:
                f.write(json.dumps(entry.to_dict()) + '\n')
        except Exception as e:
            logging.error(f"❌ Failed to persist ledger entry: {e}")

    async def _replication_loop(self):
        """Loop de replicação assíncrona para outros observatórios."""
        while True:
            try:
                entry = await self.replication_queue.get()

                # Replicar para outros validadores da federação
                target_nodes = [
                    nid for nid in self.config.get('validators', [])
                    if nid != self.node_id
                ]

                replication_tasks = [
                    self._replicate_to_node(entry, target_node)
                    for target_node in target_nodes
                ]

                if replication_tasks:
                    results = await asyncio.gather(*replication_tasks, return_exceptions=True)
                    successful = sum(1 for r in results if isinstance(r, bool) and r)
                    self.audit_metrics['entries_replicated'] += successful

                self.replication_queue.task_done()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"⚠️ Replication loop error: {e}")
                await asyncio.sleep(1.0)

    async def _replicate_to_node(
        self,
        entry: FederatedLedgerEntry,
        target_node_id: str
    ) -> bool:
        """Replica entrada para nó específico."""
        start_time = time.time()

        # Preparar mensagem de replicação
        replication_msg = {
            'type': 'LEDGER_REPLICATION',
            'entry': entry.to_dict(),
            'source_node': self.node_id,
            'timestamp': time.time()
        }

        # Assinar mensagem
        if self.key_manager:
            replication_msg['signature'] = self.key_manager.sign_content(
                json.dumps(replication_msg, sort_keys=True)
            )

        # Enviar para nó alvo (simulado)
        logging.debug(f"🔄 Replicating entry {entry.entry_id[:8]} to {target_node_id}")

        # Simular latência de rede
        await asyncio.sleep(np.random.uniform(0.01, 0.05))

        # Em produção: enviar via protocolo P2P com confirmação
        success = True  # Simular sucesso

        # Atualizar métricas de latência
        latency_ms = (time.time() - start_time) * 1000
        old_avg = self.audit_metrics['avg_replication_latency_ms']
        n = self.audit_metrics['entries_replicated'] + 1
        self.audit_metrics['avg_replication_latency_ms'] = (
            (old_avg * (n - 1) + latency_ms) / n if n > 1 else latency_ms
        )

        return success

    async def verify_ledger_integrity(
        self,
        start_epoch: Optional[int] = None,
        end_epoch: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Verifica integridade do ledger local via cadeia de hashes.

        Returns:
            Dict com resultado da verificação
        """
        entries_to_verify = self.entries
        if start_epoch is not None:
            entries_to_verify = [e for e in entries_to_verify if e.epoch >= start_epoch]
        if end_epoch is not None:
            entries_to_verify = [e for e in entries_to_verify if e.epoch <= end_epoch]

        if not entries_to_verify:
            return {'status': 'no_entries_to_verify', 'verified_count': 0}

        # Verificar cadeia de hashes
        prev_hash = 'genesis'
        chain_valid = True
        invalid_entries = []

        for entry in entries_to_verify:
            computed_hash = entry.compute_hash()

            # Verificar previous_hash
            if entry.previous_hash != prev_hash:
                chain_valid = False
                invalid_entries.append({
                    'entry_id': entry.entry_id,
                    'error': 'previous_hash_mismatch',
                    'expected': prev_hash,
                    'actual': entry.previous_hash
                })

            # Verificar assinatura se key_manager disponível
            if self.key_manager and entry.signature:
                payload = json.dumps({
                    'entry_id': entry.entry_id,
                    'entry_type': entry.entry_type.name,
                    'data': entry.data,
                    'node_id': entry.node_id,
                    'timestamp': entry.timestamp,
                    'epoch': entry.epoch,
                    'previous_hash': entry.previous_hash,
                    'merkle_root': entry.merkle_root
                }, sort_keys=True)

                if not self.key_manager.verify_signature(
                    content_hash=payload,
                    signature=entry.signature,
                    signer_node_id=entry.node_id
                ):
                    chain_valid = False
                    invalid_entries.append({
                        'entry_id': entry.entry_id,
                        'error': 'signature_verification_failed'
                    })

            prev_hash = computed_hash

        # Verificar Merkle root
        merkle_valid = True
        if self.merkle_tree and entries_to_verify:
            expected_root = hashlib.sha256(
                ''.join(sorted([e.compute_hash() for e in entries_to_verify])).encode()
            ).hexdigest()
            if expected_root != entries_to_verify[-1].merkle_root:
                merkle_valid = False

        self.audit_metrics['integrity_verifications'] += 1
        if not chain_valid or not merkle_valid:
            self.audit_metrics['integrity_failures'] += 1

        return {
            'status': 'valid' if (chain_valid and merkle_valid) else 'invalid',
            'chain_valid': chain_valid,
            'merkle_valid': merkle_valid,
            'entries_verified': len(entries_to_verify),
            'invalid_entries': invalid_entries,
            'first_entry_epoch': entries_to_verify[0].epoch if entries_to_verify else None,
            'last_entry_epoch': entries_to_verify[-1].epoch if entries_to_verify else None,
            'verification_timestamp': time.time()
        }

    def register_ledger_callback(self, callback: Callable[[Dict], None]):
        """Registra callback para notificação de novas entradas no ledger."""
        self.ledger_callbacks.append(callback)

    def export_ledger_snapshot(
        self,
        output_path: str,
        start_epoch: Optional[int] = None,
        end_epoch: Optional[int] = None,
        include_proofs: bool = True
    ) -> str:
        """Exporta snapshot do ledger para auditoria externa."""
        entries_to_export = self.entries
        if start_epoch is not None:
            entries_to_export = [e for e in entries_to_export if e.epoch >= start_epoch]
        if end_epoch is not None:
            entries_to_export = [e for e in entries_to_export if e.epoch <= end_epoch]

        snapshot = {
            'export_timestamp': time.time(),
            'node_id': self.node_id,
            'epoch_range': {
                'start': start_epoch,
                'end': end_epoch
            },
            'entry_count': len(entries_to_export),
            'entries': [e.to_dict() for e in entries_to_export]
        }

        # Salvar snapshot
        with open(output_path, 'w') as f:
            json.dump(snapshot, f, indent=2, default=str)

        logging.info(f"📋 Ledger snapshot exported to {output_path}")
        return output_path

    def get_ledger_health(self) -> Dict[str, Any]:
        """Retorna métricas de saúde do ledger distribuído."""
        return {
            'node_id': self.node_id,
            'total_entries': len(self.entries),
            'current_epoch': self.current_epoch,
            'last_entry_timestamp': self.entries[-1].timestamp if self.entries else None,
            'merkle_tree_valid': self.merkle_tree is not None,
            'replication_queue_size': self.replication_queue.qsize(),
            'metrics': self.audit_metrics,
            'storage_path': str(self.ledger_path)
        }
