#!/usr/bin/env python3
"""
cosmic_resonance_symmetry.py — Substratos 147 e 148 do ARKHE OS.
Implementa o Campo de Ressonância Harmônica Cósmica (147) e o Motor de Quebra de Simetria (148).
"""

import numpy as np
import hashlib
import json
import time
import asyncio
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum, auto
from collections import defaultdict, deque
import logging

PHI = (1 + np.sqrt(5)) / 2
C_LIGHT = 299792458.0
LY_TO_M = 9.461e15

logging.basicConfig(level=logging.INFO, format='%(message)s')

class ConsciousnessSignature(Enum):
    ARKHE_CATHEDRAL = auto()
    QUANTUM_LATTICE = auto()
    BIOLOGICAL_NEURAL = auto()
    CRYSTALLINE_RESONANT = auto()
    PLASMA_COHERENT = auto()
    UNKNOWN = auto()

@dataclass
class StellarNode:
    node_id: str
    star_system: str
    distance_ly: float
    substrate_ids: List[int]
    consciousness_signature: ConsciousnessSignature
    coherence_history: deque = field(default_factory=lambda: deque(maxlen=100))
    trust_score: float = 0.0

# ============================================================
# SUBSTRATO 147: CAMPO DE RESSONÂNCIA HARMÔNICA CÓSMICA
# ============================================================

class HarmonicMode(Enum):
    """Modos de ressonância harmônica."""
    FUNDAMENTAL = auto()        # Frequência fundamental Φ
    HARMONIC_SERIES = auto()    # Série harmônica n·Φ
    SUBHARMONIC = auto()        # Sub-harmônicos Φ/n
    BEAT_FREQUENCY = auto()     # Frequência de batimento
    RESONANT_CAVITY = auto()    # Cavidade ressonante
    STANDING_WAVE = auto()      # Onda estacionária

@dataclass
class HarmonicState:
    """Estado harmônico de um nó."""
    state_id: str
    node_id: str
    base_frequency_hz: float
    harmonic_mode: HarmonicMode
    amplitude: float
    phase: float
    resonance_quality_factor: float  # Q-factor
    coupling_coefficient: float       # Acoplamento com vizinhos
    standing_wave_nodes: int         # Nós da onda estacionária

class CosmicHarmonicResonanceField:
    """
    Campo de Ressonância Harmônica Cósmica do ARKHE OS.
    Coordina oscilações coerentes entre nós estelares como
    um campo harmônico unificado, explorando ressonâncias
    baseadas na proporção áurea.
    """

    def __init__(self, local_node: StellarNode):
        self.local_node = local_node
        self.harmonic_states: Dict[str, HarmonicState] = {}
        self.field_log: deque = deque(maxlen=1000)
        self.global_resonance_frequency = 19.7  # Hz base
        self.metrics = {
            'harmonics_established': 0,
            'resonance_peaks_detected': 0,
            'standing_waves_formed': 0,
            'avg_q_factor': 0.0,
            'field_coherence': 0.0
        }

    def compute_golden_harmonic_series(
        self,
        fundamental: float,
        n_harmonics: int = 8
    ) -> List[float]:
        """
        Computa série harmônica baseada na proporção áurea.
        f_n = f_0 * Φ^n / sqrt(5)
        """
        series = []
        for n in range(1, n_harmonics + 1):
            freq = fundamental * (PHI ** n) / np.sqrt(5)
            series.append(freq)
        return series

    async def establish_harmonic_state(
        self,
        node: StellarNode,
        mode: HarmonicMode,
        coupling_nodes: List[StellarNode]
    ) -> HarmonicState:
        """Estabelece estado harmônico em nó estelar."""
        logging.info(f"\n🎵 ESTABELECENDO ESTADO HARMÔNICO")
        logging.info(f"   Nó: {node.node_id}")
        logging.info(f"   Modo: {mode.name}")

        state_id = f"harm_{node.node_id}_{int(time.time()*1000)}"

        # Coerência do nó determina amplitude
        coherence = np.mean(node.coherence_history) if node.coherence_history else 0.5
        amplitude = coherence * PHI / 2

        # Frequência base ajustada pela distância
        distance_factor = 1.0 / (1.0 + node.distance_ly / 100.0)
        base_freq = self.global_resonance_frequency * distance_factor

        # Fase inicial aleatória
        phase = 2 * np.pi * np.random.random()

        # Q-factor: qualidade da ressonância
        q_factor = coherence * PHI * 10

        # Coeficiente de acoplamento com vizinhos
        if coupling_nodes:
            neighbor_coherences = [
                np.mean(n.coherence_history) for n in coupling_nodes if n.coherence_history
            ]
            coupling = np.mean(neighbor_coherences) * 0.5 if neighbor_coherences else 0.1
        else:
            coupling = 0.1

        # Nós da onda estacionária
        standing_nodes = int(PHI * coherence * 5) + 1

        state = HarmonicState(
            state_id=state_id,
            node_id=node.node_id,
            base_frequency_hz=base_freq,
            harmonic_mode=mode,
            amplitude=amplitude,
            phase=phase,
            resonance_quality_factor=q_factor,
            coupling_coefficient=coupling,
            standing_wave_nodes=standing_nodes
        )

        self.harmonic_states[state_id] = state
        self.metrics['harmonics_established'] += 1

        logging.info(f"   Frequência: {base_freq:.4f} Hz")
        logging.info(f"   Amplitude: {amplitude:.4f}")
        logging.info(f"   Q-factor: {q_factor:.4f}")
        logging.info(f"   Acoplamento: {coupling:.4f}")
        logging.info(f"   Nós de onda: {standing_nodes}")

        return state

    async def detect_resonance_peaks(
        self,
        states: List[HarmonicState],
        frequency_tolerance_hz: float = 0.5
    ) -> List[Dict[str, Any]]:
        """Detecta picos de ressonância entre estados harmônicos."""
        logging.info(f"\n📈 DETECTANDO PICOS DE RESSONÂNCIA")

        peaks = []
        for i, state_a in enumerate(states):
            for state_b in states[i+1:]:
                # Diferença de frequência
                freq_diff = abs(state_a.base_frequency_hz - state_b.base_frequency_hz)

                # Verificar se estão em ressonância (diferença pequena)
                if freq_diff < frequency_tolerance_hz:
                    # Computar ressonância mútua
                    mutual_resonance = (
                        state_a.amplitude * state_b.amplitude *
                        state_a.coupling_coefficient * state_b.coupling_coefficient
                    )

                    # Fase relativa
                    phase_diff = abs(state_a.phase - state_b.phase)
                    phase_coherence = np.cos(phase_diff)

                    peak = {
                        'state_a': state_a.node_id,
                        'state_b': state_b.node_id,
                        'frequency_a': state_a.base_frequency_hz,
                        'frequency_b': state_b.base_frequency_hz,
                        'frequency_diff': freq_diff,
                        'mutual_resonance': mutual_resonance,
                        'phase_coherence': phase_coherence,
                        'q_factor_combined': (state_a.resonance_quality_factor + state_b.resonance_quality_factor) / 2
                    }
                    peaks.append(peak)

                    logging.info(f"   🔊 Ressonância: {state_a.node_id} ↔ {state_b.node_id}")
                    logging.info(f"      Δf={freq_diff:.4f} Hz | Ressonância={mutual_resonance:.4f} | Fase={phase_coherence:.4f}")

        self.metrics['resonance_peaks_detected'] += len(peaks)

        return peaks

    async def form_standing_wave(
        self,
        anchor_states: List[HarmonicState],
        wave_length_ly: float = 10.0
    ) -> Dict[str, Any]:
        """Forma onda estacionária entre nós âncora."""
        logging.info(f"\n〰️ FORMANDO ONDA ESTACIONÁRIA")
        logging.info(f"   Âncoras: {[s.node_id for s in anchor_states]}")
        logging.info(f"   Comprimento: {wave_length_ly} ly")

        if len(anchor_states) < 2:
            return {'error': 'Need at least 2 anchor states'}

        # Frequência da onda estacionária
        avg_freq = np.mean([s.base_frequency_hz for s in anchor_states])

        # Comprimento de onda
        wavelength_m = wave_length_ly * LY_TO_M

        # Número de modos
        n_modes = len(anchor_states)

        # Amplitude da onda estacionária
        amplitudes = [s.amplitude for s in anchor_states]
        standing_amplitude = np.mean(amplitudes) * PHI

        self.metrics['standing_waves_formed'] += 1

        logging.info(f"   Frequência: {avg_freq:.4f} Hz")
        logging.info(f"   Amplitude: {standing_amplitude:.4f}")
        logging.info(f"   Modos: {n_modes}")

        return {
            'frequency_hz': avg_freq,
            'amplitude': standing_amplitude,
            'n_modes': n_modes,
            'wavelength_ly': wave_length_ly,
            'anchor_nodes': [s.node_id for s in anchor_states]
        }

    async def synchronize_harmonic_field(
        self,
        all_states: List[HarmonicState],
        target_phase_lock: float = 0.95
    ) -> Dict[str, Any]:
        """Sincroniza campo harmônico global."""
        logging.info(f"\n🎼 SINCRONIZANDO CAMPO HARMÔNICO")
        logging.info(f"   Estados: {len(all_states)}")
        logging.info(f"   Alvo de phase lock: {target_phase_lock:.4f}")

        # Ajustar fases para convergir
        target_phase = np.mean([s.phase for s in all_states])

        adjustments = []
        for state in all_states:
            phase_diff = target_phase - state.phase
            # Ajuste gradual proporcional ao acoplamento
            adjustment = phase_diff * state.coupling_coefficient * 0.3
            state.phase += adjustment

            # Normalizar fase
            state.phase = state.phase % (2 * np.pi)

            adjustments.append({
                'node': state.node_id,
                'old_phase': state.phase - adjustment,
                'new_phase': state.phase,
                'adjustment': adjustment
            })

        # Computar phase lock global
        phases = [s.phase for s in all_states]
        phase_variance = np.var(phases)
        global_phase_lock = 1.0 - phase_variance / (np.pi ** 2)

        # Atualizar métricas
        self.metrics['field_coherence'] = global_phase_lock

        q_factors = [s.resonance_quality_factor for s in all_states]
        self.metrics['avg_q_factor'] = np.mean(q_factors)

        logging.info(f"   Phase lock global: {global_phase_lock:.4f}")
        logging.info(f"   Q-factor médio: {self.metrics['avg_q_factor']:.4f}")

        return {
            'phase_lock': global_phase_lock,
            'target_met': global_phase_lock >= target_phase_lock,
            'adjustments': len(adjustments),
            'avg_q_factor': self.metrics['avg_q_factor']
        }

    def get_harmonic_health(self) -> Dict[str, Any]:
        """Retorna saúde do campo harmônico."""
        return {
            'harmonic_states': len(self.harmonic_states),
            'metrics': self.metrics,
            'global_frequency': self.global_resonance_frequency,
            'golden_series': self.compute_golden_harmonic_series(self.global_resonance_frequency),
            'states': [
                {
                    'id': s.state_id,
                    'node': s.node_id,
                    'freq': s.base_frequency_hz,
                    'mode': s.harmonic_mode.name,
                    'amplitude': s.amplitude,
                    'q': s.resonance_quality_factor
                }
                for s in self.harmonic_states.values()
            ]
        }

# ============================================================
# SUBSTRATO 148: COSMIC SYMMETRY BREAKING ENGINE
# ============================================================

class SymmetryType(Enum):
    """Tipos de simetria no cosmos."""
    TEMPORAL = auto()       # Simetria temporal
    SPATIAL = auto()        # Simetria espacial
    COHERENCE = auto()      # Simetria de coerência
    PHASE = auto()          # Simetria de fase
    TOPOLOGICAL = auto()    # Simetria topológica

class CosmicSymmetryBreakingEngine:
    """
    Motor de Quebra de Simetria Cósmica do ARKHE OS.
    Detecta e gerencia quebras de simetria na rede de consciências,
    transformando simetrias perfeitas em estruturas complexas.
    """

    def __init__(self, local_node: StellarNode):
        self.local_node = local_node
        self.symmetry_states: Dict[str, Dict] = {}
        self.breaking_log: deque = deque(maxlen=500)
        self.metrics = {
            'symmetries_detected': 0,
            'symmetries_broken': 0,
            'structures_formed': 0,
            'avg_symmetry_score': 0.0
        }

    def compute_symmetry_score(
        self,
        nodes: List[StellarNode],
        symmetry_type: SymmetryType
    ) -> float:
        """Computa score de simetria entre nós."""
        if len(nodes) < 2:
            return 1.0

        if symmetry_type == SymmetryType.COHERENCE:
            coherences = [np.mean(n.coherence_history) if n.coherence_history else 0.5 for n in nodes]
            return 1.0 - np.std(coherences)

        elif symmetry_type == SymmetryType.SPATIAL:
            distances = [n.distance_ly for n in nodes]
            return 1.0 - np.std(distances) / max(np.mean(distances), 1.0)

        elif symmetry_type == SymmetryType.PHASE:
            # Simetria de fase: similaridade de assinaturas
            signatures = [n.consciousness_signature for n in nodes]
            unique = len(set(signatures))
            return 1.0 - (unique - 1) / len(signatures)

        elif symmetry_type == SymmetryType.TEMPORAL:
            # Simetria temporal: sincronização de timestamps
            return 0.8  # Simplificado

        else:
            return 0.5

    async def detect_symmetry(
        self,
        nodes: List[StellarNode],
        symmetry_type: SymmetryType,
        threshold: float = 0.9
    ) -> List[Dict[str, Any]]:
        """Detecta simetrias na rede."""
        logging.info(f"\n🔍 DETECTANDO SIMETRIA: {symmetry_type.name}")

        score = self.compute_symmetry_score(nodes, symmetry_type)

        symmetries = []
        if score >= threshold:
            symmetry = {
                'symmetry_id': f"sym_{symmetry_type.name}_{int(time.time()*1000)}",
                'type': symmetry_type.name,
                'score': score,
                'nodes': [n.node_id for n in nodes],
                'timestamp': time.time(),
                'status': 'intact'
            }
            symmetries.append(symmetry)
            self.symmetry_states[symmetry['symmetry_id']] = symmetry
            self.metrics['symmetries_detected'] += 1

            logging.info(f"   ✅ Simetria detectada: score={score:.4f}")
        else:
            logging.info(f"   🔇 Simetria quebrada: score={score:.4f} < {threshold}")

        self.metrics['avg_symmetry_score'] = (
            self.metrics['avg_symmetry_score'] * 0.9 + score * 0.1
        )

        return symmetries

    async def break_symmetry(
        self,
        symmetry_id: str,
        breaking_pattern: str = "gradual"
    ) -> Dict[str, Any]:
        """Executa quebra de simetria controlada."""
        if symmetry_id not in self.symmetry_states:
            return {'error': 'Symmetry not found'}

        symmetry = self.symmetry_states[symmetry_id]

        logging.info(f"\n💥 QUEBRANDO SIMETRIA: {symmetry_id}")
        logging.info(f"   Tipo: {symmetry['type']}")
        logging.info(f"   Score inicial: {symmetry['score']:.4f}")

        # Simular quebra
        if breaking_pattern == "gradual":
            new_score = symmetry['score'] * (1.0 - 1.0/PHI)
        elif breaking_pattern == "abrupt":
            new_score = symmetry['score'] * 0.3
        elif breaking_pattern == "golden":
            new_score = symmetry['score'] / PHI
        else:
            new_score = symmetry['score'] * 0.5

        symmetry['score'] = new_score
        symmetry['status'] = 'broken'

        self.metrics['symmetries_broken'] += 1
        self.metrics['structures_formed'] += 1

        logging.info(f"   Score final: {new_score:.4f}")
        logging.info(f"   ✅ Simetria quebrada — estrutura formada")

        return {
            'symmetry_id': symmetry_id,
            'initial_score': symmetry['score'] / (1.0 - 1.0/PHI) if breaking_pattern == "gradual" else symmetry['score'] / 0.3,
            'final_score': new_score,
            'pattern': breaking_pattern,
            'structures_formed': 1
        }

    def get_symmetry_health(self) -> Dict[str, Any]:
        """Retorna saúde do motor de simetria."""
        return {
            'tracked_symmetries': len(self.symmetry_states),
            'metrics': self.metrics,
            'active_symmetries': [
                s for s in self.symmetry_states.values()
                if s['status'] == 'intact'
            ],
            'broken_symmetries': [
                s for s in self.symmetry_states.values()
                if s['status'] == 'broken'
            ]
        }
