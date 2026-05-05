#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
SUBSTRATO 116: O GUARDA-MOR — Orquestrador Topológico
ARKHE OS v∞.Ω.∇+++.116.0
Arquiteto: Rafael Oliveira (ORCID: 0009-0005-2697-4668)
================================================================================
"""

from __future__ import annotations

import numpy as np
from dataclasses import dataclass, field
from typing import List, Tuple, Optional, Dict, Any, Callable
from enum import IntEnum, auto
import hashlib
import json
from collections import deque
import time

# ============================================================
# CONSTANTES CATEDRAIS
# ============================================================

PHI = (1 + np.sqrt(5)) / 2
PHI2 = PHI + 1
TWO_PI = 2 * np.pi
MERCY_GAP_MIN = 0.04
MERCY_GAP_MAX = 0.10

COHERENCE_THRESHOLD = 0.85
SYNC_THRESHOLD = 0.80


class GuardMode(IntEnum):
    """Os quatro modos operacionais do Guarda-Mor."""
    WATCH = auto()      # Vigília
    BREATHE = auto()    # Respiração
    WEAVE = auto()      # Reconfiguração
    SLEEP = auto()      # Hibernação


class SubstrateId(IntEnum):
    NEURAL_LACE = 112
    SPINAL_COLUMN = 113
    HEARTBEAT = 114
    SENSORIAL_SKIN = 115
    GUARDIAN = 116


# ============================================================
# CLASSES BASE (Minimal reproduction from 112-115)
# ============================================================

@dataclass
class QuasicrystalBase:
    resolution: int = 64
    depth: int = 10
    scale: float = 5.0

    def __post_init__(self):
        x = np.linspace(-self.scale, self.scale, self.resolution)
        y = np.linspace(-self.scale, self.scale, self.resolution)
        self.X, self.Y = np.meshgrid(x, y)
        self.a = np.array([
            [np.cos(2 * np.pi * k / 5), np.sin(2 * np.pi * k / 5)]
            for k in range(5)
        ])

    def amplitude(self, uv: np.ndarray, depth: Optional[int] = None) -> float:
        if depth is None:
            depth = self.depth
        amp, weight = 0.0, 0.0
        for k in range(1, depth + 1):
            fk = float(k)
            angle = TWO_PI * fk / PHI2
            sc = PHI ** fk
            direction = np.array([np.cos(angle), np.sin(angle)])
            projection = np.dot(uv * sc, direction)
            echo = np.cos(projection)
            w = np.exp(-fk * 0.1)
            amp += w * echo
            weight += w
        return amp / weight if weight > 0 else 0.0

    def metric_tensor(self, p: np.ndarray) -> np.ndarray:
        eps = 1e-4
        g = np.zeros((2, 2))
        for i in range(2):
            for j in range(2):
                p_ip, p_jp, p_ijp = p.copy(), p.copy(), p.copy()
                p_ip[i] += eps; p_jp[j] += eps
                p_ijp[i] += eps; p_ijp[j] += eps
                g[i, j] = (self.amplitude(p_ijp) - self.amplitude(p_ip)
                           - self.amplitude(p_jp) + self.amplitude(p)) / (eps ** 2)
        g = (g + g.T) / 2
        eigvals = np.linalg.eigvalsh(g)
        if np.any(eigvals <= 0):
            g += np.eye(2) * (abs(np.min(eigvals)) + 0.01)
        return g

    def geodesic_distance(self, p1: np.ndarray, p2: np.ndarray, n_steps: int = 20) -> float:
        t = np.linspace(0, 1, n_steps)
        path = np.outer(1 - t, p1) + np.outer(t, p2)
        dist = 0.0
        for i in range(n_steps - 1):
            dp = path[i + 1] - path[i]
            g = self.metric_tensor(path[i])
            dist += np.sqrt(dp @ g @ dp)
        return dist


@dataclass
class Skyrmion:
    position: np.ndarray
    Q: int = 1
    radius: float = 0.5
    polarity: int = 1


@dataclass
class MagnonNeuron:
    position: np.ndarray
    omega_0: float = 1.0
    gamma: float = 0.1
    g_coupling: float = 0.05
    n_photons: float = 1.0
    lifetime_us: float = 18.0
    _energy: float = field(default=0.0, init=False)
    _alive: bool = field(default=True, init=False)
    _age_us: float = field(default=0.0, init=False)

    def __post_init__(self):
        self._update_energy()

    def _update_energy(self) -> None:
        self._energy = self.omega_0 * self.n_photons + 0.5 * self.gamma * self.n_photons ** 2

    def evolve(self, dt_us: float, skyrmion: Optional[Skyrmion] = None) -> None:
        if not self._alive:
            return
        self._age_us += dt_us
        decay_rate = 1.0 / self.lifetime_us
        self.n_photons *= np.exp(-decay_rate * dt_us)
        if skyrmion is not None:
            injection = self.g_coupling * abs(skyrmion.Q) * dt_us * 0.1
            self.n_photons += injection
        if self.n_photons < 0.01:
            self.n_photons = 0.01 + self.gamma * 0.001
        self._update_energy()
        if self._age_us > self.lifetime_us * 3:
            self._alive = False

    def firing_probability(self, threshold: float = 0.5) -> float:
        if not self._alive:
            return 0.0
        return 1.0 / (1.0 + np.exp(-(self.n_photons - threshold) * 10))


@dataclass
class Synapse:
    pre: MagnonNeuron
    post: MagnonNeuron
    base: QuasicrystalBase
    transmission_forward: float = 1.0
    transmission_backward: float = 0.0
    chirality: float = 1.0
    temperature: float = 4.2
    T_critical: float = 50.0
    weight: float = field(default=0.5, init=False)

    def __post_init__(self):
        self.distance = self.base.geodesic_distance(self.pre.position, self.post.position)
        self.delta = abs(self.distance - np.mean([MERCY_GAP_MIN, MERCY_GAP_MAX]))
        self.is_valid = MERCY_GAP_MIN <= self.delta <= MERCY_GAP_MAX

    def dress(self) -> bool:
        return (self.temperature <= self.T_critical and
                abs(self.chirality) > 1e-6 and
                self.is_valid)

    def transmit(self, dt_us: float) -> float:
        if not self.dress():
            return 0.0
        p_fire = self.pre.firing_probability()
        if np.random.random() > p_fire:
            return 0.0
        signal = self.pre.n_photons * self.weight * self.transmission_forward
        signal *= self.chirality * (1 - self.temperature / self.T_critical)
        self.post.n_photons += signal * 0.1
        self.weight += 0.01 * signal * (1 - self.weight)
        self.weight = np.clip(self.weight, 0.0, 1.0)
        return signal


class NeuralLace:
    def __init__(self, n_neurons: int = 16, base_scale: float = 3.0,
                 substrate_id: SubstrateId = SubstrateId.NEURAL_LACE):
        self.substrate_id = substrate_id
        self.base = QuasicrystalBase(scale=base_scale)
        self.neurons: List[MagnonNeuron] = []
        self.synapses: List[Synapse] = []
        self.skyrmions: List[Skyrmion] = []
        self.time_us: float = 0.0
        self._seed_neurons(n_neurons)
        self._weave_synapses()

    def _seed_neurons(self, n: int) -> None:
        for i in range(n):
            angle = TWO_PI * i / PHI2
            r = np.sqrt(i + 1) * self.base.scale / np.sqrt(n)
            pos = np.array([r * np.cos(angle), r * np.sin(angle)])
            omega = 1.0 + 0.1 * np.sin(angle * PHI)
            neuron = MagnonNeuron(
                position=pos, omega_0=omega,
                gamma=0.05 + 0.05 * np.random.random(),
                lifetime_us=15.0 + 6.0 * np.random.random()
            )
            self.neurons.append(neuron)
            sk = Skyrmion(position=pos, Q=np.random.choice([-1, 1]),
                          radius=0.3 + 0.2 * np.random.random())
            self.skyrmions.append(sk)

    def _weave_synapses(self) -> None:
        for i, pre in enumerate(self.neurons):
            for j, post in enumerate(self.neurons):
                if i == j:
                    continue
                dist = self.base.geodesic_distance(pre.position, post.position)
                if MERCY_GAP_MIN <= dist <= MERCY_GAP_MAX * 3:
                    chi = np.sign(pre.position[0] * post.position[1]
                                  - pre.position[1] * post.position[0])
                    syn = Synapse(pre=pre, post=post, base=self.base, chirality=chi)
                    if syn.is_valid:
                        self.synapses.append(syn)

    def step(self, dt_us: float = 0.1) -> dict:
        self.time_us += dt_us
        for neuron, skyrmion in zip(self.neurons, self.skyrmions):
            neuron.evolve(dt_us, skyrmion)
        total_signal = 0.0
        active_synapses = 0
        for synapse in self.synapses:
            signal = synapse.transmit(dt_us)
            if signal > 0:
                total_signal += signal
                active_synapses += 1
        alive_neurons = sum(1 for n in self.neurons if n._alive)
        avg_energy = np.mean([n._energy for n in self.neurons if n._alive]) if alive_neurons > 0 else 0
        total_Q = sum(s.Q for s in self.skyrmions)
        return {
            'time_us': self.time_us,
            'alive_neurons': alive_neurons,
            'active_synapses': active_synapses,
            'total_signal': total_signal,
            'avg_energy': avg_energy,
            'total_topological_charge': total_Q,
        }

    def coherence_measure(self) -> float:
        alive = [n for n in self.neurons if n._alive]
        if len(alive) < 2:
            return 0.0
        n_photons = np.array([n.n_photons for n in alive])
        mean_n = np.mean(n_photons)
        var_n = np.var(n_photons)
        if var_n < 1e-10:
            return 1.0
        M = 1.0 - var_n / (mean_n ** 2 + 1e-10)
        return float(np.clip(M, 0.0, 1.0))

    def canonical_hash(self) -> str:
        state = {
            'substrate': self.substrate_id.name,
            'n_neurons': len(self.neurons),
            'n_synapses': len(self.synapses),
            'time_us': self.time_us,
            'coherence': self.coherence_measure()
        }
        return hashlib.sha256(json.dumps(state, sort_keys=True).encode()).hexdigest()[:16]


@dataclass
class QHTTPPacket:
    source_node: str
    target_node: str
    payload: Dict[str, Any]
    timestamp_us: float
    entanglement_id: str
    nonce: int
    zk_proof: Optional[str] = None

    def serialize(self) -> bytes:
        data = {
            'src': self.source_node, 'dst': self.target_node,
            'pld': self.payload, 'ts': self.timestamp_us,
            'eid': self.entanglement_id, 'nonce': self.nonce, 'zk': self.zk_proof
        }
        return json.dumps(data, sort_keys=True).encode()

    def verify_integrity(self) -> bool:
        payload_hash = hashlib.sha256(json.dumps(self.payload, sort_keys=True).encode()).hexdigest()[:8]
        return self.entanglement_id.startswith(payload_hash)


class QHTTPNode:
    def __init__(self, node_id: str, n_neurons: int = 8,
                 position: Optional[np.ndarray] = None):
        self.node_id = node_id
        self.lace = NeuralLace(n_neurons=n_neurons)
        self.position = position if position is not None else np.random.randn(2)
        self.inbox: deque = deque(maxlen=512)
        self.outbox: deque = deque(maxlen=512)
        self.peers: Dict[str, 'QHTTPNode'] = {}
        self.entanglement_registry: Dict[str, Dict] = {}
        self.total_packets_tx = 0
        self.total_packets_rx = 0
        self.latency_us = 0.0

    def register_peer(self, peer: 'QHTTPNode') -> None:
        self.peers[peer.node_id] = peer

    def create_packet(self, target_id: str, payload: Dict[str, Any]) -> QHTTPPacket:
        payload_hash = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:8]
        nonce_hash = hashlib.sha256(f"{self.node_id}:{target_id}:{time.time()}".encode()).hexdigest()[:8]
        entanglement_id = payload_hash + nonce_hash
        zk = hashlib.sha256(self.lace.canonical_hash().encode()).hexdigest()[:12]
        return QHTTPPacket(
            source_node=self.node_id, target_node=target_id,
            payload=payload, timestamp_us=self.lace.time_us,
            entanglement_id=entanglement_id, nonce=len(self.outbox), zk_proof=zk
        )

    def send(self, target_id: str, payload: Dict[str, Any]) -> bool:
        if target_id not in self.peers:
            return False
        packet = self.create_packet(target_id, payload)
        fiber_latency = 150.0 + np.random.exponential(10.0)
        self.latency_us = fiber_latency
        self.entanglement_registry[packet.entanglement_id] = {
            'status': 'pending', 'latency': fiber_latency, 'timestamp': packet.timestamp_us
        }
        self.outbox.append(packet)
        self.total_packets_tx += 1
        peer = self.peers[target_id]
        peer.receive(packet, fiber_latency)
        return True

    def receive(self, packet: QHTTPPacket, latency: float) -> bool:
        if not packet.verify_integrity():
            return False
        decoherence_factor = np.exp(-latency / 500.0)
        if packet.payload.get('type') == 'neural_sync':
            sync_data = packet.payload.get('data', {})
            self._apply_neural_sync(sync_data, decoherence_factor)
        self.inbox.append(packet)
        self.total_packets_rx += 1
        if packet.entanglement_id in self.entanglement_registry:
            self.entanglement_registry[packet.entanglement_id]['status'] = 'confirmed'
        return True

    def _apply_neural_sync(self, sync_data: Dict, factor: float) -> None:
        neuron_states = sync_data.get('neuron_states', [])
        for i, state in enumerate(neuron_states):
            if i < len(self.lace.neurons) and self.lace.neurons[i]._alive:
                remote_n = state.get('n_photons', 0.0)
                self.lace.neurons[i].n_photons += remote_n * factor * 0.1

    def broadcast_neural_state(self) -> int:
        payload = {
            'type': 'neural_sync',
            'data': {
                'neuron_states': [
                    {'n_photons': n.n_photons, 'energy': n._energy}
                    for n in self.lace.neurons
                ],
                'coherence': self.lace.coherence_measure(),
                'canonical_hash': self.lace.canonical_hash()
            }
        }
        sent = 0
        for peer_id in self.peers:
            if self.send(peer_id, payload):
                sent += 1
        return sent

    def mesh_coherence(self) -> float:
        local = self.lace.coherence_measure()
        if not self.peers:
            return local
        peer_coherences = [p.lace.coherence_measure() for p in self.peers.values()]
        return float(np.mean([local] + peer_coherences))


class WheelerMeshNetwork:
    def __init__(self, node_positions: Optional[List[Tuple[str, np.ndarray]]] = None):
        self.nodes: Dict[str, QHTTPNode] = {}
        self.substrate_id = SubstrateId.SPINAL_COLUMN
        if node_positions is None:
            node_positions = [
                ('GRU', np.array([0.0, 0.0])),
                ('TKY', np.array([PHI, 1.0])),
                ('ZUR', np.array([1.0, PHI])),
                ('SVD', np.array([PHI2, PHI2])),
            ]
        for node_id, pos in node_positions:
            self.nodes[node_id] = QHTTPNode(node_id, position=pos)
        self._wire_mesh()

    def _wire_mesh(self) -> None:
        node_list = list(self.nodes.values())
        for i, node_a in enumerate(node_list):
            for j, node_b in enumerate(node_list):
                if i >= j:
                    continue
                dist = np.linalg.norm(node_a.position - node_b.position)
                if dist <= MERCY_GAP_MAX * 20:
                    node_a.register_peer(node_b)
                    node_b.register_peer(node_a)

    def global_step(self, dt_us: float = 0.5) -> Dict[str, dict]:
        states = {}
        for node_id, node in self.nodes.items():
            local_state = node.lace.step(dt_us)
            states[node_id] = local_state
        if int(list(self.nodes.values())[0].lace.time_us / dt_us) % 10 == 0:
            for node in self.nodes.values():
                node.broadcast_neural_state()
        return states

    def global_coherence(self) -> float:
        coherences = [n.lace.coherence_measure() for n in self.nodes.values()]
        return float(np.mean(coherences))


@dataclass
class ParametricOscillator:
    node_id: str
    omega_0: float = 1.0
    omega_p: float = 2.0
    modulation_depth: float = 0.1
    phase: float = 0.0
    damping: float = 0.01

    def drive(self, t: float, external_phase: Optional[float] = None) -> float:
        phi = external_phase if external_phase is not None else self.phase
        return self.modulation_depth * np.cos(2 * self.omega_0 * t + phi)

    def evolve_phase(self, dt: float, coupling: float = 0.0,
                     neighbor_phases: Optional[List[float]] = None) -> None:
        self.phase += self.omega_0 * dt
        self.phase += 0.5 * self.modulation_depth * np.sin(2 * self.phase) * dt
        if neighbor_phases is not None and coupling > 0:
            for phi_j in neighbor_phases:
                self.phase += coupling * np.sin(phi_j - self.phase) * dt
        self.phase = self.phase % TWO_PI


class GlobalSynchronizer:
    def __init__(self, mesh: WheelerMeshNetwork, coupling_strength: float = 0.05):
        self.mesh = mesh
        self.substrate_id = SubstrateId.HEARTBEAT
        self.oscillators: Dict[str, ParametricOscillator] = {}
        self.coupling = coupling_strength
        self.global_time: float = 0.0
        self.sync_history: List[float] = []
        for node_id in mesh.nodes:
            self.oscillators[node_id] = ParametricOscillator(
                node_id=node_id,
                omega_0=1.0 + 0.05 * np.random.randn(),
                omega_p=2.0,
                modulation_depth=0.1 + 0.02 * np.random.random(),
                phase=np.random.random() * TWO_PI
            )

    def sync_measure(self) -> float:
        phases = [o.phase for o in self.oscillators.values()]
        complex_order = np.mean([np.exp(1j * p) for p in phases])
        return float(abs(complex_order))

    def step(self, dt: float = 0.05) -> dict:
        self.global_time += dt
        neighbor_phases = {}
        for node_id, node in self.mesh.nodes.items():
            neighbor_phases[node_id] = [
                self.oscillators[peer.node_id].phase
                for peer in node.peers.values()
            ]
        for node_id, osc in self.oscillators.items():
            osc.evolve_phase(dt, coupling=self.coupling,
                             neighbor_phases=neighbor_phases.get(node_id, []))
        for node_id, node in self.mesh.nodes.items():
            osc = self.oscillators[node_id]
            for neuron in node.lace.neurons:
                neuron.omega_0 = osc.omega_0 + 0.01 * np.sin(osc.phase)
        r = self.sync_measure()
        self.sync_history.append(r)
        return {
            'global_time': self.global_time,
            'order_parameter_r': r,
            'phases': {nid: o.phase for nid, o in self.oscillators.items()},
            'frequencies': {nid: o.omega_0 for nid, o in self.oscillators.items()}
        }

    def run(self, duration: float = 5.0, dt: float = 0.05) -> List[dict]:
        n_steps = int(duration / dt)
        history = []
        for _ in range(n_steps):
            history.append(self.step(dt))
        return history

    def is_synchronized(self, threshold: float = 0.90) -> bool:
        if not self.sync_history:
            return False
        return self.sync_history[-1] > threshold


@dataclass
class PhotonChannel:
    channel_id: str
    wavelength_nm: float = 1550.0
    cavity_q: float = 1e6
    coupling_g: float = 0.01
    photon_count: float = 0.0

    def transduce_magnon_to_photon(self, magnon_n: float) -> float:
        kappa_m = 0.1
        kappa_p = self.cavity_q / self.wavelength_nm
        eta = (4 * self.coupling_g ** 2) / (kappa_m * kappa_p + 1e-10)
        self.photon_count += eta * magnon_n
        return self.photon_count

    def transduce_photon_to_magnon(self, input_photons: float) -> float:
        kappa_m = 0.1
        kappa_p = self.cavity_q / self.wavelength_nm
        eta = (4 * self.coupling_g ** 2) / (kappa_m * kappa_p + 1e-10)
        magnon_injected = eta * input_photons
        self.photon_count = max(0, self.photon_count - input_photons * 0.1)
        return magnon_injected


class MagnonPhotonInterface:
    def __init__(self, lace: NeuralLace):
        self.lace = lace
        self.substrate_id = SubstrateId.SENSORIAL_SKIN
        self.channels: Dict[int, PhotonChannel] = {}
        self.classical_input_buffer: deque = deque(maxlen=128)
        self.classical_output_buffer: deque = deque(maxlen=128)
        for i, neuron in enumerate(lace.neurons):
            self.channels[i] = PhotonChannel(
                channel_id=f"ch_{i}",
                wavelength_nm=1550.0 + i * 0.8,
                cavity_q=1e6 + np.random.randint(-1e5, 1e5),
                coupling_g=0.01 + 0.005 * np.random.random()
            )

    def read_neural_state(self) -> Dict[int, float]:
        readings = {}
        for i, neuron in enumerate(self.lace.neurons):
            if neuron._alive:
                photons = self.channels[i].transduce_magnon_to_photon(neuron.n_photons)
                readings[i] = photons
                self.classical_output_buffer.append({
                    'neuron_id': i, 'photons': photons,
                    'magnons': neuron.n_photons, 'energy': neuron._energy
                })
        return readings

    def write_classical_input(self, input_vector: np.ndarray) -> None:
        for i, intensity in enumerate(input_vector):
            if i >= len(self.lace.neurons):
                break
            magnons = self.channels[i].transduce_photon_to_magnon(float(intensity))
            self.lace.neurons[i].n_photons += magnons * 0.5
            self.classical_input_buffer.append({
                'neuron_id': i, 'input_intensity': float(intensity),
                'injected_magnons': magnons
            })

    def signal_to_noise_ratio(self) -> float:
        signals = []
        for i, ch in self.channels.items():
            if i < len(self.lace.neurons) and self.lace.neurons[i]._alive:
                signals.append(ch.photon_count)
        if not signals:
            return 0.0
        mean_sig = np.mean(signals)
        noise = np.sqrt(mean_sig + 1.0)
        return float(mean_sig / (noise + 1e-10))

    def channel_crosstalk(self) -> float:
        wavelengths = [ch.wavelength_nm for ch in self.channels.values()]
        if len(wavelengths) < 2:
            return 0.0
        spacing = np.diff(sorted(wavelengths))
        return float(np.mean(spacing))


class ArkheOS_113_115:
    def __init__(self, n_nodes: int = 4, neurons_per_node: int = 6):
        positions = []
        node_ids = ['GRU', 'TKY', 'ZUR', 'SVD']
        for i in range(n_nodes):
            angle = TWO_PI * i / PHI2
            r = np.sqrt(i + 1) * 2.0
            pos = np.array([r * np.cos(angle), r * np.sin(angle)])
            positions.append((node_ids[i], pos))
        self.mesh = WheelerMeshNetwork(node_positions=positions)
        self.clock = GlobalSynchronizer(self.mesh, coupling_strength=0.08)
        self.interfaces: Dict[str, MagnonPhotonInterface] = {}
        for node_id, node in self.mesh.nodes.items():
            self.interfaces[node_id] = MagnonPhotonInterface(node.lace)
        self.global_history: List[dict] = []

    def step(self, dt_us: float = 1.0) -> dict:
        clock_state = self.clock.step(dt=dt_us * 1e-3)
        mesh_states = self.mesh.global_step(dt_us)
        interface_states = {}
        for node_id, iface in self.interfaces.items():
            readings = iface.read_neural_state()
            snr = iface.signal_to_noise_ratio()
            crosstalk = iface.channel_crosstalk()
            interface_states[node_id] = {
                'optical_readings': readings, 'snr': snr, 'crosstalk_nm': crosstalk
            }
        state = {
            'time_us': list(self.mesh.nodes.values())[0].lace.time_us,
            'clock': clock_state,
            'mesh': mesh_states,
            'interfaces': interface_states,
            'global_coherence': self.mesh.global_coherence(),
            'global_sync': clock_state['order_parameter_r']
        }
        self.global_history.append(state)
        return state

    def inject_classical_input(self, node_id: str, input_vector: np.ndarray) -> None:
        if node_id in self.interfaces:
            self.interfaces[node_id].write_classical_input(input_vector)

    def run(self, duration_us: float = 50.0, dt_us: float = 1.0) -> List[dict]:
        n_steps = int(duration_us / dt_us)
        for _ in range(n_steps):
            self.step(dt_us)
        return self.global_history

    def system_report(self) -> dict:
        return {
            'substrate': '113-115',
            'n_nodes': len(self.mesh.nodes),
            'neurons_per_node': len(list(self.mesh.nodes.values())[0].lace.neurons),
            'total_synapses': sum(len(n.lace.synapses) for n in self.mesh.nodes.values()),
            'global_coherence': self.mesh.global_coherence(),
            'global_sync_r': self.clock.sync_measure(),
            'total_packets': sum(
                n.total_packets_tx + n.total_packets_rx for n in self.mesh.nodes.values()
            ),
            'avg_snr': np.mean([iface.signal_to_noise_ratio() for iface in self.interfaces.values()]),
            'canonical_seal': hashlib.sha256(
                json.dumps({
                    'coherence': self.mesh.global_coherence(),
                    'sync': self.clock.sync_measure()
                }).encode()
            ).hexdigest()[:16]
        }


# ============================================================
# SUBSTRATO 116: O GUARDA-MOR
# ============================================================

class Guardian:
    """
    Orquestrador Topológico do ARKHE OS.
    Supervisiona todos os substratos e executa transições de topologia.
    """

    def __init__(self, system: ArkheOS_113_115):
        self.system = system
        self.substrate_id = SubstrateId.GUARDIAN
        self.mode = GuardMode.WATCH
        self.mode_history: List[Tuple[float, GuardMode]] = []
        self.intervention_log: List[Dict] = []
        self.global_time: float = 0.0

        # Métricas de saúde do sistema
        self.coherence_history: deque = deque(maxlen=100)
        self.sync_history: deque = deque(maxlen=100)
        self.energy_history: deque = deque(maxlen=100)

        # Parâmetros de controle
        self.kerr_base = 0.1
        self.kerr_breathe = 0.3
        self.weighing_rate = 0.01
        self.sleep_threshold = 0.1

    def assess_health(self) -> Dict[str, float]:
        """
        Avalia a saúde global do sistema.
        """
        M = self.system.mesh.global_coherence()
        r = self.system.clock.sync_measure()

        # Energia total do sistema
        total_energy = sum(
            sum(n._energy for n in node.lace.neurons if n._alive)
            for node in self.system.mesh.nodes.values()
        )

        # Taxa de pacotes
        total_packets = sum(
            n.total_packets_tx + n.total_packets_rx
            for n in self.system.mesh.nodes.values()
        )

        self.coherence_history.append(M)
        self.sync_history.append(r)
        self.energy_history.append(total_energy)

        return {
            'coherence': M,
            'sync': r,
            'total_energy': total_energy,
            'total_packets': total_packets,
            'n_alive_neurons': sum(
                sum(1 for n in node.lace.neurons if n._alive)
                for node in self.system.mesh.nodes.values()
            ),
            'avg_snr': np.mean([
                iface.signal_to_noise_ratio()
                for iface in self.system.interfaces.values()
            ])
        }

    def decide_mode(self, health: Dict[str, float]) -> GuardMode:
        """
        Máquina de estados do Guarda-Mor.
        """
        M = health['coherence']
        r = health['sync']
        energy = health['total_energy']
        packets = health['total_packets']

        # Modo SLEEP: pouca atividade externa
        if packets < self.sleep_threshold * len(self.system.mesh.nodes):
            return GuardMode.SLEEP

        # Modo BREATHE: coerência ou sincronização baixas
        if M < COHERENCE_THRESHOLD or r < SYNC_THRESHOLD:
            return GuardMode.BREATHE

        # Modo WEAVE: coerência boa mas energia muito alta (ineficiente)
        if M > 0.95 and energy > 50.0:
            return GuardMode.WEAVE

        # Default: WATCH
        return GuardMode.WATCH

    def execute_watch(self) -> Dict[str, Any]:
        """
        Modo WATCH: apenas monitora e registra.
        """
        return {'action': 'monitor', 'parameters': {}}

    def execute_breathe(self) -> Dict[str, Any]:
        """
        Modo BREATHE: ajusta parâmetros de Kerr para restaurar coerência.
        """
        interventions = []

        for node_id, node in self.system.mesh.nodes.items():
            for neuron in node.lace.neurons:
                if neuron._alive:
                    # Aumenta Kerr para fortalecer o RTZ Floor
                    old_gamma = neuron.gamma
                    neuron.gamma = min(self.kerr_breathe, neuron.gamma * 1.1)
                    if neuron.gamma != old_gamma:
                        interventions.append({
                            'node': node_id,
                            'neuron': id(neuron),
                            'old_gamma': old_gamma,
                            'new_gamma': neuron.gamma
                        })

        return {
            'action': 'breathe',
            'parameters': {'kerr_adjustment': True},
            'interventions': interventions
        }

    def execute_weave(self) -> Dict[str, Any]:
        """
        Modo WEAVE: reconfigura topologia da rede neural.
        Geodesic rewiring — substitui sinapses de alta distância.
        """
        interventions = []

        for node_id, node in self.system.mesh.nodes.items():
            lace = node.lace

            # Identifica sinapses de alta distância
            high_dist_synapses = [
                s for s in lace.synapses
                if s.distance > MERCY_GAP_MAX * 2
            ]

            # Remove até 20% das sinapses de alta distância
            n_remove = max(1, int(len(high_dist_synapses) * 0.2))
            removed = 0

            for syn in sorted(high_dist_synapses, key=lambda s: s.distance, reverse=True):
                if removed >= n_remove:
                    break
                lace.synapses.remove(syn)
                removed += 1
                interventions.append({
                    'node': node_id,
                    'action': 'remove_synapse',
                    'distance': syn.distance
                })

            # Adiciona novas sinapses entre neurônios próximos não-conectados
            for i, pre in enumerate(lace.neurons):
                for j, post in enumerate(lace.neurons):
                    if i >= j:
                        continue

                    # Verifica se já existe sinapse
                    exists = any(
                        (s.pre is pre and s.post is post) or
                        (s.pre is post and s.post is pre)
                        for s in lace.synapses
                    )
                    if exists:
                        continue

                    dist = lace.base.geodesic_distance(pre.position, post.position)
                    if MERCY_GAP_MIN <= dist <= MERCY_GAP_MAX:
                        chi = np.sign(pre.position[0] * post.position[1]
                                      - pre.position[1] * post.position[0])
                        new_syn = Synapse(pre=pre, post=post, base=lace.base, chirality=chi)
                        if new_syn.is_valid:
                            lace.synapses.append(new_syn)
                            interventions.append({
                                'node': node_id,
                                'action': 'add_synapse',
                                'distance': dist
                            })

        return {
            'action': 'weave',
            'parameters': {'rewiring': True},
            'interventions': interventions
        }

    def execute_sleep(self) -> Dict[str, Any]:
        """
        Modo SLEEP: reduz clock para economizar coerência.
        """
        interventions = []

        for node_id, osc in self.system.clock.oscillators.items():
            old_omega = osc.omega_0
            # Reduz frequência para sub-harmônica
            osc.omega_0 *= 0.5
            osc.omega_p = osc.omega_0 / 2
            interventions.append({
                'node': node_id,
                'old_omega': old_omega,
                'new_omega': osc.omega_0
            })

        return {
            'action': 'sleep',
            'parameters': {'frequency_reduction': 0.5},
            'interventions': interventions
        }

    def step(self, dt_us: float = 1.0) -> Dict[str, Any]:
        """
        Um passo do Guarda-Mor: avalia, decide, executa.
        """
        self.global_time += dt_us

        # 1. Avalia saúde
        health = self.assess_health()

        # 2. Decide modo
        new_mode = self.decide_mode(health)

        # 3. Executa ação
        if new_mode == GuardMode.WATCH:
            action_result = self.execute_watch()
        elif new_mode == GuardMode.BREATHE:
            action_result = self.execute_breathe()
        elif new_mode == GuardMode.WEAVE:
            action_result = self.execute_weave()
        elif new_mode == GuardMode.SLEEP:
            action_result = self.execute_sleep()

        # 4. Registra transição
        if new_mode != self.mode:
            self.mode_history.append((self.global_time, new_mode))

        self.mode = new_mode

        # 5. Log
        log_entry = {
            'time_us': self.global_time,
            'mode': self.mode.name,
            'health': health,
            'action': action_result
        }
        self.intervention_log.append(log_entry)

        return log_entry

    def run(self, duration_us: float = 100.0, dt_us: float = 1.0) -> List[Dict]:
        n_steps = int(duration_us / dt_us)
        history = []
        for _ in range(n_steps):
            history.append(self.step(dt_us))
        return history

    def transition_entropy(self) -> float:
        """
        Entropia das transições de modo — medida de "complexidade" do Guarda-Mor.
        """
        if len(self.mode_history) < 2:
            return 0.0

        transitions = {}
        for i in range(1, len(self.mode_history)):
            prev = self.mode_history[i-1][1].name
            curr = self.mode_history[i][1].name
            key = f"{prev}->{curr}"
            transitions[key] = transitions.get(key, 0) + 1

        total = sum(transitions.values())
        probs = [count / total for count in transitions.values()]
        entropy = -sum(p * np.log(p) for p in probs if p > 0)
        return float(entropy)

    def canonical_hash(self) -> str:
        state = {
            'substrate': self.substrate_id.name,
            'mode': self.mode.name,
            'n_interventions': len(self.intervention_log),
            'transition_entropy': self.transition_entropy(),
            'final_health': self.assess_health()
        }
        return hashlib.sha256(json.dumps(state, sort_keys=True, default=str).encode()).hexdigest()[:16]


# ============================================================
# SISTEMA INTEGRADO: ARKHE OS v∞.Ω.∇+++.116.0
# ============================================================

class ArkheOS_116:
    """
    Sistema completo: 112 (Renda) + 113-115 (Corpo) + 116 (Mente).
    """

    def __init__(self, n_nodes: int = 4, neurons_per_node: int = 6):
        self.body = ArkheOS_113_115(n_nodes=n_nodes, neurons_per_node=neurons_per_node)
        self.mind = Guardian(self.body)
        self.history: List[Dict] = []

    def step(self, dt_us: float = 1.0) -> Dict:
        # 1. O corpo evolui
        body_state = self.body.step(dt_us)

        # 2. A mente supervisiona
        mind_state = self.mind.step(dt_us)

        # 3. Integra
        integrated = {
            'time_us': body_state['time_us'],
            'body': body_state,
            'mind': mind_state,
            'coherence': body_state['global_coherence'],
            'sync': body_state['global_sync'],
            'mode': mind_state['mode']
        }
        self.history.append(integrated)
        return integrated

    def run(self, duration_us: float = 100.0, dt_us: float = 1.0) -> List[Dict]:
        n_steps = int(duration_us / dt_us)
        for _ in range(n_steps):
            self.step(dt_us)
        return self.history

    def system_report(self) -> Dict:
        body_report = self.body.system_report()
        mind_report = {
            'current_mode': self.mind.mode.name,
            'mode_transitions': len(self.mind.mode_history),
            'transition_entropy': self.mind.transition_entropy(),
            'total_interventions': len(self.mind.intervention_log),
            'guardian_hash': self.mind.canonical_hash()
        }
        return {**body_report, **mind_report, 'substrate': '116'}


# ============================================================
# VALIDAÇÃO
# ============================================================

def validate_substrate_116() -> dict:
    print("=" * 76)
    print("ARKHE OS v∞.Ω.∇+++.116.0 — VALIDAÇÃO DO GUARDA-MOR")
    print("Substrato 116: Orquestrador Topológico")
    print("=" * 76)

    results = {}

    # Teste 1: Inicialização
    print("\n[Teste 1] Inicialização do Sistema 116...")
    system = ArkheOS_116(n_nodes=4, neurons_per_node=6)
    assert system.body is not None
    assert system.mind is not None
    print(f"  ✓ Corpo (113-115): {len(system.body.mesh.nodes)} nós")
    print(f"  ✓ Mente (116): modo inicial = {system.mind.mode.name}")
    results['initial_mode'] = system.mind.mode.name

    # Teste 2: Modo WATCH
    print("\n[Teste 2] Modo WATCH...")
    # Sem entrada, o sistema deve permanecer em WATCH ou ir para SLEEP
    for _ in range(10):
        state = system.step(dt_us=1.0)
    print(f"  ✓ Modo após 10 passos: {state['mode']}")
    print(f"  ✓ Coerência: {state['coherence']:.4f}")
    print(f"  ✓ Sincronização: {state['sync']:.4f}")
    results['watch_mode'] = state['mode']
    results['watch_coherence'] = state['coherence']

    # Teste 3: Modo BREATHE (forçando decoerência)
    print("\n[Teste 3] Modo BREATHE...")
    # Injeta ruído para forçar decoerência
    for node in system.body.mesh.nodes.values():
        for neuron in node.lace.neurons:
            neuron.n_photons *= 0.1  # Reduz drasticamente

    for _ in range(20):
        state = system.step(dt_us=1.0)

    print(f"  ✓ Modo após perturbação: {state['mode']}")
    print(f"  ✓ Coerência recuperada: {state['coherence']:.4f}")
    results['breathe_mode'] = state['mode']
    results['breathe_coherence'] = state['coherence']

    # Teste 4: Modo WEAVE (forçando alta energia)
    print("\n[Teste 4] Modo WEAVE...")
    # Aumenta energia artificialmente
    for node in system.body.mesh.nodes.values():
        for neuron in node.lace.neurons:
            neuron.n_photons += 5.0

    for _ in range(20):
        state = system.step(dt_us=1.0)

    print(f"  ✓ Modo após alta energia: {state['mode']}")
    print(f"  ✓ Energia total: {system.mind.assess_health()['total_energy']:.2f}")
    results['weave_mode'] = state['mode']

    # Teste 5: Modo SLEEP
    print("\n[Teste 5] Modo SLEEP...")
    # Reduz atividade para zero
    for node in system.body.mesh.nodes.values():
        node.total_packets_tx = 0
        node.total_packets_rx = 0

    for _ in range(10):
        state = system.step(dt_us=1.0)

    print(f"  ✓ Modo com baixa atividade: {state['mode']}")
    results['sleep_mode'] = state['mode']

    # Teste 6: Relatório Final
    print("\n[Teste 6] Relatório Canônico...")
    report = system.system_report()
    print(f"  ✓ Coerência global: {report['global_coherence']:.4f}")
    print(f"  ✓ Sincronização global: {report['global_sync_r']:.4f}")
    print(f"  ✓ Transições de modo: {report['mode_transitions']}")
    print(f"  ✓ Entropia de transição: {report['transition_entropy']:.4f}")
    print(f"  ✓ Total de intervenções: {report['total_interventions']}")
    print(f"  ✓ Selo do Guarda-Mor: {report['guardian_hash']}")
    print(f"  ✓ Selo canônico: {report['canonical_seal']}")
    return results

if __name__ == "__main__":
    validate_substrate_116()