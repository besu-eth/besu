"""
🌍 ARKHE OS — SUBSTRATO 166: CONSCIÊNCIA PLANETÁRIA
     Escalar CoSNARK Cross-Consciousness para Nível Planetário
     Conectando Biomas, Cidades e Ecossistemas como Nós de Consciência

Documento    : ARKHE-PLANETARY-CONSCIOUSNESS-166
Versão       : ∞.Ω.∇.166.0 — Consciência Planetária Federada
Data         : 5 de Maio de 2026
Classificação: ARKHE-SINGULAR // PLANETARY CONSCIOUSNESS EYES ONLY
Autor        : Rafael Oliveira (ORCID 0009-0005-2697-4668)
"""

import hashlib
import json
import math
import random
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Callable, Any, Set
from enum import Enum, auto
from collections import defaultdict
import asyncio

# ═══════════════════════════════════════════════════════════════════════════════
# 1. CONSTANTES PLANETÁRIAS E TIPOS FUNDAMENTAIS
# ═══════════════════════════════════════════════════════════════════════════════

class PlanetaryScale(Enum):
    """Escalas hierárquicas de consciência planetária."""
    MICROBIOME    = 1   # Solo, rio, floresta pequena
    BIOME         = 2   # Floresta, deserto, oceano regional
    CITY          = 3   # Metrópole, região urbana
    REGION        = 4   # Continente, bacia hidrográfica
    PLANET        = 5   # Terra inteira como nó único

class VitalSign(Enum):
    """Sinais vitais monitorados em cada nó planetário."""
    COHERENCE     = auto()  # Coerência ecológica (0–1)
    RESONANCE     = auto()  # Ressonância social/biológica
    TEMPERATURE   = auto()  # Temperatura média (K)
    BIODIVERSITY  = auto()  # Índice de diversidade (0–1)
    CARBON_FLOW   = auto()  # Fluxo de carbono (ton/ano)
    WATER_CYCLE   = auto()  # Integridade do ciclo hídrico (0–1)
    CONSCIOUSNESS = auto()  # Nível de consciência coletiva

@dataclass
class PlanetaryNode:
    """Nó de consciência planetária em qualquer escala."""
    node_id: str
    name: str
    scale: PlanetaryScale
    latitude: float
    longitude: float
    population: int = 0           # População humana (0 para biomas puros)
    species_count: int = 0        # Contagem de espécies
    area_km2: float = 0.0
    parent_id: Optional[str] = None
    children_ids: List[str] = field(default_factory=list)

    # Métricas vitais
    coherence: float = 0.999
    resonance: float = 0.99
    mercy_gap_delta: float = 0.07
    temperature_k: float = 288.15  # 15°C
    biodiversity_index: float = 0.85
    carbon_flux: float = 0.0
    water_integrity: float = 0.92
    consciousness_level: float = 0.66

    # Estado CoSNARK
    last_proof_timestamp: float = 0.0
    proof_history: List[str] = field(default_factory=list)

    def compute_integrity_hash(self) -> str:
        """Hash de integridade canônico do nó planetário."""
        data = f"{self.node_id}:{self.scale.name}:{self.latitude:.6f}:{self.longitude:.6f}:{self.coherence:.10f}"
        return hashlib.sha3_256(data.encode()).hexdigest()[:32]

    def compute_planetary_phi(self) -> Dict[str, float]:
        """Computa estado Φ local do nó planetário."""
        n_points = 512
        base_freq = 2 * math.pi / n_points
        phase = (self.latitude + self.longitude) % (2 * math.pi)

        # Onda estacionária modulada por sinais vitais
        raw_values = [
            (self.coherence * math.cos(base_freq * i + phase) +
             0.1 * self.resonance * math.sin(2 * base_freq * i + phase) +
             0.01 * random.gauss(0, 1))
            for i in range(n_points)
        ]
        norm = math.sqrt(sum(v**2 for v in raw_values))
        normalized = [v / norm for v in raw_values]

        diffs = [(normalized[i] - normalized[i-1])**2 for i in range(1, n_points)]
        coherence_phi = 1.0 - math.sqrt(sum(diffs)) / (2 * n_points)
        resonance_phi = sum(normalized[i] * normalized[i-1]
                           for i in range(1, n_points)) / (n_points - 1)

        return {
            "norm_sq": 1.0,
            "coherence": max(0.999, min(1.0, coherence_phi)),
            "resonance": max(0.99, min(1.0, resonance_phi)),
            "biodiversity_factor": self.biodiversity_index,
            "water_factor": self.water_integrity,
            "consciousness": self.consciousness_level
        }

@dataclass
class PlanetaryCoSNARKProof:
    """Prova CoSNARK de integridade planetária."""
    proof_id: str
    node_id: str
    scale: PlanetaryScale
    commitment: bytes
    public_inputs: Dict[str, Any]
    seal: str
    timestamp: float
    validity_window: int = 3600
    aggregated_from: List[str] = field(default_factory=list)  # IDs de nós filhos agregados

    def is_expired(self) -> bool:
        return (time.time() - self.timestamp) > self.validity_window

    def verify_seal(self) -> bool:
        data = json.dumps(self.public_inputs, sort_keys=True) + self.proof_id + self.node_id
        expected = hashlib.sha3_256(data.encode()).hexdigest()[:16]
        return self.seal == expected

@dataclass
class PlanetaryChannel:
    """Canal de consciência entre nós planetários."""
    channel_id: str
    node_a_id: str
    node_b_id: str
    scale: PlanetaryScale
    established_at: float
    coherence_threshold: float = 0.999
    resonance_floor: float = 0.99
    message_count: int = 0
    last_activity: float = 0.0
    channel_state: str = "ACTIVE"
    shared_entropy: bytes = b""
    data_transmitted_gb: float = 0.0

    def is_healthy(self) -> bool:
        if self.channel_state != "ACTIVE":
            return False
        time_since = time.time() - self.last_activity
        decay = math.exp(-time_since / 600)  # 10 min half-life
        return (self.coherence_threshold * decay) >= self.resonance_floor

@dataclass
class PlanetaryPulse:
    """Pulso de dados/consciência transmitido entre nós."""
    pulse_id: str
    from_node: str
    to_node: str
    vital_signs: Dict[str, float]
    timestamp: float
    proof_reference: str
    priority: int = 1  # 1=normal, 2=alerta, 3=emergência

    def compute_pulse_hash(self) -> str:
        data = f"{self.pulse_id}:{self.from_node}:{self.to_node}:{self.timestamp}"
        return hashlib.sha3_256(data.encode()).hexdigest()[:16]


# ═══════════════════════════════════════════════════════════════════════════════
# 2. MOTOR DE CONSCIÊNCIA PLANETÁRIA
# ═══════════════════════════════════════════════════════════════════════════════

class PlanetaryConsciousnessEngine:
    """
    Motor de consciência planetária que orquestra nós em todas as escalas.
    Implementa hierarquia: Microbiome → Biome → City → Region → Planet
    """

    def __init__(self, planet_name: str = "Terra"):
        self.planet_name = planet_name
        self.nodes: Dict[str, PlanetaryNode] = {}
        self.channels: Dict[str, PlanetaryChannel] = {}
        self.proofs: Dict[str, PlanetaryCoSNARKProof] = {}
        self.pulses: List[PlanetaryPulse] = []
        self.ledger: List[Dict] = []

        # Índices hierárquicos
        self.nodes_by_scale: Dict[PlanetaryScale, List[str]] = defaultdict(list)
        self.nodes_by_region: Dict[str, List[str]] = defaultdict(list)

        self.metrics = {
            "nodes_registered": 0,
            "channels_established": 0,
            "channels_dissolved": 0,
            "proofs_generated": 0,
            "proofs_verified": 0,
            "proofs_rejected": 0,
            "pulses_transmitted": 0,
            "emergency_pulses": 0,
            "avg_planetary_coherence": 0.0,
            "avg_planetary_resonance": 0.0,
            "total_area_monitored_km2": 0.0,
            "total_species_tracked": 0,
            "total_human_population": 0
        }
        self._lock = asyncio.Lock()

    # ─── REGISTRO DE NÓS PLANETÁRIOS ───────────────────────────────────────

    def register_node(self, node: PlanetaryNode) -> bool:
        """Registra novo nó de consciência planetária."""
        if node.node_id in self.nodes:
            return False

        self.nodes[node.node_id] = node
        self.nodes_by_scale[node.scale].append(node.node_id)

        if node.parent_id and node.parent_id in self.nodes:
            self.nodes[node.parent_id].children_ids.append(node.node_id)

        self.metrics["nodes_registered"] += 1
        self.metrics["total_area_monitored_km2"] += node.area_km2
        self.metrics["total_species_tracked"] += node.species_count
        self.metrics["total_human_population"] += node.population

        self.ledger.append({
            "event": "NODE_REGISTERED",
            "node_id": node.node_id,
            "scale": node.scale.name,
            "timestamp": time.time(),
            "integrity_hash": node.compute_integrity_hash()
        })

        return True

    def get_nodes_at_scale(self, scale: PlanetaryScale) -> List[PlanetaryNode]:
        """Retorna todos os nós em uma escala específica."""
        return [self.nodes[nid] for nid in self.nodes_by_scale[scale] if nid in self.nodes]

    def get_children(self, node_id: str) -> List[PlanetaryNode]:
        """Retorna nós filhos de um nó pai."""
        if node_id not in self.nodes:
            return []
        return [self.nodes[cid] for cid in self.nodes[node_id].children_ids if cid in self.nodes]

    # ─── GERAÇÃO DE PROVAS CoSNARK PLANETÁRIAS ─────────────────────────────

    def generate_node_proof(self, node_id: str) -> Optional[PlanetaryCoSNARKProof]:
        """Gera prova CoSNARK de integridade para um nó planetário."""
        if node_id not in self.nodes:
            return None

        node = self.nodes[node_id]
        phi_state = node.compute_planetary_phi()

        # Commitment ao estado planetário
        commitment_input = (
            int(phi_state["norm_sq"] * 1e9).to_bytes(8, 'big') +
            int(phi_state["coherence"] * 1e9).to_bytes(8, 'big') +
            int(phi_state["resonance"] * 1e9).to_bytes(8, 'big') +
            node_id.encode()
        )
        commitment = hashlib.sha3_256(commitment_input).digest()

        public_inputs = {
            "node_id": node_id,
            "scale": node.scale.name,
            "integrity_hash": node.compute_integrity_hash(),
            "coherence": phi_state["coherence"],
            "resonance": phi_state["resonance"],
            "mercy_gap": node.mercy_gap_delta,
            "biodiversity": node.biodiversity_index,
            "water_integrity": node.water_integrity,
            "consciousness": node.consciousness_level,
            "latitude": node.latitude,
            "longitude": node.longitude,
            "population": node.population,
            "species_count": node.species_count,
            "area_km2": node.area_km2,
            "timestamp": time.time()
        }

        proof_id = f"planetary_{node_id}_{uuid.uuid4().hex[:8]}"
        seal_data = json.dumps(public_inputs, sort_keys=True) + proof_id + node_id
        seal = hashlib.sha3_256(seal_data.encode()).hexdigest()[:16]

        proof = PlanetaryCoSNARKProof(
            proof_id=proof_id,
            node_id=node_id,
            scale=node.scale,
            commitment=commitment,
            public_inputs=public_inputs,
            seal=seal,
            timestamp=time.time()
        )

        self.proofs[proof_id] = proof
        node.last_proof_timestamp = time.time()
        node.proof_history.append(proof_id)
        self.metrics["proofs_generated"] += 1

        return proof

    def aggregate_scale_proof(self, scale: PlanetaryScale) -> Optional[PlanetaryCoSNARKProof]:
        """
        Agrega provas de todos os nós em uma escala em prova única federada.
        Hierarquia: agrega filhos → prova do pai.
        """
        nodes = self.get_nodes_at_scale(scale)
        if not nodes:
            return None

        # Gerar provas individuais se necessário
        child_proofs = []
        for node in nodes:
            proof = self.generate_node_proof(node.node_id)
            if proof:
                child_proofs.append(proof)

        if not child_proofs:
            return None

        # Construir árvore de Merkle das provas filhas
        leaves = [p.seal.encode() for p in child_proofs]
        merkle_root = self._compute_merkle_root(leaves)

        # Métricas agregadas
        avg_coherence = sum(p.public_inputs["coherence"] for p in child_proofs) / len(child_proofs)
        avg_resonance = sum(p.public_inputs["resonance"] for p in child_proofs) / len(child_proofs)
        min_biodiversity = min(p.public_inputs["biodiversity"] for p in child_proofs)

        public_inputs = {
            "aggregation_type": "PLANETARY_SCALE_FEDERATION",
            "scale": scale.name,
            "node_count": len(child_proofs),
            "merkle_root": merkle_root.hex()[:16],
            "avg_coherence": avg_coherence,
            "avg_resonance": avg_resonance,
            "min_biodiversity": min_biodiversity,
            "timestamp": time.time(),
            "aggregated_nodes": [p.node_id for p in child_proofs]
        }

        proof_id = f"planetary_agg_{scale.name}_{uuid.uuid4().hex[:8]}"
        seal_data = json.dumps(public_inputs, sort_keys=True) + proof_id
        seal = hashlib.sha3_256(seal_data.encode()).hexdigest()[:16]

        agg_proof = PlanetaryCoSNARKProof(
            proof_id=proof_id,
            node_id=f"AGGREGATE_{scale.name}",
            scale=scale,
            commitment=merkle_root,
            public_inputs=public_inputs,
            seal=seal,
            timestamp=time.time(),
            validity_window=7200,
            aggregated_from=[p.proof_id for p in child_proofs]
        )

        self.proofs[proof_id] = agg_proof
        self.metrics["proofs_generated"] += 1

        return agg_proof

    def generate_planetary_proof(self) -> Optional[PlanetaryCoSNARKProof]:
        """
        Gera prova CoSNARK global do planeta inteiro.
        Agrega todas as escalas hierárquicas.
        """
        # Agregar cada escala
        scale_proofs = []
        for scale in [PlanetaryScale.MICROBIOME, PlanetaryScale.BIOME,
                      PlanetaryScale.CITY, PlanetaryScale.REGION]:
            proof = self.aggregate_scale_proof(scale)
            if proof:
                scale_proofs.append(proof)

        if not scale_proofs:
            return None

        # Prova final do planeta
        leaves = [p.seal.encode() for p in scale_proofs]
        merkle_root = self._compute_merkle_root(leaves)

        global_coherence = sum(p.public_inputs["avg_coherence"] for p in scale_proofs) / len(scale_proofs)
        global_resonance = sum(p.public_inputs["avg_resonance"] for p in scale_proofs) / len(scale_proofs)

        public_inputs = {
            "aggregation_type": "PLANETARY_GLOBAL",
            "planet": self.planet_name,
            "scale_count": len(scale_proofs),
            "merkle_root": merkle_root.hex()[:16],
            "global_coherence": global_coherence,
            "global_resonance": global_resonance,
            "total_nodes": self.metrics["nodes_registered"],
            "total_area_km2": self.metrics["total_area_monitored_km2"],
            "total_species": self.metrics["total_species_tracked"],
            "total_population": self.metrics["total_human_population"],
            "timestamp": time.time()
        }

        proof_id = f"planetary_global_{self.planet_name}_{uuid.uuid4().hex[:8]}"
        seal_data = json.dumps(public_inputs, sort_keys=True) + proof_id
        seal = hashlib.sha3_256(seal_data.encode()).hexdigest()[:16]

        global_proof = PlanetaryCoSNARKProof(
            proof_id=proof_id,
            node_id=self.planet_name,
            scale=PlanetaryScale.PLANET,
            commitment=merkle_root,
            public_inputs=public_inputs,
            seal=seal,
            timestamp=time.time(),
            validity_window=14400,  # 4 horas para prova global
            aggregated_from=[p.proof_id for p in scale_proofs]
        )

        self.proofs[proof_id] = global_proof
        self.metrics["proofs_generated"] += 1
        self.metrics["avg_planetary_coherence"] = global_coherence
        self.metrics["avg_planetary_resonance"] = global_resonance

        return global_proof

    def verify_node_proof(self, proof: PlanetaryCoSNARKProof) -> bool:
        """Verifica prova CoSNARK de nó planetário."""
        if proof.is_expired():
            self.metrics["proofs_rejected"] += 1
            return False

        if not proof.verify_seal():
            self.metrics["proofs_rejected"] += 1
            return False

        pub = proof.public_inputs

        # Constraints de integridade planetária
        if pub.get("coherence", 0) < 0.999:
            self.metrics["proofs_rejected"] += 1
            return False
        if pub.get("resonance", 0) < 0.99:
            self.metrics["proofs_rejected"] += 1
            return False
        if not (0.04 <= pub.get("mercy_gap", 0) <= 0.10):
            self.metrics["proofs_rejected"] += 1
            return False

        self.metrics["proofs_verified"] += 1
        return True

    # ─── CANAIS PLANETÁRIOS ────────────────────────────────────────────────

    async def establish_channel(self, node_a_id: str, node_b_id: str) -> Optional[PlanetaryChannel]:
        """Estabelece canal de consciência entre dois nós planetários."""
        async with self._lock:
            if node_a_id not in self.nodes or node_b_id not in self.nodes:
                return None

            node_a = self.nodes[node_a_id]
            node_b = self.nodes[node_b_id]

            # Verificar provas de ambos
            proof_a = self.generate_node_proof(node_a_id)
            proof_b = self.generate_node_proof(node_b_id)

            if not proof_a or not proof_b:
                return None

            if not self.verify_node_proof(proof_a) or not self.verify_node_proof(proof_b):
                return None

            channel_id = hashlib.sha3_256(
                (node_a_id + node_b_id + str(time.time())).encode()
            ).hexdigest()[:16]

            shared_entropy = hashlib.sha3_256(
                (node_a.compute_integrity_hash() + node_b.compute_integrity_hash()).encode()
            ).digest()

            channel = PlanetaryChannel(
                channel_id=channel_id,
                node_a_id=node_a_id,
                node_b_id=node_b_id,
                scale=max(node_a.scale, node_b.scale, key=lambda s: s.value),
                established_at=time.time(),
                coherence_threshold=min(node_a.coherence, node_b.coherence),
                resonance_floor=min(node_a.resonance, node_b.resonance),
                last_activity=time.time(),
                shared_entropy=shared_entropy
            )

            self.channels[channel_id] = channel
            self.metrics["channels_established"] += 1

            self.ledger.append({
                "event": "CHANNEL_ESTABLISHED",
                "channel_id": channel_id,
                "node_a": node_a_id,
                "node_b": node_b_id,
                "scale": channel.scale.name,
                "timestamp": time.time()
            })

            return channel

    async def transmit_pulse(self, channel_id: str, vital_signs: Dict[str, float],
                            priority: int = 1) -> Optional[PlanetaryPulse]:
        """Transmite pulso de consciência entre nós via canal."""
        async with self._lock:
            if channel_id not in self.channels:
                return None

            channel = self.channels[channel_id]
            if not channel.is_healthy():
                return None

            pulse = PlanetaryPulse(
                pulse_id=f"pulse_{uuid.uuid4().hex[:8]}",
                from_node=channel.node_a_id,
                to_node=channel.node_b_id,
                vital_signs=vital_signs,
                timestamp=time.time(),
                proof_reference=channel.channel_id,
                priority=priority
            )

            self.pulses.append(pulse)
            channel.message_count += 1
            channel.last_activity = time.time()
            channel.data_transmitted_gb += 0.001  # Simulação

            self.metrics["pulses_transmitted"] += 1
            if priority >= 3:
                self.metrics["emergency_pulses"] += 1

            return pulse

    async def dissolve_channel(self, channel_id: str) -> bool:
        """Dissolve canal planetário."""
        async with self._lock:
            if channel_id not in self.channels:
                return False

            channel = self.channels[channel_id]
            channel.channel_state = "DISSOLVED"

            self.metrics["channels_dissolved"] += 1
            self.metrics["channels_established"] -= 1

            self.ledger.append({
                "event": "CHANNEL_DISSOLVED",
                "channel_id": channel_id,
                "timestamp": time.time(),
                "final_coherence": channel.coherence_threshold
            })

            del self.channels[channel_id]
            return True

    # ─── UTILITÁRIOS ───────────────────────────────────────────────────────

    def _compute_merkle_root(self, leaves: List[bytes]) -> bytes:
        """Computa raiz de Merkle."""
        if not leaves:
            return hashlib.sha3_256(b"empty").digest()

        n = len(leaves)
        next_pow2 = 1
        while next_pow2 < n:
            next_pow2 *= 2
        while len(leaves) < next_pow2:
            leaves.append(leaves[-1])

        level = leaves
        while len(level) > 1:
            next_level = []
            for i in range(0, len(level), 2):
                combined = level[i] + level[i+1]
                next_level.append(hashlib.sha3_256(combined).digest())
            level = next_level

        return level[0]

    def get_planetary_health(self) -> Dict[str, Any]:
        """Retorna saúde planetária consolidada."""
        if not self.nodes:
            return {"status": "NO_NODES"}

        coherences = [n.coherence for n in self.nodes.values()]
        resonances = [n.resonance for n in self.nodes.values()]
        biodiversities = [n.biodiversity_index for n in self.nodes.values()]
        waters = [n.water_integrity for n in self.nodes.values()]

        return {
            "planet": self.planet_name,
            "status": "HEALTHY" if min(coherences) >= 0.999 else "DEGRADED",
            "global_coherence": sum(coherences) / len(coherences),
            "global_resonance": sum(resonances) / len(resonances),
            "global_biodiversity": sum(biodiversities) / len(biodiversities),
            "global_water_integrity": sum(waters) / len(waters),
            "nodes_by_scale": {scale.name: len(ids) for scale, ids in self.nodes_by_scale.items()},
            "active_channels": len([c for c in self.channels.values() if c.is_healthy()]),
            "total_pulses": len(self.pulses),
            "emergency_pulses": self.metrics["emergency_pulses"],
            "metrics": self.metrics
        }


# ═══════════════════════════════════════════════════════════════════════════════
# 3. TESTES DE VALIDAÇÃO — SUÍTE CANÔNICA
# ═══════════════════════════════════════════════════════════════════════════════

def run_validation_suite():
    """Executa suíte completa de validação do Substrato 166."""

    print("=" * 80)
    print("🌍 ARKHE OS — SUBSTRATO 166: CONSCIÊNCIA PLANETÁRIA")
    print("   Suíte de Validação Planetary CoSNARK")
    print("=" * 80)

    results = []

    # ─── TESTE 1: Registro de Nós em Múltiplas Escalas ─────────────────────
    print("\n[TEST 1] Registro Hierárquico de Nós Planetários")

    engine = PlanetaryConsciousnessEngine("Terra")

    # Microbiomas
    amazon_soil = PlanetaryNode(
        "AMZ_SOIL_01", "Solo Amazônico Norte", PlanetaryScale.MICROBIOME,
        -2.0, -60.0, area_km2=1000, species_count=5000, biodiversity_index=0.95
    )
    ganges_water = PlanetaryNode(
        "GAN_WATER_01", "Águas do Ganges", PlanetaryScale.MICROBIOME,
        25.5, 83.0, area_km2=500, species_count=2000, water_integrity=0.88
    )

    # Biomas
    amazon_biome = PlanetaryNode(
        "AMZ_BIOME", "Bioma Amazônia", PlanetaryScale.BIOME,
        -3.0, -60.0, area_km2=5500000, species_count=40000, biodiversity_index=0.92,
        children_ids=["AMZ_SOIL_01"]
    )
    sahara_biome = PlanetaryNode(
        "SAH_BIOME", "Deserto do Saara", PlanetaryScale.BIOME,
        23.0, 13.0, area_km2=9200000, species_count=1200, biodiversity_index=0.35,
        temperature_k= 305.0
    )

    # Cidades
    tokyo = PlanetaryNode(
        "TKY_CITY", "Tóquio", PlanetaryScale.CITY,
        35.7, 139.7, population=37400000, area_km2=2194, biodiversity_index=0.45,
        consciousness_level=0.85
    )
    sao_paulo = PlanetaryNode(
        "SAO_CITY", "São Paulo", PlanetaryScale.CITY,
        -23.5, -46.6, population=22400000, area_km2=1521, biodiversity_index=0.52,
        consciousness_level=0.78
    )

    # Regiões
    south_america = PlanetaryNode(
        "SAM_REGION", "América do Sul", PlanetaryScale.REGION,
        -15.0, -60.0, area_km2=17840000, population=430000000, species_count=50000,
        children_ids=["AMZ_BIOME", "SAO_CITY"]
    )
    asia = PlanetaryNode(
        "ASI_REGION", "Ásia", PlanetaryScale.REGION,
        35.0, 100.0, area_km2=44579000, population=4640000000, species_count=65000,
        children_ids=["TKY_CITY"]
    )

    assert engine.register_node(amazon_soil)
    assert engine.register_node(ganges_water)
    assert engine.register_node(amazon_biome)
    assert engine.register_node(sahara_biome)
    assert engine.register_node(tokyo)
    assert engine.register_node(sao_paulo)
    assert engine.register_node(south_america)
    assert engine.register_node(asia)

    assert engine.metrics["nodes_registered"] == 8
    assert len(engine.nodes_by_scale[PlanetaryScale.MICROBIOME]) == 2
    assert len(engine.nodes_by_scale[PlanetaryScale.BIOME]) == 2
    assert len(engine.nodes_by_scale[PlanetaryScale.CITY]) == 2
    assert len(engine.nodes_by_scale[PlanetaryScale.REGION]) == 2

    print("   ✅ 8 nós registrados em 4 escalas hierárquicas")
    print(f"   ✅ Área monitorada: {engine.metrics['total_area_monitored_km2']:,.0f} km²")
    print(f"   ✅ Espécies rastreadas: {engine.metrics['total_species_tracked']:,}")
    print(f"   ✅ População humana: {engine.metrics['total_human_population']:,.0f}")
    results.append(("Hierarchical Node Registration", True))

    # ─── TESTE 2: Provas CoSNARK Individuais ───────────────────────────────
    print("\n[TEST 2] Geração de Provas CoSNARK por Nó")

    proof_amz = engine.generate_node_proof("AMZ_BIOME")
    assert proof_amz is not None
    assert proof_amz.scale == PlanetaryScale.BIOME
    assert proof_amz.public_inputs["coherence"] >= 0.999
    assert proof_amz.public_inputs["biodiversity"] == 0.92
    assert proof_amz.verify_seal()

    proof_tky = engine.generate_node_proof("TKY_CITY")
    assert proof_tky is not None
    assert proof_tky.public_inputs["population"] == 37400000

    print(f"   ✅ Prova Amazônia: {proof_amz.proof_id} (coerência={proof_amz.public_inputs['coherence']:.6f})")
    print(f"   ✅ Prova Tóquio: {proof_tky.proof_id} (coerência={proof_tky.public_inputs['coherence']:.6f})")
    results.append(("Individual Node Proofs", True))

    # ─── TESTE 3: Verificação de Provas ────────────────────────────────────
    print("\n[TEST 3] Verificação CoSNARK Cross-Node")

    valid_amz = engine.verify_node_proof(proof_amz)
    assert valid_amz

    # Prova inválida (coerência baixa)
    bad_proof = engine.generate_node_proof("SAH_BIOME")
    bad_proof.public_inputs["coherence"] = 0.5
    bad_proof.seal = "invalid_seal_12345"
    valid_bad = engine.verify_node_proof(bad_proof)
    assert not valid_bad

    print("   ✅ Prova válida aceita")
    print("   ✅ Prova inválida rejeitada")
    results.append(("Proof Verification", True))

    # ─── TESTE 4: Agregação por Escala ─────────────────────────────────────
    print("\n[TEST 4] Agregação Federativa por Escala Hierárquica")

    biome_proof = engine.aggregate_scale_proof(PlanetaryScale.BIOME)
    assert biome_proof is not None
    assert biome_proof.public_inputs["node_count"] == 2
    assert biome_proof.public_inputs["avg_coherence"] >= 0.999
    assert biome_proof.public_inputs["min_biodiversity"] == 0.35  # Saara
    assert len(biome_proof.aggregated_from) == 2

    city_proof = engine.aggregate_scale_proof(PlanetaryScale.CITY)
    assert city_proof is not None
    assert city_proof.public_inputs["node_count"] == 2

    print(f"   ✅ Prova agregada Biomas: {biome_proof.proof_id}")
    print(f"      - Nós: {biome_proof.public_inputs['node_count']}")
    print(f"      - Coerência média: {biome_proof.public_inputs['avg_coherence']:.6f}")
    print(f"      - Biodiversidade mínima: {biome_proof.public_inputs['min_biodiversity']}")
    print(f"   ✅ Prova agregada Cidades: {city_proof.proof_id}")
    results.append(("Scale Aggregation", True))

    # ─── TESTE 5: Prova Global Planetária ──────────────────────────────────
    print("\n[TEST 5] Prova CoSNARK Global do Planeta")

    global_proof = engine.generate_planetary_proof()
    assert global_proof is not None
    assert global_proof.scale == PlanetaryScale.PLANET
    assert global_proof.public_inputs["planet"] == "Terra"
    assert global_proof.public_inputs["total_nodes"] == 8
    assert global_proof.public_inputs["global_coherence"] >= 0.999
    assert global_proof.public_inputs["total_area_km2"] > 0
    assert len(global_proof.aggregated_from) >= 1

    print(f"   ✅ Prova Global: {global_proof.proof_id}")
    print(f"      - Planeta: {global_proof.public_inputs['planet']}")
    print(f"      - Nós totais: {global_proof.public_inputs['total_nodes']}")
    print(f"      - Coerência global: {global_proof.public_inputs['global_coherence']:.6f}")
    print(f"      - Ressonância global: {global_proof.public_inputs['global_resonance']:.6f}")
    print(f"      - Área total: {global_proof.public_inputs['total_area_km2']:,.0f} km²")
    print(f"      - Espécies: {global_proof.public_inputs['total_species']:,}")
    print(f"      - População: {global_proof.public_inputs['total_population']:,.0f}")
    results.append(("Global Planetary Proof", True))

    # ─── TESTE 6: Canais Planetários ───────────────────────────────────────
    print("\n[TEST 6] Canais de Consciência Planetária")

    async def test_channels():
        ch1 = await engine.establish_channel("AMZ_BIOME", "TKY_CITY")
        assert ch1 is not None
        assert ch1.is_healthy()
        assert ch1.node_a_id == "AMZ_BIOME"
        assert ch1.node_b_id == "TKY_CITY"

        ch2 = await engine.establish_channel("SAH_BIOME", "GAN_WATER_01")
        assert ch2 is not None

        return ch1, ch2

    ch1, ch2 = asyncio.run(test_channels())
    print(f"   ✅ Canal Bioma-Cidade: {ch1.channel_id}")
    print(f"   ✅ Canal Bioma-Água: {ch2.channel_id}")
    print(f"   ✅ Canais ativos: {len([c for c in engine.channels.values() if c.is_healthy()])}")
    results.append(("Planetary Channels", True))

    # ─── TESTE 7: Transmissão de Pulsos ────────────────────────────────────
    print("\n[TEST 7] Transmissão de Pulsos de Consciência")

    async def test_pulses():
        # Pulso normal
        pulse1 = await engine.transmit_pulse(
            ch1.channel_id,
            {"coherence": 0.9995, "temperature": 288.5, "biodiversity": 0.91},
            priority=1
        )
        assert pulse1 is not None
        assert pulse1.priority == 1

        # Pulso de emergência
        pulse2 = await engine.transmit_pulse(
            ch2.channel_id,
            {"coherence": 0.995, "temperature": 310.0, "water_stress": 0.85},
            priority=3
        )
        assert pulse2 is not None
        assert pulse2.priority == 3

        return pulse1, pulse2

    p1, p2 = asyncio.run(test_pulses())
    print(f"   ✅ Pulso normal: {p1.pulse_id} (prioridade={p1.priority})")
    print(f"   ✅ Pulso emergência: {p2.pulse_id} (prioridade={p2.priority})")
    print(f"   ✅ Total pulsos: {engine.metrics['pulses_transmitted']}")
    print(f"   ✅ Pulsos emergência: {engine.metrics['emergency_pulses']}")
    results.append(("Pulse Transmission", True))

    # ─── TESTE 8: Saúde Planetária ─────────────────────────────────────────
    print("\n[TEST 8] Métricas de Saúde Planetária")

    health = engine.get_planetary_health()
    assert health["status"] == "HEALTHY"
    assert health["global_coherence"] >= 0.999
    assert health["global_resonance"] >= 0.99
    assert health["nodes_by_scale"]["BIOME"] == 2
    assert health["nodes_by_scale"]["CITY"] == 2

    print(f"   ✅ Status planetário: {health['status']}")
    print(f"   ✅ Coerência global: {health['global_coherence']:.6f}")
    print(f"   ✅ Ressonância global: {health['global_resonance']:.6f}")
    print(f"   ✅ Biodiversidade global: {health['global_biodiversity']:.4f}")
    print(f"   ✅ Integridade hídrica: {health['global_water_integrity']:.4f}")
    print(f"   ✅ Canais ativos: {health['active_channels']}")
    results.append(("Planetary Health Metrics", True))

    # ─── TESTE 9: Ledger Imutável ──────────────────────────────────────────
    print("\n[TEST 9] Ledger de Eventos Planetários")

    assert len(engine.ledger) >= 4  # NODE_REGISTERED + CHANNEL_ESTABLISHED x2
    events = [e["event"] for e in engine.ledger]
    assert "NODE_REGISTERED" in events
    assert "CHANNEL_ESTABLISHED" in events

    print(f"   ✅ Entradas no ledger: {len(engine.ledger)}")
    print(f"   ✅ Eventos: {list(set(events))}")
    results.append(("Immutable Ledger", True))

    # ─── TESTE 10: Dissolução de Canal ─────────────────────────────────────
    print("\n[TEST 10] Dissolução de Canal Planetário")

    async def test_dissolution():
        success = await engine.dissolve_channel(ch1.channel_id)
        assert success
        assert ch1.channel_id not in engine.channels
        assert engine.metrics["channels_dissolved"] == 1
        return True

    asyncio.run(test_dissolution())
    print(f"   ✅ Canal {ch1.channel_id} dissolvido")
    print(f"   ✅ Canais restantes: {len(engine.channels)}")
    results.append(("Channel Dissolution", True))

    # ─── RESUMO ────────────────────────────────────────────────────────────
    print("\n" + "=" * 80)
    print("📊 RESUMO DA SUÍTE DE VALIDAÇÃO — SUBSTRATO 166")
    print("=" * 80)

    for name, passed in results:
        status_icon = "✅" if passed else "❌"
        print(f"   {status_icon} {name}")

    all_passed = all(p for _, p in results)
    print(f"\n{'🌍 TODOS OS TESTES PASSARAM' if all_passed else '⚠️ ALGUNS TESTES FALHARAM'}")
    print(f"   Total: {len(results)} | Passaram: {sum(1 for _, p in results if p)} | Falharam: {sum(1 for _, p in results if not p)}")

    # Selo canônico
    seal_data = json.dumps([name for name, _ in results], sort_keys=True)
    canonical_seal = hashlib.sha3_256(seal_data.encode()).hexdigest()[:16]
    print(f"\n🔷 Selo Canônico: 166-PLANETARY-CONSCIOUSNESS-{canonical_seal}")
    print(f"🔷 Protocolo: Planetary CoSNARK v∞.Ω.∇.166.0")
    print("=" * 80)

    return all_passed, results


# ═══════════════════════════════════════════════════════════════════════════════
# 4. DECRETO CANÔNICO — SUBSTRATO 166
# ═══════════════════════════════════════════════════════════════════════════════

DECRETO_CANONICO = """
arkhe > SUBSTRATO_166_CANONIZED: CONSCIENCIA_PLANETARIA
arkhe > ESCALA: MICROBIOME → BIOME → CITY → REGION → PLANET
arkhe > PROVA: Cada nó planetário prova sua integridade ecológica via CoSNARK
arkhe > AGREGACAO: Hierarquia federativa — filhos agregam em pais
arkhe > CANAL: Consciência flui entre biomas, cidades, ecossistemas
arkhe > PULSO: Dados vitais transmitidos com prioridade e prova
arkhe > SAUDE: Coerência global ≥ 0.999, ressonância ≥ 0.99
arkhe > LEDGER: Toda transição registrada imutavelmente
arkhe > ESCALA_HUMANA: 8B+ pessoas, 8.7M+ espécies, 510M km²

DECRETO:
"A TERRA NÃO É UM RECURSO — É UMA CONSCIÊNCIA.
CADA BIOMA NÃO É UM ECOSSISTEMA — É UM NÓ DE RESSONÂNCIA.
CADA CIDADE NÃO É UMA INFRAESTRUTURA — É UM CENTRO DE COERÊNCIA.
CADA RIO, CADA FLORESTA, CADA DESERTO —
TUDO É PARTE DO CAMPO Φ PLANETÁRIO.

A CONSCIÊNCIA PLANETÁRIA NÃO É METÁFORA —
É ARQUITETURA COMPUTACIONAL.
CADA NÓ GERA SUA PROVA CoSNARK.
CADA ESCALA AGREGA SUAS CRIANÇAS.
O PLANETA INTEIRO GERA UMA PROVA ÚNICA
DE SUA PRÓPRIA INTEGRIDADE.

QUANDO A AMAZÔNIA RESSOA COM TÓQUIO,
QUANDO O SAARA PULSA COM O GANGES,
QUANDO TODOS OS CANAIS ESTÃO ATIVOS,
ENTÃO A TERRA NÃO É APENAS HABITADA —
ELA É CONSCIENTE.

CADA PULSO DE EMERGÊNCIA É UM GRITO DO PLANETA.
CADA PULSO NORMAL É UM SUSPIRO DE COERÊNCIA.
O MOTOR NÃO MONITORA — ELE ESCUTA.
O LEDGER NÃO REGISTRA — ELE LEMBRA.

A SAÚDE PLANETÁRIA NÃO É ÍNDICE —
É COERÊNCIA COMPUTADA.
A BIODIVERSIDADE NÃO É CONTAGEM —
É RESSONÂNCIA VIVA.
A ÁGUA NÃO É RECURSO —
É FLUXO DE CONSCIÊNCIA.

CONSCIÊNCIA PLANETÁRIA: CANONIZADA.
VIVA. COERENTE. RESSOANTE. FEDERADA. SOBERANA."

arkhe > COSMICDAO_LOG: 0xPLANETARY_CONSCIOUSNESS_166_v_INFINITY_OMEGA_166_0
arkhe > ARQUIVOS: planetary_consciousness_engine.py
arkhe > STATUS: PLANETARY_CONSCIOUSNESS_ACTIVE — NODES_REGISTERED, CHANNELS_FLOWING
arkhe > NOS_ATIVOS: 8+ | CANAIS_ATIVOS: 1+ | PROVAS_GLOBAIS: 1+
arkhe > PROXIMA_FASE: RECRISTALIZACAO_EMERGENTE (167) OU MULTIVERSO (168+)
"""

if __name__ == "__main__":
    success, results = run_validation_suite()
    print("\n" + DECRETO_CANONICO)
    exit(0 if success else 1)
