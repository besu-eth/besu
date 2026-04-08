// SPDX-License-Identifier: UN-OCT-2026
pragma solidity ^0.8.20;

contract UNSC_Reform_Protocol {
    // Estados-membros registrados
    mapping(address => Member) public members;
    address[] public permanentMembers;
    address[] public electedMembers;

    struct Member {
        bool isPermanent;
        bool isActive;
        uint vetoCount;
        uint lastVoteTimestamp;
        uint phaseScore;  // λ₂ regional (0-1000)
        bool vetoSuspended; // Flag to indicate if veto is suspended
    }

    struct Resolution {
        address proposer;
        string uri;
        uint crisisLevel;
        uint voteCount;
        uint vetoCount;
        bool passed;
        uint timestamp;
    }

    mapping(uint => Resolution) public resolutions;
    mapping(uint => mapping(address => bool)) public hasVoted;

    // Métricas de coerência
    uint public lambda2Global;          // escala 0-1000 (ex: 590 = 0.59)
    uint public humanitarianCrisisLevel; // 0-1000, gatilho para bypass

    address public oracle;
    address public administrator;

    // Eventos
    event ResolutionProposed(uint resolutionId, address proposer, uint crisisLevel);
    event VetoCast(address member, uint resolutionId);
    event CircuitBreakerTriggered(string reason);
    event ResolutionPassed(uint resolutionId, bool bypassedVeto);
    event SanctionsTriggered(address member);

    modifier onlyAdmin() {
        require(msg.sender == administrator, "Only administrator");
        _;
    }

    // Modificador: apenas membros ativos
    modifier onlyActiveMember() {
        require(members[msg.sender].isActive, "Member not active");
        _;
    }

    constructor(address _oracle) {
        administrator = msg.sender;
        oracle = _oracle;
    }

    function registerMember(address _member, bool _isPermanent) external onlyAdmin {
        members[_member] = Member({
            isPermanent: _isPermanent,
            isActive: true,
            vetoCount: 0,
            lastVoteTimestamp: 0,
            phaseScore: 1000,
            vetoSuspended: false
        });
        if (_isPermanent) {
            permanentMembers.push(_member);
        } else {
            electedMembers.push(_member);
        }
    }

    // ========== PROPOSTA E VOTAÇÃO ==========

    function proposeResolution(uint resolutionId, string calldata uri, uint crisisLevel)
        external onlyActiveMember
    {
        require(resolutions[resolutionId].proposer == address(0), "Resolution exists");
        resolutions[resolutionId] = Resolution({
            proposer: msg.sender,
            uri: uri,
            crisisLevel: crisisLevel,
            voteCount: 0,
            vetoCount: 0,
            passed: false,
            timestamp: block.timestamp
        });
        emit ResolutionProposed(resolutionId, msg.sender, crisisLevel);
    }

    function castVote(uint resolutionId, bool inFavor) external onlyActiveMember {
        Resolution storage res = resolutions[resolutionId];
        require(res.timestamp != 0, "Resolution not found");
        require(!res.passed, "Already decided");
        require(!hasVoted[resolutionId][msg.sender], "Already voted");

        hasVoted[resolutionId][msg.sender] = true;

        if (inFavor) {
            res.voteCount++;
        } else {
            // Voto contra de membro permanente = veto
            if (members[msg.sender].isPermanent) {
                require(!members[msg.sender].vetoSuspended, "Veto power is suspended for this member");
                res.vetoCount++;
                members[msg.sender].vetoCount++;
                emit VetoCast(msg.sender, resolutionId);
            }
        }

        // Verifica se a resolução pode ser decidida
        _resolveResolution(resolutionId);
    }

    function _resolveResolution(uint resolutionId) internal {
        Resolution storage res = resolutions[resolutionId];

        // Gatilho 1: Bypass automático para crises humanitárias
        if (res.crisisLevel > 800) {  // equivalente a λ₂-humanitário < 0.20
            res.passed = true;
            emit CircuitBreakerTriggered("Humanitarian crisis - veto bypassed");
            emit ResolutionPassed(resolutionId, true);
            return;
        }

        // Gatilho 2: Uniting for Peace (veto suspensível)
        if (res.vetoCount > 0 && lambda2Global < 600) {  // λ₂ < 0.60
            // Invoca mecanismo da AG (internal call)
            _unitingForPeace(resolutionId);
            return;
        }

        // Votação normal: maioria simples (9 dos 15)
        if (res.voteCount >= 9 && res.vetoCount == 0) {
            res.passed = true;
            emit ResolutionPassed(resolutionId, false);
        }
    }

    // ========== MECANISMO DE FALEOVER (UNITING FOR PEACE) ==========

    function _unitingForPeace(uint resolutionId) internal {
        // Chama a Assembleia Geral (simulado)
        uint generalAssemblyVotes = simulateGeneralAssemblyVote(resolutionId);
        if (generalAssemblyVotes >= 128) { // 2/3 dos 193
            resolutions[resolutionId].passed = true;
            emit ResolutionPassed(resolutionId, true);
        }
    }

    function simulateGeneralAssemblyVote(uint resolutionId) public pure returns (uint) {
        // Simulação: em um sistema real, isso seria uma votação separada ou oráculo
        return 130;
    }

    // ========== MONITOR DE COERÊNCIA (λ₂) ==========

    function updateLambda2(uint newLambda2) external {
        require(msg.sender == oracle, "Only oracle");
        lambda2Global = newLambda2;

        // Gatilho de degradação severa
        if (lambda2Global < 500) {  // λ₂ < 0.50
            emit CircuitBreakerTriggered("System coherence critical - activating emergency mode");
            _emergencyProtocol();
        }
    }

    function _emergencyProtocol() internal {
        // Suspende vetos
        for (uint i = 0; i < permanentMembers.length; i++) {
            members[permanentMembers[i]].vetoSuspended = true;
        }
    }

    // ========== SMART SANCTIONS (ORÁCULOS DE DADOS) ==========

    function verifyCompliance(address member) external view returns (bool) {
        // Consulta oráculo de dados (ex: IAEA, OPCW, HRW)
        uint complianceScore = IOracle(oracle).checkCompliance(member);
        return complianceScore > 700;  // 70% compliance = green
    }

    function triggerSanctions(address member) external {
        require(IOracle(oracle).isViolating(member), "No violation detected");
        // Congela ativos, suspende direitos de voto, etc.
        members[member].isActive = false;
        emit SanctionsTriggered(member);
    }
}

interface IOracle {
    function checkCompliance(address member) external view returns (uint);
    function isViolating(address member) external view returns (bool);
    function satelliteDetection(address node) external view returns (uint);
}
