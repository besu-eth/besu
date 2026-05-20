/*
 * Copyright contributors to Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.p2p.discovery;

import org.hyperledger.besu.crypto.Hash;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.ethereum.chain.VariablesStorage;
import org.hyperledger.besu.ethereum.forkid.ForkIdManager;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.DiscoveryPeerV4;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.nat.NatService;
import org.hyperledger.besu.util.NetworkUtility;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import com.google.common.net.InetAddresses;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt64;
import org.ethereum.beacon.discovery.schema.EnrField;
import org.ethereum.beacon.discovery.schema.IdentitySchema;
import org.ethereum.beacon.discovery.schema.NodeRecord;
import org.ethereum.beacon.discovery.schema.NodeRecordFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the local Ethereum Node Record (ENR) lifecycle.
 *
 * <p>This component is responsible for:
 *
 * <ul>
 *   <li>Initializing the local {@link DiscoveryPeerV4} representation
 *   <li>Creating, updating, and signing the local {@link NodeRecord}
 *   <li>Persisting the ENR sequence number and contents to disk
 *   <li>Ensuring the ENR remains consistent with the advertised address, ports, and fork ID
 *   <li>Applying peer-consensus auto-discovered IPv6 addresses via {@link
 *       #applyAutoDiscoveredIpv6Host(String, int)}
 * </ul>
 *
 * <p>The ENR is only rewritten when one or more relevant fields change.
 */
public class NodeRecordManager {
  private static final Logger LOG = LoggerFactory.getLogger(NodeRecordManager.class);
  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();

  private static final String FORK_ID_ENR_FIELD = "eth";

  private final VariablesStorage variablesStorage;
  private final NodeKey nodeKey;
  private final Bytes nodeId;
  private final Supplier<List<Bytes>> forkIdSupplier;
  private final NatService natService;

  private final ReentrantLock lock = new ReentrantLock();

  private Optional<DiscoveryPeerV4> localNode = Optional.empty();
  private HostEndpoint primaryEndpoint;
  private Optional<HostEndpoint> ipv6Endpoint = Optional.empty();

  // TCP port to use if/when an IPv6 host is auto-discovered via DiscV5 peer consensus.
  // Holds only the port — never a host — so it cannot leak into a broadcast/signed ENR.
  // Lifecycle: assigned during initializeLocalNode (single-threaded startup), then cleared
  // under `lock` by applyAutoDiscoveredIpv6Host on successful consumption.
  private Optional<Integer> ipv6AutoDiscoveryTcpPort = Optional.empty();

  /**
   * Creates a new {@link NodeRecordManager}.
   *
   * <p>The manager derives the node identifier from the provided {@link NodeKey} and lazily
   * resolves the fork ID using the supplied {@link ForkIdManager}.
   *
   * @param storageProvider provides access to persistent ENR storage
   * @param nodeKey the local node's cryptographic identity
   * @param forkIdManager supplies the current fork ID for the chain head
   * @param natService resolves externally advertised network addresses
   */
  public NodeRecordManager(
      final StorageProvider storageProvider,
      final NodeKey nodeKey,
      final ForkIdManager forkIdManager,
      final NatService natService) {

    this.variablesStorage = storageProvider.createVariablesStorage();
    this.nodeKey = nodeKey;
    this.nodeId = nodeKey.getPublicKey().getEncodedBytes();
    this.forkIdSupplier = () -> forkIdManager.getForkIdForChainHead().getForkIdAsBytesList();
    this.natService = natService;
  }

  /**
   * Returns the locally initialized discovery peer, if present.
   *
   * <p>The local node is only available after {@code initializeLocalNode} has been invoked.
   *
   * @return an {@link Optional} containing the local {@link DiscoveryPeerV4}, or empty if
   *     uninitialized
   */
  public Optional<DiscoveryPeerV4> getLocalNode() {
    return localNode;
  }

  /**
   * Initializes the local discovery peer with optional IPv6 dual-stack support.
   *
   * <p>The primary endpoint's advertised host may be overridden if the {@link NatService} detects
   * an external address. Once initialized, the local node record is immediately synchronized to
   * disk.
   *
   * <p>When {@code ipv6} is present, the resulting ENR will contain both IPv4 ({@code ip}/{@code
   * tcp}/{@code udp}) and IPv6 ({@code ip6}/{@code tcp6}/{@code udp6}) fields. When absent, only
   * the primary address fields are populated — either IPv4 or IPv6 depending on the type of the
   * primary host.
   *
   * @param primary the primary network endpoint (IPv4 or IPv6)
   * @param ipv6 an optional secondary IPv6 endpoint for dual-stack operation
   */
  public void initializeLocalNode(final HostEndpoint primary, final Optional<HostEndpoint> ipv6) {
    initializeLocalNode(primary, ipv6, Optional.empty());
  }

  /**
   * Initializes the local discovery peer, optionally registering a TCP port hint for IPv6
   * auto-discovery.
   *
   * <p>When dual-stack discovery is bound but {@code --p2p-host-ipv6} is not pinned, the operator
   * is opting in to peer-consensus auto-discovery. The TCP port hint carried here is the
   * locally-bound IPv6 RLPx port; it is consumed by {@link #applyAutoDiscoveredIpv6Host(String,
   * int)} the first time peers reach consensus on an external IPv6 address. The hint holds no host,
   * so it cannot leak into the broadcast ENR.
   *
   * @param primary the primary network endpoint (IPv4 or IPv6)
   * @param ipv6 an optional pre-pinned secondary IPv6 endpoint for dual-stack operation
   * @param ipv6AutoDiscoveryTcpPort optional locally-bound IPv6 TCP port used to construct the
   *     secondary endpoint if and when peer-consensus auto-discovery succeeds
   */
  public void initializeLocalNode(
      final HostEndpoint primary,
      final Optional<HostEndpoint> ipv6,
      final Optional<Integer> ipv6AutoDiscoveryTcpPort) {

    // Only resolve through NAT if primary is IPv4.
    // Current NAT services (UPnP, NAT-PMP) only support IPv4.
    final String resolvedHost;
    if (NetworkUtility.isIpV4Address(primary.host())) {
      resolvedHost = natService.queryExternalIPAddress(primary.host());
    } else {
      resolvedHost = primary.host();
    }

    this.primaryEndpoint =
        new HostEndpoint(resolvedHost, primary.discoveryPort(), primary.tcpPort());

    // IPv6 endpoint is used as-is. Current NAT services only support IPv4.
    this.ipv6Endpoint = ipv6;
    this.ipv6AutoDiscoveryTcpPort = ipv6AutoDiscoveryTcpPort;

    final DiscoveryPeerV4 self =
        DiscoveryPeerV4.fromEnode(
            EnodeURLImpl.builder()
                .nodeId(nodeId)
                .ipAddress(resolvedHost)
                .listeningPort(primary.tcpPort())
                .discoveryPort(primary.discoveryPort())
                .build());

    this.localNode = Optional.of(self);
    updateNodeRecord();
  }

  /**
   * Returns whether the primary endpoint advertises an IPv6 address.
   *
   * <p>Used by the DiscV5 new-address handler to short-circuit when the primary is operator-pinned
   * IPv6 (since peer-consensus auto-discovery would otherwise overwrite an explicit choice).
   *
   * @return {@code true} if the primary endpoint is IPv6 and has been initialized
   */
  public boolean isPrimaryEndpointIpv6() {
    return primaryEndpoint != null && !primaryEndpoint.isIpv4();
  }

  /**
   * Updates the stored discovery endpoints with the actual OS-assigned ports after an ephemeral
   * (port 0) bind, then writes the ENR to disk once all configured endpoints are resolved.
   *
   * <p>Each argument carries the resolved port from the corresponding ENR UDP field ({@code udp}
   * for IPv4, {@code udp6} for IPv6). The argument that maps to the <em>primary</em> endpoint
   * depends on the primary's address family: for an IPv4 primary the {@code udp} port is used; for
   * an IPv6-only primary the {@code udp6} port is used. The {@code udp6} port is additionally used
   * for the dual-stack secondary when the primary is IPv4.
   *
   * <p>Each argument is only applied when present and only if the currently stored port is 0. The
   * ENR write is performed atomically under the same lock as the endpoint update, so concurrent
   * callbacks from dual-stack UDP servers cannot interleave a write between an endpoint update.
   *
   * @param resolvedUdpPort the OS-assigned port for the {@code udp} ENR field, or empty if
   *     unchanged
   * @param resolvedUdp6Port the OS-assigned port for the {@code udp6} ENR field, or empty if
   *     unchanged
   */
  public void onDiscoveryPortResolved(
      final Optional<Integer> resolvedUdpPort, final Optional<Integer> resolvedUdp6Port) {
    lock.lock();
    try {
      // Route the resolved port to the primary endpoint based on its address family.
      // In single-stack IPv6 mode the primary is IPv6, so its port arrives in
      // resolvedUdp6Port — not resolvedUdpPort as the field name might suggest.
      final Optional<Integer> resolvedPrimaryPort =
          primaryEndpoint.isIpv4() ? resolvedUdpPort : resolvedUdp6Port;
      updatePrimaryPortIfEphemeral(resolvedPrimaryPort);
      updateIpv6PortIfEphemeral(resolvedUdp6Port);

      // Write the ENR only when every configured UDP endpoint has a real (non-zero) port.
      // In dual-stack mode the two UDP servers fire their callbacks concurrently; deferring
      // until both are resolved ensures the seq counter increments exactly once.
      if (allEndpointsResolved()) {
        doUpdateNodeRecord();
      }
    } finally {
      lock.unlock();
    }
  }

  /** Updates the primary endpoint's discovery port if it is currently ephemeral (0). */
  private void updatePrimaryPortIfEphemeral(final Optional<Integer> resolvedPort) {
    if (resolvedPort.isPresent() && primaryEndpoint.discoveryPort() == 0) {
      primaryEndpoint = primaryEndpoint.withDiscoveryPort(resolvedPort.get());
    }
  }

  /**
   * Updates the dual-stack secondary (IPv6) endpoint's discovery port if it is currently ephemeral.
   * Only applies when the primary is IPv4 (dual-stack mode); the secondary is always IPv6.
   */
  private void updateIpv6PortIfEphemeral(final Optional<Integer> resolvedUdp6Port) {
    if (primaryEndpoint.isIpv4()
        && resolvedUdp6Port.isPresent()
        && ipv6Endpoint.map(ep -> ep.discoveryPort() == 0).orElse(false)) {
      ipv6Endpoint = ipv6Endpoint.map(ep -> ep.withDiscoveryPort(resolvedUdp6Port.get()));
    }
  }

  /** Returns {@code true} when every configured UDP endpoint has a real (non-zero) port. */
  private boolean allEndpointsResolved() {
    final boolean primaryResolved = primaryEndpoint.discoveryPort() != 0;
    final boolean ipv6Resolved = ipv6Endpoint.map(ep -> ep.discoveryPort() != 0).orElse(true);
    return primaryResolved && ipv6Resolved;
  }

  /**
   * Ensures the local {@link NodeRecord} is up to date.
   *
   * <p>If a persisted ENR exists and all relevant fields match the current configuration (node ID,
   * IP address, ports, and fork ID), it is reused as-is.
   *
   * <p>If any field differs, a new ENR is created with an incremented sequence number, signed using
   * the local {@link NodeKey}, and persisted to disk.
   *
   * @throws IllegalStateException if the local node has not been initialized
   */
  public void updateNodeRecord() {
    lock.lock();
    try {
      doUpdateNodeRecord();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Applies an IPv6 host auto-discovered via DiscV5 peer consensus to the local ENR.
   *
   * <p>Constructs a new IPv6 {@link HostEndpoint} from the peer-observed host and UDP port plus the
   * locally-bound IPv6 TCP port hint registered at {@link #initializeLocalNode(HostEndpoint,
   * Optional, Optional)}. Writes a new ENR with an incremented {@code seq} and returns it.
   *
   * <p><b>Fire-once semantics.</b> Ethereum nodes are expected to keep a stable advertised address
   * for the lifetime of a session. If {@code ipv6Endpoint} is already set — either because the
   * operator pinned {@code --p2p-host-ipv6} or because a prior auto-discovery write has already
   * happened this session — this method is a no-op and returns {@link Optional#empty()}. The
   * handler enforces the same principle by short-circuiting on operator pin upstream; this method
   * provides the second guarantee against mid-session address churn.
   *
   * <p>If no IPv6 TCP port hint was registered (i.e. dual-stack bind is not active, or RLPx did not
   * bind an IPv6 socket), this method is also a no-op.
   *
   * @param host the peer-observed IPv6 host string (must already have been validated by the caller
   *     as an advertisable IPv6 unicast address — global unicast or ULA, per {@link
   *     org.hyperledger.besu.ethereum.p2p.discovery.discv5.IpV6NewAddressHandler#isAdvertisableIpv6Unicast})
   * @param udpPort the peer-observed UDP source port — used for the ENR {@code udp6} field
   * @return the updated {@link NodeRecord} if the write succeeded, otherwise empty
   */
  public Optional<NodeRecord> applyAutoDiscoveredIpv6Host(final String host, final int udpPort) {
    lock.lock();
    try {
      if (udpPort <= 0 || udpPort > 65535) {
        LOG.debug(
            "Ignoring auto-discovered IPv6 host {}: invalid peer-observed UDP port {}",
            host,
            udpPort);
        return Optional.empty();
      }
      if (ipv6Endpoint.isPresent()) {
        LOG.debug("Ignoring auto-discovered IPv6 host {}: ipv6Endpoint already set", host);
        return Optional.empty();
      }
      if (ipv6AutoDiscoveryTcpPort.isEmpty()) {
        LOG.debug("Ignoring auto-discovered IPv6 host {}: no IPv6 TCP port hint registered", host);
        return Optional.empty();
      }
      final int tcpPort = ipv6AutoDiscoveryTcpPort.get();
      ipv6Endpoint = Optional.of(new HostEndpoint(host, udpPort, tcpPort));
      // Clear the hint so the consumed state is explicit; fire-once is already enforced by the
      // ipv6Endpoint.isPresent() guard above, but this defends against future refactors that
      // might separate the two pieces of state.
      ipv6AutoDiscoveryTcpPort = Optional.empty();
      doUpdateNodeRecord();
      LOG.info(
          "Auto-discovered IPv6 endpoint via DiscV5 peer consensus: ip6={}, udp6={}, tcp6={}"
              + " (TCP port reflects local bind; pin --p2p-host-ipv6 if NAT translates TCP differently)",
          host,
          udpPort,
          tcpPort);
      return localNode.flatMap(DiscoveryPeerV4::getNodeRecord);
    } finally {
      lock.unlock();
    }
  }

  /** Writes the ENR to disk. Must be called with {@link #lock} held. */
  private void doUpdateNodeRecord() {
    final NodeRecordFactory factory = NodeRecordFactory.DEFAULT;

    final Optional<NodeRecord> existingRecord =
        variablesStorage.getLocalEnrSeqno().map(factory::fromBytes);

    final Bytes ipAddressBytes =
        Bytes.of(InetAddresses.forString(primaryEndpoint.host()).getAddress());

    final int discoveryPort = primaryEndpoint.discoveryPort();
    final int listeningPort = primaryEndpoint.tcpPort();
    final List<Bytes> forkId = forkIdSupplier.get();

    final Optional<Bytes> ipv6AddressBytes =
        ipv6Endpoint.map(ep -> Bytes.of(InetAddresses.forString(ep.host()).getAddress()));

    // Reuse the existing ENR if all relevant fields are unchanged.
    final NodeRecord nodeRecord =
        existingRecord
            .filter(
                record ->
                    nodeId.equals(record.get(EnrField.PKEY_SECP256K1))
                        && (primaryEndpoint.isIpv4()
                            ? primaryIpv4AddressMatches(
                                record, ipAddressBytes, discoveryPort, listeningPort)
                            : primaryIpv6AddressMatches(
                                record, ipAddressBytes, discoveryPort, listeningPort))
                        && forkId.equals(record.get(FORK_ID_ENR_FIELD))
                        && (!primaryEndpoint.isIpv4() || ipv6FieldsMatch(record, ipv6AddressBytes)))
            // Otherwise, create a new ENR with an incremented sequence number,
            // sign it with the local node key, and persist it to disk.
            .orElseGet(
                () ->
                    createAndPersistNodeRecord(
                        factory,
                        existingRecord,
                        ipAddressBytes,
                        discoveryPort,
                        listeningPort,
                        forkId));

    localNode.get().setNodeRecord(nodeRecord);
  }

  private boolean primaryIpv4AddressMatches(
      final NodeRecord record,
      final Bytes ipAddressBytes,
      final int discoveryPort,
      final int listeningPort) {
    return ipAddressBytes.equals(record.get(EnrField.IP_V4))
        && Integer.valueOf(discoveryPort).equals(record.get(EnrField.UDP))
        && Integer.valueOf(listeningPort).equals(record.get(EnrField.TCP));
  }

  private boolean primaryIpv6AddressMatches(
      final NodeRecord record,
      final Bytes ipAddressBytes,
      final int discoveryPort,
      final int listeningPort) {
    return ipAddressBytes.equals(record.get(EnrField.IP_V6))
        && Integer.valueOf(discoveryPort).equals(record.get(EnrField.UDP_V6))
        && Integer.valueOf(listeningPort).equals(record.get(EnrField.TCP_V6));
  }

  /**
   * Checks whether the IPv6 dual-stack fields in an existing ENR match the current configuration.
   *
   * <p>Only called when the primary address is IPv4. When {@code ipv6AddressBytes} is empty, the
   * ENR must not contain an {@code ip6} field (no dual-stack). When present, all three IPv6 fields
   * ({@code ip6}, {@code udp6}, {@code tcp6}) must match the current {@link #ipv6Endpoint}.
   */
  private boolean ipv6FieldsMatch(final NodeRecord record, final Optional<Bytes> ipv6AddressBytes) {
    if (ipv6AddressBytes.isEmpty()) {
      // No separate IPv6 endpoint configured; IP_V6 must be absent from the ENR.
      return record.get(EnrField.IP_V6) == null;
    }

    final HostEndpoint ipv6 =
        ipv6Endpoint.orElseThrow(
            () ->
                new IllegalStateException(
                    "ipv6Endpoint is unexpectedly absent during IPv6 ENR field validation"
                        + " while primary address is IPv4 (dual-stack)"));
    return ipv6AddressBytes.get().equals(record.get(EnrField.IP_V6))
        && Integer.valueOf(ipv6.discoveryPort()).equals(record.get(EnrField.UDP_V6))
        && Integer.valueOf(ipv6.tcpPort()).equals(record.get(EnrField.TCP_V6));
  }

  private NodeRecord createAndPersistNodeRecord(
      final NodeRecordFactory factory,
      final Optional<NodeRecord> existingRecord,
      final Bytes ipAddressBytes,
      final int discoveryPort,
      final int listeningPort,
      final List<Bytes> forkId) {

    final UInt64 sequence = existingRecord.map(NodeRecord::getSeq).orElse(UInt64.ZERO).add(1);

    final List<EnrField> fields = new ArrayList<>();
    fields.add(new EnrField(EnrField.ID, IdentitySchema.V4));
    fields.add(
        new EnrField(
            SIGNATURE_ALGORITHM.getCurveName(),
            SIGNATURE_ALGORITHM.compressPublicKey(SIGNATURE_ALGORITHM.createPublicKey(nodeId))));
    fields.add(new EnrField(FORK_ID_ENR_FIELD, Collections.singletonList(forkId)));

    if (primaryEndpoint.isIpv4()) {
      fields.add(new EnrField(EnrField.IP_V4, ipAddressBytes));
      fields.add(new EnrField(EnrField.TCP, listeningPort));
      fields.add(new EnrField(EnrField.UDP, discoveryPort));

      // Add separate IPv6 fields only for dual-stack (primary is IPv4 + secondary IPv6)
      ipv6Endpoint.ifPresent(
          ipv6 -> {
            fields.add(
                new EnrField(
                    EnrField.IP_V6, Bytes.of(InetAddresses.forString(ipv6.host()).getAddress())));
            fields.add(new EnrField(EnrField.TCP_V6, ipv6.tcpPort()));
            fields.add(new EnrField(EnrField.UDP_V6, ipv6.discoveryPort()));
          });
    } else {
      fields.add(new EnrField(EnrField.IP_V6, ipAddressBytes));
      fields.add(new EnrField(EnrField.TCP_V6, listeningPort));
      fields.add(new EnrField(EnrField.UDP_V6, discoveryPort));
    }

    final NodeRecord record = factory.createFromValues(sequence, fields);

    record.setSignature(
        nodeKey.sign(Hash.keccak256(record.serializeNoSignature())).encodedBytes().slice(0, 64));

    // Use DEBUG for interim writes where ephemeral ports are not yet resolved.
    if (allEndpointsResolved()) {
      LOG.info("Writing node record to disk. {}", record);
    } else {
      LOG.debug("Writing interim node record to disk (ephemeral ports pending). {}", record);
    }

    final var updater = variablesStorage.updater();
    updater.setLocalEnrSeqno(record.serialize());
    updater.commit();

    return record;
  }
}
