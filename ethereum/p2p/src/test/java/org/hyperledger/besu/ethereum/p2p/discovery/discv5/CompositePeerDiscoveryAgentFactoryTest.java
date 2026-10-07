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
package org.hyperledger.besu.ethereum.p2p.discovery.discv5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.forkid.ForkId;
import org.hyperledger.besu.ethereum.forkid.ForkIdManager;
import org.hyperledger.besu.ethereum.p2p.config.DiscoveryConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.DiscoveryMode;
import org.hyperledger.besu.ethereum.p2p.config.ImmutableNetworkingConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.NetworkingConfiguration;
import org.hyperledger.besu.ethereum.p2p.discovery.CompositePeerDiscoveryAgent;
import org.hyperledger.besu.ethereum.p2p.discovery.PeerDiscoveryAgent;
import org.hyperledger.besu.ethereum.p2p.discovery.transport.SharedDiscoveryTransport;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissions;
import org.hyperledger.besu.ethereum.p2p.rlpx.RlpxAgent;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.nat.NatService;

import java.lang.reflect.Field;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
import org.ethereum.beacon.discovery.schema.EnrField;
import org.ethereum.beacon.discovery.schema.NodeRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.commons.util.ReflectionUtils;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Covers {@link CompositePeerDiscoveryAgentFactory}'s {@link DiscoveryMode} resolution, masking-key
 * derivation, and dual-stack bind-address wiring.
 *
 * <p>Deliberately does NOT cover the V5-unsupported-curve fallback (a non-secp256k1 key forcing
 * V4-only even when {@code --discovery-mode=V5} is requested): {@link
 * org.hyperledger.besu.ethereum.p2p.discovery.NodeRecordManager} caches {@code
 * SignatureAlgorithmFactory.getInstance()} in a {@code static final} field at class-load time, so
 * switching the JVM-wide curve mid-test-run poisons every later test in the same worker that
 * constructs a {@code NodeRecordManager} with a secp256k1 key - not safe to test in-process.
 */
@ExtendWith(MockitoExtension.class)
class CompositePeerDiscoveryAgentFactoryTest {

  private NodeKey nodeKey;
  private ForkIdManager forkIdManager;

  @Mock private RlpxAgent rlpxAgent;

  @BeforeEach
  void setUp() {
    nodeKey = NodeKeyUtils.generate();
    forkIdManager = mock(ForkIdManager.class);
    final ForkId forkId = new ForkId(Bytes.EMPTY, Bytes.EMPTY);
    lenient().when(forkIdManager.getForkIdForChainHead()).thenReturn(forkId);
  }

  @Test
  void bothMode_buildsBothSubAgents() {
    final CompositePeerDiscoveryAgent composite = create(DiscoveryMode.BOTH, Optional.empty());

    assertThat(agentV4(composite)).isNotNull();
    assertThat(agentV5(composite)).isNotNull();
  }

  @Test
  void v4Mode_buildsV4Only() {
    final CompositePeerDiscoveryAgent composite = create(DiscoveryMode.V4, Optional.empty());

    assertThat(agentV4(composite)).isNotNull();
    assertThat(agentV5(composite)).isNull();
  }

  @Test
  void v5Mode_withSecp256k1Key_buildsV5Only() {
    final CompositePeerDiscoveryAgent composite = create(DiscoveryMode.V5, Optional.empty());

    assertThat(agentV4(composite)).isNull();
    assertThat(agentV5(composite)).isNotNull();
  }

  @Test
  void maskingKey_isDeterministicAndSixteenBytesForTheSameNodeKey() {
    final byte[] key1 = maskingKeyOf(transportOf(create(DiscoveryMode.BOTH, Optional.empty())));
    final byte[] key2 = maskingKeyOf(transportOf(create(DiscoveryMode.BOTH, Optional.empty())));

    assertThat(key1).hasSize(16);
    assertThat(key1).isEqualTo(key2);
  }

  @Test
  void dualStack_bindsIpv6WhenConfigured() {
    final CompositePeerDiscoveryAgent composite = create(DiscoveryMode.BOTH, Optional.of("::1"));

    final SharedDiscoveryTransport transport = transportOf(composite);
    assertThat((Optional<?>) getField(transport, SharedDiscoveryTransport.class, "ipv6BindAddress"))
        .isPresent();
  }

  @Test
  void singleStack_noIpv6BindAddressByDefault() {
    final CompositePeerDiscoveryAgent composite = create(DiscoveryMode.BOTH, Optional.empty());

    final SharedDiscoveryTransport transport = transportOf(composite);
    assertThat((Optional<?>) getField(transport, SharedDiscoveryTransport.class, "ipv6BindAddress"))
        .isEmpty();
  }

  @Test
  void v5Only_withIpv6OnlyPrimaryInterface_bindsIpv6WithNoIpv4Address() {
    // --p2p-interface=:: with no --p2p-interface-ipv6 (no dual-stack): a single-stack,
    // IPv6-only node. Valid for V5-only, since DiscV5 has no IPv4-socket requirement.
    final CompositePeerDiscoveryAgent composite = create(DiscoveryMode.V5, "::1", Optional.empty());

    assertThat(agentV4(composite)).isNull();
    assertThat(agentV5(composite)).isNotNull();

    final SharedDiscoveryTransport transport = transportOf(composite);
    assertThat((Optional<?>) getField(transport, SharedDiscoveryTransport.class, "ipv4BindAddress"))
        .isEmpty();
  }

  @Test
  void bothMode_ephemeralPort_startResultsInLocalEnrWithRealResolvedPort() throws Exception {
    // --p2p-port=0 (ephemeral): whichever of agentV4/agentV5 initializes the shared
    // NodeRecordManager second must not clobber the first's already-resolved port.
    final CompositePeerDiscoveryAgent composite = create(DiscoveryMode.BOTH, Optional.empty());

    final int resolvedPort = composite.start(30303).get(5, TimeUnit.SECONDS);
    try {
      assertThat(resolvedPort).isGreaterThan(0);

      final NodeRecord record =
          composite
              .getLocalNodeRecord()
              .orElseThrow(() -> new IllegalStateException("Local node record not initialized"));
      assertThat((Integer) record.get(EnrField.UDP)).isNotEqualTo(0);
    } finally {
      composite.stop().get(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void bothMode_withIpv6OnlyPrimaryInterface_bindsIpv6WithNoIpv4Address() {
    // DiscV4 under BOTH mode on IPv6-only is a legitimate config, matching upstream's
    // owned-channel NettyTransport - it just can't reach an IPv4 recipient (see
    // NettyTransportTest).
    final CompositePeerDiscoveryAgent composite =
        create(DiscoveryMode.BOTH, "::1", Optional.empty());

    assertThat(agentV4(composite)).isNotNull();
    assertThat(agentV5(composite)).isNotNull();

    final SharedDiscoveryTransport transport = transportOf(composite);
    assertThat((Optional<?>) getField(transport, SharedDiscoveryTransport.class, "ipv4BindAddress"))
        .isEmpty();
  }

  private CompositePeerDiscoveryAgent create(
      final DiscoveryMode mode, final Optional<String> bindHostIpv6) {
    return create(mode, "127.0.0.1", bindHostIpv6);
  }

  private CompositePeerDiscoveryAgent create(
      final DiscoveryMode mode, final String bindHost, final Optional<String> bindHostIpv6) {
    final DiscoveryConfiguration discoveryConfiguration =
        DiscoveryConfiguration.create()
            .setEnabled(true)
            .setAdvertisedHost("127.0.0.1")
            .setBindHost(bindHost)
            .setBindPort(0)
            .setDiscoveryMode(mode);
    bindHostIpv6.ifPresent(
        host -> {
          discoveryConfiguration.setBindHostIpv6(Optional.of(host));
          discoveryConfiguration.setBindPortIpv6(0);
        });
    final NetworkingConfiguration config =
        ImmutableNetworkingConfiguration.builder()
            .discoveryConfiguration(discoveryConfiguration)
            .build();

    final CompositePeerDiscoveryAgentFactory factory =
        new CompositePeerDiscoveryAgentFactory(
            nodeKey,
            config,
            PeerPermissions.noop(),
            new NatService(Optional.empty()),
            new NoOpMetricsSystem(),
            new InMemoryKeyValueStorageProvider(),
            forkIdManager);

    return (CompositePeerDiscoveryAgent) factory.create(rlpxAgent);
  }

  private static PeerDiscoveryAgent agentV4(final CompositePeerDiscoveryAgent composite) {
    return (PeerDiscoveryAgent) getField(composite, CompositePeerDiscoveryAgent.class, "agentV4");
  }

  private static PeerDiscoveryAgent agentV5(final CompositePeerDiscoveryAgent composite) {
    return (PeerDiscoveryAgent) getField(composite, CompositePeerDiscoveryAgent.class, "agentV5");
  }

  private static SharedDiscoveryTransport transportOf(final CompositePeerDiscoveryAgent composite) {
    return (SharedDiscoveryTransport)
        getField(composite, CompositePeerDiscoveryAgent.class, "transport");
  }

  private static byte[] maskingKeyOf(final SharedDiscoveryTransport transport) {
    return (byte[]) getField(transport, SharedDiscoveryTransport.class, "maskingKey");
  }

  private static Object getField(
      final Object target, final Class<?> declaringClass, final String name) {
    final Field field =
        ReflectionUtils.findFields(
                declaringClass,
                f -> f.getName().equals(name),
                ReflectionUtils.HierarchyTraversalMode.TOP_DOWN)
            .getFirst();
    field.setAccessible(true);
    try {
      return field.get(target);
    } catch (final IllegalAccessException e) {
      throw new RuntimeException(e);
    } finally {
      field.setAccessible(false);
    }
  }
}
