/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.p2p.permissions;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.p2p.peers.DefaultPeer;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissions.Action;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PeerPermissionsDenylistTest {

  private final Peer localNode = createPeer();

  public PeerPermissionsDenylistTest() throws UnknownHostException {}

  private int ipModifier = 1;

  @BeforeEach
  public void beforeTest() {
    ipModifier = 1;
  }

  @Test
  public void add_peer() throws UnknownHostException {
    PeerPermissionsDenylist denylist = PeerPermissionsDenylist.create();
    Peer peer = createPeer();

    final AtomicInteger callbackCount = new AtomicInteger(0);
    denylist.subscribeUpdate(
        (restricted, affectedPeers) -> {
          callbackCount.incrementAndGet();
          assertThat(restricted).isTrue();
          assertThat(affectedPeers).contains(Collections.singletonList(peer));
        });

    assertThat(callbackCount).hasValue(0);

    denylist.add(peer);
    assertThat(callbackCount).hasValue(1);
  }

  @Test
  public void remove_peer() throws UnknownHostException {
    PeerPermissionsDenylist denylist = PeerPermissionsDenylist.create();
    Peer peer = createPeer();
    denylist.add(peer);

    final AtomicInteger callbackCount = new AtomicInteger(0);
    denylist.subscribeUpdate(
        (restricted, affectedPeers) -> {
          callbackCount.incrementAndGet();
          assertThat(restricted).isFalse();
          assertThat(affectedPeers).contains(Collections.singletonList(peer));
        });

    assertThat(callbackCount).hasValue(0);

    denylist.remove(peer);
    assertThat(callbackCount).hasValue(1);
  }

  @Test
  public void add_id() throws UnknownHostException {
    PeerPermissionsDenylist denylist = PeerPermissionsDenylist.create();
    Peer peer = createPeer();

    final AtomicInteger callbackCount = new AtomicInteger(0);
    denylist.subscribeUpdate(
        (restricted, affectedPeers) -> {
          callbackCount.incrementAndGet();
          assertThat(restricted).isTrue();
          assertThat(affectedPeers).isEmpty();
        });

    assertThat(callbackCount).hasValue(0);

    denylist.add(peer.getId());
    assertThat(callbackCount).hasValue(1);
  }

  @Test
  public void remove_id() throws UnknownHostException {
    PeerPermissionsDenylist denylist = PeerPermissionsDenylist.create();
    Peer peer = createPeer();
    denylist.add(peer);

    final AtomicInteger callbackCount = new AtomicInteger(0);
    denylist.subscribeUpdate(
        (restricted, affectedPeers) -> {
          callbackCount.incrementAndGet();
          assertThat(restricted).isFalse();
          assertThat(affectedPeers).isEmpty();
        });

    assertThat(callbackCount).hasValue(0);

    denylist.remove(peer.getId());
    assertThat(callbackCount).hasValue(1);
  }

  @Test
  public void trackedPeerIsNotPermitted() throws UnknownHostException {
    PeerPermissionsDenylist denylist = PeerPermissionsDenylist.create();

    Peer peer = createPeer();
    checkPermissions(denylist, peer, true);

    denylist.add(peer);
    checkPermissions(denylist, peer, false);

    denylist.remove(peer);
    checkPermissions(denylist, peer, true);
  }

  @Test
  public void selfPeerIsNotPermitted() {
    PeerPermissionsDenylist denylist = PeerPermissionsDenylist.create();

    checkPermissions(denylist, localNode, false);
  }

  @Test
  public void subscribeUpdate() throws UnknownHostException {
    PeerPermissionsDenylist denylist = PeerPermissionsDenylist.create();
    final AtomicInteger callbackCount = new AtomicInteger(0);
    final AtomicInteger restrictedCallbackCount = new AtomicInteger(0);
    Peer peer = createPeer();

    denylist.subscribeUpdate(
        (permissionsRestricted, affectedPeers) -> {
          callbackCount.incrementAndGet();
          if (permissionsRestricted) {
            restrictedCallbackCount.incrementAndGet();
          }
        });

    checkPermissions(denylist, peer, true);
    assertThat(callbackCount).hasValue(0);
    assertThat(restrictedCallbackCount).hasValue(0);

    denylist.add(peer);
    assertThat(callbackCount).hasValue(1);
    assertThat(restrictedCallbackCount).hasValue(1);

    denylist.add(peer);
    assertThat(callbackCount).hasValue(1);
    assertThat(restrictedCallbackCount).hasValue(1);

    denylist.remove(peer);
    assertThat(callbackCount).hasValue(2);
    assertThat(restrictedCallbackCount).hasValue(1);

    denylist.remove(peer);
    assertThat(callbackCount).hasValue(2);
    assertThat(restrictedCallbackCount).hasValue(1);

    denylist.add(peer);
    assertThat(callbackCount).hasValue(3);
    assertThat(restrictedCallbackCount).hasValue(2);
  }

  @Test
  public void createWithLimitedCapacity() throws UnknownHostException {
    final PeerPermissionsDenylist denylist = PeerPermissionsDenylist.create(2);
    Peer peerA = createPeer();
    Peer peerB = createPeer();
    Peer peerC = createPeer();

    // All peers are initially permitted
    checkPermissions(denylist, peerA, true);
    checkPermissions(denylist, peerB, true);
    checkPermissions(denylist, peerC, true);

    // Add peerA
    denylist.add(peerA);
    checkPermissions(denylist, peerA, false);
    checkPermissions(denylist, peerB, true);
    checkPermissions(denylist, peerC, true);

    // Add peerB
    denylist.add(peerB);
    checkPermissions(denylist, peerA, false);
    checkPermissions(denylist, peerB, false);
    checkPermissions(denylist, peerC, true);

    // Add peerC
    // Limit is exceeded and peerA should drop off of the list and be allowed
    denylist.add(peerC);
    checkPermissions(denylist, peerA, true);
    checkPermissions(denylist, peerB, false);
    checkPermissions(denylist, peerC, false);
  }

  private void checkPermissions(
      final PeerPermissionsDenylist denylist, final Peer remotePeer, final boolean expectedResult) {
    for (Action action : Action.values()) {
      assertThat(denylist.isPermitted(localNode, remotePeer, action)).isEqualTo(expectedResult);
    }
  }

  @Test
  public void createWithUnlimitedCapacity() {
    final PeerPermissionsDenylist denylist = PeerPermissionsDenylist.create();
    final int peerCount = 200;
    final List<Peer> peers =
        Stream.generate(
                () -> {
                  try {
                    return createPeer();
                  } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                  }
                })
            .limit(peerCount)
            .collect(Collectors.toList());

    peers.forEach(p -> checkPermissions(denylist, p, true));
    peers.forEach(denylist::add);
    peers.forEach(p -> checkPermissions(denylist, p, false));

    peers.forEach(denylist::remove);
    peers.forEach(p -> checkPermissions(denylist, p, true));
  }

  private Peer createPeer() throws UnknownHostException {
    return DefaultPeer.fromEnodeURL(
        EnodeURLImpl.builder()
            .nodeId(Peer.randomId())
            .ipAddress(InetAddress.getByName("10.0.0." + ipModifier))
            .discoveryAndListeningPorts(EnodeURLImpl.DEFAULT_LISTENING_PORT)
            .build());
  }
}
