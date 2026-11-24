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
package org.hyperledger.besu.ethereum.p2p.rlpx.connections.netty;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.p2p.rlpx.connections.netty.PendingInboundConnectionLimiter.Slot;

import java.util.List;
import java.util.Optional;

import com.google.common.net.InetAddresses;
import org.junit.jupiter.api.Test;

public class PendingInboundConnectionLimiterTest {

  @Test
  public void refusesConnectionsOverThePendingLimit() {
    final PendingInboundConnectionLimiter limiter = limiter(2);

    final Optional<Slot> first = acquire(limiter, "192.0.2.1");
    assertThat(first).isPresent();
    assertThat(acquire(limiter, "192.0.2.2")).isPresent();
    assertThat(acquire(limiter, "192.0.2.3")).isEmpty();

    first.orElseThrow().release();

    assertThat(acquire(limiter, "192.0.2.3")).isPresent();
  }

  @Test
  public void refusesASecondPendingConnectionFromTheSameAddress() {
    final PendingInboundConnectionLimiter limiter = limiter(10);

    final Optional<Slot> first = acquire(limiter, "203.0.113.7");
    assertThat(first).isPresent();
    assertThat(acquire(limiter, "203.0.113.7")).isEmpty();
    assertThat(acquire(limiter, "203.0.113.8")).isPresent();

    first.orElseThrow().release();

    assertThat(acquire(limiter, "203.0.113.7")).isPresent();
  }

  @Test
  public void groupsIpv6SourcesBySlash64() {
    final PendingInboundConnectionLimiter limiter = limiter(10);

    assertThat(acquire(limiter, "2001:db8::1")).isPresent();
    assertThat(acquire(limiter, "2001:db8::2")).isEmpty();
    assertThat(acquire(limiter, "2001:db8:0:1::1")).isPresent();
  }

  @Test
  public void exemptsLoopbackPrivateAndLinkLocalSources() {
    final PendingInboundConnectionLimiter limiter = limiter(100);

    final List<String> localAddresses =
        List.of(
            "127.0.0.1",
            "10.1.2.3",
            "172.16.5.4",
            "192.168.1.1",
            "169.254.1.1",
            "::1",
            "fe80::1",
            "fd00::1");
    for (final String address : localAddresses) {
      assertThat(acquire(limiter, address)).as(address + " first").isPresent();
      assertThat(acquire(limiter, address)).as(address + " second").isPresent();
    }
  }

  @Test
  public void refusalAtThePendingLimitDoesNotHoldTheSource() {
    final PendingInboundConnectionLimiter limiter = limiter(1);

    final Optional<Slot> slot = acquire(limiter, "192.0.2.1");
    assertThat(slot).isPresent();
    assertThat(acquire(limiter, "192.0.2.2")).isEmpty();

    slot.orElseThrow().release();

    assertThat(acquire(limiter, "192.0.2.2")).isPresent();
  }

  @Test
  public void releasingASlotTwiceFreesItOnce() {
    final PendingInboundConnectionLimiter limiter = limiter(1);

    final Slot slot = acquire(limiter, "192.0.2.1").orElseThrow();
    slot.release();
    slot.release();

    assertThat(limiter.pendingConnections()).isZero();
    assertThat(acquire(limiter, "192.0.2.2")).isPresent();
    assertThat(acquire(limiter, "192.0.2.3")).isEmpty();
  }

  @Test
  public void unknownAddressCountsOnlyTowardThePendingLimit() {
    final PendingInboundConnectionLimiter limiter = limiter(2);

    assertThat(limiter.tryAcquire(null)).isPresent();
    assertThat(limiter.tryAcquire(null)).isPresent();
    assertThat(limiter.tryAcquire(null)).isEmpty();
  }

  private static PendingInboundConnectionLimiter limiter(final int maxPendingConnections) {
    return new PendingInboundConnectionLimiter(maxPendingConnections);
  }

  private static Optional<Slot> acquire(
      final PendingInboundConnectionLimiter limiter, final String address) {
    return limiter.tryAcquire(InetAddresses.forString(address));
  }
}
