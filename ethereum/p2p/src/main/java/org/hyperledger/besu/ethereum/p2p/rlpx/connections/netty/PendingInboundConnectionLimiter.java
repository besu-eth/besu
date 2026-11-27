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

import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bounds the inbound connections that have been accepted but are not yet through the RLPx handshake
 * (ECIES auth and HELLO). Such connections are invisible to {@code --max-peers} and to the
 * pre-STATUS cap in {@code EthPeers}, so without a bound an idle-connection flood holds one file
 * descriptor per socket until the handshake timeout.
 *
 * <p>At most {@code maxPendingConnections} connections are pending at once, and at most one per
 * source (an IPv4 address or an IPv6 /64). Loopback, private and link-local sources are exempt from
 * the per-source limit, so nodes on the same LAN or machine can still connect to each other
 * concurrently.
 */
final class PendingInboundConnectionLimiter {
  private static final Logger LOG = LoggerFactory.getLogger(PendingInboundConnectionLimiter.class);

  private final int maxPendingConnections;
  private final AtomicInteger pendingConnections = new AtomicInteger();
  private final Set<Bytes> pendingSources = ConcurrentHashMap.newKeySet();

  PendingInboundConnectionLimiter(final int maxPendingConnections) {
    this.maxPendingConnections = maxPendingConnections;
  }

  /**
   * Tries to reserve a pending-connection slot for a newly accepted inbound connection.
   *
   * @param remoteAddress the remote address of the connection, or {@code null} if unknown, in which
   *     case the connection counts only toward the global limit
   * @return the reserved slot, to be released once the handshake ends or the channel closes, or
   *     empty if a limit was reached and the connection must be closed
   */
  Optional<Slot> tryAcquire(final InetAddress remoteAddress) {
    final Bytes source = sourceKey(remoteAddress);
    if (source != null && !pendingSources.add(source)) {
      LOG.trace(
          "Refusing inbound RLPx connection from {}: a handshake from this source is already pending",
          remoteAddress);
      return Optional.empty();
    }
    if (pendingConnections.getAndUpdate(n -> n < maxPendingConnections ? n + 1 : n)
        >= maxPendingConnections) {
      if (source != null) {
        pendingSources.remove(source);
      }
      LOG.trace(
          "Refusing inbound RLPx connection from {}: {} handshakes already pending",
          remoteAddress,
          maxPendingConnections);
      return Optional.empty();
    }
    return Optional.of(new Slot(source));
  }

  int pendingConnections() {
    return pendingConnections.get();
  }

  /**
   * The per-source key: the IPv4 address, or the IPv6 /64 (otherwise a host could switch to another
   * address in its own /64 for free). The JDK never returns IPv4-mapped IPv6 addresses, so IPv4
   * clients of a dual-stack socket get IPv4 keys; keys are 4 or 8 bytes long, so the two families
   * never collide.
   *
   * @return the key, or {@code null} if the source is not limited per source
   */
  private static Bytes sourceKey(final InetAddress address) {
    if (address == null || isLocal(address)) {
      return null;
    }
    final byte[] raw = address.getAddress();
    return address instanceof Inet6Address ? Bytes.wrap(raw, 0, 8) : Bytes.wrap(raw);
  }

  /** Loopback, private (RFC 1918 and ULA fc00::/7) and link-local addresses. */
  private static boolean isLocal(final InetAddress address) {
    return address.isLoopbackAddress()
        || address.isSiteLocalAddress()
        || address.isLinkLocalAddress()
        || (address instanceof Inet6Address && (address.getAddress()[0] & 0xfe) == 0xfc);
  }

  /** A reserved pending-connection slot. Releasing it more than once has no further effect. */
  final class Slot {
    private final Bytes source;
    private final AtomicBoolean released = new AtomicBoolean();

    private Slot(final Bytes source) {
      this.source = source;
    }

    void release() {
      if (released.compareAndSet(false, true)) {
        pendingConnections.decrementAndGet();
        if (source != null) {
          pendingSources.remove(source);
        }
      }
    }
  }
}
