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
package org.hyperledger.besu.ethereum.p2p.discovery.discv4.transport;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.tuweni.bytes.Bytes;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class NettyV4TransportTest {

  private NettyV4Transport transport1;
  private NettyV4Transport transport2;

  @AfterEach
  public void tearDown() throws Exception {
    if (transport1 != null) {
      transport1.stop().get(5, TimeUnit.SECONDS);
    }
    if (transport2 != null) {
      transport2.stop().get(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void twoTransports_exchangePackets() throws Exception {
    final InetSocketAddress ephemeral = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);

    transport1 = NettyV4Transport.createOwnedChannel(ephemeral);
    transport2 = NettyV4Transport.createOwnedChannel(ephemeral);

    final AtomicReference<Bytes> receivedByTransport2 = new AtomicReference<>();
    transport2.setInboundHandler((sender, data) -> receivedByTransport2.set(data));

    final InetSocketAddress addr1 = transport1.start().get(5, TimeUnit.SECONDS);
    final InetSocketAddress addr2 = transport2.start().get(5, TimeUnit.SECONDS);

    assertThat(addr1.getPort()).isGreaterThan(0);
    assertThat(addr2.getPort()).isGreaterThan(0);

    final Bytes payload = Bytes.fromHexString("0xdeadbeef01020304");
    transport1.send(addr2, payload).get(5, TimeUnit.SECONDS);

    Awaitility.await()
        .atMost(2, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(receivedByTransport2.get()).isEqualTo(payload));
  }

  @Test
  public void sendCompletionFuture_resolvesOnSuccess() throws Exception {
    final InetSocketAddress ephemeral = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);

    transport1 = NettyV4Transport.createOwnedChannel(ephemeral);
    transport2 = NettyV4Transport.createOwnedChannel(ephemeral);

    transport2.setInboundHandler((sender, data) -> {});

    final InetSocketAddress addr2 = transport2.start().get(5, TimeUnit.SECONDS);
    transport1.start().get(5, TimeUnit.SECONDS);

    final CompletableFuture<Void> sendResult =
        transport1.send(addr2, Bytes.fromHexString("0xaabbcc"));

    assertThat(sendResult).succeedsWithin(5, TimeUnit.SECONDS);
  }

  @Test
  public void start_returnsActualBoundAddress() throws Exception {
    final InetSocketAddress ephemeral = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    transport1 = NettyV4Transport.createOwnedChannel(ephemeral);

    final InetSocketAddress bound = transport1.start().get(5, TimeUnit.SECONDS);

    assertThat(bound.getPort()).isGreaterThan(0);
    assertThat(bound.getAddress()).isEqualTo(InetAddress.getLoopbackAddress());
  }

  @Test
  public void stop_completesCleanly() throws Exception {
    final InetSocketAddress ephemeral = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    transport1 = NettyV4Transport.createOwnedChannel(ephemeral);
    transport1.start().get(5, TimeUnit.SECONDS);

    final CompletableFuture<Void> stopFuture = transport1.stop();
    transport1 = null; // tearDown won't try to stop it again

    assertThat(stopFuture).succeedsWithin(5, TimeUnit.SECONDS);
  }
}
