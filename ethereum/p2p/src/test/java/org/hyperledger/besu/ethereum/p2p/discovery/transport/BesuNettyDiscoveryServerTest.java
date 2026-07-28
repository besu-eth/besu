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
package org.hyperledger.besu.ethereum.p2p.discovery.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class BesuNettyDiscoveryServerTest {

  private static final byte[] MASKING_KEY = new byte[16];

  static {
    new Random().nextBytes(MASKING_KEY);
  }

  private SharedDiscoveryTransport transport;

  @AfterEach
  public void tearDown() throws Exception {
    if (transport != null) {
      transport.stop().get(5, TimeUnit.SECONDS);
    }
  }

  private static SharedDiscoveryTransport newTransport() {
    return SharedDiscoveryTransport.builder()
        .ipv4BindAddress(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0))
        .maskingKey(MASKING_KEY)
        .v4Enabled(true)
        .v5Enabled(true)
        .build();
  }

  @Test
  public void getListenAddress_throwsBeforeTransportStarted() {
    transport = newTransport();
    final BesuNettyDiscoveryServer server =
        new BesuNettyDiscoveryServer(transport, StandardProtocolFamily.INET);

    assertThat(catchThrowable(server::getListenAddress))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("SharedDiscoveryTransport has no channel for family")
        .hasMessageContaining("Was start() called first?");
  }

  @Test
  public void getListenAddress_returnsBoundAddressAfterStart() throws Exception {
    transport = newTransport();
    transport.start().get(5, TimeUnit.SECONDS);
    final BesuNettyDiscoveryServer server =
        new BesuNettyDiscoveryServer(transport, StandardProtocolFamily.INET);

    assertThat(server.getListenAddress())
        .isEqualTo(transport.getBoundAddress(StandardProtocolFamily.INET));
  }

  @Test
  public void getListenAddress_throwsForUnboundFamilyEvenAfterOtherFamilyStarted()
      throws Exception {
    transport = newTransport();
    transport.start().get(5, TimeUnit.SECONDS);
    // Only IPv4 was configured, so the IPv6 family never gets a channel.
    final BesuNettyDiscoveryServer server =
        new BesuNettyDiscoveryServer(transport, StandardProtocolFamily.INET6);

    assertThat(catchThrowable(server::getListenAddress)).isInstanceOf(IllegalStateException.class);
  }
}
