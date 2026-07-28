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
package org.hyperledger.besu.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import org.junit.jupiter.api.Test;

public class NetworkUtilityTest {

  @Test
  public void urlForSocketAddressHandlesIPv6() {
    final InetSocketAddress ipv6All = new InetSocketAddress("::", 80);
    assertThat(NetworkUtility.urlForSocketAddress("http", ipv6All)).doesNotContain("::");
    assertThat(NetworkUtility.urlForSocketAddress("http", ipv6All))
        .doesNotContain("0:0:0:0:0:0:0:0");
    final InetSocketAddress ipv6 = new InetSocketAddress("1:2:3:4:5:6:7:8", 80);
    assertThat(NetworkUtility.urlForSocketAddress("http", ipv6)).contains("[1:2:3:4:5:6:7:8]");
  }

  @Test
  public void assertPortIsNotAvailableForTcp() throws IOException {
    final ServerSocket serverSocket = new ServerSocket(8541);
    assertThat(NetworkUtility.isPortUnavailableForTcp(8541)).isEqualTo(true);
    serverSocket.close();
  }

  @Test
  public void assertPortIsNotAvailableForUdp() throws IOException {
    final DatagramSocket datagramSocket = new DatagramSocket(8541);
    assertThat(NetworkUtility.isPortUnavailableForUdp(8541)).isEqualTo(true);
    datagramSocket.close();
  }

  @Test
  public void assertLocalhostIdentification() {
    assertThat(NetworkUtility.isLocalhostAddress("127.0.0.1")).isTrue();
    assertThat(NetworkUtility.isLocalhostAddress("::1")).isTrue();
    assertThat(NetworkUtility.isLocalhostAddress("192.168.1.1")).isFalse();
    assertThat(NetworkUtility.isLocalhostAddress("::ffff:c0a8:101")).isFalse();
  }

  @Test
  public void assertIpV4Address() {
    assertThat(NetworkUtility.isIpV4Address("127.0.0.1")).isTrue();
    assertThat(NetworkUtility.isIpV4Address("10.0.0.0")).isTrue();
    assertThat(NetworkUtility.isIpV4Address("172.16.1.1")).isTrue();
    assertThat(NetworkUtility.isIpV4Address("127.0.0.")).isFalse();
    assertThat(NetworkUtility.isIpV4Address("256.256.256.256")).isFalse();
    // ipv6 compatible ipv4 address
    assertThat(NetworkUtility.isIpV4Address("::ffff:c0a8:5801")).isTrue();
    assertThat(NetworkUtility.isIpV4Address("0:0:0:0:0:ffff:c0a8:5801")).isTrue();
    assertThat(NetworkUtility.isIpV4Address("0000:0000:0000:0000:0000:ffff:c0a8:5801")).isTrue();
  }

  @Test
  public void assertIpV6Address() {
    assertThat(NetworkUtility.isIpV6Address("::1")).isTrue();
    assertThat(NetworkUtility.isIpV6Address("::")).isTrue();
    assertThat(NetworkUtility.isIpV6Address("2001:db8:3333:4444:5555:6666:7777:8888")).isTrue();
    assertThat(NetworkUtility.isIpV6Address("00:00::00:00::00:00")).isFalse();
  }

  @Test
  public void isMergeableDualStackBind_trueOnlyWhenSamePortAndBothWildcard() {
    assertThat(NetworkUtility.isMergeableDualStackBind("0.0.0.0", 30303, "::", 30303)).isTrue();
    // Alternate wildcard spellings recognized by isUnspecifiedAddress.
    assertThat(NetworkUtility.isMergeableDualStackBind("0.0.0.0", 30303, "0:0:0:0:0:0:0:0", 30303))
        .isTrue();
  }

  @Test
  public void isMergeableDualStackBind_falseWhenPortsDiffer() {
    assertThat(NetworkUtility.isMergeableDualStackBind("0.0.0.0", 30303, "::", 30404)).isFalse();
  }

  @Test
  public void isMergeableDualStackBind_falseWhenEitherHostIsNotWildcard() {
    assertThat(NetworkUtility.isMergeableDualStackBind("172.28.0.10", 30303, "::", 30303))
        .isFalse();
    assertThat(NetworkUtility.isMergeableDualStackBind("0.0.0.0", 30303, "fd00::10", 30303))
        .isFalse();
    assertThat(NetworkUtility.isMergeableDualStackBind("172.28.0.10", 30303, "fd00::10", 30303))
        .isFalse();
  }
}
