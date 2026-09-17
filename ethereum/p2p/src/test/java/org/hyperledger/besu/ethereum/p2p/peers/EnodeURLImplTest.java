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
package org.hyperledger.besu.ethereum.p2p.peers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.hyperledger.besu.plugin.data.EnodeURL;
import org.hyperledger.besu.util.IllegalPortException;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.mockito.Mockito;

public class EnodeURLImplTest {

  private final String VALID_NODE_ID =
      "6f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0";
  private final String IPV4_ADDRESS = "192.168.0.1";
  private final String IPV6_FULL_ADDRESS = "[2001:db8:85a3:0:0:8a2e:370:7334]";
  private final String IPV6_COMPACT_ADDRESS = "[2001:db8:85a3::8a2e:370:7334]";
  private final int P2P_PORT = 30303;
  private final int DISCOVERY_PORT = 30301;
  private final String DISCOVERY_QUERY = "discport=" + DISCOVERY_PORT;

  @Test
  public void build_withMatchingDiscoveryAndListeningPorts() {
    final EnodeURL enode =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress(IPV4_ADDRESS)
            .listeningPort(P2P_PORT)
            .discoveryPort(Optional.of(P2P_PORT))
            .build();
    assertThat(enode.getListeningPortOrZero()).isEqualTo(P2P_PORT);
    assertThat(enode.getDiscoveryPortOrZero()).isEqualTo(P2P_PORT);
  }

  @Test
  public void build_withNonMatchingDiscoveryAndListeningPorts() {
    final EnodeURL enode =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress(IPV4_ADDRESS)
            .listeningPort(P2P_PORT)
            .discoveryPort(Optional.of(DISCOVERY_PORT))
            .build();
    assertThat(enode.getListeningPortOrZero()).isEqualTo(P2P_PORT);
    assertThat(enode.getDiscoveryPortOrZero()).isEqualTo(DISCOVERY_PORT);
  }

  @Test
  public void fromString_withDiscoveryPortShouldBuildExpectedEnodeURLObject() {
    final EnodeURL expectedEnodeURL =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress(IPV4_ADDRESS)
            .listeningPort(P2P_PORT)
            .discoveryPort(Optional.of(DISCOVERY_PORT))
            .build();
    final String enodeURLString =
        "enode://" + VALID_NODE_ID + "@" + IPV4_ADDRESS + ":" + P2P_PORT + "?" + DISCOVERY_QUERY;

    final EnodeURL enodeURL = EnodeURLImpl.fromString(enodeURLString);

    assertThat(enodeURL).isEqualTo(expectedEnodeURL);
    assertThat(enodeURL.toString()).isEqualTo(enodeURLString);
  }

  @Test
  public void fromString_withoutDiscoveryPortShouldBuildExpectedEnodeURLObject() {
    final EnodeURL expectedEnodeURL =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress(IPV4_ADDRESS)
            .discoveryAndListeningPorts(P2P_PORT)
            .build();
    final String enodeURLString = "enode://" + VALID_NODE_ID + "@" + IPV4_ADDRESS + ":" + P2P_PORT;

    final EnodeURL enodeURL = EnodeURLImpl.fromString(enodeURLString);

    assertThat(enodeURL).isEqualTo(expectedEnodeURL);
    assertThat(enodeURL.toString()).isEqualTo(enodeURLString);
  }

  @Test
  public void fromString_withIPV6ShouldBuildExpectedEnodeURLObject() {
    final EnodeURL expectedEnodeURL =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress(IPV6_FULL_ADDRESS)
            .listeningPort(P2P_PORT)
            .discoveryPort(Optional.of(DISCOVERY_PORT))
            .build();
    final String enodeURLString =
        "enode://"
            + VALID_NODE_ID
            + "@"
            + IPV6_FULL_ADDRESS
            + ":"
            + P2P_PORT
            + "?"
            + DISCOVERY_QUERY;

    final EnodeURL enodeURL = EnodeURLImpl.fromString(enodeURLString);

    assertThat(enodeURL).isEqualTo(expectedEnodeURL);
  }

  @Test
  public void fromString_withIPV6InCompactFormShouldBuildExpectedEnodeURLObject() {
    final EnodeURL expectedEnodeURL =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress(IPV6_COMPACT_ADDRESS)
            .listeningPort(P2P_PORT)
            .discoveryPort(Optional.of(DISCOVERY_PORT))
            .build();
    final String enodeURLString =
        "enode://"
            + VALID_NODE_ID
            + "@"
            + IPV6_COMPACT_ADDRESS
            + ":"
            + P2P_PORT
            + "?"
            + DISCOVERY_QUERY;

    final EnodeURL enodeURL = EnodeURLImpl.fromString(enodeURLString);

    assertThat(enodeURL).isEqualTo(expectedEnodeURL);
    assertThat(enodeURL.toString()).isEqualTo(enodeURLString);
  }

  @Test
  public void fromString_with0ValuedDiscoveryPort() {
    final String query = "discport=0";
    final String enodeURLString =
        "enode://" + VALID_NODE_ID + "@" + IPV6_COMPACT_ADDRESS + ":" + P2P_PORT + "?" + query;

    EnodeURL enodeURL = EnodeURLImpl.fromString(enodeURLString);
    assertThat(enodeURL.getNodeId().toUnprefixedHexString()).isEqualTo(VALID_NODE_ID);
    assertThat("[" + enodeURL.getIpAsString() + "]").isEqualTo(IPV6_FULL_ADDRESS);
    assertThat(enodeURL.getListeningPort()).isEqualTo(Optional.of(P2P_PORT));
    assertThat(enodeURL.getDiscoveryPortOrZero()).isEqualTo(0);
    assertThat(enodeURL.isListening()).isTrue();
    assertThat(enodeURL.isRunningDiscovery()).isFalse();

    assertThat(enodeURL.toString()).isEqualTo(enodeURLString);
  }

  @Test
  public void fromString_with0ValuedListeningPort() {
    final String enodeURLString = "enode://" + VALID_NODE_ID + "@" + IPV4_ADDRESS + ":" + 0;

    EnodeURL enodeURL = EnodeURLImpl.fromString(enodeURLString);
    assertThat(enodeURL.getNodeId().toUnprefixedHexString()).isEqualTo(VALID_NODE_ID);
    assertThat(enodeURL.getIpAsString()).isEqualTo(IPV4_ADDRESS);
    assertThat(enodeURL.getListeningPortOrZero()).isEqualTo(0);
    assertThat(enodeURL.getDiscoveryPortOrZero()).isEqualTo(0);
    assertThat(enodeURL.isListening()).isFalse();
    assertThat(enodeURL.isRunningDiscovery()).isFalse();

    assertThat(enodeURL.toString()).isEqualTo(enodeURLString);
  }

  @Test
  public void fromString_with0ValuedListeningPortAndExplicit0ValuedDiscPort() {
    final String query = "discport=0";
    final String enodeURLString =
        "enode://" + VALID_NODE_ID + "@" + IPV4_ADDRESS + ":" + 0 + "?" + query;

    EnodeURL enodeURL = EnodeURLImpl.fromString(enodeURLString);
    assertThat(enodeURL.getNodeId().toUnprefixedHexString()).isEqualTo(VALID_NODE_ID);
    assertThat(enodeURL.getIpAsString()).isEqualTo(IPV4_ADDRESS);
    assertThat(enodeURL.getListeningPortOrZero()).isEqualTo(0);
    assertThat(enodeURL.getDiscoveryPortOrZero()).isEqualTo(0);
    assertThat(enodeURL.isListening()).isFalse();
    assertThat(enodeURL.isRunningDiscovery()).isFalse();
  }

  @Test
  public void fromString_withoutNodeIdShouldFail() {
    final String enodeURLString = "enode://@" + IPV4_ADDRESS + ":" + P2P_PORT;
    final Throwable thrown = catchThrowable(() -> EnodeURLImpl.fromString(enodeURLString));

    assertThat(thrown)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Missing node ID.");
  }

  @Test
  public void fromString_withInvalidSizeNodeIdShouldFail() {
    final String enodeURLString = "enode://wrong_size_string@" + IPV4_ADDRESS + ":" + P2P_PORT;
    final Throwable thrown = catchThrowable(() -> EnodeURLImpl.fromString(enodeURLString));

    assertThat(thrown)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid node ID");
  }

  @Test
  public void fromString_withInvalidHexCharacterNodeIdShouldFail() {
    final String enodeURLString =
        "enode://0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000@"
            + IPV4_ADDRESS
            + ":"
            + P2P_PORT;
    final Throwable thrown = catchThrowable(() -> EnodeURLImpl.fromString(enodeURLString));

    assertThat(thrown)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid node ID");
  }

  @Test
  public void fromString_withoutIpShouldFail() {
    final String enodeURLString = "enode://" + VALID_NODE_ID + "@:" + P2P_PORT;
    final Throwable thrown = catchThrowable(() -> EnodeURLImpl.fromString(enodeURLString));

    assertThat(thrown)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Missing or invalid host or ip address.");
  }

  @Test
  public void fromString_withInvalidIpFormatShouldFail() {
    final String enodeURLString = "enode://" + VALID_NODE_ID + "@192.0.1:" + P2P_PORT;
    final Throwable thrown = catchThrowable(() -> EnodeURLImpl.fromString(enodeURLString));

    assertThat(thrown)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Missing or invalid host or ip address.");
  }

  @Test
  public void fromString_withoutListeningPortShouldFail() {
    final String enodeURLString = "enode://" + VALID_NODE_ID + "@" + IPV4_ADDRESS + ":";
    final Throwable thrown = catchThrowable(() -> EnodeURLImpl.fromString(enodeURLString));

    assertThat(thrown).hasCauseInstanceOf(IllegalPortException.class);
  }

  @Test
  public void fromString_withoutListeningPortAndWithDiscoveryPortShouldFail() {
    final String enodeURLString =
        "enode://" + VALID_NODE_ID + "@" + IPV4_ADDRESS + ":?discport=30301";
    final Throwable thrown = catchThrowable(() -> EnodeURLImpl.fromString(enodeURLString));

    assertThat(thrown).hasCauseInstanceOf(IllegalPortException.class);
  }

  @Test
  public void fromString_withAboveRangeListeningPortShouldFail() {
    final String enodeURLString = "enode://" + VALID_NODE_ID + "@" + IPV4_ADDRESS + ":98765";
    final Throwable thrown = catchThrowable(() -> EnodeURLImpl.fromString(enodeURLString));

    assertThat(thrown).hasCauseInstanceOf(IllegalPortException.class);
  }

  @Test
  public void fromString_withAboveRangeDiscoveryPortShouldFail() {
    final String enodeURLString =
        "enode://" + VALID_NODE_ID + "@" + IPV4_ADDRESS + ":" + P2P_PORT + "?discport=98765";
    final Throwable thrown = catchThrowable(() -> EnodeURLImpl.fromString(enodeURLString));

    assertThat(thrown).hasCauseInstanceOf(IllegalPortException.class);
  }

  @Test
  public void fromString_withMisspelledDiscoveryParam() {
    final String query = "adiscport=1234";
    final String enodeURLString =
        "enode://" + VALID_NODE_ID + "@" + IPV6_FULL_ADDRESS + ":" + P2P_PORT + "?" + query;

    assertThatThrownBy(() -> EnodeURLImpl.fromString(enodeURLString))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid discovery port: '" + query + "'");
  }

  @Test
  public void fromString_withAdditionalTrailingQueryParam() {
    final String query = "discport=1234&other=y";
    final String enodeURLString =
        "enode://" + VALID_NODE_ID + "@" + IPV6_FULL_ADDRESS + ":" + P2P_PORT + "?" + query;

    assertThatThrownBy(() -> EnodeURLImpl.fromString(enodeURLString))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid discovery port: '" + query + "'");
  }

  @Test
  public void fromString_withAdditionalLeadingQueryParam() {
    final String query = "other=123&discport=1234";
    final String enodeURLString =
        "enode://" + VALID_NODE_ID + "@" + IPV6_FULL_ADDRESS + ":" + P2P_PORT + "?" + query;

    assertThatThrownBy(() -> EnodeURLImpl.fromString(enodeURLString))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid discovery port: '" + query + "'");
  }

  @Test
  public void fromString_withAdditionalLeadingAndTrailingQueryParams() {
    final String query = "other=123&discport=1234&other2=456";
    final String enodeURLString =
        "enode://" + VALID_NODE_ID + "@" + IPV6_FULL_ADDRESS + ":" + P2P_PORT + "?" + query;

    assertThatThrownBy(() -> EnodeURLImpl.fromString(enodeURLString))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid discovery port: '" + query + "'");
  }

  @Test
  public void fromString_withMultipleDiscoveryParams() {
    final String query = "discport=1234&discport=456";
    final String enodeURLString =
        "enode://" + VALID_NODE_ID + "@" + IPV6_FULL_ADDRESS + ":" + P2P_PORT + "?" + query;

    assertThatThrownBy(() -> EnodeURLImpl.fromString(enodeURLString))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid discovery port: '" + query + "'");
  }

  @Test
  public void fromString_withNullEnodeURLShouldFail() {
    final Throwable thrown = catchThrowable(() -> EnodeURLImpl.fromString(null));

    assertThat(thrown)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid empty value");
  }

  @Test
  public void fromString_withEmptyEnodeURLShouldFail() {
    final Throwable thrown = catchThrowable(() -> EnodeURLImpl.fromString(""));

    assertThat(thrown)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid empty value.");
  }

  @Test
  public void fromString_withWhitespaceEnodeURLShouldFail() {
    final Throwable thrown = catchThrowable(() -> EnodeURLImpl.fromString(" "));

    assertThat(thrown)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid empty value.");
  }

  @Test
  public void fromStringInvalidNodeIdLengthHasDescriptiveMessage() {
    String invalidEnodeURL =
        String.format("enode://%s@%s:%d", VALID_NODE_ID.substring(1), IPV4_ADDRESS, P2P_PORT);
    assertThatThrownBy(() -> EnodeURLImpl.fromString(invalidEnodeURL))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageEndingWith(
            "Invalid node ID: node ID must have exactly 128 hexadecimal characters and should not include any '0x' hex prefix.");
  }

  @Test
  public void fromString_withHostnameEnodeURLShouldFailWhenDnsDisabled() {
    final String enodeURLString =
        "enode://" + VALID_NODE_ID + "@" + "localhost" + ":" + P2P_PORT + "?" + DISCOVERY_QUERY;

    assertThatThrownBy(
            () ->
                EnodeURLImpl.fromString(
                    enodeURLString,
                    ImmutableEnodeDnsConfiguration.builder()
                        .dnsEnabled(false)
                        .updateEnabled(false)
                        .build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid enode URL syntax");
  }

  @Test
  public void fromString_withHostnameEnodeURLShouldFailWhenDnsDisabledAndUpdateEnabled() {
    final String enodeURLString =
        "enode://" + VALID_NODE_ID + "@" + "localhost" + ":" + P2P_PORT + "?" + DISCOVERY_QUERY;

    assertThatThrownBy(
            () ->
                EnodeURLImpl.fromString(
                    enodeURLString,
                    ImmutableEnodeDnsConfiguration.builder()
                        .dnsEnabled(false)
                        .updateEnabled(true)
                        .build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid enode URL syntax");
  }

  @Test
  public void fromString_withHostnameEnodeURLShouldWorkWhenDnsEnabled() {
    final EnodeURL expectedEnodeURL =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress("127.0.0.1")
            .listeningPort(P2P_PORT)
            .discoveryPort(Optional.of(DISCOVERY_PORT))
            .build();
    final String enodeURLString =
        "enode://" + VALID_NODE_ID + "@" + "localhost" + ":" + P2P_PORT + "?" + DISCOVERY_QUERY;

    final EnodeURL enodeURL =
        EnodeURLImpl.fromString(
            enodeURLString,
            ImmutableEnodeDnsConfiguration.builder().dnsEnabled(true).updateEnabled(false).build());
    ;

    assertThat(enodeURL).isEqualTo(expectedEnodeURL);
  }

  @Test
  public void fromString_withHostnameEnodeURLShouldWorkWhenDnsEnabledAndUpdateEnabled() {
    final EnodeURL expectedEnodeURL =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress("127.0.0.1")
            .listeningPort(P2P_PORT)
            .discoveryPort(Optional.of(DISCOVERY_PORT))
            .build();
    final String enodeURLString =
        "enode://" + VALID_NODE_ID + "@" + "localhost" + ":" + P2P_PORT + "?" + DISCOVERY_QUERY;

    final EnodeURL enodeURL =
        EnodeURLImpl.fromString(
            enodeURLString,
            ImmutableEnodeDnsConfiguration.builder().dnsEnabled(true).updateEnabled(false).build());
    ;

    assertThat(enodeURL).isEqualTo(expectedEnodeURL);
  }

  @Test
  public void toURI_WithDiscoveryPortCreateExpectedURI() {
    final String enodeURLString =
        "enode://" + VALID_NODE_ID + "@" + IPV4_ADDRESS + ":" + P2P_PORT + "?" + DISCOVERY_QUERY;
    final URI expectedURI = URI.create(enodeURLString);
    final URI createdURI = EnodeURLImpl.fromString(enodeURLString).toURI();

    assertThat(createdURI).isEqualTo(expectedURI);
  }

  @Test
  public void toURI_WithoutDiscoveryPortCreateExpectedURI() {
    final String enodeURLString = "enode://" + VALID_NODE_ID + "@" + IPV4_ADDRESS + ":" + P2P_PORT;
    final URI expectedURI = URI.create(enodeURLString);
    final URI createdURI = EnodeURLImpl.fromString(enodeURLString).toURI();

    assertThat(createdURI).isEqualTo(expectedURI);
  }

  @Test
  public void toURI_WithHostnameShouldWorkWhenDnsEnabled() {
    final String enodeURLString = "enode://" + VALID_NODE_ID + "@" + "localhost" + ":" + P2P_PORT;
    final URI expectedURI =
        URI.create("enode://" + VALID_NODE_ID + "@" + "127.0.0.1" + ":" + P2P_PORT);
    final URI createdURI =
        EnodeURLImpl.fromString(
                enodeURLString,
                ImmutableEnodeDnsConfiguration.builder()
                    .dnsEnabled(true)
                    .updateEnabled(false)
                    .build())
            .toURI();

    assertThat(createdURI).isEqualTo(expectedURI);
  }

  @Test
  @DisabledOnOs(
      value = OS.MAC,
      disabledReason = "canonical lookup may not match dns lookup for local machine")
  public void toURI_WithHostnameShouldWorkWhenDnsEnabledAndUpdateEnabled()
      throws UnknownHostException {
    final String enodeURLString =
        "enode://"
            + VALID_NODE_ID
            + "@"
            + InetAddress.getLocalHost().getCanonicalHostName()
            + ":"
            + P2P_PORT;
    final URI expectedURI = URI.create(enodeURLString);
    final URI createdURI =
        EnodeURLImpl.fromString(
                enodeURLString,
                ImmutableEnodeDnsConfiguration.builder()
                    .dnsEnabled(true)
                    .updateEnabled(true)
                    .build())
            .toURI();

    assertThat(createdURI).isEqualTo(expectedURI);
  }

  @Test
  public void fromURI_withHostnameShouldFailWhenDnsDisabled() {
    final String enodeURLString =
        "enode://" + VALID_NODE_ID + "@" + "localhost" + ":" + P2P_PORT + "?" + DISCOVERY_QUERY;
    final URI expectedURI = URI.create(enodeURLString);

    assertThatThrownBy(
            () ->
                EnodeURLImpl.fromURI(
                    expectedURI,
                    ImmutableEnodeDnsConfiguration.builder()
                        .dnsEnabled(false)
                        .updateEnabled(false)
                        .build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid ip address");
  }

  @Test
  public void fromURI_withHostnameEnodeURLShouldWorkWhenDnsEnabled() {
    final EnodeURL expectedEnodeURL =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress("127.0.0.1")
            .listeningPort(P2P_PORT)
            .discoveryPort(Optional.of(DISCOVERY_PORT))
            .build();

    final String enodeURLString =
        "enode://" + VALID_NODE_ID + "@" + "localhost" + ":" + P2P_PORT + "?" + DISCOVERY_QUERY;
    final URI expectedURI = URI.create(enodeURLString);

    final EnodeURL enodeURL =
        EnodeURLImpl.fromURI(
            expectedURI,
            ImmutableEnodeDnsConfiguration.builder().dnsEnabled(true).updateEnabled(false).build());
    assertThat(enodeURL).isEqualTo(expectedEnodeURL);
  }

  @Test
  public void builder_setInvalidPorts() {
    final EnodeURLImpl.Builder validBuilder =
        EnodeURLImpl.builder().nodeId(VALID_NODE_ID).ipAddress(IPV4_ADDRESS);

    Stream.<ThrowableAssert.ThrowingCallable>of(
            () -> validBuilder.listeningPort(200_000).disableDiscovery().build(),
            () -> validBuilder.listeningPort(-2).disableDiscovery().build(),
            () -> validBuilder.discoveryPort(-1).disableListening().build(),
            () -> validBuilder.discoveryPort(100_000).disableListening().build())
        .forEach(assertThatExceptionOfType(IllegalPortException.class)::isThrownBy);
  }

  @Test
  public void builder_setEachPortExplicitly() {
    final EnodeURL enodeURL =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress(IPV4_ADDRESS)
            .listeningPort(P2P_PORT)
            .discoveryPort(DISCOVERY_PORT)
            .build();

    assertThat(enodeURL.getNodeId().toUnprefixedHexString()).isEqualTo(VALID_NODE_ID);
    assertThat(enodeURL.getIpAsString()).isEqualTo(IPV4_ADDRESS);
    assertThat(enodeURL.getListeningPortOrZero()).isEqualTo(P2P_PORT);
    assertThat(enodeURL.getDiscoveryPortOrZero()).isEqualTo(DISCOVERY_PORT);
    assertThat(enodeURL.isListening()).isTrue();
    assertThat(enodeURL.isRunningDiscovery()).isTrue();
  }

  @Test
  public void builder_setPortsTogether() {
    final EnodeURL enodeURL =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress(IPV4_ADDRESS)
            .discoveryAndListeningPorts(P2P_PORT)
            .build();

    assertThat(enodeURL.getNodeId().toUnprefixedHexString()).isEqualTo(VALID_NODE_ID);
    assertThat(enodeURL.getIpAsString()).isEqualTo(IPV4_ADDRESS);
    assertThat(enodeURL.getListeningPortOrZero()).isEqualTo(P2P_PORT);
    assertThat(enodeURL.getDiscoveryPortOrZero()).isEqualTo(P2P_PORT);
    assertThat(enodeURL.isListening()).isTrue();
    assertThat(enodeURL.isRunningDiscovery()).isTrue();
  }

  @Test
  public void builder_setDefaultPorts() {
    final EnodeURL enodeURL =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress(IPV4_ADDRESS)
            .useDefaultPorts()
            .build();

    assertThat(enodeURL.getNodeId().toUnprefixedHexString()).isEqualTo(VALID_NODE_ID);
    assertThat(enodeURL.getIpAsString()).isEqualTo(IPV4_ADDRESS);
    assertThat(enodeURL.getListeningPortOrZero()).isEqualTo(EnodeURLImpl.DEFAULT_LISTENING_PORT);
    assertThat(enodeURL.getDiscoveryPortOrZero()).isEqualTo(EnodeURLImpl.DEFAULT_LISTENING_PORT);
    assertThat(enodeURL.isListening()).isTrue();
    assertThat(enodeURL.isRunningDiscovery()).isTrue();
  }

  @Test
  public void builder_discoveryDisabled() {
    final EnodeURL enodeURL =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress(IPV4_ADDRESS)
            .listeningPort(P2P_PORT)
            .disableDiscovery()
            .build();

    assertThat(enodeURL.getNodeId().toUnprefixedHexString()).isEqualTo(VALID_NODE_ID);
    assertThat(enodeURL.getIpAsString()).isEqualTo(IPV4_ADDRESS);
    assertThat(enodeURL.getListeningPortOrZero()).isEqualTo(P2P_PORT);
    assertThat(enodeURL.getDiscoveryPortOrZero()).isEqualTo(0);
    assertThat(enodeURL.isListening()).isTrue();
    assertThat(enodeURL.isRunningDiscovery()).isFalse();
  }

  @Test
  public void builder_listeningDisabled() {
    final EnodeURL enodeURL =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress(IPV4_ADDRESS)
            .discoveryPort(P2P_PORT)
            .disableListening()
            .build();

    assertThat(enodeURL.getNodeId().toUnprefixedHexString()).isEqualTo(VALID_NODE_ID);
    assertThat(enodeURL.getIpAsString()).isEqualTo(IPV4_ADDRESS);
    assertThat(enodeURL.getListeningPortOrZero()).isEqualTo(0);
    assertThat(enodeURL.getDiscoveryPortOrZero()).isEqualTo(P2P_PORT);
    assertThat(enodeURL.isListening()).isFalse();
    assertThat(enodeURL.isRunningDiscovery()).isTrue();
  }

  @Test
  public void builder_listeningAndDiscoveryDisabled() {
    final EnodeURL enodeURL =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress(IPV4_ADDRESS)
            .disableDiscovery()
            .disableListening()
            .build();

    assertThat(enodeURL.getNodeId().toUnprefixedHexString()).isEqualTo(VALID_NODE_ID);
    assertThat(enodeURL.getIpAsString()).isEqualTo(IPV4_ADDRESS);
    assertThat(enodeURL.getListeningPortOrZero()).isEqualTo(0);
    assertThat(enodeURL.getDiscoveryPortOrZero()).isEqualTo(0);
    assertThat(enodeURL.isListening()).isFalse();
    assertThat(enodeURL.isRunningDiscovery()).isFalse();
  }

  @Test
  public void builder_setPortsTo0() {
    final EnodeURL enodeURL =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress(IPV4_ADDRESS)
            .discoveryAndListeningPorts(0)
            .build();

    assertThat(enodeURL.getNodeId().toUnprefixedHexString()).isEqualTo(VALID_NODE_ID);
    assertThat(enodeURL.getIpAsString()).isEqualTo(IPV4_ADDRESS);
    assertThat(enodeURL.getListeningPortOrZero()).isEqualTo(0);
    assertThat(enodeURL.getDiscoveryPortOrZero()).isEqualTo(0);
    assertThat(enodeURL.isListening()).isFalse();
    assertThat(enodeURL.isRunningDiscovery()).isFalse();
  }

  @Test
  public void builder_setDiscoveryPortTo0() {
    final EnodeURL enodeURL =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress(IPV4_ADDRESS)
            .discoveryPort(0)
            .listeningPort(P2P_PORT)
            .build();

    assertThat(enodeURL.getNodeId().toUnprefixedHexString()).isEqualTo(VALID_NODE_ID);
    assertThat(enodeURL.getIpAsString()).isEqualTo(IPV4_ADDRESS);
    assertThat(enodeURL.getListeningPortOrZero()).isEqualTo(P2P_PORT);
    assertThat(enodeURL.getDiscoveryPortOrZero()).isEqualTo(0);
    assertThat(enodeURL.isListening()).isTrue();
    assertThat(enodeURL.isRunningDiscovery()).isFalse();
  }

  @Test
  public void builder_setListeningPortTo0() {
    final EnodeURL enodeURL =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress(IPV4_ADDRESS)
            .discoveryPort(P2P_PORT)
            .listeningPort(0)
            .build();

    assertThat(enodeURL.getNodeId().toUnprefixedHexString()).isEqualTo(VALID_NODE_ID);
    assertThat(enodeURL.getIpAsString()).isEqualTo(IPV4_ADDRESS);
    assertThat(enodeURL.getListeningPortOrZero()).isEqualTo(0);
    assertThat(enodeURL.getDiscoveryPortOrZero()).isEqualTo(P2P_PORT);
    assertThat(enodeURL.isListening()).isFalse();
    assertThat(enodeURL.isRunningDiscovery()).isTrue();
  }

  @Test
  public void builder_setDiscoveryPortToEmptyValue() {
    final EnodeURL enodeURL =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress(IPV4_ADDRESS)
            .discoveryPort(Optional.empty())
            .listeningPort(P2P_PORT)
            .build();

    assertThat(enodeURL.getNodeId().toUnprefixedHexString()).isEqualTo(VALID_NODE_ID);
    assertThat(enodeURL.getIpAsString()).isEqualTo(IPV4_ADDRESS);
    assertThat(enodeURL.getListeningPortOrZero()).isEqualTo(P2P_PORT);
    assertThat(enodeURL.getDiscoveryPortOrZero()).isEqualTo(0);
    assertThat(enodeURL.isListening()).isTrue();
    assertThat(enodeURL.isRunningDiscovery()).isFalse();
  }

  @Test
  public void builder_setListeningPortToEmptyValue() {
    final EnodeURL enodeURL =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress(IPV4_ADDRESS)
            .discoveryPort(P2P_PORT)
            .listeningPort(Optional.empty())
            .build();

    assertThat(enodeURL.getNodeId().toUnprefixedHexString()).isEqualTo(VALID_NODE_ID);
    assertThat(enodeURL.getIpAsString()).isEqualTo(IPV4_ADDRESS);
    assertThat(enodeURL.getListeningPortOrZero()).isEqualTo(0);
    assertThat(enodeURL.getDiscoveryPortOrZero()).isEqualTo(P2P_PORT);
    assertThat(enodeURL.isListening()).isFalse();
    assertThat(enodeURL.isRunningDiscovery()).isTrue();
  }

  @Test
  public void builder_discoveryNotSpecified() {
    final EnodeURLImpl.Builder builder =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress(IPV4_ADDRESS)
            .listeningPort(P2P_PORT);

    assertThatThrownBy(builder::build)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Discovery port must be configured");
  }

  @Test
  public void builder_listeningPortNotSpecified() {
    final EnodeURLImpl.Builder builder =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress(IPV4_ADDRESS)
            .discoveryPort(P2P_PORT);

    assertThatThrownBy(builder::build)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Listening port must be configured");
  }

  @Test
  public void builder_nodeIdNotSpecified() {
    final EnodeURLImpl.Builder builder =
        EnodeURLImpl.builder().ipAddress(IPV4_ADDRESS).discoveryAndListeningPorts(P2P_PORT);

    assertThatThrownBy(builder::build)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Node id must be configured");
  }

  @Test
  public void builder_ipNotSpecified() {
    final EnodeURLImpl.Builder builder =
        EnodeURLImpl.builder().nodeId(VALID_NODE_ID).discoveryAndListeningPorts(P2P_PORT);

    assertThatThrownBy(builder::build)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Ip address must be configured");
  }

  @Test
  public void sameListeningEndpoint_forMatchingEnodes() {
    final EnodeURLImpl enodeA =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress(IPV4_ADDRESS)
            .listeningPort(P2P_PORT)
            .discoveryPort(DISCOVERY_PORT)
            .build();
    final EnodeURLImpl enodeB =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress(IPV4_ADDRESS)
            .listeningPort(P2P_PORT)
            .discoveryPort(DISCOVERY_PORT + 1)
            .build();

    assertThat(EnodeURLImpl.sameListeningEndpoint(enodeA, enodeB)).isTrue();
  }

  @Test
  public void sameListeningEndpoint_differentListeningPorts() {
    final EnodeURLImpl enodeA =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress(IPV4_ADDRESS)
            .listeningPort(P2P_PORT)
            .discoveryPort(DISCOVERY_PORT)
            .build();
    final EnodeURLImpl enodeB =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress(IPV4_ADDRESS)
            .listeningPort(P2P_PORT + 1)
            .discoveryPort(DISCOVERY_PORT)
            .build();

    assertThat(EnodeURLImpl.sameListeningEndpoint(enodeA, enodeB)).isFalse();
  }

  @Test
  public void sameListeningEndpoint_differentIps() {
    final EnodeURLImpl enodeA =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress(IPV6_COMPACT_ADDRESS)
            .listeningPort(P2P_PORT)
            .discoveryPort(DISCOVERY_PORT)
            .build();
    final EnodeURLImpl enodeB =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress(IPV4_ADDRESS)
            .listeningPort(P2P_PORT)
            .discoveryPort(DISCOVERY_PORT)
            .build();

    assertThat(EnodeURLImpl.sameListeningEndpoint(enodeA, enodeB)).isFalse();
  }

  @Test
  public void sameListeningEndpoint_listeningDisabledForOne() {
    final EnodeURLImpl enodeA =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress(IPV4_ADDRESS)
            .disableListening()
            .discoveryPort(DISCOVERY_PORT)
            .build();
    final EnodeURLImpl enodeB =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress(IPV4_ADDRESS)
            .listeningPort(P2P_PORT)
            .discoveryPort(DISCOVERY_PORT)
            .build();

    assertThat(EnodeURLImpl.sameListeningEndpoint(enodeA, enodeB)).isFalse();
  }

  @Test
  public void sameListeningEndpoint_listeningDisabledForBoth() {
    final EnodeURLImpl enodeA =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress(IPV4_ADDRESS)
            .disableListening()
            .discoveryPort(DISCOVERY_PORT)
            .build();
    final EnodeURLImpl enodeB =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress(IPV4_ADDRESS)
            .disableListening()
            .discoveryPort(DISCOVERY_PORT)
            .build();

    assertThat(EnodeURLImpl.sameListeningEndpoint(enodeA, enodeB)).isTrue();
  }

  @Test
  public void parseNodeId_invalid() {
    final String invalidId = "0x10";
    assertThatThrownBy(() -> EnodeURLImpl.parseNodeId(invalidId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Expected 64 bytes in " + invalidId);
  }

  @Test
  public void parseNodeId_valid() {
    final String validId = VALID_NODE_ID;
    final Bytes nodeId = EnodeURLImpl.parseNodeId(validId);
    assertThat(nodeId.size()).isEqualTo(EnodeURLImpl.NODE_ID_SIZE);
    assertThat(nodeId.toUnprefixedHexString()).isEqualTo(validId);
  }

  @Test
  public void toURIWithoutDiscoveryPortShouldProduceEntryWithHostnameWhenUsingLoopbackAddress()
      throws UnknownHostException {
    String enodeString = String.format("enode://%s@%s:%d", VALID_NODE_ID, "127.0.0.1", 9999);

    final EnodeURL enodeA =
        EnodeURLImpl.fromString(
            enodeString,
            ImmutableEnodeDnsConfiguration.builder().dnsEnabled(true).updateEnabled(true).build());

    URI expected =
        URI.create(
            String.format(
                "enode://%s@%s:%d", VALID_NODE_ID, InetAddress.getLocalHost().getHostName(), 9999));
    assertThat(enodeA.toURIWithoutDiscoveryPort()).isEqualTo(expected);
  }

  @Test
  public void toURIWithoutDiscoveryPortShouldUseLoopbackWhenDomainNotFound() {
    String enodeString =
        String.format("enode://%s@%s:%d", VALID_NODE_ID, "besu-is-awesome.example.com", 9999);

    final EnodeURL enodeA =
        EnodeURLImpl.fromString(
            enodeString,
            ImmutableEnodeDnsConfiguration.builder().dnsEnabled(true).updateEnabled(true).build());

    URI expected = URI.create(String.format("enode://%s@%s:%d", VALID_NODE_ID, "127.0.0.1", 9999));
    assertThat(enodeA.toURIWithoutDiscoveryPort()).isEqualTo(expected);
  }

  @Test
  public void toURIWithoutDiscoveryPortShouldKeepDomainWhenFound() {
    String enodeString = String.format("enode://%s@%s:%d", VALID_NODE_ID, "hyperledger.org", 9999);

    final EnodeURL enodeA =
        EnodeURLImpl.fromString(
            enodeString,
            ImmutableEnodeDnsConfiguration.builder().dnsEnabled(true).updateEnabled(true).build());

    URI expected =
        URI.create(String.format("enode://%s@%s:%d", VALID_NODE_ID, "hyperledger.org", 9999));
    assertThat(enodeA.toURIWithoutDiscoveryPort()).isEqualTo(expected);
  }

  @Test
  public void getHostShouldReturnHostNameIpv4() {
    final EnodeURL enode =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress(IPV4_ADDRESS)
            .disableListening()
            .discoveryPort(DISCOVERY_PORT)
            .build();
    assertEquals(IPV4_ADDRESS, enode.getHost());
  }

  @Test
  public void getHostShouldReturnHostNameIpv6Compact() {
    final EnodeURL enode =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress(IPV6_COMPACT_ADDRESS)
            .disableListening()
            .discoveryPort(DISCOVERY_PORT)
            .build();
    assertEquals(IPV6_COMPACT_ADDRESS, enode.getHost());
  }

  @Test
  public void getHostShouldReturnHostNameIpv6CompactFromFull() {
    final EnodeURL enode =
        EnodeURLImpl.builder()
            .nodeId(VALID_NODE_ID)
            .ipAddress(IPV6_FULL_ADDRESS)
            .disableListening()
            .discoveryPort(DISCOVERY_PORT)
            .build();
    assertEquals(IPV6_COMPACT_ADDRESS, enode.getHost());
  }

  @Test
  public void getHostShouldReturnHostNameWhenHostContainsUnderscore() {
    EnodeURL enodeMock = Mockito.mock(EnodeURLImpl.class);
    URI uriMock = Mockito.mock(URI.class);
    Mockito.when(enodeMock.getHost()).thenCallRealMethod();
    Mockito.when(enodeMock.toURIWithoutDiscoveryPort()).thenReturn(uriMock);
    Mockito.when(uriMock.getHost()).thenReturn(null);
    String hostName = "host_name";
    Mockito.when(uriMock.toString())
        .thenReturn("enode://" + VALID_NODE_ID + "@" + hostName + ":9999");
    assertEquals(hostName, enodeMock.getHost());
  }

  @Test
  public void getHostShouldReturnEmptyStringWhenUriToStringDoesNotContainAt() {
    EnodeURL enodeMock = Mockito.mock(EnodeURLImpl.class);
    URI uriMock = Mockito.mock(URI.class);
    Mockito.when(enodeMock.getHost()).thenCallRealMethod();
    Mockito.when(enodeMock.toURIWithoutDiscoveryPort()).thenReturn(uriMock);
    Mockito.when(uriMock.getHost()).thenReturn(null);
    String hostName = "host_name";
    Mockito.when(uriMock.toString()).thenReturn("enode://" + VALID_NODE_ID + hostName + ":9999");
    assertEquals("", enodeMock.getHost());
  }

  @Test
  public void getHostShouldReturnEmptyStringWhenUriToStringReturnsColonBeforeAt() {
    EnodeURL enodeMock = Mockito.mock(EnodeURLImpl.class);
    URI uriMock = Mockito.mock(URI.class);
    Mockito.when(enodeMock.getHost()).thenCallRealMethod();
    Mockito.when(enodeMock.toURIWithoutDiscoveryPort()).thenReturn(uriMock);
    Mockito.when(uriMock.getHost()).thenReturn(null);
    String hostName = "host_name";
    Mockito.when(uriMock.toString()).thenReturn("enode://" + VALID_NODE_ID + "@" + hostName);
    assertEquals("", enodeMock.getHost());
  }

  @Test
  public void getHostShouldReturnHostNameWhenDomainFound() {
    String site = "hyperledger.org";
    String enodeString = String.format("enode://%s@%s:%d", VALID_NODE_ID, site, 9999);
    final EnodeURL enode =
        EnodeURLImpl.fromString(
            enodeString,
            ImmutableEnodeDnsConfiguration.builder().dnsEnabled(true).updateEnabled(true).build());
    assertEquals(site, enode.getHost());
  }

  @Test
  public void getHostShouldReturnLoopBackWhenDomainNotFound() {
    String site = "besu-is-awesome.example.com";
    String enodeString = String.format("enode://%s@%s:%d", VALID_NODE_ID, site, 9999);
    final EnodeURL enode =
        EnodeURLImpl.fromString(
            enodeString,
            ImmutableEnodeDnsConfiguration.builder().dnsEnabled(true).updateEnabled(true).build());
    assertEquals("127.0.0.1", enode.getHost());
  }
}
