/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.eth;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.eth.messages.EthProtocolMessages;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class EthProtocolTest {

  private static final List<Integer> ETH68_MESSAGES =
      List.of(
          EthProtocolMessages.STATUS,
          EthProtocolMessages.NEW_BLOCK_HASHES,
          EthProtocolMessages.TRANSACTIONS,
          EthProtocolMessages.GET_BLOCK_HEADERS,
          EthProtocolMessages.BLOCK_HEADERS,
          EthProtocolMessages.GET_BLOCK_BODIES,
          EthProtocolMessages.BLOCK_BODIES,
          EthProtocolMessages.NEW_BLOCK,
          EthProtocolMessages.GET_RECEIPTS,
          EthProtocolMessages.RECEIPTS,
          EthProtocolMessages.NEW_POOLED_TRANSACTION_HASHES,
          EthProtocolMessages.GET_POOLED_TRANSACTIONS,
          EthProtocolMessages.POOLED_TRANSACTIONS);

  private static final List<Integer> ETH69_MESSAGES =
      concat(ETH68_MESSAGES, EthProtocolMessages.BLOCK_RANGE_UPDATE);

  private static final List<Integer> ETH71_MESSAGES =
      concat(
          ETH69_MESSAGES,
          EthProtocolMessages.GET_BLOCK_ACCESS_LISTS,
          EthProtocolMessages.BLOCK_ACCESS_LISTS);

  private static final List<Integer> ETH72_MESSAGES =
      concat(ETH71_MESSAGES, EthProtocolMessages.GET_CELLS, EthProtocolMessages.CELLS);

  // The highest message code in the protocol, one past it is never valid
  private static final int MAX_CODE = EthProtocolMessages.CELLS;

  private static List<Integer> concat(final List<Integer> messages, final Integer... added) {
    return Stream.concat(messages.stream(), Stream.of(added)).toList();
  }

  static Stream<Arguments> versions() {
    return Stream.of(
        Arguments.of(67, 0, List.of()),
        Arguments.of(68, 17, ETH68_MESSAGES),
        Arguments.of(69, 18, ETH69_MESSAGES),
        Arguments.of(70, 18, ETH69_MESSAGES),
        Arguments.of(71, 20, ETH71_MESSAGES),
        Arguments.of(72, 22, ETH72_MESSAGES),
        Arguments.of(73, 0, List.of()));
  }

  @ParameterizedTest(name = "eth/{0}")
  @MethodSource("versions")
  void messageSpace(final int version, final int expectedSpace, final List<Integer> ignored) {
    assertThat(EthProtocol.get().messageSpace(version)).isEqualTo(expectedSpace);
  }

  @ParameterizedTest(name = "eth/{0}")
  @MethodSource("versions")
  void isValidMessageCode(
      final int version, final int ignored, final List<Integer> expectedMessages) {
    for (int code = 0; code <= MAX_CODE + 1; code++) {
      assertThat(EthProtocol.get().isValidMessageCode(version, code))
          .describedAs("eth/%d message code 0x%02x", version, code)
          .isEqualTo(expectedMessages.contains(code));
    }
  }

  @Test
  void latestIsTheLastAdvertisedVersion() {
    assertThat(EthProtocol.LATEST).isEqualTo(EthProtocolVersion.V71.getCapability());
  }

  @Test
  void ethSeventyTwoIsKnownButNotAdvertised() {
    // Its messages encode, decode and validate, so the wire format can be exercised, but no peer
    // can negotiate it until the rest of the sparse blobpool is in place.
    assertThat(EthProtocolVersion.V72.isAdvertised()).isFalse();
    assertThat(EthProtocolVersion.latest()).isEqualTo(EthProtocolVersion.V71);
    assertThat(EthProtocol.get().isValidMessageCode(72, EthProtocolMessages.GET_CELLS)).isTrue();
  }

  @Test
  void theCellMessagesAreFramedWithARequestId() {
    // GetCells and Cells are a request and its response, so they are framed with a request id like
    // every other such pair. Without it EthProtocolManager hands the server the id as part of the
    // request body and sends the response back with none, which no requester can match to its
    // request.
    assertThat(EthProtocol.requestIdCompatible(EthProtocolMessages.GET_CELLS)).isTrue();
    assertThat(EthProtocol.requestIdCompatible(EthProtocolMessages.CELLS)).isTrue();
  }

  @Test
  void theCellMessagesAreNamed() {
    assertThat(EthProtocol.get().messageName(72, EthProtocolMessages.GET_CELLS))
        .isEqualTo("GetCells");
    assertThat(EthProtocol.get().messageName(72, EthProtocolMessages.CELLS)).isEqualTo("Cells");
  }

  @Test
  void everyVersionHasAnEthCapability() {
    for (final EthProtocolVersion version : EthProtocolVersion.values()) {
      assertThat(version.getCapability().getName()).isEqualTo(EthProtocol.NAME);
      assertThat(version.getCapability().getVersion()).isEqualTo(version.getVersion());
    }
  }
}
