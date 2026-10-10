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

import org.hyperledger.besu.ethereum.eth.messages.snap.SnapV1;
import org.hyperledger.besu.ethereum.eth.messages.snap.SnapV2;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.SubProtocol;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class SnapProtocolTest {

  private static final List<Integer> SNAP1_MESSAGES =
      List.of(
          SnapV1.GET_ACCOUNT_RANGE,
          SnapV1.ACCOUNT_RANGE,
          SnapV1.GET_STORAGE_RANGE,
          SnapV1.STORAGE_RANGE,
          SnapV1.GET_BYTECODES,
          SnapV1.BYTECODES,
          SnapV1.GET_TRIE_NODES,
          SnapV1.TRIE_NODES);

  private static final List<Integer> SNAP2_MESSAGES =
      List.of(
          SnapV2.GET_ACCOUNT_RANGE,
          SnapV2.ACCOUNT_RANGE,
          SnapV2.GET_STORAGE_RANGE,
          SnapV2.STORAGE_RANGE,
          SnapV2.GET_BYTECODES,
          SnapV2.BYTECODES,
          SnapV2.GET_BLOCK_ACCESS_LISTS,
          SnapV2.BLOCK_ACCESS_LISTS);

  // One past the highest message code of any version, which is never valid
  private static final int PAST_MAX_CODE = SnapV2.BLOCK_ACCESS_LISTS + 1;

  static Stream<Arguments> versions() {
    return Stream.of(
        Arguments.of(0, 0, List.of()),
        Arguments.of(1, 17, SNAP1_MESSAGES),
        Arguments.of(2, 17, SNAP2_MESSAGES),
        Arguments.of(3, 0, List.of()));
  }

  @ParameterizedTest(name = "snap/{0}")
  @MethodSource("versions")
  void messageSpace(final int version, final int expectedSpace, final List<Integer> ignored) {
    assertThat(SnapProtocol.get().messageSpace(version)).isEqualTo(expectedSpace);
  }

  @ParameterizedTest(name = "snap/{0}")
  @MethodSource("versions")
  void isValidMessageCode(
      final int version, final int ignored, final List<Integer> expectedMessages) {
    for (int code = 0; code <= PAST_MAX_CODE; code++) {
      assertThat(SnapProtocol.get().isValidMessageCode(version, code))
          .describedAs("snap/%d message code 0x%02x", version, code)
          .isEqualTo(expectedMessages.contains(code));
    }
  }

  @Test
  void messageSpaceCoversEverySupportedMessageCode() {
    for (final SnapProtocolVersion version : SnapProtocolVersion.values()) {
      assertThat(version.getSupportedMessages())
          .describedAs("snap/%d supported messages", version.getVersion())
          .allSatisfy(code -> assertThat(code).isLessThan(version.getMessageSpace()));
    }
  }

  @Test
  void messageNamesDependOnTheVersion() {
    final SnapProtocol snap = SnapProtocol.get();
    assertThat(snap.messageName(1, SnapV1.GET_TRIE_NODES)).isEqualTo("GetTrieNodes");
    assertThat(snap.messageName(2, SnapV2.GET_BLOCK_ACCESS_LISTS)).isEqualTo("GetBlockAccessLists");
    // trie nodes were removed in snap/2, and block access lists did not exist in snap/1
    assertThat(snap.messageName(2, SnapV1.GET_TRIE_NODES))
        .isEqualTo(SubProtocol.INVALID_MESSAGE_NAME);
    assertThat(snap.messageName(1, SnapV2.GET_BLOCK_ACCESS_LISTS))
        .isEqualTo(SubProtocol.INVALID_MESSAGE_NAME);
    assertThat(snap.messageName(3, SnapV1.GET_ACCOUNT_RANGE))
        .isEqualTo(SubProtocol.INVALID_MESSAGE_NAME);
  }

  @Test
  void everyVersionHasASnapCapability() {
    for (final SnapProtocolVersion version : SnapProtocolVersion.values()) {
      assertThat(version.getCapability().getName()).isEqualTo(SnapProtocol.NAME);
      assertThat(version.getCapability().getVersion()).isEqualTo(version.getVersion());
    }
  }
}
