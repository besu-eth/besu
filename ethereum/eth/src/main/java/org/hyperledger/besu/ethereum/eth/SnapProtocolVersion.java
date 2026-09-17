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

import org.hyperledger.besu.ethereum.eth.messages.snap.SnapV1;
import org.hyperledger.besu.ethereum.eth.messages.snap.SnapV2;

import java.util.Collections;
import java.util.List;

/**
 * Snap protocol messages as defined in <a
 * href="https://github.com/ethereum/devp2p/blob/master/caps/snap.md">Snap Protocol</a>
 */
public class SnapProtocolVersion {
  public static final int V1 = 1;
  public static final int V2 = 2;

  private static final List<Integer> snap1Messages =
      List.of(
          SnapV1.GET_ACCOUNT_RANGE,
          SnapV1.ACCOUNT_RANGE,
          SnapV1.GET_STORAGE_RANGE,
          SnapV1.STORAGE_RANGE,
          SnapV1.GET_BYTECODES,
          SnapV1.BYTECODES,
          SnapV1.GET_TRIE_NODES,
          SnapV1.TRIE_NODES);

  private static final List<Integer> snap2Messages =
      List.of(
          SnapV2.GET_ACCOUNT_RANGE,
          SnapV2.ACCOUNT_RANGE,
          SnapV2.GET_STORAGE_RANGE,
          SnapV2.STORAGE_RANGE,
          SnapV2.GET_BYTECODES,
          SnapV2.BYTECODES,
          SnapV2.GET_BLOCK_ACCESS_LISTS,
          SnapV2.BLOCK_ACCESS_LISTS);

  /**
   * Returns a list of integers containing the supported messages given the protocol version.
   *
   * @param protocolVersion the protocol version
   * @return a list containing the codes of supported messages
   */
  public static List<Integer> getSupportedMessages(final int protocolVersion) {
    return switch (protocolVersion) {
      case SnapProtocolVersion.V1 -> snap1Messages;
      case SnapProtocolVersion.V2 -> snap2Messages;
      default -> Collections.emptyList();
    };
  }
}
