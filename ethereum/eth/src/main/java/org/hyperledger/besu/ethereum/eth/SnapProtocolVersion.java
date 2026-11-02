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
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;

import java.util.List;

/**
 * Snap protocol versions as defined in <a
 * href="https://github.com/ethereum/devp2p/blob/master/caps/snap.md">Snap Protocol</a>, with the
 * messages each of them supports.
 */
public enum SnapProtocolVersion {
  /** snap/1 */
  V1(
      1,
      17,
      List.of(
          SnapV1.GET_ACCOUNT_RANGE,
          SnapV1.ACCOUNT_RANGE,
          SnapV1.GET_STORAGE_RANGE,
          SnapV1.STORAGE_RANGE,
          SnapV1.GET_BYTECODES,
          SnapV1.BYTECODES,
          SnapV1.GET_TRIE_NODES,
          SnapV1.TRIE_NODES)),
  /** snap/2 EIP-8189 */
  V2(
      2,
      17,
      List.of(
          SnapV2.GET_ACCOUNT_RANGE,
          SnapV2.ACCOUNT_RANGE,
          SnapV2.GET_STORAGE_RANGE,
          SnapV2.STORAGE_RANGE,
          SnapV2.GET_BYTECODES,
          SnapV2.BYTECODES,
          SnapV2.GET_BLOCK_ACCESS_LISTS,
          SnapV2.BLOCK_ACCESS_LISTS));

  private static final SnapProtocolVersion[] VERSIONS = values();

  private final int version;
  private final Capability capability;
  private final int messageSpace;
  private final List<Integer> supportedMessages;

  SnapProtocolVersion(
      final int version, final int messageSpace, final List<Integer> supportedMessages) {
    this.version = version;
    this.capability = Capability.create(SnapProtocol.NAME, version);
    this.messageSpace = messageSpace;
    this.supportedMessages = supportedMessages;
  }

  /**
   * The version number as exchanged on the wire.
   *
   * @return the version number
   */
  public int getVersion() {
    return version;
  }

  /**
   * The snap capability advertised for this version.
   *
   * @return the capability
   */
  public Capability getCapability() {
    return capability;
  }

  /**
   * The number of message codes reserved by this version. It is not derived from the supported
   * messages, since it is a wire level value that peers must agree on.
   *
   * @return the message space size
   */
  public int getMessageSpace() {
    return messageSpace;
  }

  /**
   * The codes of the messages supported by this version.
   *
   * @return a list containing the codes of supported messages
   */
  public List<Integer> getSupportedMessages() {
    return supportedMessages;
  }

  /**
   * Finds the protocol version matching a raw version number. This is called for every message
   * sent, so it does not allocate.
   *
   * @param protocolVersion the raw protocol version number
   * @return the matching version, or null if it is not a known one
   */
  public static SnapProtocolVersion fromVersion(final int protocolVersion) {
    for (final SnapProtocolVersion v : VERSIONS) {
      if (v.version == protocolVersion) {
        return v;
      }
    }
    return null;
  }
}
