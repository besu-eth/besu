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

import org.hyperledger.besu.ethereum.eth.messages.EthProtocolMessages;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;

import java.util.ArrayList;
import java.util.List;

/**
 * Eth protocol versions as defined in <a
 * href="https://github.com/ethereum/devp2p/blob/master/caps/eth.md">Ethereum Wire Protocol
 * (ETH)</a>, with the messages each of them supports. The constants are declared in ascending
 * order, so the last one is the latest version.
 */
public enum EthProtocolVersion {
  /** eth/68 */
  V68(68, Messages.ETH68),
  /**
   * eth/69 EIP-7642
   *
   * <p>Version 69 added the BlockRangeUpdate message.
   */
  V69(69, Messages.ETH69),
  /** eth/70 uses the same messages as eth/69 */
  V70(70, Messages.ETH69),
  /** eth/71 */
  V71(71, Messages.ETH71);

  private static final EthProtocolVersion[] VERSIONS = values();

  private final int version;
  private final Capability capability;
  private final int messageSpace;
  private final List<Integer> supportedMessages;

  EthProtocolVersion(final int version, final List<Integer> supportedMessages) {
    this.version = version;
    this.capability = Capability.create(EthProtocol.NAME, version);
    this.supportedMessages = supportedMessages;
    // message codes start at 0, so the space is the highest supported code plus one
    this.messageSpace = supportedMessages.stream().mapToInt(Integer::intValue).max().orElse(-1) + 1;
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
   * The eth capability advertised for this version.
   *
   * @return the capability
   */
  public Capability getCapability() {
    return capability;
  }

  /**
   * The number of message codes reserved by this version, which is the highest supported message
   * code plus one.
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
   * The latest known version.
   *
   * @return the latest version
   */
  public static EthProtocolVersion latest() {
    return VERSIONS[VERSIONS.length - 1];
  }

  /**
   * Whether a raw version number, possibly not a known one, uses the eth/69+ status layout.
   *
   * @param protocolVersion the raw protocol version number
   * @return true if the version is 69 or later
   */
  public static boolean hasBlockRange(final int protocolVersion) {
    return protocolVersion >= V69.version;
  }

  /**
   * Finds the protocol version matching a raw version number. This is called for every message
   * sent, so it does not allocate.
   *
   * @param protocolVersion the raw protocol version number
   * @return the matching version, or null if it is not a known one
   */
  public static EthProtocolVersion fromVersion(final int protocolVersion) {
    for (final EthProtocolVersion v : VERSIONS) {
      if (v.version == protocolVersion) {
        return v;
      }
    }
    return null;
  }

  // Held in a nested class since an enum constant can't reference the enum's own static fields
  private static final class Messages {
    private static final List<Integer> ETH68 =
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

    private static final List<Integer> ETH69 = with(ETH68, EthProtocolMessages.BLOCK_RANGE_UPDATE);

    private static final List<Integer> ETH71 =
        with(
            ETH69,
            EthProtocolMessages.GET_BLOCK_ACCESS_LISTS,
            EthProtocolMessages.BLOCK_ACCESS_LISTS);

    private static List<Integer> with(final List<Integer> previous, final Integer... added) {
      final List<Integer> messages = new ArrayList<>(previous);
      messages.addAll(List.of(added));
      return List.copyOf(messages);
    }
  }
}
