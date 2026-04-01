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
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.SubProtocol;

import java.util.BitSet;
import java.util.Set;

/**
 * Eth protocol messages as defined in <a
 * href="https://github.com/ethereum/devp2p/blob/master/caps/eth.md">Ethereum Wire Protocol
 * (ETH)</a>
 */
public class EthProtocol implements SubProtocol {
  public static final String NAME = "eth";
  private static final EthProtocol INSTANCE = new EthProtocol();
  public static final Capability ETH68 = Capability.create(NAME, EthProtocolVersion.V68);
  public static final Capability ETH69 = Capability.create(NAME, EthProtocolVersion.V69);
  public static final Capability ETH70 = Capability.create(NAME, EthProtocolVersion.V70);
  public static final Capability ETH71 = Capability.create(NAME, EthProtocolVersion.V71);
  public static final BitSet REQUEST_ID_MESSAGES;

  static {
    final var requestIdMessages =
        Set.of(
            EthProtocolMessages.GET_BLOCK_HEADERS,
            EthProtocolMessages.BLOCK_HEADERS,
            EthProtocolMessages.GET_BLOCK_BODIES,
            EthProtocolMessages.BLOCK_BODIES,
            EthProtocolMessages.GET_POOLED_TRANSACTIONS,
            EthProtocolMessages.POOLED_TRANSACTIONS,
            EthProtocolMessages.GET_NODE_DATA,
            EthProtocolMessages.NODE_DATA,
            EthProtocolMessages.GET_RECEIPTS,
            EthProtocolMessages.RECEIPTS,
            EthProtocolMessages.GET_BLOCK_ACCESS_LISTS,
            EthProtocolMessages.BLOCK_ACCESS_LISTS);
    REQUEST_ID_MESSAGES =
        new BitSet(requestIdMessages.stream().mapToInt(i -> i).max().getAsInt() + 1);
    requestIdMessages.forEach(REQUEST_ID_MESSAGES::set);
  }

  // Latest version of the Eth protocol
  public static final Capability LATEST = ETH71;

  public static boolean requestIdCompatible(final int code) {
    return REQUEST_ID_MESSAGES.get(code);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int messageSpace(final int protocolVersion) {
    return switch (protocolVersion) {
      case EthProtocolVersion.V68 -> 17;
      case EthProtocolVersion.V69, EthProtocolVersion.V70 -> 18;
      case EthProtocolVersion.V71 -> 20;
      default -> 0;
    };
  }

  @Override
  public boolean isValidMessageCode(final int protocolVersion, final int code) {
    return EthProtocolVersion.getSupportedMessages(protocolVersion).contains(code);
  }

  @Override
  public String messageName(final int protocolVersion, final int code) {
    return switch (code) {
      case EthProtocolMessages.STATUS -> "Status";
      case EthProtocolMessages.NEW_BLOCK_HASHES -> "NewBlockHashes";
      case EthProtocolMessages.TRANSACTIONS -> "Transactions";
      case EthProtocolMessages.GET_BLOCK_HEADERS -> "GetBlockHeaders";
      case EthProtocolMessages.BLOCK_HEADERS -> "BlockHeaders";
      case EthProtocolMessages.GET_BLOCK_BODIES -> "GetBlockBodies";
      case EthProtocolMessages.BLOCK_BODIES -> "BlockBodies";
      case EthProtocolMessages.NEW_BLOCK -> "NewBlock";
      case EthProtocolMessages.NEW_POOLED_TRANSACTION_HASHES -> "NewPooledTransactionHashes";
      case EthProtocolMessages.GET_POOLED_TRANSACTIONS -> "GetPooledTransactions";
      case EthProtocolMessages.POOLED_TRANSACTIONS -> "PooledTransactions";
      case EthProtocolMessages.GET_NODE_DATA -> "GetNodeData";
      case EthProtocolMessages.NODE_DATA -> "NodeData";
      case EthProtocolMessages.GET_RECEIPTS -> "GetReceipts";
      case EthProtocolMessages.RECEIPTS -> "Receipts";
      case EthProtocolMessages.BLOCK_RANGE_UPDATE -> "BlockRangeUpdate";
      case EthProtocolMessages.GET_BLOCK_ACCESS_LISTS -> "GetBlockAccessLists";
      case EthProtocolMessages.BLOCK_ACCESS_LISTS -> "BlockAccessLists";
      default -> INVALID_MESSAGE_NAME;
    };
  }

  public static EthProtocol get() {
    return INSTANCE;
  }

  public static boolean isEth69Compatible(final Capability capability) {
    return NAME.equals(capability.getName()) && capability.getVersion() >= ETH69.getVersion();
  }

  public static boolean isEth70Compatible(final Capability capability) {
    return NAME.equals(capability.getName()) && capability.getVersion() >= ETH70.getVersion();
  }

  public static boolean isEth71Compatible(final Capability capability) {
    return NAME.equals(capability.getName()) && capability.getVersion() >= ETH71.getVersion();
  }
}
