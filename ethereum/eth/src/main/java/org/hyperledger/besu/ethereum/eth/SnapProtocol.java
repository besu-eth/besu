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
package org.hyperledger.besu.ethereum.eth;

import org.hyperledger.besu.ethereum.eth.messages.snap.SnapV1;
import org.hyperledger.besu.ethereum.eth.messages.snap.SnapV2;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.SubProtocol;

/**
 * Snap protocol messages as defined in https://github.com/ethereum/devp2p/blob/master/caps/snap.md}
 */
public class SnapProtocol implements SubProtocol {
  public static final String NAME = "snap";
  public static final Capability SNAP1 = Capability.create(NAME, SnapProtocolVersion.V1);
  public static final Capability SNAP2 = Capability.create(NAME, SnapProtocolVersion.V2);

  private static final SnapProtocol INSTANCE = new SnapProtocol();

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int messageSpace(final int protocolVersion) {
    return switch (protocolVersion) {
      case SnapProtocolVersion.V1, SnapProtocolVersion.V2 -> 17;
      default -> 0;
    };
  }

  @Override
  public boolean isValidMessageCode(final int protocolVersion, final int code) {
    return SnapProtocolVersion.getSupportedMessages(protocolVersion).contains(code);
  }

  @Override
  public String messageName(final int protocolVersion, final int code) {
    return switch (protocolVersion) {
      case SnapProtocolVersion.V1 -> messageNameV1(code);
      case SnapProtocolVersion.V2 -> messageNameV2(code);
      default -> INVALID_MESSAGE_NAME;
    };
  }

  private String messageNameV1(final int code) {
    return switch (code) {
      case SnapV1.GET_ACCOUNT_RANGE -> "GetAccountRange";
      case SnapV1.ACCOUNT_RANGE -> "AccountRange";
      case SnapV1.GET_STORAGE_RANGE -> "GetStorageRange";
      case SnapV1.STORAGE_RANGE -> "StorageRange";
      case SnapV1.GET_BYTECODES -> "GetBytecodes";
      case SnapV1.BYTECODES -> "Bytecodes";
      case SnapV1.GET_TRIE_NODES -> "GetTrieNodes";
      case SnapV1.TRIE_NODES -> "TrieNodes";
      default -> INVALID_MESSAGE_NAME;
    };
  }

  private String messageNameV2(final int code) {
    return switch (code) {
      case SnapV2.GET_ACCOUNT_RANGE -> "GetAccountRange";
      case SnapV2.ACCOUNT_RANGE -> "AccountRange";
      case SnapV2.GET_STORAGE_RANGE -> "GetStorageRange";
      case SnapV2.STORAGE_RANGE -> "StorageRange";
      case SnapV2.GET_BYTECODES -> "GetBytecodes";
      case SnapV2.BYTECODES -> "Bytecodes";
      case SnapV2.GET_BLOCK_ACCESS_LISTS -> "GetBlockAccessLists";
      case SnapV2.BLOCK_ACCESS_LISTS -> "BlockAccessLists";
      default -> INVALID_MESSAGE_NAME;
    };
  }

  public static SnapProtocol get() {
    return INSTANCE;
  }
}
