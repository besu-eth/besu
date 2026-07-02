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
package org.hyperledger.besu.consensus.qbft.adaptor;

import org.hyperledger.besu.consensus.qbft.core.types.QbftBlock;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockHeader;
import org.hyperledger.besu.consensus.qbft.core.types.QbftMessage;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Message;

/** Utility class to convert between Besu and QBFT types. */
public class AdaptorUtil {

  /** Private constructor to prevent instantiation. */
  private AdaptorUtil() {}

  /**
   * Convert a QBFT block to a Besu block.
   *
   * @param block the QBFT block
   * @return the Besu block
   */
  public static Block toBesuBlock(final QbftBlock block) {
    if (block instanceof QbftBlockAdaptor qbftBlockAdaptor) {
      return qbftBlockAdaptor.getBesuBlock();
    } else {
      throw new IllegalArgumentException("Unsupported block type");
    }
  }

  /**
   * Convert a QBFT block header to a Besu block header.
   *
   * @param header the QBFT block header
   * @return the Besu block header
   */
  public static BlockHeader toBesuBlockHeader(final QbftBlockHeader header) {
    if (header instanceof QbftBlockHeaderAdaptor qbftBlockHeaderAdaptor) {
      return qbftBlockHeaderAdaptor.getBesuBlockHeader();
    } else {
      throw new IllegalArgumentException("Unsupported block header type");
    }
  }

  /**
   * Convert a QBFT message to a Besu message.
   *
   * @param message the QBFT message
   * @return the Besu message
   */
  public static Message toBesuMessage(final QbftMessage message) {
    if (message instanceof QbftMessageAdaptor qbftMessageAdaptor) {
      return qbftMessageAdaptor.getBesuMessage();
    } else {
      throw new IllegalArgumentException("Unsupported message type");
    }
  }
}
