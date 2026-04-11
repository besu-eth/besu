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
package org.hyperledger.besu.ethereum.eth.messages;

import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import org.apache.tuweni.bytes.Bytes;

public final class PaginatedReceiptsMessage extends ReceiptsMessage {
  private Boolean lastBlockIncomplete;

  private PaginatedReceiptsMessage(final Bytes data, final Boolean lastBlockIncomplete) {
    super(data);
    this.lastBlockIncomplete = lastBlockIncomplete;
  }

  public static PaginatedReceiptsMessage readFrom(final MessageData message) {
    if (message instanceof PaginatedReceiptsMessage) {
      return (PaginatedReceiptsMessage) message;
    }
    final int code = message.getCode();
    if (code != EthProtocolMessages.RECEIPTS) {
      throw new IllegalArgumentException(
          String.format("Message has code %d and thus is not a ReceiptsMessage.", code));
    }

    return new PaginatedReceiptsMessage(message.getData(), null);
  }

  public static PaginatedReceiptsMessage createUnsafe(
      final Bytes data, final boolean lastBlockIncomplete) {
    return new PaginatedReceiptsMessage(data, lastBlockIncomplete);
  }

  public boolean lastBlockIncomplete() {
    if (lastBlockIncomplete == null) {
      deserialize();
    }
    return lastBlockIncomplete;
  }

  @Override
  protected void deserialize() {
    final RLPInput input = new BytesValueRLPInput(data, false);
    lastBlockIncomplete = input.readLongScalar() == 1;
    deserializeReceiptLists(input);
  }
}
