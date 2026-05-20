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
package org.hyperledger.besu.ethereum.eth.messages.snap;

import org.hyperledger.besu.ethereum.p2p.rlpx.wire.AbstractSnapMessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import kotlin.collections.ArrayDeque;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.immutables.value.Value;

public final class StorageRangeMessage extends AbstractSnapMessageData {

  public StorageRangeMessage(final Bytes data) {
    super(data);
  }

  public static StorageRangeMessage readFrom(final MessageData message) {
    if (message instanceof StorageRangeMessage) {
      return (StorageRangeMessage) message;
    }
    final int code = message.getCode();
    if (code != SnapV1.STORAGE_RANGE) {
      throw new IllegalArgumentException(
          String.format("Message has code %d and thus is not a StorageRangeMessage.", code));
    }
    return new StorageRangeMessage(message.getData());
  }

  public static StorageRangeMessage create(
      final ArrayDeque<NavigableMap<Bytes32, Bytes>> slots, final List<Bytes> proof) {
    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    tmp.startList();
    tmp.writeList(
        slots,
        (accountList, accountRlpOutput) ->
            accountRlpOutput.writeList(
                accountList.entrySet(),
                (entry, slotRlpOutput) -> {
                  slotRlpOutput.startList();
                  slotRlpOutput.writeBytes(entry.getKey());
                  slotRlpOutput.writeBytes(entry.getValue());
                  slotRlpOutput.endList();
                }));
    tmp.writeList(proof, (bytes, rlpOutput) -> rlpOutput.writeBytes(bytes));
    tmp.endList();
    return new StorageRangeMessage(tmp.encoded());
  }

  @Override
  public int getCode() {
    return SnapV1.STORAGE_RANGE;
  }

  public SlotRangeData slotsData(final boolean withRequestId) {
    final ArrayDeque<NavigableMap<Bytes32, Bytes>> slots = new ArrayDeque<>();
    final ArrayDeque<Bytes> proofs = new ArrayDeque<>();
    final RLPInput input = new BytesValueRLPInput(data, false);
    input.enterList();

    if (withRequestId) input.skipNext();

    input.readList(
        accountRlpInput -> {
          slots.add(new TreeMap<>());
          return accountRlpInput.readList(
              slotRlpInput -> {
                slotRlpInput.enterList();
                slots.last().put(slotRlpInput.readBytes32(), slotRlpInput.readBytes());
                slotRlpInput.leaveList();
                return Void.TYPE; // we don't need the response
              });
        });

    input.enterList();
    while (!input.isEndOfCurrentList()) {
      proofs.add(input.readBytes());
    }
    input.leaveList();

    input.leaveList();
    return ImmutableSlotRangeData.builder().slots(slots).proofs(proofs).build();
  }

  @Value.Immutable
  public interface SlotRangeData {

    ArrayDeque<NavigableMap<Bytes32, Bytes>> slots();

    ArrayDeque<Bytes> proofs();
  }
}
