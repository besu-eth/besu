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
package org.hyperledger.besu.ethereum.eth.messages;

import org.hyperledger.besu.ethereum.core.encoding.BlockAccessListDecoder;
import org.hyperledger.besu.ethereum.core.encoding.BlockAccessListEncoder;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

public final class BlockAccessListsMessageData {
  private BlockAccessListsMessageData() {}

  public static Bytes encodeEthResponse(
      final Iterable<Optional<BlockAccessList>> blockAccessLists) {
    final BytesValueRLPOutput output = new BytesValueRLPOutput();
    output.startList();
    blockAccessLists.forEach(
        maybeBlockAccessList -> writeBlockAccessList(output, maybeBlockAccessList));
    output.endList();
    return output.encoded();
  }

  public static Bytes encodeSnapResponse(
      final Iterable<Optional<BlockAccessList>> blockAccessLists) {
    final BytesValueRLPOutput output = new BytesValueRLPOutput();
    output.startList();
    // Snap splices request-id into this body: [[access-lists]].
    output.writeRaw(encodeEthResponse(blockAccessLists));
    output.endList();
    return output.encoded();
  }

  public static Iterable<Optional<BlockAccessList>> decodeEthResponse(final Bytes data) {
    return decodeEntries(
        data,
        false,
        false,
        input -> {
          if (input.nextIsNull()) {
            input.skipNext();
            return Optional.empty();
          }
          return Optional.of(BlockAccessListDecoder.decode(input.readAsRlp()));
        });
  }

  // snap outbound bodies before wrapMessageData: [[access-lists]]
  // snap wire payloads after wrapMessageData: [request-id, [access-lists]]
  // AbstractSnapMessageData.unwrapMessageData extracts the id but preserves the wire payload.
  // Decode withRequestId=true for inbound snap responses; false only for pre-wrap message bodies.
  public static Iterable<Optional<BlockAccessList>> decodeSnapResponse(
      final Bytes data, final boolean withRequestId) {
    return decodeEntries(
        data,
        withRequestId,
        true,
        input -> {
          if (input.nextIsNull()) {
            input.skipNext();
            return Optional.empty();
          }
          return Optional.of(BlockAccessListDecoder.decode(input.readAsRlp()));
        });
  }

  public static Iterable<Bytes> decodeEthResponseRaw(final Bytes data) {
    return decodeEntries(data, false, false, input -> input.readAsRlp().raw());
  }

  public static Iterable<Bytes> decodeSnapResponseRaw(
      final Bytes data, final boolean withRequestId) {
    return decodeEntries(data, withRequestId, true, input -> input.readAsRlp().raw());
  }

  private static void writeBlockAccessList(
      final BytesValueRLPOutput output, final Optional<BlockAccessList> maybeBlockAccessList) {
    if (maybeBlockAccessList.isPresent()) {
      final BlockAccessList blockAccessList = maybeBlockAccessList.get();
      if (blockAccessList.rawRlp().isPresent()) {
        output.writeRaw(blockAccessList.rawRlp().get());
      } else {
        BlockAccessListEncoder.encode(blockAccessList, output);
      }
    } else {
      output.writeBytes(Bytes.EMPTY);
    }
  }

  private static <T> Iterable<T> decodeEntries(
      final Bytes data,
      final boolean withRequestId,
      final boolean nested,
      final EntryReader<T> entryReader) {
    // eth body: [access-lists]; snap body: [request-id?, [access-lists]].
    return () ->
        new Iterator<>() {
          private final RLPInput input = new BytesValueRLPInput(data, false);
          private boolean initialized = false;
          private boolean complete = false;

          private void ensureInitialized() {
            if (!initialized) {
              input.enterList();
              if (withRequestId) {
                input.skipNext();
              }
              // Snap has an extra list level around access-lists; eth is already there.
              if (nested) {
                input.enterList();
              }
              initialized = true;
            }
          }

          @Override
          public boolean hasNext() {
            if (complete) {
              return false;
            }
            ensureInitialized();
            if (!input.isEndOfCurrentList()) {
              return true;
            }
            if (nested) {
              input.leaveListLenient();
            }
            input.leaveListLenient();
            complete = true;
            return false;
          }

          @Override
          public T next() {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
            return entryReader.read(input);
          }
        };
  }

  @FunctionalInterface
  private interface EntryReader<T> {
    T read(RLPInput input);
  }
}
