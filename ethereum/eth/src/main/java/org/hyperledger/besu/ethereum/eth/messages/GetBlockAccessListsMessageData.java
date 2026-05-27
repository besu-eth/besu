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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.tuweni.bytes.Bytes;

public final class GetBlockAccessListsMessageData {
  private GetBlockAccessListsMessageData() {}

  public static Bytes encodeEthRequest(final Iterable<Hash> blockHashes) {
    final BytesValueRLPOutput output = new BytesValueRLPOutput();
    output.startList();
    blockHashes.forEach(hash -> output.writeBytes(hash.getBytes()));
    output.endList();
    return output.encoded();
  }

  public static Bytes encodeSnapRequest(
      final Iterable<Hash> blockHashes, final BigInteger responseBytes) {
    final BytesValueRLPOutput output = new BytesValueRLPOutput();
    output.startList();
    // Snap splices request-id into this body: [[hashes], bytes].
    output.writeRaw(encodeEthRequest(blockHashes));
    output.writeBigIntegerScalar(responseBytes);
    output.endList();
    return output.encoded();
  }

  public static Iterable<Hash> decodeEthRequest(final Bytes data) {
    return decodeHashes(data, false, false);
  }

  // snap outbound bodies before wrapMessageData: [[hashes], bytes]
  // snap wire payloads after wrapMessageData: [request-id, [hashes], bytes]
  // AbstractSnapMessageData.unwrapMessageData extracts the id but preserves the wire payload.
  // Decode withRequestId=true for inbound snap requests; false only for pre-wrap message bodies.
  public static Iterable<Hash> decodeSnapRequest(final Bytes data, final boolean withRequestId) {
    return decodeHashes(data, withRequestId, true);
  }

  public static BigInteger decodeSnapResponseBytes(final Bytes data, final boolean withRequestId) {
    final RLPInput input = new BytesValueRLPInput(data, false);
    input.enterList();
    if (withRequestId) {
      input.skipNext();
    }
    // Skip [hashes], then read snap's trailing bytes limit.
    input.enterList();
    while (!input.isEndOfCurrentList()) {
      input.skipNext();
    }
    input.leaveListLenient();
    final BigInteger responseBytes = input.readBigIntegerScalar();
    input.leaveListLenient();
    return responseBytes;
  }

  private static Iterable<Hash> decodeHashes(
      final Bytes data, final boolean withRequestId, final boolean nested) {
    // eth body: [hashes]; snap body: [request-id?, [hashes], bytes].
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
              // Snap has an extra list level around hashes; eth is already there.
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
            // After snap's hash list, discard fields not used by the hash iterator.
            if (nested) {
              input.leaveListLenient();
              if (!input.isEndOfCurrentList()) {
                input.skipNext();
              }
            }
            input.leaveListLenient();
            complete = true;
            return false;
          }

          @Override
          public Hash next() {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
            return Hash.wrap(input.readBytes32());
          }
        };
  }
}
