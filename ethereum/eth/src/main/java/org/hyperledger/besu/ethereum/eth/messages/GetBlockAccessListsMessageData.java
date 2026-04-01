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
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

public final class GetBlockAccessListsMessageData {
  private GetBlockAccessListsMessageData() {}

  public static Bytes encode(
      final Optional<BigInteger> requestId, final Iterable<Hash> blockHashes) {
    final BytesValueRLPOutput output = new BytesValueRLPOutput();
    output.startList();
    requestId.ifPresent(output::writeBigIntegerScalar);
    blockHashes.forEach(hash -> output.writeBytes(hash.getBytes()));
    output.endList();
    return output.encoded();
  }

  public static Iterable<Hash> decode(final Bytes data, final boolean withRequestId) {
    return () ->
        new Iterator<>() {
          private final RLPInput input = new BytesValueRLPInput(data, false);
          private boolean initialized = false;

          private void ensureInitialized() {
            if (!initialized) {
              input.enterList();
              if (withRequestId) {
                input.skipNext();
              }
              initialized = true;
            }
          }

          @Override
          public boolean hasNext() {
            ensureInitialized();
            return !input.isEndOfCurrentList();
          }

          @Override
          public Hash next() {
            ensureInitialized();
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
            return Hash.wrap(input.readBytes32());
          }
        };
  }
}
