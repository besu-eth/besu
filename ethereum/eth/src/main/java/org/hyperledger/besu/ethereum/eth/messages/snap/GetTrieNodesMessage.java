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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.AbstractSnapMessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.immutables.value.Value;

public final class GetTrieNodesMessage extends AbstractSnapMessageData {

  // Compact-encoded Keccak256 hash is at most 33 bytes (1 metadata + 32 data)
  static final int MAX_PATH_SIZE = 33;
  // Maximum total paths decoded across all groups, matches geth's maxTrieNodeLookups
  static final int MAX_TOTAL_PATHS = 1024;

  public GetTrieNodesMessage(final Bytes data) {
    super(data);
  }

  public static GetTrieNodesMessage readFrom(final MessageData message) {
    if (message instanceof GetTrieNodesMessage) {
      return (GetTrieNodesMessage) message;
    }
    final int code = message.getCode();
    if (code != SnapV1.GET_TRIE_NODES) {
      throw new IllegalArgumentException(
          String.format("Message has code %d and thus is not a GetTrieNodes.", code));
    }
    return new GetTrieNodesMessage(message.getData());
  }

  public static GetTrieNodesMessage create(
      final Hash worldStateRootHash, final List<List<Bytes>> paths) {
    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    tmp.startList();
    tmp.writeBytes(worldStateRootHash.getBytes());
    tmp.writeList(
        paths,
        (path, rlpOutput) ->
            rlpOutput.writeList(path, (b, subRlpOutput) -> subRlpOutput.writeBytes(b)));
    tmp.writeBigIntegerScalar(SIZE_REQUEST);
    tmp.endList();
    return new GetTrieNodesMessage(tmp.encoded());
  }

  @Override
  public int getCode() {
    return SnapV1.GET_TRIE_NODES;
  }

  public TrieNodesPaths paths(final boolean withRequestId) {
    final RLPInput input = new BytesValueRLPInput(data, false);
    input.enterList();
    if (withRequestId) input.skipNext();
    final Hash rootHash = Hash.wrap(Bytes32.wrap(input.readBytes32()));
    // Zero-copy slice of the RLP-encoded path groups list; decoded on demand by paths().
    final Bytes rawPaths = input.readAsRlp().raw();
    final BigInteger responseBytes = input.readBigIntegerScalar();
    input.leaveList();
    return ImmutableTrieNodesPaths.builder()
        .worldStateRootHash(rootHash)
        .rawPaths(rawPaths)
        .responseBytes(responseBytes)
        .build();
  }

  @Value.Immutable
  public interface TrieNodesPaths {

    Hash worldStateRootHash();

    /** Zero-copy slice of the RLP-encoded path groups list from the wire message. */
    Bytes rawPaths();

    BigInteger responseBytes();

    /**
     * Lazily decodes path groups and their paths from {@link #rawPaths()} entirely on demand. Each
     * call returns a fresh independent iterator backed by a new {@link AtomicInteger} budget
     * counter, so multiple iterations are fully independent. Within a single outer iteration, each
     * inner {@link Iterable} shares the same counter: the inner iterator charges 1 per path
     * decoded, capping total decoded paths at {@link GetTrieNodesMessage#MAX_TOTAL_PATHS}. Per-path
     * size is validated against {@link GetTrieNodesMessage#MAX_PATH_SIZE}.
     */
    default Iterable<Iterable<Bytes>> paths() {
      return () -> {
        final BytesValueRLPInput outerInput = new BytesValueRLPInput(rawPaths(), false);
        outerInput.enterList();
        final AtomicInteger totalPaths = new AtomicInteger();

        return new Iterator<>() {
          private Iterable<Bytes> peeked = null;
          private boolean exhausted = false;

          @Override
          public boolean hasNext() {
            if (peeked != null) return true;
            if (exhausted
                || outerInput.isEndOfCurrentList()
                || totalPaths.get() >= MAX_TOTAL_PATHS) {
              exhausted = true;
              return false;
            }
            final Bytes groupSlice = outerInput.readAsRlp().raw();
            peeked =
                () -> {
                  final BytesValueRLPInput innerInput = new BytesValueRLPInput(groupSlice, false);
                  innerInput.enterList();
                  return new Iterator<Bytes>() {
                    private Bytes innerPeeked = null;
                    private boolean innerExhausted = false;

                    @Override
                    public boolean hasNext() {
                      if (innerPeeked != null) return true;
                      if (innerExhausted
                          || innerInput.isEndOfCurrentList()
                          || totalPaths.get() >= MAX_TOTAL_PATHS) {
                        innerExhausted = true;
                        return false;
                      }
                      final Bytes path = innerInput.readBytes();
                      if (path.size() > MAX_PATH_SIZE) {
                        throw new RLPException(
                            "Trie node path size "
                                + path.size()
                                + " exceeds maximum "
                                + MAX_PATH_SIZE);
                      }
                      totalPaths.incrementAndGet();
                      innerPeeked = path;
                      return true;
                    }

                    @Override
                    public Bytes next() {
                      if (!hasNext()) throw new NoSuchElementException();
                      final Bytes result = innerPeeked;
                      innerPeeked = null;
                      return result;
                    }
                  };
                };
            return true;
          }

          @Override
          public Iterable<Bytes> next() {
            if (!hasNext()) throw new NoSuchElementException();
            final Iterable<Bytes> result = peeked;
            peeked = null;
            return result;
          }
        };
      };
    }
  }
}
