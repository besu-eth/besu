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
package org.hyperledger.besu.ethereum.p2p.rlpx.wire;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.math.BigInteger;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractSnapMessageData extends AbstractMessageData {

    @VisibleForTesting
  public static final BigInteger SIZE_REQUEST = BigInteger.valueOf(524288); // 512 * 1024

  private Optional<Hash> rootHash;

  public AbstractSnapMessageData(final Bytes data) {
    super(data);
    rootHash = Optional.empty();
  }

  public Optional<Hash> getRootHash() {
    return rootHash;
  }

  public void setRootHash(final Optional<Hash> rootHash) {
    this.rootHash = rootHash;
  }

  @Override
  public MessageData wrapMessageData(final BigInteger requestId) {
    return new RawMessage(getCode(), wrap(requestId));
  }

  @Override
  public Map.Entry<BigInteger, MessageData> unwrapMessageData() {
    final RLPInput messageDataRLP = RLP.input(getData());
    messageDataRLP.enterList();
    final BigInteger requestId = messageDataRLP.readBigIntegerScalar();
    messageDataRLP.leaveListLenient();
    return new AbstractMap.SimpleImmutableEntry<>(requestId, new RawMessage(getCode(), getData()));
  }

  /**
   * Wraps this message by prepending a requestId to the existing RLP list. Uses zero-copy splicing:
   * the payload bytes are shared via {@link Bytes#wrap(Bytes...)} rather than copied.
   *
   * <p>Original: [field1, field2, ...] Wrapped: [requestId, field1, field2, ...]
   */
  protected Bytes wrap(final BigInteger requestId) {
    final Bytes reqIdBytes = RLP.encode(out -> out.writeBigIntegerScalar(requestId));
    // Slice past the RLP list header to get content (view, no copy)
    final Bytes content = data.slice(RLP.listHeaderSize(data));
    // Build new list header and concatenate without copying the payload
    return Bytes.wrap(RLP.listHeader(reqIdBytes.size() + content.size()), reqIdBytes, content);
  }

  public static MessageData create(final Message message) {
    return new AbstractSnapMessageData(message.getData().getData()) {
      @Override
      public int getCode() {
        return message.getData().getCode();
      }
    };
  }
}
