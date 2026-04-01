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
package org.hyperledger.besu.ethereum.forkid;

import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;
import org.hyperledger.besu.util.EndianUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

public class ForkId {
  final Bytes hash;
  final Bytes next;
  Bytes forkIdRLP;

  public ForkId(final Bytes hash, final Bytes next) {
    this.hash = hash;
    this.next = next;
    createForkIdRLP();
  }

  public ForkId(final Bytes hash, final long next) {
    this(hash, Bytes.wrap(EndianUtils.longToBigEndian(next)).trimLeadingZeros());
  }

  public static Optional<ForkId> fromRawForkId(final Object rawForkId) {
    if (rawForkId != null) {
      try {
        @SuppressWarnings("unchecked")
        final List<List<Bytes>> typedRawForkId = (List<List<Bytes>>) rawForkId;
        return Optional.of(
            new ForkId(typedRawForkId.getFirst().getFirst(), typedRawForkId.getFirst().get(1)));
      } catch (final Exception e) {
        return Optional.empty();
      }
    }
    return Optional.empty();
  }

  public long getNext() {
    return next.toLong();
  }

  public Bytes getHash() {
    return hash;
  }

  public List<Bytes> getForkIdAsBytesList() {
    final List<Bytes> bytesList = new ArrayList<>();
    bytesList.add(hash);
    bytesList.add(next);

    return bytesList;
  }

  void createForkIdRLP() {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    writeTo(out);
    forkIdRLP = out.encoded();
  }

  public void writeTo(final RLPOutput out) {
    out.startList();
    out.writeBytes(hash);
    out.writeBytes(next);
    out.endList();
  }

  public static ForkId readFrom(final RLPInput in) {
    in.enterList();
    final Bytes hash = in.readBytes();
    final long next = in.readLongScalar();
    in.leaveList();
    return new ForkId(hash, next);
  }

  public List<ForkId> asList() {
    final ArrayList<ForkId> forRLP = new ArrayList<>();
    forRLP.add(this);
    return forRLP;
  }

  @Override
  public String toString() {
    return "ForkId(hash=" + this.hash + ", next=" + next.toLong() + ")";
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof ForkId other) {
      final long thisNext = next.toLong();
      return other.getHash().equals(this.hash) && thisNext == other.getNext();
    }
    return false;
  }

  @Override
  public int hashCode() {
    return 31 * this.hash.hashCode() * this.next.hashCode();
  }
}
