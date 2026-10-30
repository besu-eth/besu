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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.code;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * Stores code by its hash together with its jump destination analysis, so that a load never has to
 * walk the code. A column family written by this strategy carries a marker entry, because its
 * values cannot be told apart from bare code by inspection; the marker is written in the same step
 * as the values, so the column family holds either only bare code or only analysed code.
 */
public class JumpDestCodeStorageStrategy extends CodeHashCodeStorageStrategy {

  /** Reserved key naming the strategy every other entry of the column family was written by. */
  public static final byte[] MARKER_KEY = "codeStorageStrategy".getBytes(StandardCharsets.UTF_8);

  /**
   * The marker value of this strategy: its name and the version of the analysis stored with the
   * code, so that code analysed by another version is not read as if it were current.
   */
  public static final byte[] MARKER =
      ("jumpDest:" + Code.JUMP_DEST_ANALYSIS_VERSION).getBytes(StandardCharsets.UTF_8);

  private static final int HEADER_SIZE = Integer.BYTES;

  @Override
  public Optional<Code> getFlatCode(
      final Hash codeHash, final Hash accountHash, final SegmentedKeyValueStorage storage) {
    return storage
        .get(CODE_STORAGE, codeHash.getBytes().toArrayUnsafe())
        .map(value -> decode(value, codeHash));
  }

  @Override
  public Optional<Bytes> getFlatCodeBytes(
      final Hash codeHash, final Hash accountHash, final SegmentedKeyValueStorage storage) {
    return storage
        .get(CODE_STORAGE, codeHash.getBytes().toArrayUnsafe())
        .map(JumpDestCodeStorageStrategy::decodeCode);
  }

  @Override
  public void putFlatCode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Hash codeHash,
      final Bytes code) {
    transaction.put(CODE_STORAGE, codeHash.getBytes().toArrayUnsafe(), encode(code));
  }

  @Override
  public void markEmpty(final SegmentedKeyValueStorage storage) {
    final SegmentedKeyValueStorageTransaction transaction = storage.startTransaction();
    transaction.put(CODE_STORAGE, MARKER_KEY, MARKER);
    transaction.commit();
  }

  public static boolean isMarkerKey(final byte[] key) {
    return Arrays.equals(key, MARKER_KEY);
  }

  /**
   * Whether the column family was written by this strategy.
   *
   * @param storage the storage holding the code column family
   * @return true when the marker of this strategy is present
   * @throws IllegalStateException when the column family carries any other marker, which includes
   *     this strategy with another version of the analysis
   */
  public static boolean isMarked(final SegmentedKeyValueStorage storage) {
    return storage
        .get(CODE_STORAGE, MARKER_KEY)
        .map(
            marker -> {
              if (!Arrays.equals(marker, MARKER)) {
                throw new IllegalStateException(
                    "Unknown code storage strategy "
                        + new String(marker, StandardCharsets.UTF_8)
                        + ", expected "
                        + new String(MARKER, StandardCharsets.UTF_8));
              }
              return true;
            })
        .orElse(false);
  }

  /**
   * The value this strategy stores for the given code: the code length, the code and its jump
   * destination bitmask, one bit per code byte.
   *
   * @param code the code
   * @return the value to store
   */
  public static byte[] encode(final Bytes code) {
    final long[] jumpDestBitMask = Code.jumpDestBitMaskOf(code);
    final byte[] value = new byte[HEADER_SIZE + code.size() + jumpDestBitMask.length * Long.BYTES];
    final ByteBuffer buffer = ByteBuffer.wrap(value);
    buffer.putInt(code.size()).put(code.toArrayUnsafe());
    buffer.asLongBuffer().put(jumpDestBitMask);
    return value;
  }

  /**
   * The code a stored value holds, with its jump destination analysis set. The code is copied out
   * of the value, because the EVM reads it as an array of its own.
   *
   * @param value the stored value
   * @param codeHash the hash of the code
   * @return the code
   */
  public static Code decode(final byte[] value, final Hash codeHash) {
    final int codeSize = codeSize(value);
    final long[] jumpDestBitMask = new long[maskLength(codeSize)];
    ByteBuffer.wrap(value).position(HEADER_SIZE + codeSize).asLongBuffer().get(jumpDestBitMask);
    final Code code = new Code(Bytes.wrap(value, HEADER_SIZE, codeSize), codeHash);
    code.setJumpDestBitMask(jumpDestBitMask);
    return code;
  }

  /**
   * The bytes of the code a stored value holds, as a slice of the value.
   *
   * @param value the stored value
   * @return the bytes of the code
   */
  public static Bytes decodeCode(final byte[] value) {
    return Bytes.wrap(value, HEADER_SIZE, codeSize(value));
  }

  /** The code length of a stored value, checked against the size of the value before it is used. */
  private static int codeSize(final byte[] value) {
    if (value.length < HEADER_SIZE) {
      throw new IllegalStateException("Stored code value of " + value.length + " bytes");
    }
    final int codeSize = ByteBuffer.wrap(value).getInt();
    if (codeSize < 0
        || value.length
            != HEADER_SIZE + (long) codeSize + (long) maskLength(codeSize) * Long.BYTES) {
      throw new IllegalStateException(
          "Stored code of " + codeSize + " bytes has a value of " + value.length + " bytes");
    }
    return codeSize;
  }

  private static int maskLength(final int codeSize) {
    return (codeSize >> 6) + 1;
  }
}
