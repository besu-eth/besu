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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.evm.Code;

import java.nio.ByteBuffer;

import org.apache.tuweni.bytes.Bytes;

/**
 * The versions of the value layout of the code column family. Every value starts with its version
 * byte, so that values of different versions can sit next to each other and be told apart. A new
 * layout gets the next version and becomes {@link #CURRENT}; the older ones stay to decode what
 * they wrote.
 */
public enum CodeStorageFormat {
  /**
   * The code length, the code and its jump destination bitmask, so that a load never has to walk
   * the code.
   */
  V1(1) {
    private static final int HEADER_SIZE = Byte.BYTES + Integer.BYTES;

    @Override
    public byte[] encode(final Bytes code) {
      final long[] jumpDestBitMask = Code.jumpDestBitMaskOf(code);
      final byte[] value =
          new byte[HEADER_SIZE + code.size() + jumpDestBitMask.length * Long.BYTES];
      final ByteBuffer buffer = ByteBuffer.wrap(value);
      buffer.put(version).putInt(code.size()).put(code.toArrayUnsafe());
      buffer.asLongBuffer().put(jumpDestBitMask);
      return value;
    }

    @Override
    public Code decode(final byte[] value, final Hash codeHash) {
      final ByteBuffer buffer = ByteBuffer.wrap(value, Byte.BYTES, value.length - Byte.BYTES);
      final int codeSize = buffer.getInt();
      final long[] jumpDestBitMask = new long[(codeSize >> 6) + 1];
      if (value.length != HEADER_SIZE + codeSize + jumpDestBitMask.length * Long.BYTES) {
        throw new IllegalStateException(
            "Stored code of " + codeSize + " bytes has a value of " + value.length + " bytes");
      }
      buffer.position(HEADER_SIZE + codeSize);
      buffer.asLongBuffer().get(jumpDestBitMask);
      final Code code = new Code(Bytes.wrap(value, HEADER_SIZE, codeSize), codeHash);
      code.setJumpDestBitMask(jumpDestBitMask);
      return code;
    }
  };

  /** The version every new value is written in. */
  public static final CodeStorageFormat CURRENT = V1;

  /** The version byte, the first byte of every value. */
  final byte version;

  CodeStorageFormat(final int version) {
    this.version = (byte) version;
  }

  /**
   * Encodes code into a value of this version, starting with its version byte.
   *
   * @param code the code
   * @return the value to store
   */
  public abstract byte[] encode(Bytes code);

  /**
   * Decodes a value known to be of this version.
   *
   * @param value the stored value, starting with this version's byte
   * @param codeHash the hash of the code, or null to compute it when it is asked for
   * @return the code with its jump destination analysis
   */
  public abstract Code decode(byte[] value, Hash codeHash);

  /**
   * The version a stored value was written in, named by its first byte.
   *
   * @param value the stored value
   * @return the version to decode it with
   */
  public static CodeStorageFormat of(final byte[] value) {
    if (value.length == 0) {
      throw new IllegalStateException("Empty code storage value");
    }
    for (final CodeStorageFormat format : values()) {
      if (format.version == value[0]) {
        return format;
      }
    }
    throw new IllegalStateException("Unknown code storage format " + value[0]);
  }
}
