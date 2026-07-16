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
package org.hyperledger.besu.evm;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.evm.code.OpcodeInfo;
import org.hyperledger.besu.evm.operation.JumpDestOperation;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import com.google.common.base.MoreObjects;
import org.apache.tuweni.bytes.Bytes;

/** Represents EVM code associated with an account. */
public class Code {

  /** The constant EMPTY_CODE. */
  public static final Code EMPTY_CODE = new Code(Bytes.EMPTY);

  /** The bytes representing the code. */
  private final Bytes bytes;

  /** The hash of the code, needed for accessing metadata about the bytecode */
  private Hash codeHash;

  private Integer size;

  /** Bit mask for jump destinations, used to optimize JUMP/JUMPI operations */
  private long[] jumpDestBitMask = null;

  /**
   * Public constructor.
   *
   * @param byteCode The byte representation of the code.
   */
  public Code(final Bytes byteCode) {
    this(byteCode, byteCode.isEmpty() ? Hash.EMPTY : null);
  }

  /**
   * Public constructor.
   *
   * @param byteCode The byte representation of the code.
   * @param codeHash the hash of the bytecode
   */
  public Code(final Bytes byteCode, final Hash codeHash) {
    this.bytes = Bytes.wrap(byteCode.toArrayUnsafe());
    this.codeHash = codeHash;
  }

  /**
   * Returns true if the object is equal to this; otherwise false.
   *
   * @param other The object to compare this with.
   * @return True if the object is equal to this, otherwise false.
   */
  @Override
  public boolean equals(final Object other) {
    if (other == null) return false;
    if (other == this) return true;
    if (!(other instanceof Code that)) return false;

    return this.getCodeHash().equals(that.getCodeHash());
  }

  @Override
  public int hashCode() {
    return bytes.hashCode();
  }

  /**
   * Size of the Code, in bytes
   *
   * @return The number of bytes in the code.
   */
  public int getSize() {
    if (size == null) {
      size = bytes.size();
    }

    return size;
  }

  /**
   * Get the bytes for the code.
   *
   * @return code bytes.
   */
  public Bytes getBytes() {
    return bytes;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("bytes", bytes).toString();
  }

  /**
   * Hash of the code
   *
   * @return hash of the code.
   */
  public Hash getCodeHash() {
    if (codeHash != null) {
      return codeHash;
    }

    codeHash = Hash.hash(bytes);
    return codeHash;
  }

  /**
   * Is the target jump location valid?
   *
   * @param jumpDestination index from PC=0.
   * @return true if the operation is both a valid opcode and a JUMPDEST
   */
  public boolean isJumpDestInvalid(final int jumpDestination) {
    if (jumpDestination < 0 || jumpDestination >= getSize()) {
      return true;
    }

    if (jumpDestBitMask == null) {
      jumpDestBitMask = calculateJumpDestBitMask();
    }

    // This selects which long in the array holds the bit for the given offset:
    //	1)	>>> 6 is equivalent to jumpDestination / 64
    //	2)	Each long holds 64 bits, so this finds the correct chunk
    final long targetLong = jumpDestBitMask[jumpDestination >>> 6];

    // 1) & 0x3F is jumpDestination % 64
    // 2)	1L << ... gives a mask for the specific bit in that long
    final long targetBit = 1L << (jumpDestination & 0x3F);

    // If the bit is not set, then it is an invalid jump destination
    return (targetLong & targetBit) == 0L;
  }

  /**
   * A more readable representation of the hex bytes, including whitespace and comments after hashes
   *
   * @return The pretty printed code
   */
  public String prettyPrint() {
    int i = 0;
    int len = bytes.size();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(out);
    ps.println("0x # Legacy EVM Code");
    while (i < len) {
      i += printInstruction(i, ps);
    }
    return out.toString(StandardCharsets.UTF_8);
  }

  /**
   * Returns a bitmask of valid jump destinations for this code. The bitmask is an array of longs,
   * where each bit represents a potential jump destination in the code.
   *
   * @return an array of long values representing the jump destinations, or null if not set
   */
  public long[] getJumpDestBitMask() {
    return jumpDestBitMask;
  }

  /**
   * Sets the jump destination bitmask for this code. This method is intended to be used by the
   * EVM's JumpService to set the valid jump destinations for the code.
   *
   * @param jumpDestBitMask an array of long values representing the jump destinations
   */
  public void setJumpDestBitMask(final long[] jumpDestBitMask) {
    this.jumpDestBitMask = jumpDestBitMask;
  }

  /**
   * Computes a bitmask where each bit set to 1 indicates a valid `JUMPDEST` opcode in the EVM
   * bytecode. The bitmap is organized in 64-byte chunks, each represented as a `long` (64 bits).
   * This is used for efficiently validating dynamic jumps (`JUMP`, `JUMPI`) at runtime.
   */
  long[] calculateJumpDestBitMask() {
    // Total number of bytes in the bytecode
    final int size = getSize();

    // Allocate enough longs to cover all bytes, one long (64 bits) per 64-byte chunk
    final long[] bitmap = new long[(size >> 6) + 1];

    // Get the raw EVM bytecode as a byte array (no copying)
    final byte[] rawCode = getBytes().toArrayUnsafe();
    final int length = rawCode.length;

    // Iterate through the bytecode
    for (int i = 0; i < length; ) {
      // One 64-bit entry corresponds to 64 bytecode positions
      long thisEntry = 0L;

      // Compute which bitmap entry we are in (i / 64)
      final int entryPos = i >> 6;

      // Compute the number of bytes we can safely examine in this 64-byte window
      final int max = Math.min(64, length - (entryPos << 6));

      // j is the position within this 64-byte window
      int j = i & 0x3F;

      // Scan through this 64-byte chunk of the bytecode
      for (; j < max; i++, j++) {
        final byte operationNum = rawCode[i];

        // Skip all opcodes below 0x5b (JUMPDEST), since only PUSH1–PUSH32 and JUMPDEST matter
        if (operationNum >= JumpDestOperation.OPCODE) {
          switch (operationNum) {
            // JUMPDEST opcode (0x5b): mark as a valid jump destination
            case JumpDestOperation.OPCODE:
              thisEntry |= 1L << j; // Set the bit at position j
              break;
            // PUSH1–PUSH32 opcodes (0x60–0x7f): these consume 1-32 bytes of data that should be
            // skipped
            case 0x60:
              i += 1;
              j += 1;
              break;
            case 0x61:
              i += 2;
              j += 2;
              break;
            case 0x62:
              i += 3;
              j += 3;
              break;
            case 0x63:
              i += 4;
              j += 4;
              break;
            case 0x64:
              i += 5;
              j += 5;
              break;
            case 0x65:
              i += 6;
              j += 6;
              break;
            case 0x66:
              i += 7;
              j += 7;
              break;
            case 0x67:
              i += 8;
              j += 8;
              break;
            case 0x68:
              i += 9;
              j += 9;
              break;
            case 0x69:
              i += 10;
              j += 10;
              break;
            case 0x6a:
              i += 11;
              j += 11;
              break;
            case 0x6b:
              i += 12;
              j += 12;
              break;
            case 0x6c:
              i += 13;
              j += 13;
              break;
            case 0x6d:
              i += 14;
              j += 14;
              break;
            case 0x6e:
              i += 15;
              j += 15;
              break;
            case 0x6f:
              i += 16;
              j += 16;
              break;
            case 0x70:
              i += 17;
              j += 17;
              break;
            case 0x71:
              i += 18;
              j += 18;
              break;
            case 0x72:
              i += 19;
              j += 19;
              break;
            case 0x73:
              i += 20;
              j += 20;
              break;
            case 0x74:
              i += 21;
              j += 21;
              break;
            case 0x75:
              i += 22;
              j += 22;
              break;
            case 0x76:
              i += 23;
              j += 23;
              break;
            case 0x77:
              i += 24;
              j += 24;
              break;
            case 0x78:
              i += 25;
              j += 25;
              break;
            case 0x79:
              i += 26;
              j += 26;
              break;
            case 0x7a:
              i += 27;
              j += 27;
              break;
            case 0x7b:
              i += 28;
              j += 28;
              break;
            case 0x7c:
              i += 29;
              j += 29;
              break;
            case 0x7d:
              i += 30;
              j += 30;
              break;
            case 0x7e:
              i += 31;
              j += 31;
              break;
            case 0x7f:
              i += 32;
              j += 32;
              break;
            default:
              // No default case needed: any unhandled opcode >= 0x5b but not PUSH or JUMPDEST is
              // skipped
          }
        }
      }

      // Store the computed bitmask for this 64-byte chunk
      bitmap[entryPos] = thisEntry;
    }

    // Return the full jump destination bitmask
    return bitmap;
  }

  /**
   * Prints an individual instruction, including immediate data
   *
   * @param offset Offset within the code
   * @param out the print stream to write to
   * @return the number of bytes to advance the PC (includes consideration of immediate arguments)
   */
  public int printInstruction(final int offset, final PrintStream out) {
    int codeByte = bytes.get(offset) & 0xff;
    OpcodeInfo info = OpcodeInfo.getOpcode(codeByte);
    String push = "";
    String decimalPush = "";
    if (info.pcAdvance() > 1) {
      int start = Math.min(bytes.size(), offset + 1);
      int end = Math.min(bytes.size(), info.pcAdvance() - 1);
      Bytes slice = bytes.slice(start, end);
      push = slice.toUnprefixedHexString();
      if (info.pcAdvance() < 5) {
        decimalPush = "(" + slice.toLong() + ")";
      }
    }
    String name = info.name();
    if (codeByte == 0x5b) {
      name = "JUMPDEST";
    }
    out.printf("%02x%s # [ %d ] %s%s%n", codeByte, push, offset, name, decimalPush);
    return Math.max(1, info.pcAdvance());
  }
}
