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
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
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

  private final int size;

  /** Bit mask for jump destinations, used to optimize JUMP/JUMPI operations */
  private long[] jumpDestBitMask = null;

  private static final VarHandle LITTLE_ENDIAN_LONG =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

  private static final long HIGH_BITS = 0x8080808080808080L;
  private static final long LOW_BITS = 0x7f7f7f7f7f7f7f7fL;
  private static final long JUMPDEST_BYTES = 0x5b5b5b5b5b5b5b5bL;
  private static final long GATHER_HIGH_BITS = 0x0002040810204081L;

  /**
   * The chunks with at most this many PUSH opcodes are analysed by walking their PUSHes. Each PUSH
   * costs about as much as walking five bytes, so up to here the PUSH walk costs no more than the
   * byte walk, and above it the byte walk is taken.
   */
  private static final int MAX_PUSHES_TO_WALK = 12;

  /**
   * For each opcode, its length including immediate data in bits 0-7 (2 to 33 for PUSH1-PUSH32, 1
   * otherwise), and in bit 8 whether it is JUMPDEST.
   */
  private static final int[] OPCODE_INFO = new int[256];

  static {
    for (int opcode = 0; opcode < 256; opcode++) {
      final int length = opcode >= 0x60 && opcode <= 0x7f ? opcode - 0x5e : 1;
      OPCODE_INFO[opcode] = length | (opcode == JumpDestOperation.OPCODE ? 1 << 8 : 0);
    }
  }

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
    this.size = this.bytes.size();
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
   *
   * <p>No step branches on an individual opcode. A branch per opcode mispredicts on every byte of
   * code that mixes opcodes at random, several times the cost of the analysis itself, and the code
   * is chosen by whoever deploys it. Instead each 64-byte chunk takes one of two paths, chosen by
   * the number of PUSH opcodes among its bytes:
   *
   * <ul>
   *   <li>Up to {@link #MAX_PUSHES_TO_WALK}: {@link #walkPushes} steps from PUSH to PUSH, masking
   *       their immediate data out of the chunk's JUMPDEST bytes. A chunk without PUSH, as in code
   *       made of one-byte opcodes, takes no step at all.
   *   <li>More: {@link #walkChunk} walks the chunk byte by byte, at a cost that does not depend on
   *       the bytes.
   * </ul>
   *
   * @return the jump destination bitmask
   */
  long[] calculateJumpDestBitMask() {
    final byte[] rawCode = getBytes().toArrayUnsafe();
    final int length = rawCode.length;
    final long[] bitmap = new long[(length >> 6) + 1];
    final int fullChunks = length >> 6;
    int pushDataRemaining = 0;
    for (int chunk = 0; chunk < fullChunks; chunk++) {
      final long pushes = chunkPushes(rawCode, chunk << 6);
      if (Long.bitCount(pushes) <= MAX_PUSHES_TO_WALK) {
        pushDataRemaining = walkPushes(rawCode, bitmap, chunk, pushes, pushDataRemaining);
      } else {
        pushDataRemaining = walkChunk(rawCode, bitmap, chunk, pushDataRemaining);
      }
    }
    if (fullChunks << 6 < length) {
      walkChunk(rawCode, bitmap, fullChunks, pushDataRemaining);
    }
    return bitmap;
  }

  /**
   * Marks the JUMPDESTs of one full chunk by stepping from PUSH to PUSH. The lowest PUSH byte that
   * is not immediate data is an instruction, so masking its data and repeating finds exactly the
   * chunk's PUSH instructions.
   *
   * @return the number of PUSH data bytes that continue into the next chunk
   */
  private static int walkPushes(
      final byte[] code,
      final long[] bitmap,
      final int chunk,
      final long pushes,
      final int pushDataRemaining) {
    final int chunkStart = chunk << 6;
    long pushData = (1L << pushDataRemaining) - 1;
    long instructions = pushes & ~pushData;
    int carry = 0;
    while (instructions != 0) {
      final int position = Long.numberOfTrailingZeros(instructions);
      final int dataLength = (code[chunkStart + position] & 0x1f) + 1;
      // Shifted in two steps, so that a PUSH in the last byte leaves no data in this chunk
      final long data = (((1L << dataLength) - 1) << position) << 1;
      pushData |= data;
      instructions &= ~data & (instructions - 1);
      carry = Math.max(0, position + dataLength - 63);
    }
    bitmap[chunk] = chunkJumpDests(code, chunkStart) & ~pushData;
    return carry;
  }

  /**
   * Marks the JUMPDESTs of one chunk, visiting every byte and carrying {@code next}, the number of
   * PUSH data bytes still to come minus one. It is negative exactly at an instruction, and its sign
   * both selects the JUMPDEST bit and picks the next value, so no step branches on the opcode.
   *
   * @return the number of PUSH data bytes that continue into the next chunk
   */
  private static int walkChunk(
      final byte[] code, final long[] bitmap, final int chunk, final int pushDataRemaining) {
    final int chunkStart = chunk << 6;
    final int chunkEnd = Math.min(chunkStart + 64, code.length);
    int next = pushDataRemaining - 1;
    long jumpDests = 0L;
    for (int i = chunkStart; i < chunkEnd; i++) {
      final int info = OPCODE_INFO[code[i] & 0xff];
      final int isInstruction = next >> 31;
      jumpDests |= (long) ((info >>> 8) & isInstruction) << i;
      next = next + (isInstruction & (info & 0xff)) - 1;
    }
    bitmap[chunk] = jumpDests;
    return next + 1;
  }

  /** The PUSH1-PUSH32 bytes (0x60-0x7f) among the 64 from {@code chunkStart}, one bit per byte. */
  private static long chunkPushes(final byte[] code, final int chunkStart) {
    long pushes = 0L;
    for (int word = 0; word < 8; word++) {
      final long bytes = (long) LITTLE_ENDIAN_LONG.get(code, chunkStart + (word << 3));
      // The high bit of each byte whose top three bits are 011
      final long matches = ~bytes & (bytes << 1) & (bytes << 2) & HIGH_BITS;
      pushes |= ((matches * GATHER_HIGH_BITS) >>> 56) << (word << 3);
    }
    return pushes;
  }

  /** The JUMPDEST bytes among the 64 from {@code chunkStart}, one bit per byte. */
  private static long chunkJumpDests(final byte[] code, final int chunkStart) {
    long jumpDests = 0L;
    for (int word = 0; word < 8; word++) {
      final long candidates =
          (long) LITTLE_ENDIAN_LONG.get(code, chunkStart + (word << 3)) ^ JUMPDEST_BYTES;
      // The high bit of each byte that is zero, i.e. of each byte that was JUMPDEST
      final long matches = ~(candidates | ((candidates & LOW_BITS) + LOW_BITS)) & HIGH_BITS;
      // Gathers the eight high bits into the top byte, lowest byte first
      jumpDests |= ((matches * GATHER_HIGH_BITS) >>> 56) << (word << 3);
    }
    return jumpDests;
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
