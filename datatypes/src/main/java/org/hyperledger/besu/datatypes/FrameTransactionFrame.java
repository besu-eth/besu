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
package org.hyperledger.besu.datatypes;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * Represents a single frame within an EIP-8141 Frame Transaction. Each frame is a contract call
 * that handles validation, gas payment approval, or user operation execution.
 *
 * @param mode the execution context (0=DEFAULT, 1=VERIFY, 2=SENDER)
 * @param flags bit-packed feature flags (bits 0-1: approval scope, bit 2: atomic batch)
 * @param target destination address (empty means use sender)
 * @param gasLimit maximum gas allocation for this frame
 * @param value ETH value transferred (non-zero only in SENDER mode)
 * @param data calldata for frame execution
 */
public record FrameTransactionFrame(
    int mode, int flags, Optional<Address> target, long gasLimit, Wei value, Bytes data) {

  /** DEFAULT mode: execute with ENTRY_POINT as caller. */
  public static final int MODE_DEFAULT = 0;

  /** VERIFY mode: static execution for validation. */
  public static final int MODE_VERIFY = 1;

  /** SENDER mode: execute with sender as caller. */
  public static final int MODE_SENDER = 2;

  /** No approval. */
  public static final int APPROVAL_NONE = 0;

  /** Payment approval only. */
  public static final int APPROVAL_PAYMENT = 1;

  /** Execution approval only. */
  public static final int APPROVAL_EXECUTION = 2;

  /** Both payment and execution approval. */
  public static final int APPROVAL_BOTH = 3;

  /** Atomic batch flag bit position. */
  public static final int ATOMIC_BATCH_FLAG = 4; // bit 2 = value 4

  /** Maximum number of frames per transaction. */
  public static final int MAX_FRAMES = 64;

  /**
   * Returns the approval scope from the flags (bits 0-1).
   *
   * @return the approval scope value (0-3)
   */
  public int approvalScope() {
    return flags & 0x03;
  }

  /**
   * Returns whether the atomic batch flag is set (bit 2).
   *
   * @return true if this frame is part of an atomic batch
   */
  public boolean isAtomicBatch() {
    return (flags & ATOMIC_BATCH_FLAG) != 0;
  }

  /**
   * Resolves the target address, using the provided sender if target is empty.
   *
   * @param sender the transaction sender address
   * @return the resolved target address
   */
  public Address resolveTarget(final Address sender) {
    return target.orElse(sender);
  }
}
