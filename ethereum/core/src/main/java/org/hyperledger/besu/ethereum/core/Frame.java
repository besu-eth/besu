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
package org.hyperledger.besu.ethereum.core;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * A single execution frame of an EIP-8141 frame transaction.
 *
 * @param mode the execution mode ({@link #MODE_DEFAULT}, {@link #MODE_VERIFY} or {@link
 *     #MODE_SENDER})
 * @param flags the frame flags (bits 0-1 approval scope, bit 2 atomic batch)
 * @param target the destination address, or empty to target the transaction sender
 * @param executionGasLimit the maximum execution gas for the frame
 * @param stateGasLimit the maximum EIP-8037 state gas for the frame
 * @param value the wei transferred from the sender (SENDER mode only)
 * @param data the calldata for the top-level call of the frame
 */
public record Frame(
    int mode,
    int flags,
    Optional<Address> target,
    long executionGasLimit,
    long stateGasLimit,
    Wei value,
    Bytes data) {

  /** Execute the frame with ENTRY_POINT as the caller. */
  public static final int MODE_DEFAULT = 0;

  /** Execute the frame statically as transaction validation; a failure invalidates the tx. */
  public static final int MODE_VERIFY = 1;

  /** Execute the frame with the transaction sender as the caller. */
  public static final int MODE_SENDER = 2;

  /** Flag bit approving payment ({@code APPROVE_PAYMENT}). */
  public static final int FLAG_APPROVE_PAYMENT = 0x1;

  /** Flag bit approving sender execution ({@code APPROVE_EXECUTION}). */
  public static final int FLAG_APPROVE_EXECUTION = 0x2;

  /** Mask covering the approval scope bits. */
  public static final int APPROVE_SCOPE_MASK = 0x3;

  /** Flag bit marking the frame as atomically batched with the following frame. */
  public static final int ATOMIC_BATCH_FLAG = 0x4;

  /**
   * The approval scope allowed by this frame's flags.
   *
   * @return flags masked to the approval scope bits
   */
  public int allowedApprovalScope() {
    return flags & APPROVE_SCOPE_MASK;
  }

  /**
   * Whether this frame is batched with the following frame.
   *
   * @return true when the atomic batch flag is set
   */
  public boolean isAtomicBatch() {
    return (flags & ATOMIC_BATCH_FLAG) != 0;
  }

  /**
   * The frame target resolved against the transaction sender.
   *
   * @param sender the transaction sender
   * @return the target address, or the sender when the target is absent
   */
  public Address resolvedTarget(final Address sender) {
    return target.orElse(sender);
  }
}
