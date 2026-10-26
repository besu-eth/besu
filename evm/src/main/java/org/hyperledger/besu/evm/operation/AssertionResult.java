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
package org.hyperledger.besu.evm.operation;

import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;

import org.apache.tuweni.bytes.Bytes;
import org.jspecify.annotations.Nullable;

/**
 * What an EIP-7906 assertion operation reads: a stack value of at most 32 bytes, or the event data
 * EVENTDATACOPY copies from, or the reason it halts exceptionally. The v1 operations and their EVM
 * v2 twins share the semantics that produce it, and differ only in how they read their operands and
 * write the value.
 *
 * @param value the value, null when the operation halts
 * @param halt the halt reason, null when the operation produces a value
 */
public record AssertionResult(@Nullable Bytes value, @Nullable ExceptionalHaltReason halt) {

  /** The halt of an operand the operation does not define, or of a read outside POST_TX. */
  public static final AssertionResult INVALID =
      new AssertionResult(null, ExceptionalHaltReason.INVALID_OPERATION);

  /** The halt of an index past the end of a table, or of a range past the end of data. */
  public static final AssertionResult OUT_OF_BOUNDS =
      new AssertionResult(null, ExceptionalHaltReason.OUT_OF_BOUNDS);

  /**
   * A value.
   *
   * @param value the value: a stack value of at most 32 bytes, or event data
   * @return the result
   */
  public static AssertionResult of(final Bytes value) {
    return new AssertionResult(value, null);
  }
}
