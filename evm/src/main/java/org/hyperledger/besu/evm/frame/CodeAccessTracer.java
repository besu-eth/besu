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
package org.hyperledger.besu.evm.frame;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;

/**
 * Receives the code accesses of an execution: the code reads and writes. A {@link MessageFrame}
 * carries one so that the operations, which only see the frame, can report the accesses they
 * observe. Every {@code OperationTracer} is one.
 */
public interface CodeAccessTracer {

  /** A tracer that receives no code access events. */
  CodeAccessTracer NONE = new CodeAccessTracer() {};

  /**
   * Returns whether this tracer receives the code access events, {@link #traceCodeRead} and {@link
   * #traceCodeWrite}, used for instance to collect the bytecodes of an EIP-8025 execution witness.
   * Emitting them costs extra account lookups on hot paths, so they are only emitted to tracers
   * that opt in.
   *
   * @return {@code true} to receive the code access events; {@code false} by default
   */
  default boolean isCodeAccessTracing() {
    return false;
  }

  /**
   * Trace a read of an account's code: the EVM needed the bytecode of {@code address} at this
   * point, to execute it, resolve a delegation designator, or answer EXTCODESIZE/EXTCODECOPY.
   * Emitted by the code that performs the read, not inferred, so a read is reported even when the
   * operation then fails. Only emitted when {@link #isCodeAccessTracing()}.
   *
   * @param address the account whose code was read
   * @param codeHash the hash of the code read, as it was at the time of the read
   */
  default void traceCodeRead(final Address address, final Hash codeHash) {}

  /**
   * Trace a write of an account's code: a CREATE/CREATE2 code deposit or an EIP-7702 delegation
   * designator. A write made by a frame that later reverts is undone with that frame, which the
   * tracer sees exit as {@code COMPLETED_FAILED}. A delegation designator written while charging a
   * transaction's top frame is discarded if those charges fail, see {@code
   * OperationTracer#traceStartTransactionFailed}. Only emitted when {@link #isCodeAccessTracing()}.
   *
   * @param address the account whose code was written
   * @param codeHash the hash of the code written
   */
  default void traceCodeWrite(final Address address, final Hash codeHash) {}
}
