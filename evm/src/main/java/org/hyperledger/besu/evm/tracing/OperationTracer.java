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
package org.hyperledger.besu.evm.tracing;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;
import org.hyperledger.besu.evm.worldstate.WorldView;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;

/** The interface Operation tracer. */
public interface OperationTracer {

  /** The constant NO_TRACING. */
  OperationTracer NO_TRACING =
      new OperationTracer() {
        @Override
        public boolean isEnabled() {
          return false;
        }
      };

  /**
   * Trace pre execution.
   *
   * @param frame the frame
   */
  default void tracePreExecution(final MessageFrame frame) {}

  /**
   * Trace post execution.
   *
   * @param frame the frame
   * @param operationResult the operation result
   */
  default void tracePostExecution(
      final MessageFrame frame, final OperationResult operationResult) {}

  /**
   * Trace precompile call.
   *
   * @param frame the frame
   * @param gasRequirement the gas requirement
   * @param output the output
   */
  default void tracePrecompileCall(
      final MessageFrame frame, final long gasRequirement, final Bytes output) {}

  /**
   * Trace account creation result.
   *
   * @param frame the frame
   * @param haltReason the halt reason
   */
  default void traceAccountCreationResult(
      final MessageFrame frame, final Optional<ExceptionalHaltReason> haltReason) {}

  /**
   * Trace the start of a transaction, before sender account alteration but after validation.
   *
   * @param worldView an immutable view of the execution context
   * @param transaction the transaction which will be processed
   */
  default void tracePrepareTransaction(final WorldView worldView, final Transaction transaction) {}

  /**
   * Trace the start of a transaction, before execution but after sender account alteration.
   *
   * @param worldView an immutable view of the execution context
   * @param transaction the transaction which will be processed
   */
  default void traceStartTransaction(final WorldView worldView, final Transaction transaction) {}

  /**
   * Trace the end of a transaction just before mining reward.
   *
   * @param worldView an immutable view of the execution context
   * @param tx the transaction that just concluded
   * @param miningReward the reward that the mining beneficiary will receive.
   */
  default void traceBeforeRewardTransaction(
      final WorldView worldView, final Transaction tx, final Wei miningReward) {}

  /**
   * Trace the end of a transaction.
   *
   * @param worldView an immutable view of the execution context
   * @param tx the transaction that just concluded
   * @param status true if the transaction is successful, false otherwise
   * @param output the bytes output from the transaction
   * @param logs the logs emitted by this transaction
   * @param gasUsed the gas used by the entire transaction
   * @param selfDestructs the set of addresses that self-destructed during the transaction
   * @param timeNs the time in nanoseconds it took to execute the transaction
   */
  default void traceEndTransaction(
      final WorldView worldView,
      final Transaction tx,
      final boolean status,
      final Bytes output,
      final List<Log> logs,
      final long gasUsed,
      final Set<Address> selfDestructs,
      final long timeNs) {}

  /**
   * Trace the entering of a new context
   *
   * @param frame the frame
   */
  default void traceContextEnter(final MessageFrame frame) {}

  /**
   * Trace the re-entry in a context from a child context
   *
   * @param frame the frame
   */
  default void traceContextReEnter(final MessageFrame frame) {}

  /**
   * Trace the exiting of a context
   *
   * @param frame the frame
   */
  default void traceContextExit(final MessageFrame frame) {}

  /**
   * Returns a boolean indicating whether extended tracing is enabled.
   *
   * @return <code>true</code> if extended tracing is enabled, <code>false</code> otherwise.
   */
  default boolean isExtendedTracing() {
    return false;
  }

  /**
   * Get the trace frames collected during execution. This is primarily used for debug tracing
   * operations.
   *
   * @return the list of trace frames, empty by default
   */
  default java.util.List<TraceFrame> getTraceFrames() {
    return java.util.Collections.emptyList();
  }

  /**
   * Check to tell whether tracing enabled/disabled.
   *
   * @return true if tracing is enabled and false otherwise
   */
  default boolean isEnabled() {
    return true;
  }
}
