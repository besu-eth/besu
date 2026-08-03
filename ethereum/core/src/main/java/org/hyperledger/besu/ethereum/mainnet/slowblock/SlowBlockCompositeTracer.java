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
package org.hyperledger.besu.ethereum.mainnet.slowblock;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldView;
import org.hyperledger.besu.plugin.services.tracer.BlockAwareOperationTracer;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;

/**
 * Fans one EVM tracing call out to the slow-block tracer and a plugin's block-import tracer.
 *
 * <p>Used only when both are active. Slow-block tracing on its own passes a single tracer object
 * straight through to the EVM, keeping the operation-level call site monomorphic; this composite is
 * the deliberate second shape, so the worst case is bimorphic rather than the megamorphic stack of
 * interfaces an earlier attempt produced.
 *
 * <p>Whether system calls are traced follows the plugin tracer alone, so composing does not change
 * what a plugin sees. The trade is that when a plugin tracer is active, slow-block EVM counters
 * omit system-call operations unless the plugin also asked for them.
 */
public class SlowBlockCompositeTracer implements BlockAwareOperationTracer {

  private final OperationTracer slowBlockTracer;
  private final BlockAwareOperationTracer pluginTracer;

  /**
   * Creates a composite of exactly two tracers.
   *
   * @param slowBlockTracer the slow-block tracer
   * @param pluginTracer the plugin-supplied block import tracer
   */
  public SlowBlockCompositeTracer(
      final OperationTracer slowBlockTracer, final BlockAwareOperationTracer pluginTracer) {
    this.slowBlockTracer = slowBlockTracer;
    this.pluginTracer = pluginTracer;
  }

  @Override
  public boolean isSystemCallTracingEnabled() {
    return pluginTracer.isSystemCallTracingEnabled();
  }

  @Override
  public void tracePreExecution(final MessageFrame frame) {
    slowBlockTracer.tracePreExecution(frame);
    pluginTracer.tracePreExecution(frame);
  }

  @Override
  public void tracePostExecution(final MessageFrame frame, final OperationResult operationResult) {
    slowBlockTracer.tracePostExecution(frame, operationResult);
    pluginTracer.tracePostExecution(frame, operationResult);
  }

  @Override
  public void tracePrecompileCall(
      final MessageFrame frame, final long gasRequirement, final Bytes output) {
    slowBlockTracer.tracePrecompileCall(frame, gasRequirement, output);
    pluginTracer.tracePrecompileCall(frame, gasRequirement, output);
  }

  @Override
  public void traceAccountCreationResult(
      final MessageFrame frame, final Optional<ExceptionalHaltReason> haltReason) {
    slowBlockTracer.traceAccountCreationResult(frame, haltReason);
    pluginTracer.traceAccountCreationResult(frame, haltReason);
  }

  @Override
  public void tracePrepareTransaction(final WorldView worldView, final Transaction transaction) {
    slowBlockTracer.tracePrepareTransaction(worldView, transaction);
    pluginTracer.tracePrepareTransaction(worldView, transaction);
  }

  @Override
  public void traceStartTransaction(final WorldView worldView, final Transaction transaction) {
    slowBlockTracer.traceStartTransaction(worldView, transaction);
    pluginTracer.traceStartTransaction(worldView, transaction);
  }

  @Override
  public void traceBeforeRewardTransaction(
      final WorldView worldView, final Transaction tx, final Wei miningReward) {
    slowBlockTracer.traceBeforeRewardTransaction(worldView, tx, miningReward);
    pluginTracer.traceBeforeRewardTransaction(worldView, tx, miningReward);
  }

  @Override
  public void traceEndTransaction(
      final WorldView worldView,
      final Transaction tx,
      final boolean status,
      final Bytes output,
      final List<Log> logs,
      final long gasUsed,
      final Set<Address> selfDestructs,
      final long timeNs) {
    slowBlockTracer.traceEndTransaction(
        worldView, tx, status, output, logs, gasUsed, selfDestructs, timeNs);
    pluginTracer.traceEndTransaction(
        worldView, tx, status, output, logs, gasUsed, selfDestructs, timeNs);
  }

  @Override
  public void traceContextEnter(final MessageFrame frame) {
    slowBlockTracer.traceContextEnter(frame);
    pluginTracer.traceContextEnter(frame);
  }

  @Override
  public void traceContextReEnter(final MessageFrame frame) {
    slowBlockTracer.traceContextReEnter(frame);
    pluginTracer.traceContextReEnter(frame);
  }

  @Override
  public void traceContextExit(final MessageFrame frame) {
    slowBlockTracer.traceContextExit(frame);
    pluginTracer.traceContextExit(frame);
  }

  @Override
  public boolean isExtendedTracing() {
    return slowBlockTracer.isExtendedTracing() || pluginTracer.isExtendedTracing();
  }

  // traceStartBlock and traceEndBlock are deliberately not forwarded: block processing drives the
  // block lifecycle on the plugin tracer and the slow-block tracer directly, not through here.

  @Override
  public List<org.hyperledger.besu.evm.tracing.TraceFrame> getTraceFrames() {
    // Slow-block tracing never collects frames; only the plugin tracer can have any.
    return pluginTracer.getTraceFrames();
  }

  @Override
  public boolean isEnabled() {
    return true;
  }
}
