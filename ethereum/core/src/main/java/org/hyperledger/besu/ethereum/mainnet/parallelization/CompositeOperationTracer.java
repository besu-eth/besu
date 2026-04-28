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
package org.hyperledger.besu.ethereum.mainnet.parallelization;

import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldView;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/** An {@link OperationTracer} that delegates every call to a fixed list of child tracers. */
public class CompositeOperationTracer implements OperationTracer {

  private final List<OperationTracer> tracers;

  public static CompositeOperationTracer of(final OperationTracer... tracers) {
    return new CompositeOperationTracer(List.of(tracers));
  }

  private CompositeOperationTracer(final List<OperationTracer> tracers) {
    this.tracers = tracers;
  }

  @Override
  public void tracePreExecution(final MessageFrame frame) {
    tracers.forEach(t -> t.tracePreExecution(frame));
  }

  @Override
  public void tracePostExecution(final MessageFrame frame, final OperationResult operationResult) {
    tracers.forEach(t -> t.tracePostExecution(frame, operationResult));
  }

  @Override
  public void tracePrecompileCall(
      final MessageFrame frame, final long gasRequirement, final Bytes output) {
    tracers.forEach(t -> t.tracePrecompileCall(frame, gasRequirement, output));
  }

  @Override
  public void traceAccountCreationResult(
      final MessageFrame frame, final Optional<ExceptionalHaltReason> haltReason) {
    tracers.forEach(t -> t.traceAccountCreationResult(frame, haltReason));
  }

  @Override
  public void tracePrepareTransaction(final WorldView worldView, final Transaction transaction) {
    tracers.forEach(t -> t.tracePrepareTransaction(worldView, transaction));
  }

  @Override
  public void traceStartTransaction(final WorldView worldView, final Transaction transaction) {
    tracers.forEach(t -> t.traceStartTransaction(worldView, transaction));
  }

  @Override
  public void traceBeforeRewardTransaction(
      final WorldView worldView, final Transaction tx, final Wei miningReward) {
    tracers.forEach(t -> t.traceBeforeRewardTransaction(worldView, tx, miningReward));
  }

  @Override
  public void traceEndTransaction(
      final WorldView worldView,
      final Transaction tx,
      final boolean status,
      final Bytes output,
      final java.util.List<org.hyperledger.besu.datatypes.Log> logs,
      final long gasUsed,
      final java.util.Set<org.hyperledger.besu.datatypes.Address> selfDestructs,
      final long timeNs) {
    tracers.forEach(
        t ->
            t.traceEndTransaction(
                worldView, tx, status, output, logs, gasUsed, selfDestructs, timeNs));
  }

  @Override
  public void traceContextEnter(final MessageFrame frame) {
    tracers.forEach(t -> t.traceContextEnter(frame));
  }

  @Override
  public void traceContextReEnter(final MessageFrame frame) {
    tracers.forEach(t -> t.traceContextReEnter(frame));
  }

  @Override
  public void traceContextExit(final MessageFrame frame) {
    tracers.forEach(t -> t.traceContextExit(frame));
  }

  @Override
  public boolean isExtendedTracing() {
    return tracers.stream().anyMatch(OperationTracer::isExtendedTracing);
  }

  /**
   * Returns {@code true} if {@code tracer} is an instance of {@code tracerType}, or if {@code
   * tracer} is a {@link CompositeOperationTracer} that contains a child of that type.
   *
   * @param tracer the tracer to inspect
   * @param tracerType the type to look for
   * @param <T> the tracer type
   * @return {@code true} when a matching tracer is found
   */
  public static <T extends OperationTracer> boolean hasTracer(
      final OperationTracer tracer, final Class<T> tracerType) {
    if (tracerType.isInstance(tracer)) {
      return true;
    }
    if (tracer instanceof CompositeOperationTracer composite) {
      return composite.tracers.stream().anyMatch(tracerType::isInstance);
    }
    return false;
  }

  /**
   * Returns the first child of {@code tracerType} from {@code tracer}, or the tracer itself if it
   * matches. Returns {@link Optional#empty()} when no match is found.
   *
   * @param tracer the tracer to inspect
   * @param tracerType the type to look for
   * @param <T> the tracer type
   * @return an {@link Optional} containing the matched tracer, or empty
   */
  public static <T extends OperationTracer> Optional<T> findTracer(
      final OperationTracer tracer, final Class<T> tracerType) {
    if (tracerType.isInstance(tracer)) {
      return Optional.of(tracerType.cast(tracer));
    }
    if (tracer instanceof CompositeOperationTracer composite) {
      return composite.tracers.stream()
          .filter(tracerType::isInstance)
          .map(tracerType::cast)
          .findFirst();
    }
    return Optional.empty();
  }
}
