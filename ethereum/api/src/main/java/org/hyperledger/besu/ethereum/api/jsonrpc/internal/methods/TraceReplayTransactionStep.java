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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.TraceTypeParameter.TraceType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TraceReplayResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.diff.StateTraceGenerator;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.flat.FlatTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.flat.FlatTraceGenerator;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.vm.VmTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.vm.VmTraceGenerator;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class TraceReplayTransactionStep
    implements Function<TransactionTrace, CompletableFuture<TraceReplayResult>> {

  private final ProtocolSchedule protocolSchedule;
  private final Block block;
  private final Set<TraceType> traceTypes;

  public TraceReplayTransactionStep(
      final ProtocolSchedule protocolSchedule, final Block block, final Set<TraceType> traceTypes) {
    this.protocolSchedule = protocolSchedule;
    this.block = block;
    this.traceTypes = traceTypes;
  }

  @Override
  public CompletableFuture<TraceReplayResult> apply(final TransactionTrace transactionTrace) {
    final TraceReplayResult.Builder builder = TraceReplayResult.builder();
    transactionTrace
        .getResult()
        .getRevertReason()
        .filter(__ -> transactionTrace.getResult().getExceptionalHaltReason().isEmpty())
        .ifPresent(revertReason -> builder.revertReason(revertReason.toHexString()));

    builder.output(transactionTrace.getResult().getOutput().toString());
    builder.transactionHash(transactionTrace.getTransaction().getHash().getBytes().toHexString());

    if (traceTypes.contains(TraceType.STATE_DIFF)) {
      new StateTraceGenerator()
          .generateStateDiff(transactionTrace)
          .forEachOrdered(builder::stateDiff);
    }

    if (traceTypes.contains(TraceType.TRACE)) {
      FlatTraceGenerator.generateFromTransactionTrace(
              protocolSchedule, transactionTrace, block, new AtomicInteger())
          .forEachOrdered(trace -> builder.addTrace((FlatTrace) trace));
    }

    if (traceTypes.contains(TraceType.VM_TRACE)) {
      new VmTraceGenerator(transactionTrace)
          .generateTraceStream()
          .forEachOrdered(vmTrace -> builder.vmTrace((VmTrace) vmTrace));
    }
    return CompletableFuture.completedFuture(builder.build());
  }
}
