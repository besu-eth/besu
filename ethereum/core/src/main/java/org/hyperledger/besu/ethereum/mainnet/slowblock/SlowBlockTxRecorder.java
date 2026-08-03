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
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.AbstractCallOperation;
import org.hyperledger.besu.evm.operation.AbstractCreateOperation;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.operation.SLoadOperation;
import org.hyperledger.besu.evm.operation.SStoreOperation;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.util.HashSet;
import java.util.Set;

/**
 * Collects the EVM-level slow-block counters for a single unit of execution: one background
 * transaction on the parallel path, or everything the block-level path runs (system calls,
 * withdrawals, replayed transactions).
 *
 * <p>Each background transaction gets its own recorder so nothing is shared across threads, and the
 * recorder is merged into the block aggregate only if that transaction's result is actually used —
 * speculative work that loses is discarded with its world state.
 *
 * <p>Deliberately cheap: plain int counters, a single small per-execution set, no {@code
 * tracePreExecution} hook and no stack peeking. Unique storage slots come from the Block Access
 * List instead, which is why there is no per-operation set insertion here.
 */
public class SlowBlockTxRecorder implements OperationTracer {

  private int sloadCount;
  private int sstoreCount;
  private int callCount;
  private int createCount;

  /**
   * Addresses whose code was executed. Frames with empty code (plain value transfers and precompile
   * calls) are excluded so this counts contracts rather than call targets.
   */
  private final Set<Address> contractsExecuted = new HashSet<>();

  @Override
  public void tracePostExecution(
      final MessageFrame frame, final Operation.OperationResult operationResult) {
    switch (frame.getCurrentOperation()) {
      case SLoadOperation _ -> sloadCount++;
      case SStoreOperation _ -> sstoreCount++;
      case AbstractCallOperation _ -> callCount++; // CALL, CALLCODE, DELEGATECALL, STATICCALL
      case AbstractCreateOperation _ -> createCount++; // CREATE, CREATE2
      default -> {
        // no slow-block counter for other operations
      }
    }
  }

  @Override
  public void traceContextEnter(final MessageFrame frame) {
    if (frame.getCode() == null || frame.getCode().getSize() == 0) {
      return;
    }
    final Address contract = frame.getContractAddress();
    if (contract != null) {
      contractsExecuted.add(contract);
    }
  }

  /**
   * Folds this recorder's counters into the per-block aggregate.
   *
   * @param metrics the block aggregate to merge into
   */
  public void mergeInto(final SlowBlockMetrics metrics) {
    metrics.addEvmCounts(sloadCount, sstoreCount, callCount, createCount);
    metrics.addContractsExecuted(contractsExecuted);
  }

  int sloadCount() {
    return sloadCount;
  }

  int sstoreCount() {
    return sstoreCount;
  }

  int callCount() {
    return callCount;
  }

  int createCount() {
    return createCount;
  }

  Set<Address> contractsExecuted() {
    return contractsExecuted;
  }
}
