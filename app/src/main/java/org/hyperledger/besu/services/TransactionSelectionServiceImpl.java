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
package org.hyperledger.besu.services;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.PendingTransaction;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.worldstate.WorldView;
import org.hyperledger.besu.plugin.data.BlockBody;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.data.ProcessableBlockHeader;
import org.hyperledger.besu.plugin.data.TransactionProcessingResult;
import org.hyperledger.besu.plugin.data.TransactionSelectionResult;
import org.hyperledger.besu.plugin.services.TransactionSelectionService;
import org.hyperledger.besu.plugin.services.tracer.BlockAwareOperationTracer;
import org.hyperledger.besu.plugin.services.txselection.BlockTransactionSelectionService;
import org.hyperledger.besu.plugin.services.txselection.PluginTransactionSelector;
import org.hyperledger.besu.plugin.services.txselection.PluginTransactionSelectorFactory;
import org.hyperledger.besu.plugin.services.txselection.SelectorsStateManager;
import org.hyperledger.besu.plugin.services.txselection.TransactionEvaluationContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;

/** The Transaction Selection service implementation. */
public class TransactionSelectionServiceImpl implements TransactionSelectionService {

  /** Default Constructor. */
  public TransactionSelectionServiceImpl() {}

  private List<PluginTransactionSelectorFactory> factories;

  @Override
  public PluginTransactionSelector createPluginTransactionSelector(
      final ProcessableBlockHeader processableBlockHeader,
      final SelectorsStateManager selectorsStateManager) {
    if (factories == null) {
      return PluginTransactionSelector.ACCEPT_ALL;
    }
    return new AggregatedPluginTransactionSelector(
        factories.stream()
            .map(factory -> factory.create(processableBlockHeader, selectorsStateManager))
            .toList());
  }

  @Override
  public void selectPendingTransactions(
      final BlockTransactionSelectionService selectionService,
      final ProcessableBlockHeader pendingBlockHeader,
      final List<? extends PendingTransaction> candidatePendingTransactions) {
    if (factories != null) {
      factories.forEach(
          factory ->
              factory.selectPendingTransactions(
                  selectionService, pendingBlockHeader, candidatePendingTransactions));
    }
  }

  @Override
  public void registerPluginTransactionSelectorFactory(
      final PluginTransactionSelectorFactory pluginTransactionSelectorFactory) {
    if (factories == null) {
      factories = new ArrayList<>();
    }
    factories.add(pluginTransactionSelectorFactory);
  }

  private static class AggregatedPluginTransactionSelector implements PluginTransactionSelector {
    private final List<PluginTransactionSelector> pluginTransactionSelectors;

    AggregatedPluginTransactionSelector(
        final List<PluginTransactionSelector> pluginTransactionSelectors) {
      this.pluginTransactionSelectors = pluginTransactionSelectors;
    }

    @Override
    public BlockAwareOperationTracer getOperationTracer() {
      return new TracerAggregator(
          pluginTransactionSelectors.stream()
              .map(PluginTransactionSelector::getOperationTracer)
              .toList());
    }

    @Override
    public TransactionSelectionResult evaluateTransactionPreProcessing(
        final TransactionEvaluationContext evaluationContext) {
      return pluginTransactionSelectors.stream()
          .map(selector -> selector.evaluateTransactionPreProcessing(evaluationContext))
          .filter(result -> !result.selected())
          .findAny()
          .orElse(TransactionSelectionResult.SELECTED);
    }

    @Override
    public TransactionSelectionResult evaluateTransactionPostProcessing(
        final TransactionEvaluationContext evaluationContext,
        final TransactionProcessingResult processingResult) {

      return pluginTransactionSelectors.stream()
          .map(
              selector ->
                  selector.evaluateTransactionPostProcessing(evaluationContext, processingResult))
          .filter(result -> !result.selected())
          .findAny()
          .orElse(TransactionSelectionResult.SELECTED);
    }

    @Override
    public void onTransactionSelected(
        final TransactionEvaluationContext evaluationContext,
        final TransactionProcessingResult processingResult) {
      pluginTransactionSelectors.forEach(
          pluginTransactionSelector ->
              pluginTransactionSelector.onTransactionSelected(evaluationContext, processingResult));
    }

    @Override
    public void onTransactionNotSelected(
        final TransactionEvaluationContext evaluationContext,
        final TransactionSelectionResult transactionSelectionResult) {
      pluginTransactionSelectors.forEach(
          pluginTransactionSelector ->
              pluginTransactionSelector.onTransactionNotSelected(
                  evaluationContext, transactionSelectionResult));
    }
  }

  private static class TracerAggregator implements BlockAwareOperationTracer {
    private final List<BlockAwareOperationTracer> blockAwareOperationTracers;

    public TracerAggregator(final List<BlockAwareOperationTracer> blockAwareOperationTracers) {
      this.blockAwareOperationTracers = blockAwareOperationTracers;
    }

    @Override
    public void traceStartBlock(
        final WorldView worldView,
        final BlockHeader blockHeader,
        final BlockBody blockBody,
        final Address miningBeneficiary) {
      blockAwareOperationTracers.forEach(
          blockAwareOperationTracer ->
              blockAwareOperationTracer.traceStartBlock(
                  worldView, blockHeader, blockBody, miningBeneficiary));
    }

    @Override
    public void traceEndBlock(final BlockHeader blockHeader, final BlockBody blockBody) {
      blockAwareOperationTracers.forEach(
          blockAwareOperationTracer ->
              blockAwareOperationTracer.traceEndBlock(blockHeader, blockBody));
    }

    @Override
    public void traceStartBlock(
        final WorldView worldView,
        final ProcessableBlockHeader processableBlockHeader,
        final Address miningBeneficiary) {
      blockAwareOperationTracers.forEach(
          blockAwareOperationTracer ->
              blockAwareOperationTracer.traceStartBlock(
                  worldView, processableBlockHeader, miningBeneficiary));
    }

    @Override
    public boolean isExtendedTracing() {
      return blockAwareOperationTracers.stream()
          .anyMatch(BlockAwareOperationTracer::isExtendedTracing);
    }

    @Override
    public void tracePreExecution(final MessageFrame frame) {
      blockAwareOperationTracers.forEach(
          blockAwareOperationTracer -> blockAwareOperationTracer.tracePreExecution(frame));
    }

    @Override
    public void tracePostExecution(
        final MessageFrame frame, final Operation.OperationResult operationResult) {
      blockAwareOperationTracers.forEach(
          blockAwareOperationTracer ->
              blockAwareOperationTracer.tracePostExecution(frame, operationResult));
    }

    @Override
    public void tracePrecompileCall(
        final MessageFrame frame, final long gasRequirement, final Bytes output) {
      blockAwareOperationTracers.forEach(
          blockAwareOperationTracer ->
              blockAwareOperationTracer.tracePrecompileCall(frame, gasRequirement, output));
    }

    @Override
    public void traceAccountCreationResult(
        final MessageFrame frame, final Optional<ExceptionalHaltReason> haltReason) {
      blockAwareOperationTracers.forEach(
          blockAwareOperationTracer ->
              blockAwareOperationTracer.traceAccountCreationResult(frame, haltReason));
    }

    @Override
    public void tracePrepareTransaction(final WorldView worldView, final Transaction transaction) {
      blockAwareOperationTracers.forEach(
          blockAwareOperationTracer ->
              blockAwareOperationTracer.tracePrepareTransaction(worldView, transaction));
    }

    @Override
    public void traceStartTransaction(final WorldView worldView, final Transaction transaction) {
      blockAwareOperationTracers.forEach(
          blockAwareOperationTracer ->
              blockAwareOperationTracer.traceStartTransaction(worldView, transaction));
    }

    @Override
    public void traceBeforeRewardTransaction(
        final WorldView worldView, final Transaction tx, final Wei miningReward) {
      blockAwareOperationTracers.forEach(
          blockAwareOperationTracer ->
              blockAwareOperationTracer.traceBeforeRewardTransaction(worldView, tx, miningReward));
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
      blockAwareOperationTracers.forEach(
          blockAwareOperationTracer ->
              blockAwareOperationTracer.traceEndTransaction(
                  worldView, tx, status, output, logs, gasUsed, selfDestructs, timeNs));
    }

    @Override
    public void traceContextEnter(final MessageFrame frame) {
      blockAwareOperationTracers.forEach(
          blockAwareOperationTracer -> blockAwareOperationTracer.traceContextEnter(frame));
    }

    @Override
    public void traceContextReEnter(final MessageFrame frame) {
      blockAwareOperationTracers.forEach(
          blockAwareOperationTracer -> blockAwareOperationTracer.traceContextReEnter(frame));
    }

    @Override
    public void traceContextExit(final MessageFrame frame) {
      blockAwareOperationTracers.forEach(
          blockAwareOperationTracer -> blockAwareOperationTracer.traceContextExit(frame));
    }
  }
}
