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
package org.hyperledger.besu.ethereum.blockcreation;

import static org.hyperledger.besu.ethereum.core.BlockHeaderBuilder.createPending;
import static org.hyperledger.besu.ethereum.mainnet.feemarket.ExcessBlobGasCalculator.calculateExcessBlobGasForParent;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.blockcreation.txselection.BlockTransactionSelector;
import org.hyperledger.besu.ethereum.blockcreation.txselection.TransactionSelectionResults;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.core.Request;
import org.hyperledger.besu.ethereum.core.SealableBlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.AbstractBlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.ScheduleBasedBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.WithdrawalsProcessor;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.AccessLocationTracker;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BlockAccessListBuilder;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListFactory;
import org.hyperledger.besu.ethereum.mainnet.feemarket.ExcessBlobGasCalculator;
import org.hyperledger.besu.ethereum.mainnet.requests.RequestProcessingContext;
import org.hyperledger.besu.ethereum.mainnet.requests.RequestProcessorCoordinator;
import org.hyperledger.besu.ethereum.mainnet.systemcall.BlockProcessingContext;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.securitymodule.SecurityModuleException;
import org.hyperledger.besu.plugin.services.txselection.PluginTransactionSelector;
import org.hyperledger.besu.plugin.services.txselection.SelectorsStateManager;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.Lists;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractBlockCreator implements AsyncBlockCreator {

  public interface ExtraDataCalculator {

    Bytes get(final BlockHeader parent);
  }

  private static final Logger LOG = LoggerFactory.getLogger(AbstractBlockCreator.class);

  private final MiningBeneficiaryCalculator miningBeneficiaryCalculator;
  private final ExtraDataCalculator extraDataCalculator;
  private final TransactionPool transactionPool;
  protected final MiningConfiguration miningConfiguration;
  protected final ProtocolContext protocolContext;
  protected final ProtocolSchedule protocolSchedule;
  protected final BlockHeaderFunctions blockHeaderFunctions;
  private final EthScheduler ethScheduler;
  private final AtomicBoolean isCancelled = new AtomicBoolean(false);
  private volatile BlockTransactionSelector selector;

  protected AbstractBlockCreator(
      final MiningConfiguration miningConfiguration,
      final MiningBeneficiaryCalculator miningBeneficiaryCalculator,
      final ExtraDataCalculator extraDataCalculator,
      final TransactionPool transactionPool,
      final ProtocolContext protocolContext,
      final ProtocolSchedule protocolSchedule,
      final EthScheduler ethScheduler) {
    this.miningConfiguration = miningConfiguration;
    this.miningBeneficiaryCalculator = miningBeneficiaryCalculator;
    this.extraDataCalculator = extraDataCalculator;
    this.transactionPool = transactionPool;
    this.protocolContext = protocolContext;
    this.protocolSchedule = protocolSchedule;
    this.ethScheduler = ethScheduler;
    blockHeaderFunctions = ScheduleBasedBlockHeaderFunctions.create(protocolSchedule);
  }

  /**
   * Create block will create a new block at the head of the blockchain specified in the
   * protocolContext.
   *
   * <p>It will select transactions from the PendingTransaction list for inclusion in the Block
   * body, and will supply an empty Ommers list.
   *
   * <p>Once transactions have been selected and applied to a disposable/temporary world state, the
   * block reward is paid to the relevant coinbase, and a sealable header is constructed.
   *
   * <p>The sealableHeader is then provided to child instances for sealing (i.e. proof of work or
   * otherwise).
   *
   * <p>The constructed block is then returned.
   *
   * @return a block with appropriately selected transactions, seals and ommers.
   */
  @Override
  public BlockCreationResult createBlock(final long timestamp, final BlockHeader parentHeader) {
    return createBlock(Optional.empty(), Optional.empty(), timestamp, parentHeader);
  }

  @Override
  public BlockCreationResult createBlock(
      final List<Transaction> transactions,
      final List<BlockHeader> ommers,
      final long timestamp,
      final BlockHeader parentHeader) {
    return createBlock(Optional.of(transactions), Optional.of(ommers), timestamp, parentHeader);
  }

  @Override
  public BlockCreationResult createBlock(
      final Optional<List<Transaction>> maybeTransactions,
      final Optional<List<BlockHeader>> maybeOmmers,
      final long timestamp,
      final BlockHeader parentHeader) {
    return createBlock(
        maybeTransactions,
        maybeOmmers,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        timestamp,
        true,
        parentHeader);
  }

  @Override
  public BlockCreationResult createEmptyWithdrawalsBlock(
      final long timestamp, final BlockHeader parentHeader) {
    throw new UnsupportedOperationException("Only used by BFT block creators");
  }

  public BlockCreationResult createBlock(
      final Optional<List<Transaction>> maybeTransactions,
      final Optional<List<BlockHeader>> maybeOmmers,
      final Optional<List<Withdrawal>> maybeWithdrawals,
      final long timestamp,
      final BlockHeader parentHeader) {
    return createBlock(
        maybeTransactions,
        maybeOmmers,
        maybeWithdrawals,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        timestamp,
        true,
        parentHeader);
  }

  public BlockCreationResult createBlock(
      final Optional<List<Transaction>> maybeTransactions,
      final Optional<List<BlockHeader>> maybeOmmers,
      final Optional<List<Withdrawal>> maybeWithdrawals,
      final Optional<Bytes32> maybePrevRandao,
      final Optional<Bytes32> maybeParentBeaconBlockRoot,
      final Optional<Long> maybeSlotNumber,
      final long timestamp,
      final boolean rewardCoinbase,
      final BlockHeader parentHeader) {

    final var timings = new BlockCreationTiming();

    try (final MutableWorldState disposableWorldState = duplicateWorldStateAtParent(parentHeader)) {
      timings.register("duplicateWorldState");
      final ProtocolSpec newProtocolSpec =
          protocolSchedule.getForNextBlockHeader(parentHeader, timestamp);

      final ProcessableBlockHeader processableBlockHeader =
          createPending(
                  newProtocolSpec,
                  parentHeader,
                  miningConfiguration,
                  timestamp,
                  maybePrevRandao,
                  maybeParentBeaconBlockRoot,
                  maybeSlotNumber)
              .buildProcessableBlockHeader();

      final Address miningBeneficiary =
          miningBeneficiaryCalculator.getMiningBeneficiary(timestamp, processableBlockHeader);

      final List<BlockHeader> ommers = maybeOmmers.orElse(selectOmmers());

      final var selectorsStateManager = new SelectorsStateManager();
      final var pluginTransactionSelector =
          miningConfiguration
              .getTransactionSelectionService()
              .createPluginTransactionSelector(processableBlockHeader, selectorsStateManager);
      final var operationTracer = pluginTransactionSelector.getOperationTracer();
      operationTracer.traceStartBlock(
          disposableWorldState, processableBlockHeader, miningBeneficiary);

      final Optional<BlockAccessListBuilder> blockAccessListBuilder =
          newProtocolSpec
              .getBlockAccessListFactory()
              .map(BlockAccessListFactory::newBlockAccessListBuilder);
      final Optional<AccessLocationTracker> preExecutionAccessLocationTracker =
          blockAccessListBuilder.map(
              b -> BlockAccessListBuilder.createPreExecutionAccessLocationTracker());

      BlockProcessingContext blockProcessingContext =
          new BlockProcessingContext(
              processableBlockHeader,
              disposableWorldState,
              newProtocolSpec,
              newProtocolSpec
                  .getPreExecutionProcessor()
                  .createBlockHashLookup(protocolContext.getBlockchain(), processableBlockHeader),
              operationTracer,
              blockAccessListBuilder);
      newProtocolSpec
          .getPreExecutionProcessor()
          .process(blockProcessingContext, preExecutionAccessLocationTracker);

      timings.register("preTxsSelection");
      final TransactionSelectionResults transactionResults =
          selectTransactions(
              processableBlockHeader,
              disposableWorldState,
              maybeTransactions,
              miningBeneficiary,
              newProtocolSpec,
              pluginTransactionSelector,
              selectorsStateManager,
              parentHeader,
              blockAccessListBuilder);
      transactionResults.logSelectionStats();
      timings.register("txsSelection");

      final Optional<AccessLocationTracker> postExecutionAccessLocationTracker =
          blockAccessListBuilder.map(
              b ->
                  BlockAccessListBuilder.createPostExecutionAccessLocationTracker(
                      transactionResults.getSelectedTransactions().size()));

      final Optional<WithdrawalsProcessor> maybeWithdrawalsProcessor =
          newProtocolSpec.getWithdrawalsProcessor();

      final boolean withdrawalsCanBeProcessed =
          maybeWithdrawalsProcessor.isPresent() && maybeWithdrawals.isPresent();
      if (withdrawalsCanBeProcessed) {
        maybeWithdrawalsProcessor
            .get()
            .processWithdrawals(
                maybeWithdrawals.get(),
                disposableWorldState.updater(),
                postExecutionAccessLocationTracker,
                blockAccessListBuilder);
      }

      // EIP-7685: process EL requests
      final Optional<RequestProcessorCoordinator> requestProcessor =
          newProtocolSpec.getRequestProcessorCoordinator();
      RequestProcessingContext requestProcessingContext =
          new RequestProcessingContext(blockProcessingContext, transactionResults.getReceipts());

      Optional<List<Request>> maybeRequests =
          requestProcessor.map(
              processor ->
                  processor.process(requestProcessingContext, postExecutionAccessLocationTracker));

      postExecutionAccessLocationTracker.ifPresent(
          tracker ->
              blockAccessListBuilder.ifPresent(
                  builder -> builder.apply(tracker, disposableWorldState.updater().updater())));

      if (rewardCoinbase
          && !rewardBeneficiary(
              disposableWorldState,
              processableBlockHeader,
              ommers,
              miningBeneficiary,
              newProtocolSpec.getBlockReward(),
              newProtocolSpec.isSkipZeroBlockRewards(),
              newProtocolSpec)) {
        LOG.trace("Failed to apply mining reward, exiting.");
        throw new RuntimeException("Failed to apply mining reward.");
      }

      final GasUsage usage =
          computeExcessBlobGas(transactionResults, newProtocolSpec, parentHeader);

      BlockHeaderBuilder builder =
          BlockHeaderBuilder.create()
              .populateFrom(processableBlockHeader)
              .ommersHash(BodyValidation.ommersHash(ommers))
              .stateRoot(disposableWorldState.rootHash())
              .transactionsRoot(
                  BodyValidation.transactionsRoot(transactionResults.getSelectedTransactions()))
              .receiptsRoot(BodyValidation.receiptsRoot(transactionResults.getReceipts()))
              .logsBloom(BodyValidation.logsBloom(transactionResults.getReceipts()))
              .gasUsed(
                  Math.max(
                      transactionResults.getCumulativeRegularGasUsed(),
                      transactionResults.getCumulativeStateGasUsed()))
              .extraData(extraDataCalculator.get(parentHeader))
              .withdrawalsRoot(
                  withdrawalsCanBeProcessed
                      ? BodyValidation.withdrawalsRoot(maybeWithdrawals.get())
                      : null)
              .requestsHash(maybeRequests.map(BodyValidation::requestsHash).orElse(null));

      final Optional<BlockAccessList> blockAccessList =
          blockAccessListBuilder.map(BlockAccessListBuilder::build);
      blockAccessList.ifPresent(b -> builder.balHash(BodyValidation.balHash(b)));
      if (usage != null) {
        builder.blobGasUsed(usage.used.toLong()).excessBlobGas(usage.excessBlobGas);
      }

      final SealableBlockHeader sealableBlockHeader = builder.buildSealableBlockHeader();

      final BlockHeader blockHeader = createFinalBlockHeader(sealableBlockHeader);

      final Optional<List<Withdrawal>> withdrawals =
          withdrawalsCanBeProcessed ? maybeWithdrawals : Optional.empty();
      final BlockBody blockBody =
          new BlockBody(transactionResults.getSelectedTransactions(), ommers, withdrawals);
      final Block block = new Block(blockHeader, blockBody);

      operationTracer.traceEndBlock(blockHeader, blockBody);
      timings.register("blockAssembled");
      return new BlockCreationResult(
          block, transactionResults, timings, blockAccessList, maybeRequests);
    } catch (final SecurityModuleException ex) {
      throw new IllegalStateException("Failed to create block signature", ex);
    } catch (final CancellationException | StorageException ex) {
      throw ex;
    } catch (final Exception ex) {
      throw new IllegalStateException(
          "Block creation failed unexpectedly. Will restart on next block added to chain.", ex);
    }
  }

  record GasUsage(BlobGas excessBlobGas, BlobGas used) {}

  private GasUsage computeExcessBlobGas(
      final TransactionSelectionResults transactionResults,
      final ProtocolSpec newProtocolSpec,
      final BlockHeader parentHeader) {

    if (newProtocolSpec.getFeeMarket().implementsBlobFee()) {
      final var gasCalculator = newProtocolSpec.getGasCalculator();
      final int newBlobsCount =
          transactionResults.getTransactionsByType(TransactionType.BLOB).stream()
              .map(tx -> tx.getVersionedHashes().orElseThrow())
              .mapToInt(List::size)
              .sum();
      // casting parent excess blob gas to long since for the moment it should be well below that
      // limit
      BlobGas excessBlobGas =
          ExcessBlobGasCalculator.calculateExcessBlobGasForParent(newProtocolSpec, parentHeader);
      BlobGas used = BlobGas.of(gasCalculator.blobGasCost(newBlobsCount));
      return new GasUsage(excessBlobGas, used);
    }
    return null;
  }

  private TransactionSelectionResults selectTransactions(
      final ProcessableBlockHeader processableBlockHeader,
      final MutableWorldState disposableWorldState,
      final Optional<List<Transaction>> transactions,
      final Address miningBeneficiary,
      final ProtocolSpec protocolSpec,
      final PluginTransactionSelector pluginTransactionSelector,
      final SelectorsStateManager selectorsStateManager,
      final BlockHeader parentHeader,
      final Optional<BlockAccessListBuilder> blockAccessListBuilder)
      throws RuntimeException {
    final MainnetTransactionProcessor transactionProcessor = protocolSpec.getTransactionProcessor();

    final AbstractBlockProcessor.TransactionReceiptFactory transactionReceiptFactory =
        protocolSpec.getTransactionReceiptFactory();

    Wei blobGasPrice =
        protocolSpec
            .getFeeMarket()
            .blobGasPricePerGas(calculateExcessBlobGasForParent(protocolSpec, parentHeader));

    this.selector =
        new BlockTransactionSelector(
            miningConfiguration,
            transactionProcessor,
            protocolContext.getBlockchain(),
            disposableWorldState,
            transactionPool,
            processableBlockHeader,
            transactionReceiptFactory,
            miningBeneficiary,
            blobGasPrice,
            protocolSpec,
            pluginTransactionSelector,
            ethScheduler,
            selectorsStateManager,
            blockAccessListBuilder);

    if (transactions.isPresent()) {
      return selector.evaluateTransactions(transactions.get());
    } else {
      return selector.buildTransactionListForBlock();
    }
  }

  private MutableWorldState duplicateWorldStateAtParent(final BlockHeader parentHeader) {
    final Hash parentStateRoot = parentHeader.getStateRoot();
    return protocolContext
        .getWorldStateArchive()
        .getWorldState(withBlockHeaderAndNoUpdateNodeHead(parentHeader))
        .orElseThrow(
            () -> {
              LOG.info("Unable to create block because world state is not available");
              return new CancellationException(
                  "World state not available for block "
                      + parentHeader.getNumber()
                      + " with state root "
                      + parentStateRoot);
            });
  }

  private List<BlockHeader> selectOmmers() {
    return Lists.newArrayList();
  }

  @Override
  public void cancel() {
    LOG.debug("Block creation cancellation requested");
    isCancelled.set(true);
    final var currSelector = selector;
    if (currSelector != null) {
      currSelector.cancel();
    }
  }

  @Override
  public boolean isCancelled() {
    return isCancelled.get();
  }

  /* Copied from BlockProcessor (with modifications). */
  boolean rewardBeneficiary(
      final MutableWorldState worldState,
      final ProcessableBlockHeader header,
      final List<BlockHeader> ommers,
      final Address miningBeneficiary,
      final Wei blockReward,
      final boolean skipZeroBlockRewards,
      final ProtocolSpec protocolSpec) {

    // TODO(tmm): Added to make this work, should come from blockProcessor.
    final int MAX_GENERATION = 6;
    if (skipZeroBlockRewards && blockReward.isZero()) {
      return true;
    }

    final Wei coinbaseReward =
        protocolSpec
            .getBlockProcessor()
            .getCoinbaseReward(blockReward, header.getNumber(), ommers.size());
    final WorldUpdater updater = worldState.updater();
    final MutableAccount beneficiary = updater.getOrCreate(miningBeneficiary);

    beneficiary.incrementBalance(coinbaseReward);
    for (final BlockHeader ommerHeader : ommers) {
      if (ommerHeader.getNumber() - header.getNumber() > MAX_GENERATION) {
        LOG.trace(
            "Block processing error: ommer block number {} more than {} generations current block number {}",
            ommerHeader.getNumber(),
            MAX_GENERATION,
            header.getNumber());
        return false;
      }

      final MutableAccount ommerCoinbase = updater.getOrCreate(ommerHeader.getCoinbase());
      final Wei ommerReward =
          protocolSpec
              .getBlockProcessor()
              .getOmmerReward(blockReward, header.getNumber(), ommerHeader.getNumber());
      ommerCoinbase.incrementBalance(ommerReward);
    }

    updater.commit();

    return true;
  }

  protected abstract BlockHeader createFinalBlockHeader(
      final SealableBlockHeader sealableBlockHeader);

  @FunctionalInterface
  public interface MiningBeneficiaryCalculator {
    Address getMiningBeneficiary(long blockTimestamp, ProcessableBlockHeader parentHeader);
  }
}
