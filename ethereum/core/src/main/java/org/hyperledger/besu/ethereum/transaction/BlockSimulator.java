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
package org.hyperledger.besu.ethereum.transaction;

import static org.hyperledger.besu.ethereum.mainnet.feemarket.ExcessBlobGasCalculator.calculateExcessBlobGasForParent;
import static org.hyperledger.besu.ethereum.transaction.BlockStateCalls.fillBlockStateCalls;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead;

import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StateOverride;
import org.hyperledger.besu.datatypes.StateOverrideMap;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.ParsedExtraData;
import org.hyperledger.besu.ethereum.core.Request;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.MiningBeneficiaryCalculator;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.WithdrawalsValidator.AllowedWithdrawals;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.AccessLocationTracker;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BlockAccessListBuilder;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListFactory;
import org.hyperledger.besu.ethereum.mainnet.feemarket.BaseFeeMarket;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.mainnet.requests.RequestProcessingContext;
import org.hyperledger.besu.ethereum.mainnet.requests.RequestProcessorCoordinator;
import org.hyperledger.besu.ethereum.mainnet.systemcall.BlockProcessingContext;
import org.hyperledger.besu.ethereum.transaction.exceptions.BlockStateCallError;
import org.hyperledger.besu.ethereum.transaction.exceptions.BlockStateCallException;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.tracing.EthTransferLogOperationTracer;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.data.BlockOverrides;
import org.hyperledger.besu.plugin.services.tracer.BlockAwareOperationTracer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simulates the execution of a block, processing transactions and applying state overrides. This
 * class is responsible for simulating the execution of a block, which involves processing
 * transactions and applying state overrides. It provides a way to test and validate the behavior of
 * a block without actually executing it on the blockchain. The simulator takes into account various
 * factors, such as the block header, transaction calls, and state overrides, to simulate the
 * execution of the block. It returns a list of simulation results, which include the final block
 * header, transaction receipts, and other relevant information.
 */
public class BlockSimulator {

  private static final Logger LOG = LoggerFactory.getLogger(BlockSimulator.class);

  private static final TransactionValidationParams STRICT_VALIDATION_PARAMS =
      TransactionValidationParams.blockSimulatorStrict();

  private static final TransactionValidationParams SIMULATION_PARAMS =
      TransactionValidationParams.blockSimulatorNonStrict();

  private final TransactionSimulator transactionSimulator;
  private final WorldStateArchive worldStateArchive;
  private final ProtocolSchedule protocolSchedule;
  private final MiningConfiguration miningConfiguration;
  private final Blockchain blockchain;
  private final long rpcGasCap;

  public BlockSimulator(
      final WorldStateArchive worldStateArchive,
      final ProtocolSchedule protocolSchedule,
      final TransactionSimulator transactionSimulator,
      final MiningConfiguration miningConfiguration,
      final Blockchain blockchain,
      final long rpcGasCap) {
    this.worldStateArchive = worldStateArchive;
    this.protocolSchedule = protocolSchedule;
    this.miningConfiguration = miningConfiguration;
    this.transactionSimulator = transactionSimulator;
    this.blockchain = blockchain;
    this.rpcGasCap = rpcGasCap;
  }

  /**
   * Processes a list of BlockStateCalls sequentially, collecting the results.
   *
   * @param header The block header for all simulations.
   * @param blockSimulationParameter The BlockSimulationParameter containing the block state calls.
   * @return A list of BlockSimulationResult objects from processing each BlockStateCall.
   */
  public List<BlockSimulationResult> process(
      final BlockHeader header, final BlockSimulationParameter blockSimulationParameter) {
    return process(header, blockSimulationParameter, OperationTracer.NO_TRACING);
  }

  /**
   * Processes a list of BlockStateCalls sequentially, collecting the results.
   *
   * @param header The block header for all simulations.
   * @param blockSimulationParameter The BlockSimulationParameter containing the block state calls.
   * @param operationTracer The operation tracer to use during simulation.
   * @return A list of BlockSimulationResult objects from processing each BlockStateCall.
   */
  public List<BlockSimulationResult> process(
      final BlockHeader header,
      final BlockSimulationParameter blockSimulationParameter,
      final OperationTracer operationTracer) {
    try (final MutableWorldState ws = getWorldState(header)) {
      return process(header, blockSimulationParameter, ws, operationTracer);
    } catch (IllegalArgumentException | BlockStateCallException e) {
      throw e;
    } catch (final Exception e) {
      throw new RuntimeException("Error simulating block", e);
    }
  }

  /**
   * Processes a list of BlockStateCalls sequentially, collecting the results.
   *
   * @param blockHeader The block header for all simulations.
   * @param simulationParameter The BlockSimulationParameter containing the block state calls.
   * @param worldState The initial MutableWorldState to start with.
   * @return A list of BlockSimulationResult objects from processing each BlockStateCall.
   */
  public List<BlockSimulationResult> process(
      final BlockHeader blockHeader,
      final BlockSimulationParameter simulationParameter,
      final MutableWorldState worldState) {
    return process(blockHeader, simulationParameter, worldState, OperationTracer.NO_TRACING);
  }

  /**
   * Processes a list of BlockStateCalls sequentially, collecting the results.
   *
   * @param blockHeader The block header for all simulations.
   * @param simulationParameter The BlockSimulationParameter containing the block state calls.
   * @param worldState The initial MutableWorldState to start with.
   * @param operationTracer The operation tracer to use during simulation.
   * @return A list of BlockSimulationResult objects from processing each BlockStateCall.
   */
  public List<BlockSimulationResult> process(
      final BlockHeader blockHeader,
      final BlockSimulationParameter simulationParameter,
      final MutableWorldState worldState,
      final OperationTracer operationTracer) {
    int countStateCalls = simulationParameter.getBlockStateCalls().size();
    List<BlockSimulationResult> results = new ArrayList<>(countStateCalls);

    // Fill gaps between blocks and set the correct block number and timestamp
    List<BlockStateCall> blockStateCalls =
        fillBlockStateCalls(simulationParameter.getBlockStateCalls(), blockHeader);

    BlockHeader currentBlockHeader = blockHeader;
    HashMap<Long, Hash> blockHashCache = HashMap.newHashMap(countStateCalls);
    long simulationCumulativeGasUsed = 0;
    for (BlockStateCall stateCall : blockStateCalls) {
      BlockSimulationResult result =
          processBlockStateCall(
              currentBlockHeader,
              stateCall,
              worldState,
              simulationParameter.isValidation(),
              simulationParameter.isTraceTransfers(),
              simulationParameter.isReturnTrieLog(),
              simulationParameter::getFakeSignature,
              blockHashCache,
              simulationCumulativeGasUsed,
              operationTracer);
      results.add(result);
      BlockHeader resultBlockHeader = result.getBlock().getHeader();
      blockHashCache.put(resultBlockHeader.getNumber(), resultBlockHeader.getHash());
      currentBlockHeader = resultBlockHeader;
      simulationCumulativeGasUsed += resultBlockHeader.getGasUsed();
    }
    return results;
  }

  /**
   * Processes a single BlockStateCall, simulating the block execution.
   *
   * @param baseBlockHeader The block header for the simulation.
   * @param blockStateCall The BlockStateCall to process.
   * @param ws The MutableWorldState to use for the simulation.
   * @param operationTracer The operation tracer to use
   * @return A BlockSimulationResult from processing the BlockStateCall.
   */
  private BlockSimulationResult processBlockStateCall(
      final BlockHeader baseBlockHeader,
      final BlockStateCall blockStateCall,
      final MutableWorldState ws,
      final boolean shouldValidate,
      final boolean isTraceTransfers,
      final boolean returnTrieLog,
      final Supplier<SECPSignature> signatureSupplier,
      final Map<Long, Hash> blockHashCache,
      final long simulationCumulativeGasUsed,
      final OperationTracer operationTracer) {

    BlockOverrides blockOverrides = blockStateCall.getBlockOverrides();
    // Use the parent's actual difficulty (not Difficulty.ZERO) so that
    // TransitionProtocolSchedule correctly identifies pre-merge blocks: it dispatches
    // to the post-merge schedule when isPostMerge()=true AND difficulty==0, so a PoW
    // parent must preserve its non-zero difficulty to select the pre-merge spec.
    final BlockHeader syntheticNextBlockHeader =
        BlockHeaderBuilder.fromHeader(baseBlockHeader)
            .number(baseBlockHeader.getNumber() + 1)
            .timestamp(blockOverrides.getTimestamp().orElseThrow())
            .parentHash(baseBlockHeader.getBlockHash())
            .blockHeaderFunctions(new MainnetBlockHeaderFunctions())
            .buildBlockHeader();
    ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(syntheticNextBlockHeader);

    BlockHeader overridenBaseBlockHeader =
        overrideBlockHeader(baseBlockHeader, protocolSpec, blockOverrides, shouldValidate);

    blockStateCall
        .getStateOverrideMap()
        .ifPresent(stateOverrideMap -> applyStateOverrides(stateOverrideMap, ws));

    // Create the transaction processor with precompile address overrides
    MainnetTransactionProcessor transactionProcessor =
        new SimulationTransactionProcessorFactory(protocolSchedule)
            .getTransactionProcessor(
                overridenBaseBlockHeader, blockStateCall.getStateOverrideMap());

    BlockHashLookup blockHashLookup =
        createBlockHashLookup(
            blockOverrides, protocolSpec, overridenBaseBlockHeader, blockHashCache);

    Optional<BlockAccessListBuilder> blockAccessListBuilder =
        protocolSpec
            .getBlockAccessListFactory()
            .map(BlockAccessListFactory::newBlockAccessListBuilder);

    Optional<AccessLocationTracker> preExecutionAccessLocationTracker =
        blockAccessListBuilder.map(
            b -> BlockAccessListBuilder.createPreExecutionAccessLocationTracker());

    final BlockProcessingContext blockProcessingContext =
        new BlockProcessingContext(
            overridenBaseBlockHeader,
            ws,
            protocolSpec,
            blockHashLookup,
            operationTracer,
            Optional.empty());

    // if operationTracer is block-aware, traceStart and hold onto the option ref for traceEnd
    var maybeBlockAwareOperationTracer =
        Optional.of(operationTracer)
            .filter(z -> z instanceof BlockAwareOperationTracer)
            .map(BlockAwareOperationTracer.class::cast);

    maybeBlockAwareOperationTracer.ifPresent(
        tracer -> {
          LOG.trace("traceStartBlock sim for {}", overridenBaseBlockHeader.toLogString());
          tracer.traceStartBlock(
              ws, overridenBaseBlockHeader, overridenBaseBlockHeader.getCoinbase());
        });

    protocolSpec
        .getPreExecutionProcessor()
        .process(blockProcessingContext, preExecutionAccessLocationTracker);

    BlockStateCallSimulationResult blockStateCallSimulationResult =
        processTransactions(
            overridenBaseBlockHeader,
            blockStateCall,
            ws,
            protocolSpec,
            shouldValidate,
            isTraceTransfers,
            transactionProcessor,
            blockHashLookup,
            signatureSupplier,
            simulationCumulativeGasUsed,
            blockAccessListBuilder,
            operationTracer);

    Optional<AccessLocationTracker> postExecutionAccessLocationTracker =
        blockAccessListBuilder.map(
            b ->
                BlockAccessListBuilder.createPostExecutionAccessLocationTracker(
                    blockStateCallSimulationResult.getTransactions().size()));

    // EIP-7685: process EL requests
    final Optional<RequestProcessorCoordinator> requestProcessor =
        protocolSpec.getRequestProcessorCoordinator();
    Optional<List<Request>> maybeRequests = Optional.empty();
    if (requestProcessor.isPresent()) {
      RequestProcessingContext requestProcessingContext =
          new RequestProcessingContext(
              blockProcessingContext, blockStateCallSimulationResult.getReceipts());
      maybeRequests =
          Optional.of(
              requestProcessor
                  .get()
                  .process(requestProcessingContext, postExecutionAccessLocationTracker));
    }

    postExecutionAccessLocationTracker.ifPresent(
        tracker ->
            blockAccessListBuilder.ifPresent(
                builder -> builder.apply(tracker, ws.updater().updater())));

    // Apply block reward for PoW blocks, matching geth's FinalizeAndAssemble behaviour.
    // Post-merge specs have blockReward=ZERO and skipZeroBlockRewards=true, so no reward is
    // applied for them.
    Wei blockReward = protocolSpec.getBlockReward();
    if (!(protocolSpec.isSkipZeroBlockRewards() && blockReward.isZero())) {
      Address miner = overridenBaseBlockHeader.getCoinbase();
      WorldUpdater rewardUpdater = ws.updater();
      MutableAccount minerAccount = rewardUpdater.getOrCreate(miner);
      minerAccount.incrementBalance(blockReward);
      rewardUpdater.commit();
    }

    var finalBlock =
        createFinalBlock(
            overridenBaseBlockHeader,
            blockStateCallSimulationResult,
            blockOverrides,
            protocolSpec,
            ws,
            maybeRequests,
            maybeBlockAwareOperationTracer,
            returnTrieLog);

    return finalBlock;
  }

  protected BlockStateCallSimulationResult processTransactions(
      final BlockHeader blockHeader,
      final BlockStateCall blockStateCall,
      final MutableWorldState ws,
      final ProtocolSpec protocolSpec,
      final boolean shouldValidate,
      final boolean isTraceTransfers,
      final MainnetTransactionProcessor transactionProcessor,
      final BlockHashLookup blockHashLookup,
      final Supplier<SECPSignature> signatureSupplier,
      final long simulationCumulativeGasUsed,
      final Optional<BlockAccessListBuilder> blockAccessListBuilder,
      final OperationTracer operationTracer) {

    TransactionValidationParams transactionValidationParams =
        shouldValidate ? STRICT_VALIDATION_PARAMS : SIMULATION_PARAMS;

    BlockStateCallSimulationResult blockStateCallSimulationResult =
        new BlockStateCallSimulationResult(
            protocolSpec, calculateSimulationGasCap(blockHeader, simulationCumulativeGasUsed));

    MiningBeneficiaryCalculator miningBeneficiaryCalculator =
        blockStateCall
            .getBlockOverrides()
            .getFeeRecipient()
            .<MiningBeneficiaryCalculator>map(feeRecipient -> header -> feeRecipient)
            .orElseGet(protocolSpec::getMiningBeneficiaryCalculator);

    final WorldUpdater blockUpdater = ws.updater();
    for (int transactionLocation = 0;
        transactionLocation < blockStateCall.getCalls().size();
        transactionLocation++) {
      final WorldUpdater transactionUpdater = blockUpdater.updater();
      final CallParameter callParameter = blockStateCall.getCalls().get(transactionLocation);

      // Custom tracer and EthTraceTransfers are mutually exclusive
      OperationTracer finalOperationTracer = operationTracer;
      if (isTraceTransfers) {
        if (finalOperationTracer == OperationTracer.NO_TRACING) {
          finalOperationTracer = new EthTransferLogOperationTracer();
        } else {
          // this shouldn't happen, and isTraceTransfers will go away with Glamsterdam
          throw new IllegalArgumentException(
              "A custom tracer and traceTransfers cannot be used together."
                  + " Disable traceTransfers or omit the custom tracer.");
        }
      }

      if (callParameter.getGas().isPresent()
          && callParameter.getGas().getAsLong()
              > blockStateCallSimulationResult.getRemainingGas()) {
        throw new BlockStateCallException(
            BlockStateCallError.BLOCK_GAS_LIMIT_EXCEEDED.getMessage(),
            BlockStateCallError.BLOCK_GAS_LIMIT_EXCEEDED);
      }

      long gasLimit =
          transactionSimulator.calculateSimulationGasCap(
              blockHeader,
              callParameter.getGas(),
              blockStateCallSimulationResult.getRemainingGas());

      BiFunction<ProtocolSpec, Optional<BlockHeader>, Wei> blobGasPricePerGasSupplier =
          getBlobGasPricePerGasSupplier(
              blockStateCall.getBlockOverrides(), transactionValidationParams);

      final Optional<AccessLocationTracker> transactionLocationTracker =
          createTransactionAccessLocationTracker(blockAccessListBuilder, transactionLocation);
      final Optional<TransactionSimulatorResult> transactionSimulatorResult =
          transactionSimulator.processWithWorldUpdater(
              callParameter,
              Optional.empty(), // We have already applied state overrides on block level
              transactionValidationParams,
              finalOperationTracer,
              blockHeader,
              transactionUpdater,
              miningBeneficiaryCalculator.calculateBeneficiary(blockHeader),
              gasLimit,
              transactionProcessor,
              blobGasPricePerGasSupplier,
              blockHashLookup,
              signatureSupplier,
              transactionLocationTracker);

      TransactionSimulatorResult transactionSimulationResult =
          transactionSimulatorResult.orElseThrow(
              () ->
                  new BlockStateCallException(
                      "Transaction simulation returned no result",
                      BlockStateCallError.EMPTY_SIMULATION_RESULT));

      if (transactionSimulationResult.isInvalid()) {
        throw new BlockStateCallException(
            transactionSimulationResult.getValidationResult().getErrorMessage(),
            transactionSimulationResult.getValidationResult().getInvalidReason());
      }

      transactionSimulationResult
          .result()
          .getPartialBlockAccessView()
          .ifPresent(view -> blockAccessListBuilder.ifPresent(builder -> builder.apply(view)));

      transactionUpdater.commit();
      blockUpdater.commit();

      // Restore the original gas pricing from CallParameter into the Transaction.
      // TransactionSimulator zeroes gas prices when isAllowExceedingBalance is true to avoid
      // upfront cost failures, but for eth_simulateV1 results we need the caller-provided values
      // so that gasPrice, maxFeePerGas, maxPriorityFeePerGas, transactionHash, and
      // transactionsRoot are correct in the response.
      if (transactionValidationParams.isAllowExceedingBalance()) {
        transactionSimulationResult = restoreGasPricing(transactionSimulationResult, callParameter);
      }

      blockStateCallSimulationResult.add(transactionSimulationResult, ws, finalOperationTracer);
    }

    blockAccessListBuilder.ifPresent(b -> blockStateCallSimulationResult.set(b.build()));
    return blockStateCallSimulationResult;
  }

  /**
   * Restores the original gas pricing from the CallParameter into the processed Transaction. During
   * simulation, TransactionSimulator zeroes gas prices when isAllowExceedingBalance is true to
   * avoid upfront cost failures. This method rebuilds the Transaction with the caller's original
   * gas pricing so that the result fields (gasPrice, maxFeePerGas, etc.) and derived values
   * (transactionHash, transactionsRoot) are correct.
   */
  private TransactionSimulatorResult restoreGasPricing(
      final TransactionSimulatorResult result, final CallParameter callParameter) {
    final Transaction original = result.transaction();
    final boolean hasGasPrice = callParameter.getGasPrice().isPresent();
    final boolean hasMaxFeePerGas = callParameter.getMaxFeePerGas().isPresent();
    final boolean hasMaxPriorityFeePerGas = callParameter.getMaxPriorityFeePerGas().isPresent();
    final boolean hasMaxFeePerBlobGas =
        original.getType().supportsBlob() && callParameter.getMaxFeePerBlobGas().isPresent();

    if (!hasGasPrice && !hasMaxFeePerGas && !hasMaxPriorityFeePerGas && !hasMaxFeePerBlobGas) {
      return result;
    }

    final Transaction.Builder builder = Transaction.builder().copiedFrom(original);
    callParameter.getGasPrice().ifPresent(builder::gasPrice);
    callParameter.getMaxFeePerGas().ifPresent(builder::maxFeePerGas);
    callParameter.getMaxPriorityFeePerGas().ifPresent(builder::maxPriorityFeePerGas);
    if (original.getType().supportsBlob()) {
      callParameter.getMaxFeePerBlobGas().ifPresent(builder::maxFeePerBlobGas);
    }

    return new TransactionSimulatorResult(builder.build(), result.result());
  }

  private Optional<AccessLocationTracker> createTransactionAccessLocationTracker(
      final Optional<BlockAccessListBuilder> blockAccessListBuilder,
      final int transactionLocation) {
    return blockAccessListBuilder.map(
        b -> BlockAccessListBuilder.createTransactionAccessLocationTracker(transactionLocation));
  }

  private BlockSimulationResult createFinalBlock(
      final BlockHeader blockHeader,
      final BlockStateCallSimulationResult simResult,
      final BlockOverrides blockOverrides,
      final ProtocolSpec protocolSpec,
      final MutableWorldState ws,
      final Optional<List<Request>> maybeRequests,
      final Optional<BlockAwareOperationTracer> maybeBlockAwareOperationTracer,
      final boolean returnTrieLog) {

    List<Transaction> transactions = simResult.getTransactions();
    List<TransactionReceipt> receipts = simResult.getReceipts();

    boolean isShanghaiPlus = protocolSpec.getWithdrawalsValidator() instanceof AllowedWithdrawals;
    boolean isCancunPlus = protocolSpec.getFeeMarket().implementsBlobFee();

    BlockHeaderBuilder headerBuilder =
        BlockHeaderBuilder.createDefault()
            .populateFrom(blockHeader)
            .ommersHash(BodyValidation.ommersHash(List.of()))
            .stateRoot(blockOverrides.getStateRoot().orElseGet(ws::frontierRootHash))
            .transactionsRoot(BodyValidation.transactionsRoot(transactions))
            .receiptsRoot(BodyValidation.receiptsRoot(receipts))
            .logsBloom(BodyValidation.logsBloom(receipts))
            .gasUsed(simResult.getCumulativeGasUsed())
            .requestsHash(maybeRequests.map(BodyValidation::requestsHash).orElse(null))
            .balHash(simResult.getBlockAccessList().map(BodyValidation::balHash).orElse(null))
            .extraData(blockOverrides.getExtraData().orElse(Bytes.EMPTY))
            .blockHeaderFunctions(new BlockStateCallBlockHeaderFunctions(blockOverrides));

    if (isCancunPlus) {
      headerBuilder.blobGasUsed(simResult.getCumulativeBlobGasUsed());
    } else {
      // Clear defaults set by createDefault() for pre-Cancun blocks
      headerBuilder.blobGasUsed(null);
      headerBuilder.excessBlobGas(null);
    }

    if (isShanghaiPlus) {
      headerBuilder.withdrawalsRoot(BodyValidation.withdrawalsRoot(List.of()));
    }

    BlockHeader finalBlockHeader = headerBuilder.buildBlockHeader();

    Optional<List<Withdrawal>> withdrawals =
        isShanghaiPlus ? Optional.of(List.of()) : Optional.empty();

    Block block = new Block(finalBlockHeader, new BlockBody(transactions, List.of(), withdrawals));

    // if we have a block-aware operation tracer, trace end block here
    maybeBlockAwareOperationTracer.ifPresent(
        tracer -> {
          LOG.trace("traceEndBlock sim for {}", finalBlockHeader.toLogString());
          tracer.traceEndBlock(finalBlockHeader, block.getBody());
        });

    if (returnTrieLog && ws instanceof PathBasedWorldState) {
      // if requested and path-based worldstate, return result with trielog and serializer:
      var pathBasedArchive = (PathBasedWorldStateProvider) worldStateArchive;
      var pathBasedAccumulator = ((PathBasedWorldState) ws).getAccumulator();
      var trieLogFactory = pathBasedArchive.getTrieLogManager().getTrieLogFactory();
      var trieLog = trieLogFactory.create(pathBasedAccumulator, finalBlockHeader);
      return new BlockSimulationResult(
          block, simResult, trieLog, log -> Bytes.wrap(trieLogFactory.serialize(log)));
    } else {
      // otherwise return result w/o trielog
      return new BlockSimulationResult(block, simResult);
    }
  }

  /**
   * Applies state overrides to the world state.
   *
   * @param stateOverrideMap The StateOverrideMap containing the state overrides.
   * @param ws The MutableWorldState to apply the overrides to.
   */
  @VisibleForTesting
  protected void applyStateOverrides(
      final StateOverrideMap stateOverrideMap, final MutableWorldState ws) {
    var updater = ws.updater();
    for (Address accountToOverride : stateOverrideMap.keySet()) {
      final StateOverride override = stateOverrideMap.get(accountToOverride);
      if (hasAccountStateOverrides(override)) {
        MutableAccount account = updater.getOrCreate(accountToOverride);
        TransactionSimulator.applyOverrides(account, override);
      }
    }
    updater.commit();
  }

  private boolean hasAccountStateOverrides(final StateOverride override) {
    return override.getNonce().isPresent()
        || override.getBalance().isPresent()
        || override.getCode().isPresent()
        || override.getState().isPresent()
        || override.getStateDiff().isPresent();
  }

  /**
   * Applies block header overrides to the block header.
   *
   * @param header The original block header.
   * @param newProtocolSpec The ProtocolSpec for the block.
   * @param blockOverrides The BlockOverrides to apply.
   * @return The modified block header.
   */
  @VisibleForTesting
  protected BlockHeader overrideBlockHeader(
      final BlockHeader header,
      final ProtocolSpec newProtocolSpec,
      final BlockOverrides blockOverrides,
      final boolean shouldValidate) {
    long timestamp = blockOverrides.getTimestamp().orElseThrow();
    long blockNumber = blockOverrides.getBlockNumber().orElseThrow();

    // The fee recipient is stored in the header coinbase field. Precedence:
    // (1) an explicit feeRecipient in blockOverrides, otherwise
    // (2) the parent header's coinbase — this allows a feeRecipient set in an earlier simulated
    // block to persist across subsequent blocks without needing to repeat it in every override.
    BlockHeaderBuilder builder =
        BlockHeaderBuilder.createDefault()
            .parentHash(header.getHash())
            .timestamp(timestamp)
            .number(blockNumber)
            .coinbase(blockOverrides.getFeeRecipient().orElse(header.getCoinbase()))
            .difficulty(
                blockOverrides.getDifficulty().map(Difficulty::of).orElseGet(header::getDifficulty))
            .gasLimit(
                blockOverrides
                    .getGasLimit()
                    .orElseGet(() -> getNextGasLimit(newProtocolSpec, header, blockNumber)))
            .extraData(blockOverrides.getExtraData().orElse(Bytes.EMPTY))
            .prevRandao(blockOverrides.getMixHashOrPrevRandao().orElse(Bytes32.ZERO));

    // London+: baseFee
    if (newProtocolSpec.getFeeMarket().implementsBaseFee()) {
      builder.baseFee(
          blockOverrides
              .getBaseFeePerGas()
              .orElseGet(
                  () ->
                      shouldValidate
                          ? getNextBaseFee(newProtocolSpec, header, blockNumber)
                          : Wei.ZERO));
    }

    // Cancun+: parentBeaconBlockRoot (set for all Cancun-timestamp blocks, not just PoS,
    // matching geth's eth_simulateV1 behaviour where EIP-4788 runs based on timestamp)
    if (newProtocolSpec.getFeeMarket().implementsBlobFee()) {
      builder.parentBeaconBlockRoot(blockOverrides.getParentBeaconBlockRoot().orElse(Bytes32.ZERO));
    } else {
      builder.parentBeaconBlockRoot(null);
    }

    // Cancun+: excessBlobGas
    if (newProtocolSpec.getFeeMarket().implementsBlobFee()) {
      builder.excessBlobGas(calculateExcessBlobGasForParent(newProtocolSpec, header));
    } else {
      builder.excessBlobGas(null);
      builder.blobGasUsed(null);
    }

    return builder
        .blockHeaderFunctions(new BlockStateCallBlockHeaderFunctions(blockOverrides))
        .buildBlockHeader();
  }

  private BiFunction<ProtocolSpec, Optional<BlockHeader>, Wei> getBlobGasPricePerGasSupplier(
      final BlockOverrides blockOverrides,
      final TransactionValidationParams transactionValidationParams) {
    if (blockOverrides.getBlobBaseFee().isPresent()) {
      return (protocolSchedule, blockHeader) -> blockOverrides.getBlobBaseFee().get();
    }
    return (protocolSpec, maybeParentHeader) -> {
      if (transactionValidationParams.isAllowExceedingBalance()) {
        return Wei.ZERO;
      }
      return protocolSpec
          .getFeeMarket()
          .blobGasPricePerGas(
              maybeParentHeader
                  .map(parent -> calculateExcessBlobGasForParent(protocolSpec, parent))
                  .orElse(BlobGas.ZERO));
    };
  }

  private long getNextGasLimit(
      final ProtocolSpec protocolSpec, final BlockHeader parentHeader, final long blockNumber) {
    return protocolSpec
        .getGasLimitCalculator()
        .nextGasLimit(
            parentHeader.getGasLimit(),
            miningConfiguration.getTargetGasLimit().orElse(parentHeader.getGasLimit()),
            blockNumber);
  }

  private Wei getNextBaseFee(
      final ProtocolSpec protocolSpec, final BlockHeader parentHeader, final long blockNumber) {
    return Optional.of(protocolSpec.getFeeMarket())
        .filter(FeeMarket::implementsBaseFee)
        .map(BaseFeeMarket.class::cast)
        .map(
            feeMarket ->
                feeMarket.computeBaseFee(
                    blockNumber,
                    parentHeader.getBaseFee().orElse(Wei.ZERO),
                    parentHeader.getGasUsed(),
                    feeMarket.targetGasUsed(parentHeader)))
        .orElse(null);
  }

  private MutableWorldState getWorldState(final BlockHeader blockHeader) {
    return worldStateArchive
        .getWorldState(withBlockHeaderAndNoUpdateNodeHead(blockHeader))
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "Public world state not available for block " + blockHeader.toLogString()));
  }

  private static class BlockStateCallBlockHeaderFunctions implements BlockHeaderFunctions {

    private final BlockOverrides blockOverrides;
    private final MainnetBlockHeaderFunctions blockHeaderFunctions =
        new MainnetBlockHeaderFunctions();

    private BlockStateCallBlockHeaderFunctions(final BlockOverrides blockOverrides) {
      this.blockOverrides = blockOverrides;
    }

    @Override
    public Hash hash(final BlockHeader header) {
      return blockOverrides.getBlockHash().orElseGet(() -> blockHeaderFunctions.hash(header));
    }

    @Override
    public ParsedExtraData parseExtraData(final BlockHeader header) {
      return blockHeaderFunctions.parseExtraData(header);
    }
  }

  /**
   * Creates a BlockHashLookup for the block simulation. If a BlockHashLookup is provided in the
   * BlockOverrides, it is used. Otherwise, the default BlockHashLookup is created.
   *
   * @param blockOverrides The BlockOverrides to use.
   * @param newProtocolSpec The ProtocolSpec for the block.
   * @param blockHeader The block header for the simulation.
   * @param blockHashCache A cache of block hashes.
   * @return The BlockHashLookup for the block simulation.
   */
  private BlockHashLookup createBlockHashLookup(
      final BlockOverrides blockOverrides,
      final ProtocolSpec newProtocolSpec,
      final BlockHeader blockHeader,
      final Map<Long, Hash> blockHashCache) {
    var blockCallBlockHashLookup =
        blockOverrides
            .getBlockHashLookup()
            .<BlockHashLookup>map(
                blockHashLookup -> (___, blockNumber) -> blockHashLookup.apply(blockNumber))
            .orElseGet(
                () ->
                    newProtocolSpec
                        .getPreExecutionProcessor()
                        .createBlockHashLookup(blockchain, blockHeader));
    // Fallback lookup using the real chain head, for when the primary lookup can't
    // walk back past simulated blocks to reach real chain blocks.
    var chainHeadFallback =
        newProtocolSpec
            .getPreExecutionProcessor()
            .createBlockHashLookup(blockchain, blockchain.getChainHeadHeader());
    return (frame, blockNumber) -> {
      if (blockHashCache.containsKey(blockNumber)) {
        return blockHashCache.get(blockNumber);
      }
      Hash result = blockCallBlockHashLookup.apply(frame, blockNumber);
      if (!result.equals(Hash.ZERO)) {
        return result;
      }
      return chainHeadFallback.apply(frame, blockNumber);
    };
  }

  public long calculateSimulationGasCap(
      final BlockHeader blockHeader, final long simulationCumulativeGasUsed) {
    if (rpcGasCap > 0) {
      long remainingGas = Math.max(rpcGasCap - simulationCumulativeGasUsed, 0);
      return Math.min(remainingGas, blockHeader.getGasLimit());
    }
    return blockHeader.getGasLimit();
  }
}
