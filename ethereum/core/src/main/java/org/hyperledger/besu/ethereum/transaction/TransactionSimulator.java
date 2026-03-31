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
package org.hyperledger.besu.ethereum.transaction;

import static org.hyperledger.besu.ethereum.mainnet.feemarket.ExcessBlobGasCalculator.calculateExcessBlobGasForParent;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead;

import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.CallParameter;
import org.hyperledger.besu.datatypes.CodeDelegation;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StateOverride;
import org.hyperledger.besu.datatypes.StateOverrideMap;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.AccessLocationTracker;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.vm.DebugOperationTracer;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import jakarta.validation.constraints.NotNull;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Used to process transactions for eth_call and eth_estimateGas.
 *
 * The processing won't affect the world state, it is used to execute read operations on the
 * blockchain or to estimate the transaction gas cost.
 */
public class TransactionSimulator {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionSimulator.class);
  private static final Supplier<SignatureAlgorithm> SIGNATURE_ALGORITHM =
      Suppliers.memoize(SignatureAlgorithmFactory::getInstance);

  // Dummy signature for transactions to not fail being processed.
  private static final SECPSignature FAKE_SIGNATURE =
      SIGNATURE_ALGORITHM
          .get()
          .createSignature(
              SIGNATURE_ALGORITHM.get().getHalfCurveOrder(),
              SIGNATURE_ALGORITHM.get().getHalfCurveOrder(),
              (byte) 0);

  // TODO: Identify a better default from account to use, such as the registered
  // coinbase or an account currently unlocked by the client.
  private static final Address DEFAULT_FROM =
      Address.fromHexString("0x0000000000000000000000000000000000000000");

  private final Blockchain blockchain;
  private final WorldStateArchive worldStateArchive;
  private final ProtocolSchedule protocolSchedule;
  private final MiningConfiguration miningConfiguration;
  private final SimulationTransactionProcessorFactory simulationTransactionProcessorFactory;
  private final long rpcGasCap;

  public TransactionSimulator(
      final Blockchain blockchain,
      final WorldStateArchive worldStateArchive,
      final ProtocolSchedule protocolSchedule,
      final MiningConfiguration miningConfiguration,
      final long rpcGasCap) {
    this.blockchain = blockchain;
    this.worldStateArchive = worldStateArchive;
    this.protocolSchedule = protocolSchedule;
    this.miningConfiguration = miningConfiguration;
    this.rpcGasCap = rpcGasCap;
    this.simulationTransactionProcessorFactory =
        new SimulationTransactionProcessorFactory(protocolSchedule);
  }

  public Optional<TransactionSimulatorResult> process(
      final CallParameter callParams,
      final TransactionValidationParams transactionValidationParams,
      final OperationTracer operationTracer,
      final long blockNumber) {
    final BlockHeader header = blockchain.getBlockHeader(blockNumber).orElse(null);
    return process(
        callParams,
        Optional.empty(),
        transactionValidationParams,
        operationTracer,
        (mutableWorldState, transactionSimulatorResult) -> transactionSimulatorResult,
        header);
  }

  public Optional<TransactionSimulatorResult> process(
      final CallParameter callParams,
      final Optional<StateOverrideMap> maybeStateOverrides,
      final TransactionValidationParams transactionValidationParams,
      final OperationTracer operationTracer,
      final BlockHeader blockHeader) {
    return process(
        callParams,
        maybeStateOverrides,
        transactionValidationParams,
        operationTracer,
        (mutableWorldState, transactionSimulatorResult) -> transactionSimulatorResult,
        blockHeader);
  }

  public Optional<TransactionSimulatorResult> processOnPending(
      final CallParameter callParams,
      final Optional<StateOverrideMap> maybeStateOverrides,
      final TransactionValidationParams transactionValidationParams,
      final OperationTracer operationTracer,
      final ProcessableBlockHeader pendingBlockHeader) {

    try (final MutableWorldState disposableWorldState =
        duplicateWorldStateAtParent(pendingBlockHeader.getParentHash())) {
      WorldUpdater updater = getEffectiveWorldStateUpdater(disposableWorldState);

      // in order to trace the state diff we need to make sure that
      // the world updater always has a parent
      if (operationTracer instanceof DebugOperationTracer) {
        updater = updater.parentUpdater().isPresent() ? updater : updater.updater();
      }

      return processWithWorldUpdater(
          callParams,
          maybeStateOverrides,
          transactionValidationParams,
          operationTracer,
          pendingBlockHeader,
          updater,
          pendingBlockHeader.getCoinbase(),
          Optional.empty());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public ProcessableBlockHeader simulatePendingBlockHeader() {
    final var chainHeadHeader = blockchain.getChainHeadHeader();
    final var currentProtocolSpec = protocolSchedule.getByBlockHeader(chainHeadHeader);
    final var timestamp =
        currentProtocolSpec
            .getSlotDuration()
            .plusSeconds(chainHeadHeader.getTimestamp())
            .getSeconds();

    final ProtocolSpec protocolSpec =
        protocolSchedule.getForNextBlockHeader(chainHeadHeader, timestamp);

    final var simulatedBlockHeader =
        BlockHeaderBuilder.createPending(
                protocolSpec,
                chainHeadHeader,
                miningConfiguration,
                timestamp,
                Optional.empty(),
                Optional.empty(),
                Optional.empty())
            .buildProcessableBlockHeader();

    LOG.trace("Simulated block header: {}", simulatedBlockHeader);

    return simulatedBlockHeader;
  }

  private MutableWorldState duplicateWorldStateAtParent(final Hash parentHash) {
    final var parentHeader =
        blockchain
            .getBlockHeader(parentHash)
            .orElseThrow(
                () ->
                    new IllegalStateException("Block with hash " + parentHash + " not available"));

    final Hash parentStateRoot = parentHeader.getStateRoot();
    return worldStateArchive
        .getWorldState(withBlockHeaderAndNoUpdateNodeHead(parentHeader))
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "World state not available for block "
                        + parentHeader.getNumber()
                        + " with state root "
                        + parentStateRoot));
  }

  public Optional<TransactionSimulatorResult> processAtHead(final CallParameter callParams) {
    final var chainHeadHash = blockchain.getChainHeadHash();
    return process(
        callParams,
        TransactionValidationParams.transactionSimulatorAllowExceedingBalance(),
        OperationTracer.NO_TRACING,
        (mutableWorldState, transactionSimulatorResult) -> transactionSimulatorResult,
        blockchain
            .getBlockHeader(chainHeadHash)
            .or(() -> blockchain.getBlockHeaderSafe(chainHeadHash))
            .orElse(null));
  }

  /**
   * Processes a transaction simulation with the provided parameters and executes pre-worldstate
   * close actions.
   *
   * @param callParams The call parameters for the transaction.
   * @param transactionValidationParams The validation parameters for the transaction.
   * @param operationTracer The tracer for capturing operations during processing.
   * @param preWorldStateCloseGuard The pre-worldstate close guard for executing pre-close actions.
   * @param header The block header.
   * @return An Optional containing the result of the processing.
   */
  public <U> Optional<U> process(
      final CallParameter callParams,
      final TransactionValidationParams transactionValidationParams,
      final OperationTracer operationTracer,
      final PreCloseStateHandler<U> preWorldStateCloseGuard,
      final BlockHeader header) {
    return process(
        callParams,
        Optional.empty(),
        transactionValidationParams,
        operationTracer,
        preWorldStateCloseGuard,
        header);
  }

  /**
   * Processes a transaction simulation with the provided parameters and executes pre-worldstate
   * close actions.
   *
   * @param callParams The call parameters for the transaction.
   * @param maybeStateOverrides The map of state overrides to apply to the state for this
   *     transaction.
   * @param transactionValidationParams The validation parameters for the transaction.
   * @param operationTracer The tracer for capturing operations during processing.
   * @param preWorldStateCloseGuard The pre-worldstate close guard for executing pre-close actions.
   * @param header The block header.
   * @return An Optional containing the result of the processing.
   */
  public <U> Optional<U> process(
      final CallParameter callParams,
      final Optional<StateOverrideMap> maybeStateOverrides,
      final TransactionValidationParams transactionValidationParams,
      final OperationTracer operationTracer,
      final PreCloseStateHandler<U> preWorldStateCloseGuard,
      final BlockHeader header) {
    if (header == null) {
      return Optional.empty();
    }

    try (final MutableWorldState ws = getWorldState(header)) {

      WorldUpdater updater = getEffectiveWorldStateUpdater(ws);
      if (ws instanceof BonsaiWorldState bonsaiWorldState) {
        bonsaiWorldState.disableCacheMerkleTrieLoader();
      }
      // in order to trace the state diff we need to make sure that
      // the world updater always has a parent
      if (operationTracer instanceof DebugOperationTracer) {
        updater = updater.parentUpdater().isPresent() ? updater : updater.updater();
      }

      final var miningBeneficiary =
          protocolSchedule
              .getByBlockHeader(header)
              .getMiningBeneficiaryCalculator()
              .calculateBeneficiary(header);

      return preWorldStateCloseGuard.apply(
          ws,
          processWithWorldUpdater(
              callParams,
              maybeStateOverrides,
              transactionValidationParams,
              operationTracer,
              header,
              updater,
              miningBeneficiary,
              Optional.empty()));

    } catch (final Exception e) {
      return Optional.empty();
    }
  }

  public Optional<TransactionSimulatorResult> process(
      final CallParameter callParams, final Hash blockHeaderHash) {
    final BlockHeader header = blockchain.getBlockHeader(blockHeaderHash).orElse(null);
    return process(
        callParams,
        TransactionValidationParams.transactionSimulator(),
        OperationTracer.NO_TRACING,
        (mutableWorldState, transactionSimulatorResult) -> transactionSimulatorResult,
        header);
  }

  public Optional<TransactionSimulatorResult> process(
      final CallParameter callParams, final long blockNumber) {
    return process(
        callParams,
        TransactionValidationParams.transactionSimulator(),
        OperationTracer.NO_TRACING,
        blockNumber);
  }

  private MutableWorldState getWorldState(final BlockHeader header) {
    return worldStateArchive
        .getWorldState(withBlockHeaderAndNoUpdateNodeHead(header))
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "Public world state not available for block " + header.toLogString()));
  }

  @NotNull
  public Optional<TransactionSimulatorResult> processWithWorldUpdater(
      final CallParameter callParams,
      final Optional<StateOverrideMap> maybeStateOverrides,
      final TransactionValidationParams transactionValidationParams,
      final OperationTracer operationTracer,
      final ProcessableBlockHeader processableHeader,
      final WorldUpdater updater,
      final Address miningBeneficiary,
      final Optional<AccessLocationTracker> accessLocationTracker) {

    final long simulationGasCap =
        calculateSimulationGasCap(
            processableHeader, callParams.getGas(), processableHeader.getGasLimit());

    MainnetTransactionProcessor transactionProcessor =
        simulationTransactionProcessorFactory.getTransactionProcessor(
            processableHeader, maybeStateOverrides);

    BiFunction<ProtocolSpec, Optional<BlockHeader>, Wei> blobGasPricePerGasSupplier =
        (protocolSpec, maybeParentHeader) -> {
          if (transactionValidationParams.isAllowExceedingBalance()
              && !transactionValidationParams.isPreserveCallerGasPricing()) {
            return Wei.ZERO;
          }
          return protocolSpec
              .getFeeMarket()
              .blobGasPricePerGas(
                  maybeParentHeader
                      .map(parent -> calculateExcessBlobGasForParent(protocolSpec, parent))
                      .orElse(BlobGas.ZERO));
        };

    final ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(processableHeader);
    final BlockHashLookup blockHashLookup =
        protocolSpec
            .getPreExecutionProcessor()
            .createBlockHashLookup(blockchain, processableHeader);
    return processWithWorldUpdater(
        callParams,
        maybeStateOverrides,
        transactionValidationParams,
        operationTracer,
        processableHeader,
        updater,
        miningBeneficiary,
        simulationGasCap,
        transactionProcessor,
        blobGasPricePerGasSupplier,
        blockHashLookup,
        () -> FAKE_SIGNATURE,
        accessLocationTracker);
  }

  @NotNull
  public Optional<TransactionSimulatorResult> processWithWorldUpdater(
      final CallParameter callParams,
      final Optional<StateOverrideMap> maybeStateOverrides,
      final TransactionValidationParams transactionValidationParams,
      final OperationTracer operationTracer,
      final ProcessableBlockHeader processableHeader,
      final WorldUpdater updater,
      final Address miningBeneficiary,
      final long simulationGasCap,
      final MainnetTransactionProcessor transactionProcessor,
      final BiFunction<ProtocolSpec, Optional<BlockHeader>, Wei> blobGasPricePerGasCalculator,
      final BlockHashLookup blockHashLookup,
      final Supplier<SECPSignature> signatureSupplier,
      final Optional<AccessLocationTracker> accessLocationTracker) {

    final ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(processableHeader);
    final Address senderAddress = callParams.getSender().orElse(DEFAULT_FROM);

    final ProcessableBlockHeader blockHeaderToProcess;
    if (transactionValidationParams.isAllowExceedingBalance()
        && !transactionValidationParams.isPreserveCallerGasPricing()
        && processableHeader.getBaseFee().isPresent()) {
      blockHeaderToProcess =
          new BlockHeaderBuilder()
              .populateFrom(processableHeader)
              .baseFee(Wei.ZERO)
              .blockHeaderFunctions(protocolSpec.getBlockHeaderFunctions())
              .buildProcessableBlockHeader();
    } else {
      blockHeaderToProcess = processableHeader;
    }
    if (maybeStateOverrides.isPresent()) {
      for (Address accountToOverride : maybeStateOverrides.get().keySet()) {
        final StateOverride overrides = maybeStateOverrides.get().get(accountToOverride);
        applyOverrides(updater.getOrCreate(accountToOverride), overrides);
      }
    }

    final long nonce =
        callParams
            .getNonce()
            .orElseGet(
                () ->
                    Optional.ofNullable(updater.get(senderAddress))
                        .map(Account::getNonce)
                        .orElse(0L));

    final Optional<BlockHeader> maybeParentHeader =
        blockchain.getBlockHeader(processableHeader.getParentHash());
    final Wei blobGasPrice = blobGasPricePerGasCalculator.apply(protocolSpec, maybeParentHeader);

    final Optional<Transaction> maybeTransaction =
        buildTransaction(
            callParams,
            transactionValidationParams,
            processableHeader,
            senderAddress,
            nonce,
            simulationGasCap,
            blobGasPrice,
            signatureSupplier);
    if (maybeTransaction.isEmpty()) {
      return Optional.empty();
    }

    final Transaction transaction = maybeTransaction.get();
    final TransactionProcessingResult result =
        transactionProcessor.processTransaction(
            updater,
            blockHeaderToProcess,
            transaction,
            miningBeneficiary,
            operationTracer,
            blockHashLookup,
            transactionValidationParams,
            blobGasPrice,
            accessLocationTracker);

    return Optional.of(new TransactionSimulatorResult(transaction, result));
  }

  @VisibleForTesting
  protected static void applyOverrides(final MutableAccount account, final StateOverride override) {
    LOG.debug("applying overrides to state for account {}", account.getAddress());
    override.getNonce().ifPresent(account::setNonce);
    override.getBalance().ifPresent(account::setBalance);
    override.getCode().ifPresent(code -> account.setCode(Bytes.fromHexString(code)));
    override
        .getState()
        .ifPresent(
            d -> {
              account.clearStorage();
              d.forEach(
                  (key, value) ->
                      account.setStorageValue(
                          UInt256.fromHexString(key), UInt256.fromHexString(value)));
            });
    override
        .getStateDiff()
        .ifPresent(
            d ->
                d.forEach(
                    (key, value) ->
                        account.setStorageValue(
                            UInt256.fromHexString(key), UInt256.fromHexString(value))));
  }

  public long calculateSimulationGasCap(
      final ProcessableBlockHeader blockHeader,
      final OptionalLong maybeUserProvidedGasLimit,
      final long blockGasLimit) {
    final long simulationGasCap;

    if (maybeUserProvidedGasLimit.isPresent()) {
      long userProvidedGasLimit = maybeUserProvidedGasLimit.getAsLong();
      if (rpcGasCap > 0 && userProvidedGasLimit > rpcGasCap) {
        LOG.trace(
            "User provided gas limit {} is bigger than the value of rpc-gas-cap {}, setting simulation gas cap to the latter",
            userProvidedGasLimit,
            rpcGasCap);
        simulationGasCap = rpcGasCap;
      } else {
        LOG.trace("Using provided gas limit {} set as simulation gas cap", userProvidedGasLimit);
        simulationGasCap = userProvidedGasLimit;
      }
    } else {
      final long txGasLimitCap =
          protocolSchedule
              .getByBlockHeader(blockHeader)
              .getGasLimitCalculator()
              .transactionGasLimitCap();
      if (rpcGasCap > 0) {
        simulationGasCap = Math.min(rpcGasCap, Math.min(txGasLimitCap, blockGasLimit));
        LOG.trace(
            "No user provided gas limit, setting simulation gas cap to the value of min(rpc-gas-cap={},txGasLimitCap={},blockGasLimit={})={}",
            rpcGasCap,
            txGasLimitCap,
            blockGasLimit,
            simulationGasCap);
      } else {
        simulationGasCap = Math.min(txGasLimitCap, blockGasLimit);
        LOG.trace(
            "No user provided gas limit and rpc-gas-cap options is not set, setting simulation gas cap to min(txGasLimitCap={},blockGasLimit={})={}",
            txGasLimitCap,
            blockGasLimit,
            simulationGasCap);
      }
    }
    return simulationGasCap;
  }

  private Optional<Transaction> buildTransaction(
      final CallParameter callParams,
      final TransactionValidationParams transactionValidationParams,
      final ProcessableBlockHeader processableHeader,
      final Address senderAddress,
      final long nonce,
      final long gasLimit,
      final Wei blobGasPrice,
      final Supplier<SECPSignature> signatureSupplier) {

    final Wei value = callParams.getValue().orElse(Wei.ZERO);
    final Bytes payload = callParams.getPayload().orElse(Bytes.EMPTY);

    final Transaction.Builder transactionBuilder =
        Transaction.builder()
            .nonce(nonce)
            .gasLimit(gasLimit)
            .sender(senderAddress)
            .value(value)
            .payload(payload)
            .signature(signatureSupplier.get());

    callParams.getTo().ifPresent(transactionBuilder::to);

    // Set access list if present
    callParams.getAccessList().ifPresent(transactionBuilder::accessList);
    // Set versioned hashes if present
    callParams.getBlobVersionedHashes().ifPresent(transactionBuilder::versionedHashes);

    final boolean noPricingParametersPresent = noGasPriceParametersPresent(callParams);

    final Wei gasPrice;
    final Wei maxFeePerGas;
    final Wei maxPriorityFeePerGas;
    final Wei maxFeePerBlobGas;
    if (transactionValidationParams.isPreserveCallerGasPricing()) {
      // eth_simulateV1: use caller-provided gas pricing so fees are charged from sender's balance,
      // producing the correct stateRoot and block hash.
      gasPrice = callParams.getGasPrice().orElse(Wei.ZERO);
      maxFeePerGas = callParams.getMaxFeePerGas().orElse(Wei.ZERO);
      maxPriorityFeePerGas = callParams.getMaxPriorityFeePerGas().orElse(Wei.ZERO);
      maxFeePerBlobGas = callParams.getMaxFeePerBlobGas().orElse(Wei.ZERO);
    } else if (transactionValidationParams.isAllowExceedingBalance()) {
      // eth_call: zero gas prices so callers don't need sufficient balance for gas.
      gasPrice = Wei.ZERO;
      maxFeePerGas = Wei.ZERO;
      maxPriorityFeePerGas = Wei.ZERO;
      maxFeePerBlobGas = Wei.ZERO;
    } else {
      if (noPricingParametersPresent) {
        // in case there are no gas price parameters,
        // then set the gas price to the min necessary to process the tx.
        gasPrice = processableHeader.getBaseFee().orElse(Wei.ZERO);
      } else {
        gasPrice = callParams.getGasPrice().orElse(Wei.ZERO);
      }
      maxFeePerGas = callParams.getMaxFeePerGas().orElse(gasPrice);
      maxPriorityFeePerGas = callParams.getMaxPriorityFeePerGas().orElse(gasPrice);
      maxFeePerBlobGas = callParams.getMaxFeePerBlobGas().orElse(blobGasPrice);
    }

    if (shouldSetGasPrice(callParams, processableHeader)) {
      transactionBuilder.gasPrice(gasPrice);
    }

    if (shouldSetMaxFeePerGas(callParams, processableHeader, noPricingParametersPresent)) {
      transactionBuilder.maxFeePerGas(maxFeePerGas).maxPriorityFeePerGas(maxPriorityFeePerGas);
    }

    if (shouldSetBlobGasPrice(callParams)) {
      transactionBuilder.maxFeePerBlobGas(maxFeePerBlobGas);
    }

    final List<CodeDelegation> authorizations = callParams.getCodeDelegationAuthorizations();
    if (!authorizations.isEmpty()) {
      transactionBuilder.codeDelegations(authorizations);
    }

    transactionBuilder.guessType();

    if (transactionBuilder.getTransactionType().requiresChainId()) {
      callParams
          .getChainId()
          .ifPresentOrElse(
              transactionBuilder::chainId,
              () ->
                  // needed to make some transactions valid
                  transactionBuilder.chainId(protocolSchedule.getChainId().orElse(BigInteger.ONE)));
    }

    final Transaction transaction = transactionBuilder.build();
    return Optional.ofNullable(transaction);
  }

  public WorldUpdater getEffectiveWorldStateUpdater(final MutableWorldState publicWorldState) {
    return publicWorldState.updater();
  }

  public Optional<Boolean> doesAddressExistAtHead(final Address address) {
    final BlockHeader header = blockchain.getChainHeadHeader();
    try (final MutableWorldState worldState =
        worldStateArchive.getWorldState(withBlockHeaderAndNoUpdateNodeHead(header)).orElseThrow()) {
      return doesAddressExist(worldState, address, header);
    } catch (final Exception ex) {
      return Optional.empty();
    }
  }

  public Optional<Boolean> doesAddressExist(
      final MutableWorldState worldState, final Address address, final BlockHeader header) {
    if (header == null) {
      return Optional.empty();
    }
    if (worldState == null) {
      return Optional.empty();
    }

    return Optional.of(worldState.get(address) != null);
  }

  private boolean shouldSetGasPrice(
      final CallParameter callParams, final ProcessableBlockHeader header) {
    if (header.getBaseFee().isEmpty()) {
      return true;
    }

    // if maxPriorityFeePerGas and maxFeePerGas are not set, use gasPrice
    return callParams.getMaxPriorityFeePerGas().isEmpty() && callParams.getMaxFeePerGas().isEmpty();
  }

  private boolean shouldSetMaxFeePerGas(
      final CallParameter callParams,
      final ProcessableBlockHeader header,
      final boolean noGasPriceParametersPresent) {

    // Return false if chain ID is not present
    if (protocolSchedule.getChainId().isEmpty()) {
      return false;
    }

    // Return false if base fee is not present
    if (header.getBaseFee().isEmpty()) {
      return false;
    }

    // Return true if blob gas price should be set
    if (shouldSetBlobGasPrice(callParams)) {
      return true;
    }

    // Return true if all gas price parameters are empty
    if (noGasPriceParametersPresent) {
      return true;
    }

    // Return true if either maxPriorityFeePerGas or maxFeePerGas is present.
    // This ensures the transaction is considered EIP-1559 only if these parameters are present
    return callParams.getMaxPriorityFeePerGas().isPresent()
        || callParams.getMaxFeePerGas().isPresent();
  }

  private boolean noGasPriceParametersPresent(final CallParameter callParams) {
    // Return true if all gas price parameters are empty
    return callParams.getMaxPriorityFeePerGas().isEmpty()
        && callParams.getMaxFeePerGas().isEmpty()
        && callParams.getGasPrice().isEmpty();
  }

  private boolean shouldSetBlobGasPrice(final CallParameter callParams) {
    if (protocolSchedule.getChainId().isEmpty()) {
      return false;
    }
    return callParams.getBlobVersionedHashes().isPresent();
  }
}
