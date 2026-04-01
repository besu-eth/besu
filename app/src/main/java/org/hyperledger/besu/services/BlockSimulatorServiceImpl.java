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
package org.hyperledger.besu.services;

import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.StateOverrideMap;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.transaction.BlockSimulationParameter;
import org.hyperledger.besu.ethereum.transaction.BlockSimulationResult;
import org.hyperledger.besu.ethereum.transaction.BlockSimulator;
import org.hyperledger.besu.ethereum.transaction.BlockStateCall;
import org.hyperledger.besu.ethereum.transaction.CallParameter;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.plugin.Unstable;
import org.hyperledger.besu.plugin.data.BlockOverrides;
import org.hyperledger.besu.plugin.data.PluginBlockSimulationResult;
import org.hyperledger.besu.plugin.data.TransactionSimulationResult;
import org.hyperledger.besu.plugin.services.BlockSimulationService;

import java.util.List;

/** This class is a service that simulates the processing of a block */
public class BlockSimulatorServiceImpl implements BlockSimulationService {
  private final BlockSimulator blockSimulator;
  private final WorldStateArchive worldStateArchive;
  private final Blockchain blockchain;

  // Dummy signature for transactions to not fail being processed.
  private final SECPSignature fakeSignature;

  /**
   * This constructor creates a BlockSimulatorServiceImpl object
   *
   * @param worldStateArchive the world state archive
   * @param miningConfiguration the mining configuration
   * @param transactionSimulator the transaction simulator
   * @param protocolSchedule the protocol schedule
   * @param blockchain the blockchain
   */
  public BlockSimulatorServiceImpl(
      final WorldStateArchive worldStateArchive,
      final MiningConfiguration miningConfiguration,
      final TransactionSimulator transactionSimulator,
      final ProtocolSchedule protocolSchedule,
      final Blockchain blockchain) {
    this.blockchain = blockchain;
    final SignatureAlgorithm signatureAlgorithm = SignatureAlgorithmFactory.getInstance();
    this.fakeSignature =
        signatureAlgorithm.createSignature(
            signatureAlgorithm.getHalfCurveOrder(),
            signatureAlgorithm.getHalfCurveOrder(),
            (byte) 0);
    blockSimulator =
        new BlockSimulator(
            worldStateArchive,
            protocolSchedule,
            transactionSimulator,
            miningConfiguration,
            blockchain,
            0);
    this.worldStateArchive = worldStateArchive;
  }

  /**
   * Simulate the processing of a block given a header, a list of transactions, and blockOverrides.
   *
   * @param blockNumber the block number
   * @param transactions the transactions to include in the block
   * @param blockOverrides the blockSimulationOverride of the block
   * @param stateOverrides state overrides of the block
   * @return the block context
   */
  @Override
  public PluginBlockSimulationResult simulate(
      final long blockNumber,
      final List<? extends Transaction> transactions,
      final BlockOverrides blockOverrides,
      final StateOverrideMap stateOverrides) {
    return processSimulation(
        blockNumber,
        transactions,
        blockOverrides,
        stateOverrides,
        false,
        OperationTracer.NO_TRACING);
  }

  /**
   * Simulate the processing of a block given a header, a list of transactions, blockOverrides, and
   * a tracer.
   *
   * @param blockNumber the block number
   * @param transactions the transactions to include in the block
   * @param blockOverrides the blockSimulationOverride of the block
   * @param stateOverrides state overrides of the block
   * @param tracer the operation tracer to use during simulation
   * @return the block context
   */
  @Override
  public PluginBlockSimulationResult simulate(
      final long blockNumber,
      final List<? extends Transaction> transactions,
      final BlockOverrides blockOverrides,
      final StateOverrideMap stateOverrides,
      final OperationTracer tracer) {
    return processSimulation(
        blockNumber, transactions, blockOverrides, stateOverrides, false, tracer);
  }

  /**
   * This method is experimental and should be used with caution. Simulate the processing of a block
   * given a header, a list of transactions, and blockOverrides and persist the WorldState
   *
   * @param blockNumber the block number
   * @param transactions the transactions to include in the block
   * @param blockOverrides block overrides for the block
   * @param stateOverrides state overrides of the block
   * @return the PluginBlockSimulationResult
   */
  @Unstable
  @Override
  public PluginBlockSimulationResult simulateAndPersistWorldState(
      final long blockNumber,
      final List<? extends Transaction> transactions,
      final BlockOverrides blockOverrides,
      final StateOverrideMap stateOverrides) {
    return processSimulation(
        blockNumber,
        transactions,
        blockOverrides,
        stateOverrides,
        true,
        OperationTracer.NO_TRACING);
  }

  private PluginBlockSimulationResult processSimulation(
      final long blockNumber,
      final List<? extends Transaction> transactions,
      final BlockOverrides blockOverrides,
      final StateOverrideMap stateOverrides,
      final boolean persistWorldState,
      final OperationTracer tracer) {
    BlockHeader header = getBlockHeader(blockNumber);
    List<CallParameter> callParameters =
        transactions.stream().map(CallParameter::fromTransaction).toList();
    BlockStateCall blockStateCall =
        new BlockStateCall(callParameters, blockOverrides, stateOverrides);
    try (final MutableWorldState ws = getWorldState(header, persistWorldState)) {
      BlockSimulationParameter blockSimulationParameter =
          new BlockSimulationParameter.BlockSimulationParameterBuilder()
              .blockStateCalls(List.of(blockStateCall))
              .validation(true)
              .fakeSignature(fakeSignature)
              .build();

      List<BlockSimulationResult> results =
          blockSimulator.process(header, blockSimulationParameter, ws, tracer);
      BlockSimulationResult result = results.getFirst();
      if (persistWorldState) {
        ws.persist(result.getBlock().getHeader());
      }
      return response(result);
    } catch (final Exception e) {
      throw new RuntimeException("Error simulating block", e);
    }
  }

  private BlockHeader getBlockHeader(final long blockNumber) {
    return blockchain
        .getBlockHeader(blockNumber)
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "Block header not found for block number: " + blockNumber));
  }

  private MutableWorldState getWorldState(final BlockHeader header, final boolean isPersisting) {
    final WorldStateQueryParams worldStateQueryParams =
        WorldStateQueryParams.newBuilder()
            .withBlockHeader(header)
            .withShouldWorldStateUpdateHead(isPersisting)
            .build();
    return worldStateArchive
        .getWorldState(worldStateQueryParams)
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "World state not available for block number (block hash): "
                        + header.toLogString()));
  }

  private PluginBlockSimulationResult response(final BlockSimulationResult result) {
    return new PluginBlockSimulationResult(
        result.getBlockHeader(),
        result.getBlockBody(),
        result.getReceipts(),
        result.getTransactionSimulations().stream()
            .map(
                simulation ->
                    new TransactionSimulationResult(simulation.transaction(), simulation.result()))
            .toList(),
        result.getTrieLog());
  }
}
