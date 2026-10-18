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
package org.hyperledger.besu.ethereum.mainnet.parallelization;

import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_2;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_3;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_1_KEYPAIR;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_2_KEYPAIR;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.CONTRACT_ADDRESS;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.GENESIS_CONFIG;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.MINING_BENEFICIARY;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.MINING_BENEFICIARY_KEYPAIR;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.PARALLEL_TEST_CONTRACT;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.generateKeyPair;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.CodeDelegation;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.BlockExecutionContext;
import org.hyperledger.besu.ethereum.mainnet.BlockProcessor;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for a fee recipient whose EIP-7702 delegation changes in a block that also pays
 * it a tip. Blocks are chained, so the code persisted by a block is executed by the next one.
 */
public abstract class AbstractFeeRecipientDelegationTest
    extends AbstractParallelBlockProcessorIntegrationTest {

  private static final Wei BASE_FEE = Wei.of(1);
  private static final KeyPair NEW_FEE_RECIPIENT_KEYPAIR =
      generateKeyPair("5f0e6b3b8f4a0c1d2e3f405162738495a6b7c8d9eaf0b1c2d3e4f50617283941");

  private final Address contract = Address.fromHexStringStrict(CONTRACT_ADDRESS);
  private final Address parallelTestContract = Address.fromHexStringStrict(PARALLEL_TEST_CONTRACT);

  /** Caches no code, so a worker reads the fee recipient's code as on a node with a cold cache. */
  @Override
  protected ExecutionContextTestFixture createFreshContext() {
    return ExecutionContextTestFixture.builder(GenesisConfig.fromResource(GENESIS_CONFIG))
        .dataStorageFormat(DataStorageFormat.BONSAI)
        .codeCache(
            new BonsaiCodeCache() {
              @Override
              public void put(final Hash codeHash, final Code code) {}
            })
        .build();
  }

  @Test
  @DisplayName("A tip to a new fee recipient keeps the delegation it got in the same block")
  void tipToNewFeeRecipientKeepsItsDelegation() {
    final Address feeRecipient = Address.extract(NEW_FEE_RECIPIENT_KEYPAIR.getPublicKey());

    final MutableWorldState parWorldState =
        importAndCompare(
            feeRecipient,
            block(createDelegationTransaction(0, NEW_FEE_RECIPIENT_KEYPAIR, 0, contract), tip()),
            block(
                createContractCallTransaction(
                    1, feeRecipient, "setSlot1", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(42))));

    assertContractStorage(parWorldState, feeRecipient, 0, 42);
  }

  @Test
  @DisplayName(
      "A tip to a delegated fee recipient keeps the delegation it changed in the same block")
  void tipToDelegatedFeeRecipientKeepsItsNewDelegation() {
    final MutableWorldState parWorldState =
        importAndCompare(
            MINING_BENEFICIARY,
            block(createDelegationTransaction(0, MINING_BENEFICIARY_KEYPAIR, 0, contract)),
            block(
                createDelegationTransaction(1, MINING_BENEFICIARY_KEYPAIR, 1, parallelTestContract),
                tip()),
            block(
                createContractCallTransaction(
                    2,
                    MINING_BENEFICIARY,
                    "incrementSlot1",
                    ACCOUNT_GENESIS_1_KEYPAIR,
                    Optional.empty())));

    assertContractStorage(parWorldState, MINING_BENEFICIARY, 0, 1);
  }

  /** Imports the blocks with both processors, the parallel one must reach every state root. */
  private MutableWorldState importAndCompare(
      final Address coinbase, final Transaction[]... blocks) {
    final ExecutionContextTestFixture seqCtx = createFreshContext();
    final ExecutionContextTestFixture parCtx = createFreshContext();
    MutableWorldState parWorldState = null;
    for (final Transaction[] txs : blocks) {
      final Hash stateRoot =
          discoverStateRootAtParent(
              seqCtx, seqCtx.getBlockchain().getChainHeadHeader(), BASE_FEE, coinbase, txs);
      importBlock(
          seqCtx, createSequentialProcessor(seqCtx), "Sequential", stateRoot, coinbase, txs);
      parWorldState =
          importBlock(
              parCtx, createParallelProcessor(parCtx), getVariantName(), stateRoot, coinbase, txs);
    }
    return parWorldState;
  }

  private MutableWorldState importBlock(
      final ExecutionContextTestFixture ctx,
      final BlockProcessor processor,
      final String processorName,
      final Hash stateRoot,
      final Address coinbase,
      final Transaction... txs) {
    final BlockHeader parent = ctx.getBlockchain().getChainHeadHeader();
    final MutableWorldState worldState =
        ctx.getStateArchive()
            .getWorldState(WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(parent))
            .orElseThrow();
    final Block block = createBlock(ctx, parent, stateRoot, BASE_FEE, coinbase, txs);
    final BlockProcessingResult result =
        processor.processBlock(
            BlockExecutionContext.builder()
                .protocolContext(ctx.getProtocolContext())
                .worldState(worldState)
                .block(block)
                .blockAccessList(Optional.empty())
                .build());
    assertTrue(
        result.isSuccessful(),
        processorName
            + " import of block "
            + block.getHeader().getNumber()
            + " failed: "
            + result.errorMessage.orElse("(no message)"));
    ctx.getBlockchain()
        .appendBlock(
            block, result.getYield().orElseThrow().getReceipts(), getBlockAccessList(result));
    return worldState;
  }

  private static Transaction[] block(final Transaction... txs) {
    return txs;
  }

  /** Only touches the fee recipient for its reward, so it is merged from its parallel run. */
  private Transaction tip() {
    return createTransferTransaction(
        0, 1L, 300_000L, 2L, 10L, ACCOUNT_2, ACCOUNT_GENESIS_2_KEYPAIR);
  }

  private Transaction createDelegationTransaction(
      final long nonce,
      final KeyPair authorityKeyPair,
      final long authorityNonce,
      final Address delegate) {
    final org.hyperledger.besu.datatypes.CodeDelegation delegation =
        CodeDelegation.builder()
            .chainId(BigInteger.valueOf(42))
            .address(delegate)
            .nonce(authorityNonce)
            .signAndBuild(authorityKeyPair);
    return Transaction.builder()
        .type(TransactionType.DELEGATE_CODE)
        .nonce(nonce)
        .maxPriorityFeePerGas(Wei.ZERO)
        .maxFeePerGas(Wei.of(5))
        .gasLimit(500_000L)
        .to(Address.fromHexStringStrict(ACCOUNT_3))
        .value(Wei.ZERO)
        .payload(Bytes.EMPTY)
        .codeDelegations(List.of(delegation))
        .chainId(BigInteger.valueOf(42))
        .signAndBuild(ACCOUNT_GENESIS_1_KEYPAIR);
  }
}
