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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_2;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_1_KEYPAIR;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_2_KEYPAIR;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.CONTRACT_ADDRESS;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.GENESIS_CONFIG;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.MINING_BENEFICIARY;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.MINING_BENEFICIARY_KEYPAIR;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;

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
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BlockExecutionContext;
import org.hyperledger.besu.ethereum.mainnet.BlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.ImmutableBalConfiguration;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.MissingCodeException;
import org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

/** A block calling a contract whose code is missing from storage fails as a storage fault. */
class MissingCodeBlockImportTest extends AbstractParallelBlockProcessorIntegrationTest {

  private final Address contract = Address.fromHexStringStrict(CONTRACT_ADDRESS);

  @Override
  protected String getVariantName() {
    return "Missing code";
  }

  /** Caches no code, as a node with a cold cache, e.g. after a restart, reads it from storage. */
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

  @Override
  protected BalConfiguration getBalConfiguration() {
    return ImmutableBalConfiguration.builder().isPerfectParallelizationEnabled(false).build();
  }

  @Test
  void parallelImportFailsOnMissingCode() {
    final ExecutionContextTestFixture ctx = createFreshContext();
    removeCode(ctx, contract, ctx.getStateArchive().getWorldState().get(contract).getCodeHash());
    // the parallel workers then read a parent state taken after the corruption
    advanceCanonicalChainWithOneEmptyBlock(ctx, Wei.of(5));
    final MutableWorldState worldState = ctx.getStateArchive().getWorldState();
    final Block block =
        createBlock(
            ctx,
            Hash.ZERO,
            Wei.of(5),
            createContractCallTransaction(
                0, contract, "setSlot1", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(42)));

    assertThatThrownBy(() -> process(createParallelProcessor(ctx), ctx, worldState, block))
        .isInstanceOf(MissingCodeException.class);
  }

  @Test
  void healedBlockImportsWithTheExpectedStateRoot() {
    // the transfer is applied before the call fails, a replay must not apply it twice
    final Transaction transfer =
        createTransferTransaction(
            0, 1_000L, 300_000L, 0L, 5L, ACCOUNT_2, ACCOUNT_GENESIS_2_KEYPAIR);
    final Transaction call =
        createContractCallTransaction(
            0, contract, "setSlot1", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(42));
    final Hash stateRoot = discoverStateRoot(Wei.of(5), transfer, call);

    final ExecutionContextTestFixture ctx = createFreshContext();
    final BonsaiWorldStateProvider archive = (BonsaiWorldStateProvider) ctx.getStateArchive();
    final MutableWorldState worldState = archive.getWorldState();
    final Hash codeHash = worldState.get(contract).getCodeHash();
    final Bytes code =
        archive
            .getWorldStateKeyValueStorage()
            .getCodeBytes(codeHash, contract.addressHash())
            .orElseThrow();
    removeCode(ctx, contract, codeHash);
    final Block block = createBlock(ctx, stateRoot, Wei.of(5), transfer, call);
    final BlockProcessor processor = createSequentialProcessor(ctx);

    assertThatThrownBy(() -> process(processor, ctx, worldState, block))
        .isInstanceOf(MissingCodeException.class);

    archive.setCodeFetcher(hash -> Optional.of(code));
    assertThat(archive.healMissingCode(contract, codeHash)).isTrue();

    final BlockProcessingResult result = process(processor, ctx, worldState, block);
    assertThat(result.isSuccessful()).as(result.errorMessage.orElse("")).isTrue();
    assertThat(worldState.rootHash()).isEqualTo(stateRoot);
  }

  @Test
  void healedCodeIsSeenByTheCachedWorldStates() throws Exception {
    final ExecutionContextTestFixture ctx = createFreshContext();
    final BonsaiWorldStateProvider archive = (BonsaiWorldStateProvider) ctx.getStateArchive();
    final Hash codeHash = archive.getWorldState().get(contract).getCodeHash();
    final Bytes code =
        archive
            .getWorldStateKeyValueStorage()
            .getCodeBytes(codeHash, contract.addressHash())
            .orElseThrow();
    removeCode(ctx, contract, codeHash);
    // the layer cached for this block is a snapshot taken without the code
    advanceCanonicalChainWithOneEmptyBlock(ctx, Wei.of(5));
    final BlockHeader head = ctx.getBlockchain().getChainHeadHeader();
    assertThatThrownBy(() -> cachedCode(archive, head, codeHash))
        .isInstanceOf(MissingCodeException.class);

    archive.setCodeFetcher(hash -> Optional.of(code));
    assertThat(archive.healMissingCode(contract, codeHash)).isTrue();

    assertThat(cachedCode(archive, head, codeHash)).contains(code);
  }

  @Test
  void rollingOverAMissingCodeRollsAgainOnceHealed() {
    final ExecutionContextTestFixture ctx = createFreshContext();
    final BonsaiWorldStateProvider archive = (BonsaiWorldStateProvider) ctx.getStateArchive();
    final BlockHeader genesis = ctx.getBlockchain().getChainHeadHeader();
    importBlock(ctx, delegate(MINING_BENEFICIARY_KEYPAIR, contract));
    final Bytes delegation =
        Bytes.concatenate(Bytes.fromHexString("0xef0100"), contract.getBytes());
    final Hash codeHash = Hash.hash(delegation);
    removeCode(ctx, MINING_BENEFICIARY, codeHash);

    // rolling the head back to the genesis reads the code block 1 replaced
    assertThatThrownBy(
            () ->
                archive.getWorldState(
                    WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(genesis)))
        .isInstanceOf(MissingCodeException.class);
    assertThat(archive.getWorldState().rootHash())
        .isEqualTo(ctx.getBlockchain().getChainHeadHeader().getStateRoot());

    archive.setCodeFetcher(hash -> Optional.of(delegation));
    assertThat(archive.healMissingCode(MINING_BENEFICIARY, codeHash)).isTrue();

    // the failed rolling left no change behind, so the head rolls again from block 1
    assertThat(
            archive
                .getWorldState(WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(genesis))
                .map(MutableWorldState::rootHash))
        .contains(genesis.getStateRoot());
  }

  @Test
  void blockOnlyPayingTheFeeRecipientImportsWithoutItsCode() {
    final Transaction delegation = delegate(MINING_BENEFICIARY_KEYPAIR, contract);
    final ExecutionContextTestFixture healthy = createFreshContext();
    importBlock(healthy, delegation);
    final ExecutionContextTestFixture ctx = createFreshContext();
    final BonsaiWorldStateProvider archive = (BonsaiWorldStateProvider) ctx.getStateArchive();
    importBlock(ctx, delegation);
    final Bytes designator =
        Bytes.concatenate(Bytes.fromHexString("0xef0100"), contract.getBytes());
    final Hash codeHash = Hash.hash(designator);
    removeCode(ctx, MINING_BENEFICIARY, codeHash);

    // a tip never runs the fee recipient's code, and paying it only needs its code hash
    final Transaction tip =
        createTransferTransaction(0, 1L, 300_000L, 2L, 10L, ACCOUNT_2, ACCOUNT_GENESIS_2_KEYPAIR);
    final Hash stateRoot =
        discoverStateRootAtParent(
            healthy,
            healthy.getBlockchain().getChainHeadHeader(),
            Wei.of(5),
            MINING_BENEFICIARY,
            tip);
    final BlockHeader parent = ctx.getBlockchain().getChainHeadHeader();
    final MutableWorldState worldState =
        archive
            .getWorldState(WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(parent))
            .orElseThrow();
    final Block block = createBlock(ctx, parent, stateRoot, Wei.of(5), MINING_BENEFICIARY, tip);

    final BlockProcessingResult result =
        process(createSequentialProcessor(ctx), ctx, worldState, block);
    assertThat(result.isSuccessful()).as(result.errorMessage.orElse("")).isTrue();
    assertThat(worldState.rootHash()).isEqualTo(stateRoot);
    // the code is still missing, it is healed by the first block that runs it
    assertThat(
            archive
                .getWorldStateKeyValueStorage()
                .getCodeBytes(codeHash, MINING_BENEFICIARY.addressHash()))
        .isEmpty();
  }

  private void importBlock(final ExecutionContextTestFixture ctx, final Transaction... txs) {
    final BlockHeader parent = ctx.getBlockchain().getChainHeadHeader();
    final Hash stateRoot =
        discoverStateRootAtParent(ctx, parent, Wei.of(5), MINING_BENEFICIARY, txs);
    final MutableWorldState worldState =
        ctx.getStateArchive()
            .getWorldState(WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(parent))
            .orElseThrow();
    final Block block = createBlock(ctx, parent, stateRoot, Wei.of(5), MINING_BENEFICIARY, txs);
    final BlockProcessingResult result =
        process(createSequentialProcessor(ctx), ctx, worldState, block);
    assertThat(result.isSuccessful()).isTrue();
    ctx.getBlockchain()
        .appendBlock(
            block, result.getYield().orElseThrow().getReceipts(), getBlockAccessList(result));
  }

  private Transaction delegate(final KeyPair authority, final Address target) {
    final org.hyperledger.besu.datatypes.CodeDelegation delegation =
        CodeDelegation.builder()
            .chainId(BigInteger.valueOf(42))
            .address(target)
            .nonce(0)
            .signAndBuild(authority);
    return Transaction.builder()
        .type(TransactionType.DELEGATE_CODE)
        .nonce(0)
        .maxPriorityFeePerGas(Wei.ZERO)
        .maxFeePerGas(Wei.of(5))
        .gasLimit(500_000L)
        .to(Address.fromHexStringStrict(ACCOUNT_2))
        .value(Wei.ZERO)
        .payload(Bytes.EMPTY)
        .codeDelegations(List.of(delegation))
        .chainId(BigInteger.valueOf(42))
        .signAndBuild(ACCOUNT_GENESIS_1_KEYPAIR);
  }

  private Optional<Bytes> cachedCode(
      final BonsaiWorldStateProvider archive, final BlockHeader header, final Hash codeHash)
      throws Exception {
    try (MutableWorldState worldState =
        archive
            .getWorldState(WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(header))
            .orElseThrow()) {
      return ((BonsaiWorldState) worldState).getCode(contract, codeHash).map(Code::getBytes);
    }
  }

  private BlockProcessingResult process(
      final BlockProcessor processor,
      final ExecutionContextTestFixture ctx,
      final MutableWorldState worldState,
      final Block block) {
    return processor.processBlock(
        BlockExecutionContext.builder()
            .protocolContext(ctx.getProtocolContext())
            .worldState(worldState)
            .block(block)
            .blockAccessList(Optional.empty())
            .build());
  }

  private void removeCode(
      final ExecutionContextTestFixture ctx, final Address address, final Hash codeHash) {
    // the code is keyed by code hash or by account hash, depending on the storage
    final SegmentedKeyValueStorageTransaction tx =
        ((BonsaiWorldStateProvider) ctx.getStateArchive())
            .getWorldStateKeyValueStorage()
            .getComposedWorldStateStorage()
            .startTransaction();
    tx.remove(CODE_STORAGE, codeHash.getBytes().toArrayUnsafe());
    tx.remove(CODE_STORAGE, address.addressHash().getBytes().toArrayUnsafe());
    tx.commit();
  }
}
