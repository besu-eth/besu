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
package org.hyperledger.besu.consensus.qbft.adaptor;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.common.bft.BftBlockHeaderFunctions;
import org.hyperledger.besu.consensus.common.bft.BftExtraData;
import org.hyperledger.besu.consensus.common.bft.BftHelpers;
import org.hyperledger.besu.consensus.qbft.QbftExtraDataCodec;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockValidator;
import org.hyperledger.besu.datatypes.AccessListEntry;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.BlockValidator;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.inclusionlist.InclusionListTransactions;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class QbftBlockValidatorAdaptorTest {
  @Mock private BlockValidator blockValidator;
  @Mock private ProtocolContext protocolContext;
  private final QbftExtraDataCodec codec = new QbftExtraDataCodec();
  private final BlockDataGenerator generator = new BlockDataGenerator();
  private final BlockBody body = new BlockBody(List.of(generator.transaction()), emptyList());
  private final ValidatedBlockCache validatedBlockCache = new ValidatedBlockCache();

  @Test
  void validateSuccessfullyWhenBesuValidatorSuccessful() {
    final Block block = proposal(0, body);
    stubValidation(block, Optional.empty(), new BlockProcessingResult(Optional.empty()));

    final QbftBlockValidator.ValidationResult validationResult = validate(block, Optional.empty());

    assertThat(validationResult.success()).isTrue();
    assertThat(validationResult.errorMessage()).isEmpty();
  }

  @Test
  void validateFailsWhenBesuValidatorFails() {
    final Block block = proposal(0, body);
    stubValidation(block, Optional.empty(), new BlockProcessingResult("failed"));

    final QbftBlockValidator.ValidationResult validationResult = validate(block, Optional.empty());

    assertThat(validationResult.success()).isFalse();
    assertThat(validationResult.errorMessage()).contains("failed");
  }

  @Test
  void recordsOutputsOfValidatedBlockUnderOnchainHash() {
    // round 1, so the proposal hash is not the on-chain hash
    final Block block = proposal(1, body);
    assertThat(onchainHash(block)).isNotEqualTo(block.getHash());
    final List<TransactionReceipt> receipts = generator.receipts(block);
    stubValidation(block, Optional.empty(), successWith(receipts));

    validate(block, Optional.empty());

    assertThat(validatedBlockCache.take(onchainHash(block), block.getHeader().getNumber()))
        .contains(
            new ValidatedBlockCache.ValidatedBlock(
                block.getHeader().getNumber(), body, receipts, Optional.empty()));
  }

  @Test
  void doesNotRecordBlockThatFailedValidation() {
    final Block block = proposal(0, body);
    stubValidation(block, Optional.empty(), new BlockProcessingResult("failed"));

    validate(block, Optional.empty());

    assertThat(validatedBlockCache.take(onchainHash(block), block.getHeader().getNumber()))
        .isEmpty();
  }

  @Test
  void doesNotExecuteBlockCreatedOrValidatedBefore() {
    final Block block = proposal(0, body);
    final ValidatedBlockCache.ValidatedBlock validatedBlock =
        new ValidatedBlockCache.ValidatedBlock(
            block.getHeader().getNumber(), body, List.of(), Optional.empty());
    validatedBlockCache.put(onchainHash(block), validatedBlock);

    final QbftBlockValidator.ValidationResult validationResult = validate(block, Optional.empty());

    assertThat(validationResult.success()).isTrue();
    assertThat(validationResult.errorMessage()).isEmpty();
    verifyNoInteractions(blockValidator);
    assertThat(validatedBlockCache.take(onchainHash(block), block.getHeader().getNumber()))
        .contains(validatedBlock);
  }

  @Test
  void doesNotExecuteReproposalOfValidatedBlockFromPeer() {
    final Block proposal = proposal(0, body);
    stubValidation(proposal, Optional.empty(), successWith(List.of()));
    validate(proposal, Optional.empty());

    // re-proposal in a later round, decoded from a peer message: new body object, same content
    final Block reproposal =
        proposal(2, new BlockBody(body.getTransactions(), body.getOmmers(), body.getWithdrawals()));
    final QbftBlockValidator.ValidationResult validationResult =
        validate(reproposal, Optional.empty());

    assertThat(validationResult.success()).isTrue();
    verify(blockValidator)
        .validateAndProcessBlock(
            protocolContext,
            onchainBlock(proposal),
            HeaderValidationMode.LIGHT,
            HeaderValidationMode.FULL,
            Optional.empty(),
            InclusionListTransactions.EMPTY,
            false);
    verifyNoMoreInteractions(blockValidator);
  }

  @Test
  void executesReproposalWhoseTransactionDiffersOnlyInAccessList() {
    final Transaction transaction = generator.transaction(TransactionType.ACCESS_LIST);
    final Block proposal = proposal(0, new BlockBody(List.of(transaction), emptyList()));
    stubValidation(proposal, Optional.empty(), successWith(List.of()));
    validate(proposal, Optional.empty());

    // same fields and signature, other access list: Transaction.equals does not see it
    final Transaction changed =
        Transaction.builder()
            .copiedFrom(transaction)
            .accessList(List.of(new AccessListEntry(Address.ZERO, List.of())))
            .build();
    assertThat(changed).isEqualTo(transaction);
    assertThat(changed.getHash()).isNotEqualTo(transaction.getHash());
    final Block reproposal = proposal(2, new BlockBody(List.of(changed), emptyList()));
    stubValidation(reproposal, Optional.empty(), new BlockProcessingResult("failed"));

    assertThat(validate(reproposal, Optional.empty()).success()).isFalse();
  }

  @Test
  void executesBlockCreatedBeforeWhenBlockAccessListDiffers() {
    final Block block = proposal(0, body);
    validatedBlockCache.put(
        onchainHash(block),
        new ValidatedBlockCache.ValidatedBlock(
            block.getHeader().getNumber(), body, List.of(), Optional.empty()));
    final Optional<BlockAccessList> blockAccessList = Optional.of(new BlockAccessList(List.of()));
    stubValidation(block, blockAccessList, new BlockProcessingResult("failed"));

    final QbftBlockValidator.ValidationResult validationResult = validate(block, blockAccessList);

    assertThat(validationResult.success()).isFalse();
    assertThat(validationResult.errorMessage()).contains("failed");
  }

  @Test
  void executesBlockWithSameHashWhenBodyDiffers() {
    final Block block = proposal(0, body);
    validatedBlockCache.put(
        onchainHash(block),
        new ValidatedBlockCache.ValidatedBlock(
            block.getHeader().getNumber(), body, List.of(), Optional.empty()));
    // same header, but not the transactions that were executed
    final Block otherBody =
        new Block(block.getHeader(), new BlockBody(List.of(generator.transaction()), emptyList()));
    stubValidation(otherBody, Optional.empty(), new BlockProcessingResult("failed"));

    final QbftBlockValidator.ValidationResult validationResult =
        validate(otherBody, Optional.empty());

    assertThat(validationResult.success()).isFalse();
  }

  // a proposal of the given round, hashed like proposals are
  private Block proposal(final int round, final BlockBody blockBody) {
    final BftExtraData extraData =
        new BftExtraData(
            Bytes.wrap(new byte[32]), emptyList(), Optional.empty(), round, List.of(Address.ZERO));
    return new Block(
        new BlockHeaderTestFixture()
            .number(2)
            .extraData(codec.encode(extraData))
            .blockHeaderFunctions(BftBlockHeaderFunctions.forCommittedSeal(codec))
            .buildHeader(),
        blockBody);
  }

  private Block onchainBlock(final Block block) {
    return new Block(BftHelpers.toOnchainHeader(codec, block.getHeader()), block.getBody());
  }

  private Hash onchainHash(final Block block) {
    return onchainBlock(block).getHash();
  }

  private BlockProcessingResult successWith(final List<TransactionReceipt> receipts) {
    return new BlockProcessingResult(
        Optional.of(new BlockProcessingOutputs(mock(MutableWorldState.class), receipts)));
  }

  // the validator processes the on-chain view of the proposal
  private void stubValidation(
      final Block block,
      final Optional<BlockAccessList> blockAccessList,
      final BlockProcessingResult result) {
    when(blockValidator.validateAndProcessBlock(
            protocolContext,
            onchainBlock(block),
            HeaderValidationMode.LIGHT,
            HeaderValidationMode.FULL,
            blockAccessList,
            InclusionListTransactions.EMPTY,
            false))
        .thenReturn(result);
  }

  private QbftBlockValidator.ValidationResult validate(
      final Block block, final Optional<BlockAccessList> blockAccessList) {
    return new QbftBlockValidatorAdaptor(
            blockValidator, protocolContext, codec, validatedBlockCache)
        .validateBlock(new QbftBlockAdaptor(block), blockAccessList);
  }
}
