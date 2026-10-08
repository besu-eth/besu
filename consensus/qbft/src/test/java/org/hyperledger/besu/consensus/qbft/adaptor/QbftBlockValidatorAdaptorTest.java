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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockValidator;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.BlockValidator;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class QbftBlockValidatorAdaptorTest {
  @Mock private BlockValidator blockValidator;
  @Mock private ProtocolContext protocolContext;
  @Mock private Block besuBlock;
  @Mock private QbftBlockAdaptor qbftBlock;
  private final ValidatedBlockCache validatedBlockCache = new ValidatedBlockCache();

  @Test
  void validateSuccessfullyWhenBesuValidatorSuccessful() {
    when(qbftBlock.getBesuBlock()).thenReturn(besuBlock);
    when(blockValidator.validateAndProcessBlock(
            protocolContext,
            besuBlock,
            HeaderValidationMode.LIGHT,
            HeaderValidationMode.FULL,
            Optional.empty(),
            false))
        .thenReturn(new BlockProcessingResult(Optional.empty()));

    QbftBlockValidatorAdaptor qbftBlockValidator =
        new QbftBlockValidatorAdaptor(blockValidator, protocolContext, validatedBlockCache);
    QbftBlockValidator.ValidationResult validationResult =
        qbftBlockValidator.validateBlock(qbftBlock, Optional.empty());
    assertThat(validationResult.success()).isTrue();
    assertThat(validationResult.errorMessage()).isEmpty();
  }

  @Test
  void validateFailsWhenBesuValidatorFails() {
    when(qbftBlock.getBesuBlock()).thenReturn(besuBlock);
    when(blockValidator.validateAndProcessBlock(
            protocolContext,
            besuBlock,
            HeaderValidationMode.LIGHT,
            HeaderValidationMode.FULL,
            Optional.empty(),
            false))
        .thenReturn(new BlockProcessingResult("failed"));

    QbftBlockValidatorAdaptor qbftBlockValidator =
        new QbftBlockValidatorAdaptor(blockValidator, protocolContext, validatedBlockCache);
    QbftBlockValidator.ValidationResult validationResult =
        qbftBlockValidator.validateBlock(qbftBlock, Optional.empty());
    assertThat(validationResult.success()).isFalse();
    assertThat(validationResult.errorMessage()).contains("failed");
  }

  @Test
  void recordsOutputsOfSuccessfullyValidatedBlock() {
    final BlockDataGenerator generator = new BlockDataGenerator();
    final Block block = generator.block();
    final List<TransactionReceipt> receipts = generator.receipts(block);
    when(blockValidator.validateAndProcessBlock(
            protocolContext,
            block,
            HeaderValidationMode.LIGHT,
            HeaderValidationMode.FULL,
            Optional.empty(),
            false))
        .thenReturn(
            new BlockProcessingResult(
                Optional.of(new BlockProcessingOutputs(mock(MutableWorldState.class), receipts))));

    new QbftBlockValidatorAdaptor(blockValidator, protocolContext, validatedBlockCache)
        .validateBlock(new QbftBlockAdaptor(block), Optional.empty());

    assertThat(validatedBlockCache.take(block.getHash(), block.getHeader().getNumber()))
        .contains(
            new ValidatedBlockCache.ValidatedBlock(
                block.getHeader().getNumber(), receipts, Optional.empty()));
  }

  @Test
  void doesNotRecordBlockThatFailedValidation() {
    final Block block = new BlockDataGenerator().block();
    when(blockValidator.validateAndProcessBlock(
            protocolContext,
            block,
            HeaderValidationMode.LIGHT,
            HeaderValidationMode.FULL,
            Optional.empty(),
            false))
        .thenReturn(new BlockProcessingResult("failed"));

    new QbftBlockValidatorAdaptor(blockValidator, protocolContext, validatedBlockCache)
        .validateBlock(new QbftBlockAdaptor(block), Optional.empty());

    assertThat(validatedBlockCache.take(block.getHash(), block.getHeader().getNumber())).isEmpty();
  }

  @Test
  void doesNotExecuteBlockCreatedOrValidatedBefore() {
    final Block block = new BlockDataGenerator().block();
    final ValidatedBlockCache.ValidatedBlock validatedBlock =
        new ValidatedBlockCache.ValidatedBlock(
            block.getHeader().getNumber(), List.of(), Optional.empty());
    validatedBlockCache.put(block.getHash(), validatedBlock);

    final QbftBlockValidator.ValidationResult validationResult =
        new QbftBlockValidatorAdaptor(blockValidator, protocolContext, validatedBlockCache)
            .validateBlock(new QbftBlockAdaptor(block), Optional.empty());

    assertThat(validationResult.success()).isTrue();
    assertThat(validationResult.errorMessage()).isEmpty();
    verifyNoInteractions(blockValidator);
    assertThat(validatedBlockCache.take(block.getHash(), block.getHeader().getNumber()))
        .contains(validatedBlock);
  }

  @Test
  void executesBlockCreatedBeforeWhenBlockAccessListDiffers() {
    final Block block = new BlockDataGenerator().block();
    validatedBlockCache.put(
        block.getHash(),
        new ValidatedBlockCache.ValidatedBlock(
            block.getHeader().getNumber(), List.of(), Optional.empty()));
    final Optional<BlockAccessList> blockAccessList = Optional.of(new BlockAccessList(List.of()));
    when(blockValidator.validateAndProcessBlock(
            protocolContext,
            block,
            HeaderValidationMode.LIGHT,
            HeaderValidationMode.FULL,
            blockAccessList,
            false))
        .thenReturn(new BlockProcessingResult("failed"));

    final QbftBlockValidator.ValidationResult validationResult =
        new QbftBlockValidatorAdaptor(blockValidator, protocolContext, validatedBlockCache)
            .validateBlock(new QbftBlockAdaptor(block), blockAccessList);

    assertThat(validationResult.success()).isFalse();
    assertThat(validationResult.errorMessage()).contains("failed");
  }
}
