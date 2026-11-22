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
package org.hyperledger.besu.ethereum.eth.sync.fullsync;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode.FULL;
import static org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode.SKIP_DETACHED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockImporter;
import org.hyperledger.besu.ethereum.eth.sync.tasks.exceptions.InvalidBlockException;
import org.hyperledger.besu.ethereum.mainnet.BlockImportResult;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class FullImportBlockStepTest {

  @Mock private ProtocolSchedule protocolSchedule;
  @Mock private ProtocolSpec protocolSpec;
  @Mock private ProtocolContext protocolContext;
  @Mock private BlockImporter blockImporter;
  private final BlockDataGenerator gen = new BlockDataGenerator();

  private FullImportBlockStep importBlocksStep;

  @BeforeEach
  public void setUp() {
    when(protocolSchedule.getByBlockHeader(any(BlockHeader.class))).thenReturn(protocolSpec);
    when(protocolSpec.getBlockImporter()).thenReturn(blockImporter);

    importBlocksStep =
        new FullImportBlockStep(
            protocolSchedule, protocolContext, null, SyncTerminationCondition.never());
  }

  @Test
  public void shouldImportBlock() {
    final Block block = gen.block();

    when(blockImporter.importBlock(protocolContext, block, SKIP_DETACHED))
        .thenReturn(new BlockImportResult(true));
    importBlocksStep.accept(block);

    verify(protocolSchedule).getByBlockHeader(block.getHeader());
    verify(blockImporter).importBlock(protocolContext, block, SKIP_DETACHED);
  }

  @Test
  public void shouldThrowExceptionWhenValidationFails() {
    final Block block = gen.block();

    when(blockImporter.importBlock(protocolContext, block, SKIP_DETACHED))
        .thenReturn(new BlockImportResult(false));
    assertThatThrownBy(() -> importBlocksStep.accept(block))
        .isInstanceOf(InvalidBlockException.class);
  }

  @Test
  void shouldFullyValidateArchiveBlocksWhenRequested() {
    final Block block = gen.block();
    when(blockImporter.importBlock(protocolContext, block, FULL))
        .thenReturn(new BlockImportResult(true));
    new FullImportBlockStep(
            protocolSchedule, protocolContext, null, SyncTerminationCondition.never(), FULL)
        .accept(block);
    verify(blockImporter).importBlock(protocolContext, block, FULL);
  }

  @Test
  void shouldPropagateFullValidationFailure() {
    final Block block = gen.block();
    when(blockImporter.importBlock(protocolContext, block, FULL))
        .thenReturn(new BlockImportResult(false));
    final var step =
        new FullImportBlockStep(
            protocolSchedule, protocolContext, null, SyncTerminationCondition.never(), FULL);
    assertThatThrownBy(() -> step.accept(block)).isInstanceOf(InvalidBlockException.class);
  }
}
