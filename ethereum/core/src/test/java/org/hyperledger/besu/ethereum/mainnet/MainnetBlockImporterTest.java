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
package org.hyperledger.besu.ethereum.mainnet;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.BlockValidator;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class MainnetBlockImporterTest {
  @Mock private BlockValidator blockValidator;
  @Mock private ProtocolContext context;
  @Mock private MutableBlockchain blockchain;
  @Mock private Block block;
  @Mock private Hash hash;
  private final ReentrantLock headLock = new ReentrantLock();
  private MainnetBlockImporter blockImporter;

  @BeforeEach
  public void setup() {
    blockImporter = new MainnetBlockImporter(blockValidator);
    when(context.getBlockchain()).thenReturn(blockchain);
    when(context.getHeadLock()).thenReturn(headLock);
    when(block.getHash()).thenReturn(hash);
  }

  @Test
  public void doNotImportBlockIfBlockchainAlreadyHasBlock() {
    when(blockchain.contains(hash)).thenReturn(true);

    final BlockImportResult result =
        blockImporter.importBlock(
            context, block, HeaderValidationMode.FULL, HeaderValidationMode.FULL);

    assertThat(result.isImported()).isTrue();

    verify(blockValidator, never())
        .validateAndProcessBlock(
            context, block, HeaderValidationMode.FULL, HeaderValidationMode.FULL);
    verify(blockchain, never()).appendBlock(eq(block), any(), any());
  }

  @Test
  public void importsBlockHoldingTheHeadLock() {
    final AtomicBoolean heldWhileValidating = new AtomicBoolean();
    when(blockValidator.validateAndProcessBlock(
            eq(context),
            eq(block),
            eq(HeaderValidationMode.FULL),
            eq(HeaderValidationMode.FULL),
            any(),
            eq(false)))
        .thenAnswer(
            invocation -> {
              heldWhileValidating.set(headLock.isHeldByCurrentThread());
              return new BlockProcessingResult("invalid");
            });

    final BlockImportResult result =
        blockImporter.importBlock(
            context, block, HeaderValidationMode.FULL, HeaderValidationMode.FULL);

    assertThat(result.isImported()).isFalse();
    assertThat(heldWhileValidating.get()).isTrue();
    assertThat(headLock.isLocked()).isFalse();
  }
}
