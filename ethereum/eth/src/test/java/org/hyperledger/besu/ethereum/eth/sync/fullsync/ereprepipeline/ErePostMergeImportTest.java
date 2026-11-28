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
package org.hyperledger.besu.ethereum.eth.sync.fullsync.ereprepipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.datatypes.GWei;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.eth.sync.fullsync.SyncTerminationCondition;
import org.hyperledger.besu.ethereum.eth.sync.fullsync.era1prepipeline.FileImportChainDownloader;
import org.hyperledger.besu.ethereum.eth.sync.tasks.exceptions.InvalidBlockException;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ErePostMergeImportTest {
  @TempDir Path directory;

  @Test
  void executesPoSHistoryWithoutSelectingCanonicalSafeOrFinalizedHeads() throws Exception {
    try (var chain = new ErePosTestChain(32)) {
      EreTestFixtures.write(directory, chain.blocks);
      importFiles(chain);
      assertStoredThrough(chain, 32);
      assertThat(chain.blockchain.getChainHeadBlockNumber()).isZero();
      assertThat(chain.blockchain.getBlockHashByNumber(32)).isEmpty();
      assertThat(chain.blockchain.getSafeBlock()).isEmpty();
      assertThat(chain.blockchain.getFinalized()).isEmpty();
    }
  }

  @Test
  void resumesPreviouslyExecutedPayloadsAndAnExistingPostMergeCanonicalHead() throws Exception {
    try (var chain = new ErePosTestChain(32)) {
      EreTestFixtures.write(directory, chain.blocks);
      final var importer =
          new EreImportBlockStep(
              chain.schedule,
              chain.context,
              chain.manager.ethContext(),
              chain.blocks.getFirst().getHeader(),
              () -> true);
      chain.blocks.subList(1, 9).forEach(importer);
      assertThat(chain.blockchain.rewindToBlock(chain.blocks.get(4).getHash())).isTrue();
      chain.blockchain.setSafeBlock(chain.blocks.get(4).getHash());
      chain.blockchain.setFinalized(chain.blocks.get(4).getHash());
      importFiles(chain);
      assertStoredThrough(chain, 32);
      assertThat(chain.blockchain.getChainHeadBlockNumber()).isEqualTo(4);
      assertThat(chain.blockchain.getSafeBlock()).contains(chain.blocks.get(4).getHash());
      assertThat(chain.blockchain.getFinalized()).contains(chain.blocks.get(4).getHash());
    }
  }

  @Test
  void continuesAcrossFilesUsingExecutionCursorInsteadOfCanonicalHead() throws Exception {
    try (var chain = new ErePosTestChain(8193)) {
      EreTestFixtures.write(directory, chain.blocks.subList(0, 8192));
      EreTestFixtures.write(directory, chain.blocks.subList(8192, 8194));
      importFiles(chain);
      assertStoredThrough(chain, 8193);
      assertThat(chain.blockchain.getChainHeadBlockNumber()).isZero();
    }
  }

  @Test
  void crossesTerminalDifficultyAndResumesThePostMergePartOfTheSameFile() throws Exception {
    try (var chain = new ErePosTestChain(16, true)) {
      EreTestFixtures.write(directory, chain.blocks);
      importFiles(chain);
      assertStoredThrough(chain, 16);
      assertThat(chain.blockchain.getChainHeadBlockNumber()).isEqualTo(1);
      assertThat(chain.blockchain.getChainHead().getTotalDifficulty())
          .isEqualTo(chain.terminalDifficulty);
      importFiles(chain);
      assertStoredThrough(chain, 16);
      assertThat(chain.blockchain.getChainHeadBlockNumber()).isEqualTo(1);
    }
  }

  @Test
  void doesNotOverrideForkchoiceWhileImportingTheRemainingArchive() throws Exception {
    try (var chain = new ErePosTestChain(16)) {
      EreTestFixtures.write(directory, chain.blocks);
      chain.blockchain.observeBlockAdded(
          event -> {
            if (event.getHeader().getNumber() == 8) {
              assertThat(chain.blockchain.rewindToBlock(chain.blocks.get(4).getHash())).isTrue();
            }
          });
      importFiles(chain);
      assertStoredThrough(chain, 16);
      assertThat(chain.blockchain.getChainHeadBlockNumber()).isEqualTo(4);
    }
  }

  @Test
  void rejectsInvalidPostMergeNonceBeforeStoringPayload() throws Exception {
    try (var chain = new ErePosTestChain(1)) {
      final var first = chain.blocks.get(1);
      final var invalid =
          new Block(
              BlockHeaderBuilder.fromHeader(first.getHeader())
                  .nonce(1)
                  .blockHeaderFunctions(new MainnetBlockHeaderFunctions())
                  .buildBlockHeader(),
              first.getBody());
      EreTestFixtures.write(directory, List.of(chain.blocks.getFirst(), invalid));
      assertThatThrownBy(() -> importFiles(chain))
          .hasRootCauseInstanceOf(InvalidBlockException.class);
      assertThat(chain.blockchain.getBlockBody(invalid.getHash())).isEmpty();
      assertThat(chain.blockchain.getChainHeadBlockNumber()).isZero();
    }
  }

  @Test
  void rejectsDifferentArchiveBodyForAnAlreadyExecutedPayload() throws Exception {
    try (var chain = new ErePosTestChain(16)) {
      EreTestFixtures.write(directory, chain.blocks);
      importFiles(chain);
      final var last = chain.blocks.getLast();
      final var withdrawal = last.getBody().getWithdrawals().orElseThrow().getFirst();
      final var different =
          new Withdrawal(
              withdrawal.getIndex(),
              withdrawal.getValidatorIndex(),
              withdrawal.getAddress(),
              GWei.of(2));
      final var replacements = new ArrayList<>(chain.blocks);
      replacements.set(
          16,
          new Block(
              last.getHeader(),
              new BlockBody(List.of(), List.of(), Optional.of(List.of(different)))));
      EreTestFixtures.write(directory, replacements);
      assertThatThrownBy(() -> importFiles(chain))
          .hasRootCauseInstanceOf(InvalidBlockException.class);
      assertStoredThrough(chain, 16);
      assertThat(chain.blockchain.getChainHeadBlockNumber()).isZero();
    }
  }

  @Test
  void rejectsProofOfWorkAfterTheTerminalBlock() throws Exception {
    try (var chain = new ErePosTestChain(1)) {
      final var first = chain.blocks.get(1);
      final var invalid =
          new Block(
              BlockHeaderBuilder.fromHeader(first.getHeader())
                  .difficulty(Difficulty.ONE)
                  .blockHeaderFunctions(new MainnetBlockHeaderFunctions())
                  .buildBlockHeader(),
              first.getBody());
      EreTestFixtures.write(directory, List.of(chain.blocks.getFirst(), invalid));
      assertThatThrownBy(() -> importFiles(chain))
          .hasRootCauseInstanceOf(InvalidBlockException.class);
      assertThat(chain.blockchain.getBlockBody(invalid.getHash())).isEmpty();
    }
  }

  @Test
  void resumesAfterRepairingCorruptPostMergeBodyWithCanonicalHeadUnchanged() throws Exception {
    try (var chain = new ErePosTestChain(16)) {
      final var file = EreTestFixtures.write(directory, chain.blocks);
      final byte[] original = Files.readAllBytes(file);
      final byte[] corrupt = original.clone();
      final var records = ByteBuffer.wrap(corrupt).order(ByteOrder.LITTLE_ENDIAN);
      int bodies = 0;
      for (int position = 8; position < corrupt.length; ) {
        final int type = Short.toUnsignedInt(records.getShort(position));
        final int length = records.getInt(position + 2);
        if (type == 4 && bodies++ == 8) {
          corrupt[position + 22] ^= 1;
          break;
        }
        position += length + 8;
      }
      Files.write(file, corrupt);
      assertThatThrownBy(() -> importFiles(chain)).hasRootCauseInstanceOf(IOException.class);
      assertThat(chain.blockchain.getBlockBody(chain.blocks.get(7).getHash())).isPresent();
      assertThat(chain.blockchain.getBlockBody(chain.blocks.get(8).getHash())).isEmpty();
      assertThat(chain.blockchain.getChainHeadBlockNumber()).isZero();
      Files.write(file, original);
      importFiles(chain);
      assertStoredThrough(chain, 16);
      assertThat(chain.blockchain.getChainHeadBlockNumber()).isZero();
    }
  }

  private void importFiles(final ErePosTestChain chain) throws Exception {
    final var factory =
        new EreImportPrepipelineFactory(
            new NoOpMetricsSystem(),
            directory.toUri(),
            2,
            chain.schedule,
            chain.context,
            chain.manager.ethContext(),
            SyncTerminationCondition.difficulty(chain.terminalDifficulty, chain.blockchain));
    new FileImportChainDownloader(
            factory, chain.blockchain, chain.manager.ethContext().getScheduler())
        .start()
        .get(120, TimeUnit.SECONDS);
  }

  private void assertStoredThrough(final ErePosTestChain chain, final int number) throws Exception {
    final var last = chain.blocks.get(number);
    assertThat(chain.blockchain.getBlockBody(last.getHash())).contains(last.getBody());
    try (var state =
        chain
            .archive
            .getWorldState(
                WorldStateQueryParams.newBuilder()
                    .withBlockHeader(last.getHeader())
                    .withShouldWorldStateUpdateHead(false)
                    .build())
            .orElseThrow()) {
      assertThat(state.get(ErePosTestChain.RECIPIENT).getBalance())
          .isEqualTo(Wei.of((number - (chain.transition ? 1 : 0)) * 1_000_000_000L));
    }
  }
}
