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
package org.hyperledger.besu.ethereum.chain;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.plugin.data.BadBlockCause.BadBlockReason;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

public class BadBlockManagerTest {

  final BlockchainSetupUtil chainUtil = BlockchainSetupUtil.forMainnet();
  final Block block = chainUtil.getBlock(1);
  final Block block2 = chainUtil.getBlock(2);
  final BadBlockManager badBlockManager = new BadBlockManager();

  @Test
  public void checkAndMarkBadDescendant_marksChildOfBadBlockAsBadHeader() {
    final Hash latestValidHash = Hash.fromHexStringLenient("0x1337");
    badBlockManager.addBadBlock(block, BadBlockCause.fromValidationFailure("failed"));
    badBlockManager.addLatestValidHash(block.getHash(), latestValidHash);

    assertThat(badBlockManager.checkAndMarkBadDescendant(block2.getHeader()))
        .map(BadBlockCause::getDescription)
        .contains("Descends from bad block " + block.getHeader().toLogString());
    assertThat(badBlockManager.getBadBlock(block2.getHash())).isEmpty();
    assertThat(badBlockManager.getBadHeader(block2.getHash())).contains(block2.getHeader());
    assertThat(badBlockManager.getLatestValidHash(block2.getHash())).contains(latestValidHash);
  }

  @Test
  public void checkAndMarkBadDescendant_marksChildOfBadHeaderAsBadHeader() {
    badBlockManager.addBadHeader(block.getHeader(), BadBlockCause.fromValidationFailure("failed"));

    assertThat(badBlockManager.checkAndMarkBadDescendant(block2.getHeader()))
        .map(BadBlockCause::getReason)
        .contains(BadBlockReason.DESCENDS_FROM_BAD_BLOCK);
    assertThat(badBlockManager.getBadHeader(block2.getHash())).contains(block2.getHeader());
    assertThat(badBlockManager.getLatestValidHash(block2.getHash())).isEmpty();
  }

  @Test
  public void checkAndMarkBadDescendant_doesNotReRecordKnownBadBlock() {
    badBlockManager.addBadBlock(block, BadBlockCause.fromValidationFailure("failed"));
    final BadBlockCause ownCause = BadBlockCause.fromValidationFailure("failed on its own");
    badBlockManager.addBadBlock(block2, ownCause);

    assertThat(badBlockManager.checkAndMarkBadDescendant(block2.getHeader())).contains(ownCause);
    assertThat(badBlockManager.getBadHeaders()).isEmpty();
  }

  @Test
  public void checkAndMarkBadDescendant_causeKeepsNamingTheRootAcrossDescendants() {
    final Block block3 = chainUtil.getBlock(3);
    final Block block4 = chainUtil.getBlock(4);
    badBlockManager.addBadBlock(block, BadBlockCause.fromValidationFailure("failed"));

    final BadBlockCause childCause =
        badBlockManager.checkAndMarkBadDescendant(block2.getHeader()).orElseThrow();
    final BadBlockCause grandChildCause =
        badBlockManager.checkAndMarkBadDescendant(block3.getHeader()).orElseThrow();
    final BadBlockCause greatGrandChildCause =
        badBlockManager.checkAndMarkBadDescendant(block4.getHeader()).orElseThrow();

    assertThat(childCause.getDescription())
        .isEqualTo("Descends from bad block " + block.getHeader().toLogString());
    assertThat(grandChildCause).isSameAs(childCause);
    assertThat(greatGrandChildCause).isSameAs(childCause);
    assertThat(badBlockManager.getBadBlockCause(block4.getHash())).contains(childCause);
  }

  @Test
  public void addBadDescendant_keepsTheBodyOfADescendantKnownAsBlock() {
    badBlockManager.addBadBlock(block, BadBlockCause.fromValidationFailure("failed"));

    badBlockManager.addBadDescendant(block2, block.getHeader(), Optional.empty());

    assertThat(badBlockManager.getBadBlock(block2.getHash())).contains(block2);
    assertThat(badBlockManager.getBadBlockCause(block2.getHash()))
        .map(BadBlockCause::getReason)
        .contains(BadBlockReason.DESCENDS_FROM_BAD_BLOCK);
  }

  @Test
  public void removeBadBlock_forgetsTheBlockItsCauseAndItsLatestValidHash() {
    badBlockManager.addBadBlock(block, BadBlockCause.fromValidationFailure("failed"));
    badBlockManager.addLatestValidHash(block.getHash(), Hash.fromHexStringLenient("0x1337"));
    badBlockManager.addBadHeader(block2.getHeader(), BadBlockCause.fromValidationFailure("failed"));

    badBlockManager.removeBadBlock(block.getHash());
    badBlockManager.removeBadBlock(block2.getHash());

    assertThat(badBlockManager.isBadBlock(block.getHash())).isFalse();
    assertThat(badBlockManager.isBadBlock(block2.getHash())).isFalse();
    assertThat(badBlockManager.getBadBlockCause(block.getHash())).isEmpty();
    assertThat(badBlockManager.getLatestValidHash(block.getHash())).isEmpty();
    assertThat(badBlockManager.isEmpty()).isTrue();
  }

  @Test
  public void removeBadBlock_forgetsDescendantsMarkedOnItsAccountButNotOnesThatFailedThemselves() {
    final Block block3 = chainUtil.getBlock(3);
    final Block block4 = chainUtil.getBlock(4);
    badBlockManager.addBadBlock(block, BadBlockCause.fromValidationFailure("transient"));
    badBlockManager.addBadDescendant(block2, block.getHeader(), Optional.empty());
    badBlockManager.checkAndMarkBadDescendant(block3.getHeader());
    badBlockManager.addBadBlock(block4, BadBlockCause.fromValidationFailure("failed itself"));

    badBlockManager.removeBadBlock(block.getHash());

    assertThat(badBlockManager.isBadBlock(block.getHash())).isFalse();
    assertThat(badBlockManager.isBadBlock(block2.getHash())).isFalse();
    assertThat(badBlockManager.isBadBlock(block3.getHash())).isFalse();
    assertThat(badBlockManager.getBadBlocks()).containsExactly(block4);
  }

  @Test
  public void markBadChain_marksNothingWhenTheRootIsNoLongerTracked() {
    assertThat(
            badBlockManager.markBadChain(
                block.getHeader(), List.of(), List.of(block2.getHeader()), Optional.empty()))
        .isFalse();
    assertThat(badBlockManager.isEmpty()).isTrue();
  }

  @Test
  public void removeBadBlock_leavesOtherBadBlocksAlone() {
    badBlockManager.addBadBlock(block, BadBlockCause.fromValidationFailure("failed"));
    badBlockManager.addBadBlock(block2, BadBlockCause.fromValidationFailure("failed"));

    badBlockManager.removeBadBlock(block.getHash());

    assertThat(badBlockManager.getBadBlocks()).containsExactly(block2);
  }

  @Test
  public void checkAndMarkBadDescendant_falseWhenNeitherBlockNorParentIsBad() {
    assertThat(badBlockManager.checkAndMarkBadDescendant(block2.getHeader())).isEmpty();
    assertThat(badBlockManager.getBadBlocks()).isEmpty();
    assertThat(badBlockManager.getBadHeaders()).isEmpty();
  }

  @Test
  public void addBadBlock_headerStaysDetectableAfterBodyEviction() {
    final BlockDataGenerator generator = new BlockDataGenerator();
    badBlockManager.addBadBlock(block, BadBlockCause.fromValidationFailure("failed"));

    for (int i = 0; i < BadBlockManager.MAX_BAD_BLOCKS_SIZE; i++) {
      badBlockManager.addBadBlock(generator.block(), BadBlockCause.fromValidationFailure("failed"));
    }

    assertThat(badBlockManager.getBadBlock(block.getHash())).isEmpty();
    assertThat(badBlockManager.isBadBlock(block.getHash())).isTrue();
    assertThat(badBlockManager.getBadHeader(block.getHash())).contains(block.getHeader());
  }

  @Test
  public void addBadBlock_addsBlock() {
    BadBlockManager badBlockManager = new BadBlockManager();
    final BadBlockCause cause = BadBlockCause.fromValidationFailure("failed");
    badBlockManager.addBadBlock(block, cause);

    assertThat(badBlockManager.getBadBlocks()).containsExactly(block);
  }

  @Test
  public void addBadBlockWithGeneratedBal_storesGeneratedAccessList() {
    BadBlockManager badBlockManager = new BadBlockManager();
    final BadBlockCause cause = BadBlockCause.fromValidationFailure("failed");
    final BlockAccessList generatedBal =
        new BlockAccessList(
            List.of(
                new BlockAccessList.AccountChanges(
                    Address.fromHexString("0x0000000000000000000000000000000000000002"),
                    List.of(),
                    List.of(new BlockAccessList.SlotRead(new StorageSlotKey(UInt256.valueOf(2)))),
                    List.of(),
                    List.of(),
                    List.of())));

    badBlockManager.addBadBlock(block, cause, Optional.empty(), Optional.of(generatedBal));

    assertThat(badBlockManager.getGeneratedBlockAccessList(block.getHash())).contains(generatedBal);
  }

  @Test
  public void reset_clearsBadBlocks() {
    BadBlockManager badBlockManager = new BadBlockManager();
    final BadBlockCause cause = BadBlockCause.fromValidationFailure("");
    badBlockManager.addBadBlock(block, cause);
    assertThat(badBlockManager.getBadBlocks()).containsExactly(block);

    badBlockManager.reset();

    assertThat(badBlockManager.getBadBlocks()).isEmpty();
    assertThat(badBlockManager.getBadBlockCause(block.getHash())).isEmpty();
  }

  @Test
  public void isBadBlock_falseWhenEmpty() {
    assertThat(badBlockManager.isBadBlock(block.getHash())).isFalse();
  }

  @Test
  public void isBadBlock_trueForBadHeader() {
    badBlockManager.addBadHeader(block.getHeader(), BadBlockCause.fromValidationFailure("failed"));
    assertThat(badBlockManager.isBadBlock(block.getHash())).isTrue();
  }

  @Test
  public void isBadBlock_trueForBadBlock() {
    badBlockManager.addBadBlock(block, BadBlockCause.fromValidationFailure("failed"));

    assertThat(badBlockManager.isBadBlock(block.getHash())).isTrue();
  }

  @Test
  public void subscribeToBadBlocks_listenerReceivesBadBlockEvent() {

    final AtomicReference<org.hyperledger.besu.plugin.data.BlockHeader> badBlockResult =
        new AtomicReference<>();
    final AtomicReference<org.hyperledger.besu.plugin.data.BadBlockCause> badBlockCauseResult =
        new AtomicReference<>();

    badBlockManager.subscribeToBadBlocks(
        (badBlock, cause) -> {
          badBlockResult.set(badBlock);
          badBlockCauseResult.set(cause);
        });

    final BadBlockCause cause = BadBlockCause.fromValidationFailure("fail");
    badBlockManager.addBadBlock(block, cause);

    // Check event was emitted
    assertThat(badBlockResult.get()).isEqualTo(block.getHeader());
    assertThat(badBlockCauseResult.get()).isEqualTo(cause);
  }

  @Test
  public void subscribeToBadBlocks_listenerReceivesBadHeaderEvent() {

    final AtomicReference<org.hyperledger.besu.plugin.data.BlockHeader> badBlockResult =
        new AtomicReference<>();
    final AtomicReference<org.hyperledger.besu.plugin.data.BadBlockCause> badBlockCauseResult =
        new AtomicReference<>();

    badBlockManager.subscribeToBadBlocks(
        (badBlock, cause) -> {
          badBlockResult.set(badBlock);
          badBlockCauseResult.set(cause);
        });

    final BadBlockCause cause = BadBlockCause.fromValidationFailure("fail");
    badBlockManager.addBadHeader(block.getHeader(), cause);

    // Check event was emitted
    assertThat(badBlockResult.get()).isEqualTo(block.getHeader());
    assertThat(badBlockCauseResult.get()).isEqualTo(cause);
  }

  @Test
  public void unsubscribeFromBadBlocks_listenerReceivesNoEvents() {

    final AtomicInteger eventCount = new AtomicInteger(0);
    final long subscribeId =
        badBlockManager.subscribeToBadBlocks((block, cause) -> eventCount.incrementAndGet());
    badBlockManager.unsubscribeFromBadBlocks(subscribeId);

    final BadBlockCause cause = BadBlockCause.fromValidationFailure("fail");
    badBlockManager.addBadBlock(block, cause);
    badBlockManager.addBadHeader(block2.getHeader(), cause);

    // Check no events fired
    assertThat(eventCount.get()).isEqualTo(0);
  }
}
