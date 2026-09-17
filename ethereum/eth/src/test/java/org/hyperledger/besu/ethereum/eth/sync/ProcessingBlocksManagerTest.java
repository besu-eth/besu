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
package org.hyperledger.besu.ethereum.eth.sync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.PeerReputation;
import org.hyperledger.besu.ethereum.eth.messages.NewBlockHashesMessage.BlockAnnouncement;
import org.hyperledger.besu.ethereum.eth.sync.BlockPropagationManager.ProcessingBlocksManager;

import java.util.Optional;

import org.junit.jupiter.api.Test;

class ProcessingBlocksManagerTest {

  private final ProcessingBlocksManager manager = new ProcessingBlocksManager();

  private static Hash hash(final int i) {
    return Hash.fromHexString(String.format("0x%064x", i));
  }

  private static BlockAnnouncement announcement(final int hash, final long number) {
    return new BlockAnnouncement(hash(hash), number);
  }

  private static EthPeer peerWithScore(final int score) {
    final EthPeer peer = mock(EthPeer.class);
    when(peer.getReputation()).thenReturn(new PeerReputation(score, 150));
    return peer;
  }

  private static Block block(final int hash, final long number) {
    final BlockHeader header = mock(BlockHeader.class);
    when(header.getNumber()).thenReturn(number);
    final Block block = mock(Block.class);
    when(block.getHash()).thenReturn(hash(hash));
    when(block.getHeader()).thenReturn(header);
    return block;
  }

  @Test
  void deduplicatesDifferentHashesForTheSameNumberFromTheSamePeer() {
    final EthPeer peer = peerWithScore(100);

    assertThat(manager.addRequestedBlock(announcement(1, 5), peer)).isTrue();
    // same block number, different hash, same (not strictly better) peer → rejected
    assertThat(manager.addRequestedBlock(announcement(2, 5), peer)).isFalse();
  }

  @Test
  void exactDuplicateAnnouncementIsRejected() {
    final EthPeer peer = peerWithScore(100);

    assertThat(manager.addRequestedBlock(announcement(1, 5), peer)).isTrue();
    assertThat(manager.addRequestedBlock(announcement(1, 5), peer)).isFalse();
  }

  @Test
  void differentBlockNumbersAreIndependent() {
    final EthPeer peer = peerWithScore(100);

    assertThat(manager.addRequestedBlock(announcement(1, 5), peer)).isTrue();
    assertThat(manager.addRequestedBlock(announcement(2, 6), peer)).isTrue();
  }

  @Test
  void strictlyBetterReputationSupersedesSameNumber() {
    assertThat(manager.addRequestedBlock(announcement(1, 5), peerWithScore(50))).isTrue();
    // strictly better reputation for the same number replaces the announcement
    assertThat(manager.addRequestedBlock(announcement(2, 5), peerWithScore(100))).isTrue();
    // not strictly better than the current holder (100) → rejected
    assertThat(manager.addRequestedBlock(announcement(3, 5), peerWithScore(75))).isFalse();
    assertThat(manager.addRequestedBlock(announcement(4, 5), peerWithScore(100))).isFalse();
  }

  @Test
  void registerReceivedBlockFreesTheNumberWhenHashMatches() {
    final EthPeer peer = peerWithScore(100);
    assertThat(manager.addRequestedBlock(announcement(1, 5), peer)).isTrue();

    manager.registerReceivedBlock(block(1, 5));

    // number 5 is free again
    assertThat(manager.addRequestedBlock(announcement(2, 5), peer)).isTrue();
  }

  @Test
  void registerReceivedBlockKeepsEntryWhenHashDiffers() {
    final EthPeer peer = peerWithScore(100);
    assertThat(manager.addRequestedBlock(announcement(1, 5), peer)).isTrue();

    // a received block at the same number but a different hash must not evict the tracked one
    manager.registerReceivedBlock(block(2, 5));

    assertThat(manager.addRequestedBlock(announcement(3, 5), peer)).isFalse();
  }

  @Test
  void registerFailedGetBlockFreesTheNumberWhenHashMatches() {
    final EthPeer peer = peerWithScore(100);
    assertThat(manager.addRequestedBlock(announcement(1, 5), peer)).isTrue();

    manager.registerFailedGetBlock(5, Optional.of(hash(1)));

    assertThat(manager.addRequestedBlock(announcement(2, 5), peer)).isTrue();
  }

  @Test
  void registerFailedGetBlockKeepsEntryWhenHashDiffers() {
    final EthPeer peer = peerWithScore(100);
    assertThat(manager.addRequestedBlock(announcement(1, 5), peer)).isTrue();

    // a failed fetch reported for a different hash at the same number must not evict the tracked
    // one
    manager.registerFailedGetBlock(5, Optional.of(hash(2)));

    assertThat(manager.addRequestedBlock(announcement(3, 5), peer)).isFalse();
  }

  @Test
  void registerFailedGetBlockWithoutHashClearsTheNumber() {
    final EthPeer peer = peerWithScore(100);
    assertThat(manager.addRequestedBlock(announcement(1, 5), peer)).isTrue();

    // non-announced retrieval failures clear the number unconditionally
    manager.registerFailedGetBlock(5, Optional.empty());

    assertThat(manager.addRequestedBlock(announcement(2, 5), peer)).isTrue();
  }

  @Test
  void aSinglePeerCannotScheduleMultipleFetchesForOneNumber() {
    final EthPeer peer = peerWithScore(100);
    assertThat(manager.addRequestedBlock(announcement(1, 5), peer)).isTrue();
    // every subsequent distinct hash for the same number from the same peer is rejected,
    // so one peer cannot drive more than one outstanding fetch per block number
    for (int h = 2; h <= 20; h++) {
      assertThat(manager.addRequestedBlock(announcement(h, 5), peer)).isFalse();
    }
  }

  @Test
  void importingBlocksAreTrackedAndCleared() {
    assertThat(manager.alreadyImporting(hash(1))).isFalse();
    assertThat(manager.addImportingBlock(hash(1))).isTrue();
    assertThat(manager.alreadyImporting(hash(1))).isTrue();
    // a second attempt to import the same block is rejected while in flight
    assertThat(manager.addImportingBlock(hash(1))).isFalse();

    manager.registerBlockImportDone(hash(1));
    assertThat(manager.alreadyImporting(hash(1))).isFalse();
  }
}
