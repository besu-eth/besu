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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync2;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListChanges.AccountFinalChanges;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;
import org.hyperledger.besu.plugin.services.worldstate.StateRootCommitter;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Moves the head world state onto a finalized block by applying the block access list of the block,
 * without executing it. Its execution was agreed on when it was finalized, so what is left to check
 * is that the header is valid, that the receipts match it and that the block access list leads to
 * its state root.
 */
class BalImporter {

  private static final Logger LOG = LoggerFactory.getLogger(BalImporter.class);

  private final ProtocolContext protocolContext;
  private final ProtocolSchedule protocolSchedule;

  BalImporter(final ProtocolContext protocolContext, final ProtocolSchedule protocolSchedule) {
    this.protocolContext = protocolContext;
    this.protocolSchedule = protocolSchedule;
  }

  /**
   * Applies {@code bal} to the head world state, which must be at the parent of {@code block}.
   *
   * @return false if the block is not valid this way or its world state cannot be updated, the head
   *     world state then left as it was for the block to be executed
   */
  boolean applyToWorldState(
      final Block block, final List<TransactionReceipt> receipts, final BlockAccessList bal) {
    final BlockHeader header = block.getHeader();
    final Optional<BlockHeader> parent =
        protocolContext.getBlockchain().getBlockHeader(header.getParentHash());
    final ProtocolSpec spec = protocolSchedule.getByBlockHeader(header);
    if (parent.isEmpty() || !isValid(spec, block, parent.get(), receipts, bal)) {
      LOG.atDebug()
          .setMessage("Block {} does not validate against its receipts and BAL, executing it")
          .addArgument(block::toLogString)
          .log();
      return false;
    }
    return protocolContext
        .getWorldStateArchive()
        .withHeadWorldStateLock(() -> applyAndPersist(spec, header, parent.get(), bal));
  }

  private boolean isValid(
      final ProtocolSpec spec,
      final Block block,
      final BlockHeader parent,
      final List<TransactionReceipt> receipts,
      final BlockAccessList bal) {
    final BlockHeader header = block.getHeader();
    return spec.getBlockHeaderValidator()
            .validateHeader(header, parent, protocolContext, HeaderValidationMode.FULL)
        && spec.getBlockAccessListValidator()
            .validate(Optional.of(bal), header, block.getBody().getTransactions().size())
        && spec.getBlockBodyValidator()
            .validateBodyLight(
                protocolContext, block, receipts, HeaderValidationMode.NONE, OptionalLong.empty());
  }

  private boolean applyAndPersist(
      final ProtocolSpec spec,
      final BlockHeader header,
      final BlockHeader parent,
      final BlockAccessList bal) {
    final WorldStateArchive archive = protocolContext.getWorldStateArchive();
    final Optional<MutableWorldState> maybeWorldState =
        archive.getWorldState(WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(parent));
    if (maybeWorldState.isEmpty()) {
      return false;
    }
    final MutableWorldState worldState = maybeWorldState.get();
    archive.prepareWorldStateForBlock(header, worldState);
    // started first, so a background root computation overlaps the staging of the changes
    final StateRootCommitter committer =
        spec.getStateRootCommitterFactory()
            .forBlock(protocolContext, header, Optional.of(bal), worldState.isStorageFrozen());
    try {
      final WorldUpdater updater = worldState.updater();
      applyChanges(updater, bal);
      updater.commit();
      // checks the state root, and writes the trie log before the state
      worldState.persist(header, committer);
      return true;
    } catch (final RuntimeException e) {
      committer.cancel();
      // persist drops the staged changes when it fails, not a failure before it
      if (worldState instanceof PathBasedWorldState pathBasedWorldState) {
        pathBasedWorldState.getAccumulator().reset();
      }
      LOG.atDebug()
          .setMessage("Applying the BAL of block {} failed ({}), executing it")
          .addArgument(header::toLogString)
          .addArgument(e::toString)
          .log();
      return false;
    }
  }

  /** Writes the value each changed account and slot has at the end of the block. */
  static void applyChanges(final WorldUpdater updater, final BlockAccessList bal) {
    for (final AccountFinalChanges changes : BlockAccessListChanges.latestChanges(bal)) {
      if (changes.hasAnyChange()) {
        final MutableAccount account = updater.getOrCreate(changes.address());
        changes.nonce().ifPresent(account::setNonce);
        changes.balance().ifPresent(account::setBalance);
        changes.code().ifPresent(account::setCode);
        changes
            .storageChanges()
            .forEach(
                slot ->
                    account.setStorageValue(slot.slot().getSlotKey().orElseThrow(), slot.value()));
        // EIP-161: a touched account left empty does not exist
        if (account.isEmpty()) {
          updater.deleteAccount(changes.address());
        }
      }
    }
  }
}
