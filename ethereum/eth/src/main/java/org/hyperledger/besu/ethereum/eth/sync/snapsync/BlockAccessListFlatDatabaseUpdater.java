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
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListChanges;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;

import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BlockAccessListFlatDatabaseUpdater {
  private static final Logger LOG =
      LoggerFactory.getLogger(BlockAccessListFlatDatabaseUpdater.class);

  private BlockAccessListFlatDatabaseUpdater() {}

  public static void applyFromStoredBlockAccessLists(
      final Blockchain blockchain,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final long fromBlock,
      final long toBlock) {
    if (toBlock < fromBlock) {
      LOG.warn(
          "Skipping BAL apply due to invalid block range: fromBlock={} toBlock={}",
          fromBlock,
          toBlock);
      return;
    }

    final BonsaiWorldStateKeyValueStorage.Updater bonsaiUpdater =
        worldStateStorageCoordinator.getStrategy(BonsaiWorldStateKeyValueStorage.class).updater();
    for (long blockNumber = fromBlock; blockNumber <= toBlock; blockNumber++) {
      try {
        final Optional<BlockHeader> maybeBlockHeader = blockchain.getBlockHeader(blockNumber);
        if (maybeBlockHeader.isEmpty()) {
          LOG.warn(
              "Missing header while applying BALs to flat database at block {}; continuing",
              blockNumber);
          continue;
        }
        final BlockHeader blockHeader = maybeBlockHeader.get();
        final Optional<BlockAccessList> maybeBal =
            blockchain.getBlockAccessList(blockHeader.getHash());
        if (maybeBal.isEmpty()) {
          LOG.warn(
              "Missing stored BAL while applying BALs to flat database at block {} ({}); continuing",
              blockNumber,
              blockHeader.getHash());
          continue;
        }
        applyToFlatDatabase(
            worldStateStorageCoordinator,
            bonsaiUpdater,
            blockHeader.getStateRoot(),
            maybeBal.get());
      } catch (final Exception exception) {
        LOG.warn(
            "Failed to apply stored BAL to flat database at block {}; continuing",
            blockNumber,
            exception);
      }
    }
    bonsaiUpdater.commit();
  }

  private static void applyToFlatDatabase(
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final BonsaiWorldStateKeyValueStorage.Updater bonsaiUpdater,
      final Hash stateRoot,
      final BlockAccessList bal) {
    final StoredMerklePatriciaTrie<Bytes, Bytes> accountTrie =
        new StoredMerklePatriciaTrie<>(
            worldStateStorageCoordinator::getAccountStateTrieNode,
            Bytes32.wrap(stateRoot.getBytes()),
            Function.identity(),
            Function.identity());

    for (final var accountChanges : BlockAccessListChanges.latestChanges(bal)) {
      final Hash accountHash = accountChanges.address().addressHash();
      final Optional<PmtStateTrieAccountValue> maybeTrieAccountValue =
          accountTrie
              .get(accountHash.getBytes())
              .map(RLP::input)
              .map(PmtStateTrieAccountValue::readFrom);
      if (maybeTrieAccountValue.isEmpty()) {
        LOG.warn(
            "Account {} is missing from trie at state root {} but latest BAL changes are non-empty",
            accountChanges.address(),
            stateRoot);
        continue;
      }
      final PmtStateTrieAccountValue trieAccountValue = maybeTrieAccountValue.get();

      final var updatedCode = accountChanges.code();
      final Hash updatedCodeHash = resolveUpdatedCodeHash(accountChanges, trieAccountValue);
      updatedCode.ifPresent(code -> bonsaiUpdater.putCode(accountHash, updatedCodeHash, code));

      final Hash updatedStorageRoot = trieAccountValue.getStorageRoot();
      if (!accountChanges.storageChanges().isEmpty()) {
        applyStorageChanges(accountHash, accountChanges, bonsaiUpdater);
      }

      final PmtStateTrieAccountValue updatedValue =
          new PmtStateTrieAccountValue(
              resolveUpdatedNonce(accountChanges, trieAccountValue),
              resolveUpdatedBalance(accountChanges, trieAccountValue),
              updatedStorageRoot,
              updatedCodeHash);
      bonsaiUpdater.putAccountInfoState(accountHash, RLP.encode(updatedValue::writeTo));
    }
  }

  private static void applyStorageChanges(
      final Hash accountHash,
      final BlockAccessListChanges.AccountFinalChanges accountChanges,
      final BonsaiWorldStateKeyValueStorage.Updater bonsaiUpdater) {
    for (final var storageChange : accountChanges.storageChanges()) {
      final Hash slotHash = storageChange.slot().getSlotHash();
      final UInt256 value = storageChange.value();
      if (value.equals(UInt256.ZERO)) {
        bonsaiUpdater.removeStorageValueBySlotHash(accountHash, slotHash);
      } else {
        bonsaiUpdater.putStorageValueBySlotHash(accountHash, slotHash, encodeStorageValue(value));
      }
    }
  }

  private static Bytes encodeStorageValue(final UInt256 storageValue) {
    return Bytes32.leftPad(storageValue.toMinimalBytes());
  }

  private static long resolveUpdatedNonce(
      final BlockAccessListChanges.AccountFinalChanges accountChanges,
      final PmtStateTrieAccountValue trieAccountValue) {
    return accountChanges.nonce().orElse(trieAccountValue.getNonce());
  }

  private static Wei resolveUpdatedBalance(
      final BlockAccessListChanges.AccountFinalChanges accountChanges,
      final PmtStateTrieAccountValue trieAccountValue) {
    return accountChanges.balance().orElse(trieAccountValue.getBalance());
  }

  private static Hash resolveUpdatedCodeHash(
      final BlockAccessListChanges.AccountFinalChanges accountChanges,
      final PmtStateTrieAccountValue trieAccountValue) {
    return accountChanges.code().map(Hash::hash).orElse(trieAccountValue.getCodeHash());
  }
}
