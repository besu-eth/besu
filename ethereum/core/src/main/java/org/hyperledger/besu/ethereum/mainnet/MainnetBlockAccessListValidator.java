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
package org.hyperledger.besu.ethereum.mainnet;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mainnet implementation of BlockAccessListValidator that validates block access lists according to
 * EIP-7928.
 */
public class MainnetBlockAccessListValidator implements BlockAccessListValidator {

  private static final Logger LOG = LoggerFactory.getLogger(MainnetBlockAccessListValidator.class);

  /** Canonical slot order (by slot key bytes), consistent with BlockAccessListBuilder. */
  private static int compareSlotKeysByCanonicalOrder(
      final StorageSlotKey a, final StorageSlotKey b) {
    return a.getSlotKey().orElseThrow().toBytes().compareTo(b.getSlotKey().orElseThrow().toBytes());
  }

  /** Creates a new MainnetBlockAccessListValidator. */
  public MainnetBlockAccessListValidator() {}

  @Override
  public boolean validate(
      final Optional<BlockAccessList> blockAccessList,
      final BlockHeader blockHeader,
      final int nbTransactions) {
    if (blockAccessList.isEmpty()) {
      return true;
    }
    if (nbTransactions < 0) {
      LOG.warn(
          "Invalid nbTransactions {} for block {} (must be >= 0)",
          nbTransactions,
          blockHeader.getBlockHash());
      return false;
    }
    final BlockAccessList bal = blockAccessList.get();
    final Optional<Hash> headerBalHash = blockHeader.getBalHash();

    if (headerBalHash.isEmpty()) {
      LOG.warn("Header is missing balHash for block {}", blockHeader.getBlockHash());
      return false;
    }

    final Hash providedBalHash = BodyValidation.balHash(bal);
    if (!headerBalHash.get().equals(providedBalHash)) {
      LOG.warn(
          "Block access list hash mismatch for block {}: provided={}, header={}",
          blockHeader.getBlockHash(),
          providedBalHash,
          headerBalHash.get());
      return false;
    }

    final int maxIndex = nbTransactions + 1;
    if (!validateConstraints(bal, blockHeader, maxIndex)) {
      return false;
    }
    LOG.trace("Block access list validated successfully for block {}", blockHeader.getNumber());
    return true;
  }

  /**
   * EIP-7928: checks that {@code balItems × itemCost > gasLimit}. When {@code itemCost <= 0} the
   * check is disabled.
   *
   * @param bal the block access list
   * @param gasLimit the block gas limit
   * @param itemCost per-item gas cost from the gas calculator
   * @return true if the BAL exceeds the gas-limit-derived item cap
   */
  public static boolean exceedsItemLimit(
      final BlockAccessList bal, final long gasLimit, final long itemCost) {
    if (itemCost <= 0) {
      return false;
    }
    long totalStorageKeys = 0;
    for (BlockAccessList.AccountChanges accountChange : bal.accountChanges()) {
      totalStorageKeys += accountChange.storageChanges().size();
      totalStorageKeys += accountChange.storageReads().size();
    }
    final long balItems = totalStorageKeys + bal.accountChanges().size();
    return balItems > gasLimit / itemCost;
  }

  /**
   * Validates index range (indices in [0, maxIndex]) and canonical ordering (EIP-7928) in one
   * traversal. Strict ordering implies uniqueness; the only set kept is change slots to detect
   * overlap with storage_reads.
   */
  private boolean validateConstraints(
      final BlockAccessList bal, final BlockHeader blockHeader, final int maxIndex) {
    Address prevAddress = null;

    for (BlockAccessList.AccountChanges account : bal.accountChanges()) {
      if (prevAddress != null
          && prevAddress.getBytes().compareTo(account.address().getBytes()) >= 0) {
        LOG.warn(
            "Block access list accounts not in canonical order (by address) for block {}",
            blockHeader.getBlockHash());
        return false;
      }
      prevAddress = account.address();

      final Set<StorageSlotKey> changeSlots = new HashSet<>(account.storageChanges().size());
      StorageSlotKey prevStorageSlot = null;
      int prevStorageTxIndex = -1;

      for (BlockAccessList.SlotChanges slotChanges : account.storageChanges()) {
        final StorageSlotKey slot = slotChanges.slot();
        if (prevStorageSlot != null
            && compareSlotKeysByCanonicalOrder(prevStorageSlot, slot) >= 0) {
          LOG.warn(
              "Block access list storage_changes not in canonical order (by slot) for address {} block {}",
              account.address(),
              blockHeader.getBlockHash());
          return false;
        }
        prevStorageSlot = slot;
        changeSlots.add(slot);

        prevStorageTxIndex = -1;
        for (BlockAccessList.StorageChange ch : slotChanges.changes()) {
          final int txIndex = ch.txIndex();
          if (txIndex < 0) {
            LOG.warn(
                "Block access list has negative block_access_index for address {} block {}",
                account.address(),
                blockHeader.getBlockHash());
            return false;
          }
          if (prevStorageTxIndex >= txIndex) {
            LOG.warn(
                "Block access list storage_changes not in canonical order (by block_access_index) for address {} block {}",
                account.address(),
                blockHeader.getBlockHash());
            return false;
          }
          if (txIndex > maxIndex) {
            LOG.warn(
                "Block access list has block_access_index {} exceeding max {} for block {}",
                txIndex,
                maxIndex,
                blockHeader.getBlockHash());
            return false;
          }
          prevStorageTxIndex = txIndex;
        }
      }

      StorageSlotKey prevReadSlot = null;
      for (BlockAccessList.SlotRead slotRead : account.storageReads()) {
        final StorageSlotKey slot = slotRead.slot();
        if (prevReadSlot != null && compareSlotKeysByCanonicalOrder(prevReadSlot, slot) >= 0) {
          LOG.warn(
              "Block access list storage_reads not in canonical order (by slot) for address {} block {}",
              account.address(),
              blockHeader.getBlockHash());
          return false;
        }
        prevReadSlot = slot;
        if (changeSlots.contains(slot)) {
          LOG.warn(
              "Block access list has storage key in both storage_changes and storage_reads for address {} block {}",
              account.address(),
              blockHeader.getBlockHash());
          return false;
        }
      }

      int prevTxIndex = -1;
      for (BlockAccessList.BalanceChange ch : account.balanceChanges()) {
        final int txIndex = ch.txIndex();
        if (txIndex < 0) {
          LOG.warn(
              "Block access list has negative block_access_index in balance_changes for address {} block {}",
              account.address(),
              blockHeader.getBlockHash());
          return false;
        }
        if (prevTxIndex >= txIndex) {
          LOG.warn(
              "Block access list balance_changes not in canonical order (by block_access_index) for address {} block {}",
              account.address(),
              blockHeader.getBlockHash());
          return false;
        }
        if (txIndex > maxIndex) {
          LOG.warn(
              "Block access list has block_access_index {} exceeding max {} for block {}",
              txIndex,
              maxIndex,
              blockHeader.getBlockHash());
          return false;
        }
        prevTxIndex = txIndex;
      }

      prevTxIndex = -1;
      for (BlockAccessList.NonceChange ch : account.nonceChanges()) {
        final int txIndex = ch.txIndex();
        if (txIndex < 0) {
          LOG.warn(
              "Block access list has negative block_access_index in nonce_changes for address {} block {}",
              account.address(),
              blockHeader.getBlockHash());
          return false;
        }
        if (prevTxIndex >= txIndex) {
          LOG.warn(
              "Block access list nonce_changes not in canonical order (by block_access_index) for address {} block {}",
              account.address(),
              blockHeader.getBlockHash());
          return false;
        }
        if (txIndex > maxIndex) {
          LOG.warn(
              "Block access list has block_access_index {} exceeding max {} for block {}",
              txIndex,
              maxIndex,
              blockHeader.getBlockHash());
          return false;
        }
        prevTxIndex = txIndex;
      }

      prevTxIndex = -1;
      for (BlockAccessList.CodeChange ch : account.codeChanges()) {
        final int txIndex = ch.txIndex();
        if (txIndex < 0) {
          LOG.warn(
              "Block access list has negative block_access_index in code_changes for address {} block {}",
              account.address(),
              blockHeader.getBlockHash());
          return false;
        }
        if (prevTxIndex >= txIndex) {
          LOG.warn(
              "Block access list code_changes not in canonical order (by block_access_index) for address {} block {}",
              account.address(),
              blockHeader.getBlockHash());
          return false;
        }
        if (txIndex > maxIndex) {
          LOG.warn(
              "Block access list has block_access_index {} exceeding max {} for block {}",
              txIndex,
              maxIndex,
              blockHeader.getBlockHash());
          return false;
        }
        prevTxIndex = txIndex;
      }
    }
    return true;
  }
}
