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
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
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

  private final ProtocolSchedule protocolSchedule;

  /**
   * Creates a block access list validator for the given protocol schedule and optional BAL factory.
   * Use as method reference: {@code MainnetBlockAccessListValidator::create}.
   *
   * @param protocolSchedule the protocol schedule
   * @return a validator instance or no-op when factory is empty
   */
  public static BlockAccessListValidator create(final ProtocolSchedule protocolSchedule) {
    return new MainnetBlockAccessListValidator(protocolSchedule);
  }

  /**
   * Creates a new MainnetBlockAccessListValidator.
   *
   * @param protocolSchedule the protocol schedule to get protocol specs from
   */
  public MainnetBlockAccessListValidator(final ProtocolSchedule protocolSchedule) {
    this.protocolSchedule = protocolSchedule;
  }

  @Override
  public BlockAccessListItemSizeCheck validateExecutedBlockAccessListItemSize(
      final long itemCount, final ProcessableBlockHeader blockHeader) {
    return validateExecutedBlockAccessListItemSize(
        itemCount, blockHeader, protocolSchedule.getByBlockHeader(blockHeader));
  }

  @Override
  public BlockAccessListItemSizeCheck validateExecutedBlockAccessListItemSize(
      final long itemCount,
      final ProcessableBlockHeader blockHeader,
      final ProtocolSpec protocolSpecForItemCost) {
    final long itemCost = protocolSpecForItemCost.getGasCalculator().getBlockAccessListItemCost();
    if (itemCost <= 0) {
      return BlockAccessListItemSizeCheck.withinBudget();
    }
    final long maxItems = blockHeader.getGasLimit() / itemCost;
    if (itemCount <= maxItems) {
      return BlockAccessListItemSizeCheck.withinBudget();
    }
    final String blockRef =
        blockHeader instanceof BlockHeader fullHeader
            ? fullHeader.getBlockHash().toShortLogString()
            : String.format("pending#%d", blockHeader.getNumber());
    return BlockAccessListItemSizeCheck.overBudget(
        new BlockAccessListValidationError(
            String.format(
                "Block access list size exceeds maximum allowed items for block %s with gas limit %d"
                    + " (items %d, max %d)",
                blockRef, blockHeader.getGasLimit(), itemCount, maxItems)));
  }

  @Override
  public Optional<BlockAccessListValidationError> validateExecutedBlockAccessListAfterBuild(
      final BlockAccessList executedBal,
      final BlockHeader blockHeader,
      final Optional<BlockAccessList> suppliedBlockAccessList,
      final boolean logBalDetailsOnHashMismatch) {
    if (blockHeader.getBalHash().isEmpty()) {
      return Optional.empty();
    }
    final BlockAccessListItemSizeCheck sizeCheck =
        validateExecutedBlockAccessListItemSize(
            executedBal.eip7928ItemCount(),
            blockHeader,
            protocolSchedule.getByBlockHeader(blockHeader));
    if (sizeCheck.isOverBudget()) {
      final BlockAccessListValidationError error = sizeCheck.overBudgetError().orElseThrow();
      LOG.error(error.errorMessage());
      return Optional.of(error);
    }

    return balHashMismatchAgainstHeaderIfAny(
        executedBal,
        blockHeader.getBalHash(),
        suppliedBlockAccessList,
        logBalDetailsOnHashMismatch,
        true);
  }

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

    final BlockAccessListItemSizeCheck lightSizeCheck =
        validateExecutedBlockAccessListItemSize(
            bal.eip7928ItemCount(), blockHeader, protocolSchedule.getByBlockHeader(blockHeader));
    if (lightSizeCheck.isOverBudget()) {
      LOG.warn(lightSizeCheck.overBudgetError().orElseThrow().errorMessage());
      return false;
    }

    if (balHashMismatchAgainstHeaderIfAny(bal, headerBalHash, Optional.empty(), false, false)
        .isPresent()) {
      return false;
    }

    final long maxIndex = (long) nbTransactions + 1L;
    if (!validateConstraints(bal, blockHeader, maxIndex)) {
      return false;
    }
    LOG.trace("Block access list validated successfully for block {}", blockHeader.getNumber());
    return true;
  }

  private void logBalHashMismatch(
      final String message,
      final boolean logAsError,
      final BlockAccessList balForDetails,
      final Optional<BlockAccessList> supplied,
      final boolean logDetails) {
    if (logAsError) {
      LOG.error(message);
    } else {
      LOG.warn(message);
    }
    if (logDetails) {
      LOG.error(
          "--- BAL constructed during execution ---\n{}\n--- BAL supplied for block ---\n{}",
          balForDetails.toString(),
          supplied.map(Object::toString).orElse("<no BAL present for block>"));
    }
  }

  /**
   * When the header carries a {@code balHash}, returns empty if it matches {@code bal}; otherwise
   * logs and returns a {@link BlockAccessListValidationError}.
   */
  private Optional<BlockAccessListValidationError> balHashMismatchAgainstHeaderIfAny(
      final BlockAccessList bal,
      final Optional<Hash> headerBalHashOpt,
      final Optional<BlockAccessList> suppliedBlockAccessList,
      final boolean logBalDetailsOnHashMismatch,
      final boolean logPrimaryMessageAsError) {
    if (headerBalHashOpt.isEmpty()) {
      return Optional.empty();
    }
    final Hash headerBalHash = headerBalHashOpt.get();
    final Hash computedHash = BodyValidation.balHash(bal);
    if (computedHash.equals(headerBalHash)) {
      return Optional.empty();
    }
    final String errorMessage =
        String.format(
            "Block access list hash mismatch, calculated: %s header: %s",
            computedHash.getBytes().toHexString(), headerBalHash.getBytes().toHexString());
    logBalHashMismatch(
        errorMessage,
        logPrimaryMessageAsError,
        bal,
        suppliedBlockAccessList,
        logBalDetailsOnHashMismatch);
    return Optional.of(new BlockAccessListValidationError(errorMessage));
  }

  /**
   * Validates index range (indices in [0, maxIndex]) and canonical ordering (EIP-7928) in one
   * traversal. Strict ordering implies uniqueness; the only set kept is change slots to detect
   * overlap with storage_reads.
   */
  private boolean validateConstraints(
      final BlockAccessList bal, final BlockHeader blockHeader, final long maxIndex) {
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
      long prevStorageTxIndex = -1L;

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

        prevStorageTxIndex = -1L;
        for (BlockAccessList.StorageChange ch : slotChanges.changes()) {
          final long txIndex = ch.txIndex();
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

      long prevTxIndex = -1L;
      for (BlockAccessList.BalanceChange ch : account.balanceChanges()) {
        final long txIndex = ch.txIndex();
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

      prevTxIndex = -1L;
      for (BlockAccessList.NonceChange ch : account.nonceChanges()) {
        final long txIndex = ch.txIndex();
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

      prevTxIndex = -1L;
      for (BlockAccessList.CodeChange ch : account.codeChanges()) {
        final long txIndex = ch.txIndex();
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

  /** Canonical slot order (by slot key bytes), consistent with BlockAccessListBuilder. */
  private int compareSlotKeysByCanonicalOrder(final StorageSlotKey a, final StorageSlotKey b) {
    return a.getSlotKey().orElseThrow().toBytes().compareTo(b.getSlotKey().orElseThrow().toBytes());
  }
}
