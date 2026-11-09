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
package org.hyperledger.besu.ethereum.mainnet.block.access.list;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldView;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.account.Account;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import org.apache.tuweni.bytes.Bytes;

/**
 * Account and storage-slot lookup over a {@link BlockAccessList}, built once per block and shared
 * across all transactions. Does not resolve transaction boundaries; use {@link
 * BlockAccessListOverlay} to read state as of a specific transaction index.
 */
public final class BlockAccessListAccountLookup {

  private final Map<Address, AccountEntry> accountEntries;
  private final List<BlockAccessList.AccountChanges> accountChanges;
  private final Supplier<Map<Hash, Bytes>> writtenCodeByHash =
      Suppliers.memoize(this::indexWrittenCode);
  private final Map<Hash, Bytes> preBlockCodeByHash = new ConcurrentHashMap<>();
  private volatile Map<Hash, Address> preBlockCodeOwnerByHash;

  private BlockAccessListAccountLookup(
      final Map<Address, AccountEntry> accountEntries,
      final List<BlockAccessList.AccountChanges> accountChanges) {
    this.accountEntries = accountEntries;
    this.accountChanges = accountChanges;
  }

  public static BlockAccessListAccountLookup of(final BlockAccessList blockAccessList) {
    final List<BlockAccessList.AccountChanges> accountChanges = blockAccessList.accountChanges();
    final Map<Address, AccountEntry> entries = HashMap.newHashMap(accountChanges.size());
    for (final BlockAccessList.AccountChanges changes : accountChanges) {
      entries.put(changes.address(), new AccountEntry(changes));
    }
    return new BlockAccessListAccountLookup(entries, accountChanges);
  }

  public List<BlockAccessList.AccountChanges> accountChanges() {
    return accountChanges;
  }

  public boolean isEmpty() {
    return accountChanges.isEmpty();
  }

  public Optional<BlockAccessList.AccountChanges> getAccountChanges(final Address address) {
    final AccountEntry entry = accountEntries.get(address);
    return entry == null ? Optional.empty() : Optional.of(entry.accountChanges);
  }

  public Optional<Hash> getAddressHash(final Address address) {
    final AccountEntry entry = accountEntries.get(address);
    return entry == null ? Optional.empty() : Optional.of(entry.addressHash);
  }

  /**
   * Resolves the bytes of the code that a hash-form (EIP-8298) code change adopted.
   *
   * <p>SETCODEFROM only adopts code that exists before the transaction and reads its source
   * account, so the code is either written by a bytes-form code change of this block or is the
   * pre-block code of an account in this block access list. Any bytes with the matching hash are
   * that code, so the index and the account of the entry that holds them do not matter. An
   * unresolvable hash means the block access list does not match the block.
   *
   * <p>The first hash that no bytes-form change holds reads the pre-block code hash of every
   * account in the block access list once, so later hashes, resolvable or not, read no more
   * accounts.
   *
   * @param codeHash the adopted code hash
   * @param preBlockCode reads the state the block builds on; every caller of this lookup must pass
   *     a reader of that same state
   * @return the code, or empty if neither the block access list nor the pre-block code of its
   *     accounts holds it
   */
  public Optional<Bytes> resolveAdoptedCode(
      final Hash codeHash, final PreBlockCodeReader preBlockCode) {
    final Optional<Bytes> written = resolveWrittenCode(codeHash);
    if (written.isPresent()) {
      return written;
    }
    final Bytes cached = preBlockCodeByHash.get(codeHash);
    if (cached != null) {
      return Optional.of(cached);
    }
    final Address owner = preBlockCodeOwners(preBlockCode).get(codeHash);
    if (owner == null) {
      return Optional.empty();
    }
    final Optional<Bytes> code = preBlockCode.code(owner, codeHash);
    code.ifPresent(bytes -> preBlockCodeByHash.put(codeHash, bytes));
    return code;
  }

  /**
   * Reads pre-block code for {@link #resolveAdoptedCode} from the stored state of the parent block,
   * so a block access list overlay on its accumulator does not apply.
   *
   * @param parentState the Bonsai world state the block builds on, not an updater of it
   * @return a reader of the stored code hash and code of an account
   */
  public static PreBlockCodeReader preBlockCodeOf(final BonsaiWorldView parentState) {
    return new PreBlockCodeReader() {
      @Override
      public Optional<Hash> codeHash(final Address address) {
        return Optional.ofNullable(parentState.get(address)).map(Account::getCodeHash);
      }

      @Override
      public Optional<Bytes> code(final Address address, final Hash codeHash) {
        return parentState.getCode(address, codeHash).map(Code::getBytes);
      }
    };
  }

  /** Reads code from the state a block builds on, for {@link #resolveAdoptedCode}. */
  public interface PreBlockCodeReader {

    /**
     * Returns the code hash of an account before the block.
     *
     * @param address the account
     * @return its code hash, or empty if the account does not exist
     */
    Optional<Hash> codeHash(Address address);

    /**
     * Returns the code of an account before the block.
     *
     * @param address the account, whose code hash before the block is {@code codeHash}
     * @param codeHash the hash of its code
     * @return the code, or empty if it is not stored
     */
    Optional<Bytes> code(Address address, Hash codeHash);
  }

  /**
   * Resolves an adopted code hash from the bytes-form code changes of this block access list only.
   *
   * @param codeHash the adopted code hash
   * @return the code, or empty if no bytes-form code change wrote it
   */
  Optional<Bytes> resolveWrittenCode(final Hash codeHash) {
    return Optional.ofNullable(writtenCodeByHash.get().get(codeHash));
  }

  private Map<Hash, Address> preBlockCodeOwners(final PreBlockCodeReader preBlockCode) {
    Map<Hash, Address> owners = preBlockCodeOwnerByHash;
    if (owners == null) {
      synchronized (this) {
        owners = preBlockCodeOwnerByHash;
        if (owners == null) {
          owners = indexPreBlockCode(preBlockCode);
          preBlockCodeOwnerByHash = owners;
        }
      }
    }
    return owners;
  }

  private Map<Hash, Address> indexPreBlockCode(final PreBlockCodeReader preBlockCode) {
    final Map<Hash, Address> owners = new HashMap<>();
    for (final BlockAccessList.AccountChanges changes : accountChanges) {
      preBlockCode
          .codeHash(changes.address())
          .filter(codeHash -> !Hash.EMPTY.equals(codeHash))
          .ifPresent(codeHash -> owners.putIfAbsent(codeHash, changes.address()));
    }
    return Map.copyOf(owners);
  }

  private Map<Hash, Bytes> indexWrittenCode() {
    final Map<Hash, Bytes> code = new HashMap<>();
    for (final BlockAccessList.AccountChanges changes : accountChanges) {
      for (final BlockAccessList.CodeChange change : changes.codeChanges()) {
        if (!change.isAdoptedCode()) {
          code.putIfAbsent(Hash.hash(change.newCode()), change.newCode());
        }
      }
    }
    return code;
  }

  Optional<BlockAccessList.SlotChanges> getSlotChanges(
      final Address address, final StorageSlotKey storageSlotKey) {
    final AccountEntry entry = accountEntries.get(address);
    return entry == null ? Optional.empty() : entry.slotChanges(storageSlotKey);
  }

  private static final class AccountEntry {
    private final BlockAccessList.AccountChanges accountChanges;
    private final Hash addressHash;
    private final Map<StorageSlotKey, BlockAccessList.SlotChanges> storageBySlot;

    private AccountEntry(final BlockAccessList.AccountChanges accountChanges) {
      this.accountChanges = accountChanges;
      this.addressHash = accountChanges.address().addressHash();
      this.storageBySlot = buildStorageBySlot(accountChanges.storageChanges());
    }

    private static Map<StorageSlotKey, BlockAccessList.SlotChanges> buildStorageBySlot(
        final List<BlockAccessList.SlotChanges> storageChanges) {
      if (storageChanges.isEmpty()) {
        return Map.of();
      }
      final Map<StorageSlotKey, BlockAccessList.SlotChanges> built =
          HashMap.newHashMap(storageChanges.size());
      for (final BlockAccessList.SlotChanges slotChange : storageChanges) {
        built.put(slotChange.slot(), slotChange);
      }
      return built;
    }

    Optional<BlockAccessList.SlotChanges> slotChanges(final StorageSlotKey storageSlotKey) {
      return Optional.ofNullable(storageBySlot.get(storageSlotKey));
    }
  }
}
