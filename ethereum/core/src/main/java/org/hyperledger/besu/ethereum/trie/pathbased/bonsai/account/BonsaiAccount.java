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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.PatriciaAccountValue;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldView;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.ModificationNotAllowedException;
import org.hyperledger.besu.evm.account.AccountStorageEntry;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.CodeCache;
import org.hyperledger.besu.evm.worldstate.UpdateTrackingAccount;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Bonsai account view, generic over the underlying trie format.
 *
 * <p>The storage-root state and behavior live entirely in a {@link StorageRootStrategy} carried by
 * the account — {@code BonsaiAccount} holds no root data field of its own. The MPT strategy holds a
 * mutable storage root (patched by the state-root committers at commit time).
 *
 * <p>{@code BonsaiAccount} itself is format-agnostic: callers inject the strategy selected from the
 * active storage format (world state / accumulator / decode factory).
 */
public class BonsaiAccount implements MutableAccount, AccountValue {
  protected final BonsaiWorldView context;
  protected boolean immutable;
  protected final Address address;
  protected final Hash addressHash;
  protected Hash codeHash;
  protected long nonce;
  protected Wei balance;
  protected Code code;
  protected final CodeCache codeCache;

  protected final Map<UInt256, UInt256> updatedStorage = new HashMap<>();

  private final StorageRootStrategy storageRootStrategy;

  public BonsaiAccount(
      final BonsaiWorldView context,
      final Address address,
      final Hash addressHash,
      final long nonce,
      final Wei balance,
      final StorageRootStrategy storageRootStrategy,
      final Hash codeHash,
      final boolean mutable,
      final CodeCache codeCache) {
    this.context = context;
    this.address = address;
    this.addressHash = addressHash;
    this.nonce = nonce;
    this.balance = balance;
    this.codeHash = codeHash;
    this.codeCache = codeCache;

    this.immutable = !mutable;
    this.storageRootStrategy = storageRootStrategy;

    if (codeHash.equals(Hash.EMPTY)) {
      this.code = Code.EMPTY_CODE;
    }
  }

  public BonsaiAccount(final BonsaiAccount toCopy) {
    this(toCopy, toCopy.context, false);
  }

  public BonsaiAccount(
      final BonsaiAccount toCopy, final BonsaiWorldView context, final boolean mutable) {
    this.context = context;
    this.address = toCopy.address;
    this.addressHash = toCopy.addressHash;
    this.nonce = toCopy.nonce;
    this.balance = toCopy.balance;
    this.codeHash = toCopy.codeHash;
    this.immutable = !mutable;
    this.codeCache = toCopy.codeCache;
    this.storageRootStrategy = toCopy.storageRootStrategy.copy();

    if (toCopy.code == null && toCopy.codeHash.equals(Hash.EMPTY)) {
      this.code = Code.EMPTY_CODE;
    } else {
      // as this constructor is only used for copying accounts, we assume the code must have
      // originated from the cache, so we don't need to put it in the cache again
      this.code = toCopy.code;
    }
    updatedStorage.putAll(toCopy.updatedStorage);
  }

  public BonsaiAccount(
      final BonsaiWorldView context,
      final UpdateTrackingAccount<BonsaiAccount> tracked,
      final StorageRootStrategy storageRootStrategy,
      final CodeCache codeCache) {
    this.context = context;
    this.address = tracked.getAddress();
    this.addressHash = tracked.getAddressHash();
    this.nonce = tracked.getNonce();
    this.balance = tracked.getBalance();
    this.codeHash = tracked.getCodeHash();
    this.immutable = false;
    this.codeCache = codeCache;
    this.code = new Code(tracked.getCode());
    this.storageRootStrategy = storageRootStrategy;
    updatedStorage.putAll(tracked.getUpdatedStorage());
  }

  /** Returns the {@link StorageRootStrategy} carried by this account. */
  public StorageRootStrategy getStorageRootStrategy() {
    return storageRootStrategy;
  }

  @Override
  public org.hyperledger.besu.evm.internal.CodeCache getCodeCache() {
    return codeCache;
  }

  @Override
  public Address getAddress() {
    return address;
  }

  @Override
  public Hash getAddressHash() {
    return addressHash;
  }

  @Override
  public long getNonce() {
    return nonce;
  }

  @Override
  public void setNonce(final long value) {
    if (immutable) {
      throw new ModificationNotAllowedException();
    }
    nonce = value;
  }

  @Override
  public Wei getBalance() {
    return balance;
  }

  @Override
  public void setBalance(final Wei value) {
    if (immutable) {
      throw new ModificationNotAllowedException();
    }
    balance = value;
  }

  @Override
  public Bytes getCode() {
    // always prefer the local copy to avoid unnecessary cache lookups
    if (code != null) {
      return code.getBytes();
    }

    return getOrCreateCachedCode().getBytes();
  }

  @Override
  public Code getOrCreateCachedCode() {
    // always prefer the local copy to avoid unnecessary cache lookups
    if (code != null) {
      return code;
    }

    // check if we have a cached version of the code
    final Code cachedCode =
        Optional.ofNullable(codeCache).map(c -> c.getIfPresent(codeHash)).orElse(null);

    // cache hit, overwrite code and return it
    if (cachedCode != null) {
      code = cachedCode;
      return code;
    }

    // cache miss get the code from the disk, set it and put it in the cache
    final Bytes byteCode = context.getCode(address, codeHash).orElse(Bytes.EMPTY);
    code = new Code(byteCode, codeHash);
    Optional.ofNullable(codeCache).ifPresent(c -> c.put(codeHash, code));

    return code;
  }

  @Override
  public void setCode(final Bytes byteCode) {
    if (immutable) {
      throw new ModificationNotAllowedException();
    }

    if (byteCode == null || byteCode.isEmpty()) {
      this.code = Code.EMPTY_CODE;
      this.codeHash = Hash.EMPTY;
      return;
    }

    this.codeHash = Hash.hash(byteCode);

    // check if we have a cached version of the code
    final Code cachedCode =
        Optional.ofNullable(codeCache).map(c -> c.getIfPresent(codeHash)).orElse(null);

    if (cachedCode != null) {
      this.code = cachedCode;
      return;
    }

    this.code = new Code(byteCode, codeHash);
    Optional.ofNullable(codeCache).ifPresent(c -> c.put(codeHash, this.code));
  }

  /**
   * Updates the code hash without loading bytecode. The code is resolved lazily on the next read.
   */
  public void setCodeHash(final Hash newCodeHash) {
    if (immutable) {
      throw new ModificationNotAllowedException();
    }
    this.codeHash = newCodeHash;
    this.code = null;
  }

  @Override
  public Hash getCodeHash() {
    return codeHash;
  }

  @Override
  public UInt256 getStorageValue(final UInt256 key) {
    return context.getStorageValue(address, key);
  }

  @Override
  public UInt256 getOriginalStorageValue(final UInt256 key) {
    return context.getPriorStorageValue(address, key);
  }

  public Bytes serializeAccount() {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    writeTo(out);
    return out.encoded();
  }

  @Override
  public void setStorageValue(final UInt256 key, final UInt256 value) {
    if (immutable) {
      throw new ModificationNotAllowedException();
    }
    updatedStorage.put(key, value);
  }

  @Override
  public void clearStorage() {
    updatedStorage.clear();
  }

  @Override
  public Map<UInt256, UInt256> getUpdatedStorage() {
    return updatedStorage;
  }

  @Override
  public void becomeImmutable() {
    immutable = true;
  }

  /**
   * Format-agnostic flat-DB decode factory. Reads the account RLP list {@code [nonce, balance,
   * ...storageRoot..., codeHash]} and constructs a {@link BonsaiAccount} carrying the supplied
   * {@link StorageRootStrategy}. The strategy reads (and stores) the storage-root field for MPT
   * accounts and reads nothing for binary accounts (3-field list), so this factory works for both
   * formats without any format probing in {@code BonsaiAccount}.
   *
   * @param context the surrounding world view (may be {@code null} in tests).
   * @param address the account address.
   * @param encoded the flat-DB encoded bytes.
   * @param mutable whether the returned account is mutable.
   * @param codeCache the code cache.
   * @param strategy the strategy selected by the caller from the world state's storage format (MPT
   *     strategies are mutable and must not be shared across decodes; binary is a stateless
   *     singleton).
   * @return the decoded account.
   * @throws RLPException if the encoded bytes are not valid RLP.
   */
  public static BonsaiAccount fromFlatBytes(
      final BonsaiWorldView context,
      final Address address,
      final Bytes encoded,
      final boolean mutable,
      final CodeCache codeCache,
      final StorageRootStrategy strategy)
      throws RLPException {
    final RLPInput in = RLP.input(encoded);
    final int listSize = in.enterList();

    final long nonce = in.readLongScalar();
    final Wei balance = Wei.of(in.readUInt256Scalar());
    strategy.readStorageRoot(in, listSize);
    final Hash codeHash = Hash.wrap(in.readBytes32());

    in.leaveList();

    return new BonsaiAccount(
        context,
        address,
        address.addressHash(),
        nonce,
        balance,
        strategy,
        codeHash,
        mutable,
        codeCache);
  }

  @Override
  public boolean isStorageEmpty() {
    return storageRootStrategy.isStorageEmpty();
  }

  @Override
  public NavigableMap<Bytes32, AccountStorageEntry> storageEntriesFrom(
      final Bytes32 startKeyHash, final int limit) {
    return context.getWorldStateStorage().storageEntriesFrom(this.addressHash, startKeyHash, limit);
  }

  /**
   * Writes the account RLP list. The list shape is determined by the {@link StorageRootStrategy}:
   * MPT writes {@code [nonce, balance, storageRoot, codeHash]} (4 fields); binary writes {@code
   * [nonce, balance, codeHash]} (3 fields). Works for both formats — no throw.
   */
  @Override
  public void writeTo(final RLPOutput out) {
    out.startList();

    out.writeLongScalar(nonce);
    out.writeUInt256Scalar(balance);
    storageRootStrategy.writeStorageRoot(out);
    out.writeBytes(codeHash.getBytes());

    out.endList();
  }

  /**
   * Returns the account storage root.
   *
   * <p>This is a concrete accessor on {@code BonsaiAccount} (not on {@link AccountValue}, which
   * does not expose a storage root): the storage root is delegated to the {@link
   * StorageRootStrategy}. Callers holding a generic {@link AccountValue} should use {@link
   * PatriciaAccountValue} when they need the storage root.
   */
  public Hash getStorageRoot() {
    return storageRootStrategy.getStorageRoot();
  }

  /**
   * Sets the storage root. Used by the MPT state-root committers to patch the storage root before
   * serializing the MPT account trie leaf. Throws for binary accounts.
   */
  public void setStorageRoot(final Hash storageRoot) {
    if (immutable) {
      throw new ModificationNotAllowedException();
    }
    this.storageRootStrategy.setStorageRoot(storageRoot);
  }

  @Override
  public String toString() {
    return "AccountState{"
        + "address="
        + address
        + ", nonce="
        + nonce
        + ", balance="
        + balance
        + ", storageRoot="
        + storageRootStrategy.getStorageRoot()
        + ", codeHash="
        + codeHash
        + '}';
  }

  /**
   * Throws an exception if the two accounts represent different stored states
   *
   * @param source The bonsai account to compare
   * @param account The State Trie account to compare
   * @param context a description to be added to the thrown exceptions
   * @throws IllegalStateException if the stored values differ
   */
  public static void assertCloseEnoughForDiffing(
      final BonsaiAccount source, final AccountValue account, final String context) {
    if (source == null) {
      throw new IllegalStateException(context + ": source is null but target isn't");
    } else {
      if (source.nonce != account.getNonce()) {
        throw new IllegalStateException(context + ": nonces differ");
      }
      if (!Objects.equals(source.balance, account.getBalance())) {
        throw new IllegalStateException(context + ": balances differ");
      }
      // Storage-root comparison is owned entirely by the StorageRootStrategy. Binary accounts
      // carry no storage root and the comparison is a no-op for them; MPT accounts compare their
      // held roots (and reject an MPT-vs-binary mismatch).
      source.storageRootStrategy.assertStorageRootMatches(account, context);
    }
  }
}
