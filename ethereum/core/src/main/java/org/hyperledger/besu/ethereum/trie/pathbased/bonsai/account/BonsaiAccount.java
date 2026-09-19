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

  private Hash storageRoot;

  public BonsaiAccount(
      final BonsaiWorldView context,
      final Address address,
      final Hash addressHash,
      final long nonce,
      final Wei balance,
      final Hash storageRoot,
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
    this.storageRoot = storageRoot;

    if (codeHash.equals(Hash.EMPTY)) {
      this.code = Code.EMPTY_CODE;
    }
  }

  public BonsaiAccount(
      final BonsaiWorldView context,
      final Address address,
      final AccountValue stateTrieAccount,
      final boolean mutable,
      final CodeCache codeCache) {
    this(
        context,
        address,
        address.addressHash(),
        stateTrieAccount.getNonce(),
        stateTrieAccount.getBalance(),
        stateTrieAccount.getStorageRoot(),
        stateTrieAccount.getCodeHash(),
        mutable,
        codeCache);
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
    this.storageRoot = toCopy.storageRoot;
    this.updatedStorage.putAll(toCopy.updatedStorage);

    if (toCopy.code == null && toCopy.codeHash.equals(Hash.EMPTY)) {
      this.code = Code.EMPTY_CODE;
    } else {
      // as this constructor is only used for copying accounts, we assume the code must have
      // originated from the cache, so we don't need to put it in the cache again
      this.code = toCopy.code;
    }
  }

  public BonsaiAccount(
      final BonsaiWorldView context,
      final UpdateTrackingAccount<BonsaiAccount> tracked,
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
    this.storageRoot = Hash.EMPTY_TRIE_HASH;
    this.updatedStorage.putAll(tracked.getUpdatedStorage());
  }

  public static BonsaiAccount fromRLP(
      final BonsaiWorldView context,
      final Address address,
      final Bytes encoded,
      final boolean mutable,
      final CodeCache codeCache)
      throws RLPException {
    final RLPInput in = RLP.input(encoded);
    in.enterList();

    final long nonce = in.readLongScalar();
    final Wei balance = Wei.of(in.readUInt256Scalar());
    final Hash storageRoot = Hash.wrap(in.readBytes32());
    final Hash codeHash = Hash.wrap(in.readBytes32());

    in.leaveList();

    return new BonsaiAccount(
        context,
        address,
        address.addressHash(),
        nonce,
        balance,
        storageRoot,
        codeHash,
        mutable,
        codeCache);
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

  @Override
  public NavigableMap<Bytes32, AccountStorageEntry> storageEntriesFrom(
      final Bytes32 startKeyHash, final int limit) {
    return context.getWorldStateStorage().storageEntriesFrom(this.addressHash, startKeyHash, limit);
  }

  @Override
  public void writeTo(final RLPOutput out) {
    out.startList();

    out.writeLongScalar(nonce);
    out.writeUInt256Scalar(balance);
    out.writeBytes(storageRoot.getBytes());
    out.writeBytes(codeHash.getBytes());

    out.endList();
  }

  @Override
  public Hash getStorageRoot() {
    return storageRoot;
  }

  public void setStorageRoot(final Hash storageRoot) {
    if (immutable) {
      throw new ModificationNotAllowedException();
    }
    this.storageRoot = storageRoot;
  }

  /**
   * Equal when they serialize to the same state trie value; pending storage writes are not part of
   * it.
   */
  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BonsaiAccount that)) {
      return false;
    }
    return nonce == that.nonce
        && Objects.equals(address, that.address)
        && Objects.equals(balance, that.balance)
        && Objects.equals(storageRoot, that.storageRoot)
        && Objects.equals(codeHash, that.codeHash);
  }

  @Override
  public int hashCode() {
    return Objects.hash(address, nonce, balance, storageRoot, codeHash);
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
        + storageRoot
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
      if (!Objects.equals(source.storageRoot, account.getStorageRoot())) {
        throw new IllegalStateException(context + ": Storage Roots differ");
      }
    }
  }
}
