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
package org.hyperledger.besu.ethereum.trie.pathbased.common;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldView;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.StateMetricsCollector;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.ModificationNotAllowedException;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.CodeCache;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

public abstract class PathBasedAccount implements MutableAccount, AccountValue {
  protected final PathBasedWorldView context;
  protected boolean immutable;
  protected final Address address;
  protected final Hash addressHash;
  protected Hash codeHash;
  protected long nonce;
  protected Wei balance;
  protected Code code;
  protected final CodeCache codeCache;

  protected final Map<UInt256, UInt256> updatedStorage = new HashMap<>();

  /**
   * Constructs a new PathBasedAccount instance without the account's code. This constructor is used
   * when the account's code is not required or will not be read from the database. It initializes
   * the account with its context, address, address hash, nonce, balance, code hash, and mutability
   * status.
   *
   * @param context The PathBasedWorldView context in which this account exists.
   * @param address The Ethereum address of this account.
   * @param addressHash The hash of the account's address.
   * @param nonce The nonce of the account, representing the number of transactions sent from this
   *     account.
   * @param balance The balance of the account in Wei.
   * @param codeHash The hash of the account's code.
   * @param mutable A boolean indicating if the account is mutable. If false, the account is
   *     considered immutable.
   * @param codeCache The global cache used to store and retrieve the account's code.
   */
  public PathBasedAccount(
      final PathBasedWorldView context,
      final Address address,
      final Hash addressHash,
      final long nonce,
      final Wei balance,
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

    if (codeHash.equals(Hash.EMPTY)) {
      this.code = Code.EMPTY_CODE;
    }
  }

  /**
   * Constructs a new PathBasedAccount instance with the account's code. This constructor is used
   * when all account information, including its code, are available. It initializes the account
   * with its context, address, address hash, nonce, balance, code hash, the actual code, and
   * mutability status.
   *
   * @param context The PathBasedWorldView context in which this account exists.
   * @param address The Ethereum address of this account.
   * @param addressHash The hash of the account's address.
   * @param nonce The nonce of the account, representing the number of transactions sent from this
   *     account.
   * @param balance The balance of the account in Wei.
   * @param codeHash The hash of the account's code.
   * @param code The actual bytecode of the account's smart contract. This is provided when the code
   *     is known and needs to be associated with the account.
   * @param mutable A boolean indicating if the account is mutable. If false, the account is
   *     considered immutable.
   */
  public PathBasedAccount(
      final PathBasedWorldView context,
      final Address address,
      final Hash addressHash,
      final long nonce,
      final Wei balance,
      final Hash codeHash,
      final Code code,
      final boolean mutable,
      final CodeCache codeCache) {
    this.context = context;
    this.address = address;
    this.addressHash = addressHash;
    this.nonce = nonce;
    this.balance = balance;
    this.codeHash = codeHash;
    this.immutable = !mutable;
    this.codeCache = codeCache;

    if (code == null && codeHash.equals(Hash.EMPTY)) {
      this.code = Code.EMPTY_CODE;
    } else {
      // as this constructor is only used for copying accounts, we assume the code must have
      // originated
      // from the cache, so we don't need to put it in the cache again
      this.code = code;
    }
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
      // Track code cache hit and code read for cross-client execution metrics
      final StateMetricsCollector collector = context.getStateMetricsCollector();
      collector.incrementCodeCacheHits();
      collector.incrementCodeReads();
      collector.addCodeBytesRead(code.getSize());
      return code;
    }

    // cache miss get the code from the disk, set it and put it in the cache
    final StateMetricsCollector collector = context.getStateMetricsCollector();
    final long startNanos = System.nanoTime();
    final Bytes byteCode = context.getCode(address, codeHash).orElse(Bytes.EMPTY);
    collector.addStateReadTime(System.nanoTime() - startNanos);
    code = new Code(byteCode, codeHash);
    Optional.ofNullable(codeCache).ifPresent(c -> c.put(codeHash, code));
    // Track code cache miss and code read for cross-client execution metrics
    collector.incrementCodeCacheMisses();
    collector.incrementCodeReads();
    collector.addCodeBytesRead(code.getSize());

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
  public String toString() {
    return "AccountState{"
        + "address="
        + address
        + ", nonce="
        + nonce
        + ", balance="
        + balance
        + ", codeHash="
        + codeHash
        + '}';
  }
}
