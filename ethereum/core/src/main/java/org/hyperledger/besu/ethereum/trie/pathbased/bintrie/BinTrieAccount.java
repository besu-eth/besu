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
package org.hyperledger.besu.ethereum.trie.pathbased.bintrie;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;
import org.hyperledger.besu.ethereum.stateless.bintrie.util.SuffixTreeDecoder;
import org.hyperledger.besu.ethereum.stateless.bintrie.util.SuffixTreeEncoder;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.common.PathBasedAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldView;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.account.AccountStorageEntry;
import org.hyperledger.besu.evm.worldstate.UpdateTrackingAccount;

import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Account implementation for Binary Trie worldstate. Unlike Bonsai which uses a separate storage
 * trie per account, BinTrie embeds storage directly in the main trie structure.
 */
public class BinTrieAccount extends PathBasedAccount {

  private long codeSize;

  public BinTrieAccount(
      final PathBasedWorldView context,
      final Address address,
      final Hash addressHash,
      final long nonce,
      final Wei balance,
      final Hash codeHash,
      final long codeSize,
      final boolean mutable,
      final CodeCache codeCache) {
    super(context, address, addressHash, nonce, balance, codeHash, mutable, codeCache);
    this.codeSize = codeSize;
  }

  public BinTrieAccount(
      final PathBasedWorldView context,
      final Address address,
      final AccountValue stateTrieAccount,
      final boolean mutable,
      final CodeCache codeCache) {
    super(
        context,
        address,
        address.addressHash(),
        stateTrieAccount.getNonce(),
        stateTrieAccount.getBalance(),
        stateTrieAccount.getCodeHash(),
        mutable,
        codeCache);
    this.codeSize = stateTrieAccount.getCodeSize().orElse(0L);
  }

  public BinTrieAccount(final BinTrieAccount toCopy) {
    this(toCopy, toCopy.context, false);
  }

  public BinTrieAccount(
      final BinTrieAccount toCopy, final PathBasedWorldView context, final boolean mutable) {
    super(
        context,
        toCopy.address,
        toCopy.addressHash,
        toCopy.nonce,
        toCopy.balance,
        toCopy.codeHash,
        toCopy.code,
        mutable,
        toCopy.codeCache);
    this.codeSize = toCopy.codeSize;
    updatedStorage.putAll(toCopy.updatedStorage);
  }

  public BinTrieAccount(
      final PathBasedWorldView context,
      final UpdateTrackingAccount<BinTrieAccount> tracked,
      final CodeCache codeCache) {
    super(
        context,
        tracked.getAddress(),
        tracked.getAddressHash(),
        tracked.getNonce(),
        tracked.getBalance(),
        tracked.getCodeHash(),
        new Code(tracked.getCode()),
        true,
        codeCache);
    this.codeSize = tracked.getCode() != null ? tracked.getCode().size() : 0;
    updatedStorage.putAll(tracked.getUpdatedStorage());
  }

  /** Create a BinTrieAccount from encoded basic data leaf. */
  public static BinTrieAccount fromEncodedBasicData(
      final PathBasedWorldView context,
      final Address address,
      final Bytes32 encodedBasicData,
      final Hash codeHash,
      final boolean mutable,
      final CodeCache codeCache) {

    final long nonce = SuffixTreeDecoder.decodeNonce(encodedBasicData);
    final UInt256 balance = SuffixTreeDecoder.decodeBalance(encodedBasicData);
    final int decodedCodeSize = SuffixTreeDecoder.decodeCodeSize(encodedBasicData);

    return new BinTrieAccount(
        context,
        address,
        address.addressHash(),
        nonce,
        Wei.of(balance),
        codeHash,
        decodedCodeSize,
        mutable,
        codeCache);
  }

  /** Serialize account to Binary Trie basic data leaf format. */
  public Bytes32 serializeBasicData() {
    Bytes32 result = Bytes32.ZERO;

    // Set version (1 byte)
    result = SuffixTreeEncoder.setVersionInValue(result, Bytes.of((byte) 0));

    // Set code size (3 bytes)
    final int codeSizeInt = (int) codeSize;
    result =
        SuffixTreeEncoder.setCodeSizeInValue(
            result,
            Bytes.of(
                (byte) ((codeSizeInt >> 16) & 0xFF),
                (byte) ((codeSizeInt >> 8) & 0xFF),
                (byte) (codeSizeInt & 0xFF)));

    // Set nonce (8 bytes)
    result = SuffixTreeEncoder.setNonceInValue(result, Bytes.ofUnsignedLong(nonce).slice(0, 8));

    // Set balance (16 bytes)
    final UInt256 balanceUint = UInt256.valueOf(balance.getAsBigInteger());
    result = SuffixTreeEncoder.setBalanceInValue(result, balanceUint.slice(16, 16));

    return result;
  }

  /**
   * BinTrie doesn't use a separate storage root per account. Storage is embedded directly in the
   * trie structure.
   */
  @Override
  public Hash getStorageRoot() {
    return Hash.EMPTY_TRIE_HASH;
  }

  @Override
  public boolean isStorageEmpty() {
    return updatedStorage.isEmpty();
  }

  @Override
  public NavigableMap<Bytes32, AccountStorageEntry> storageEntriesFrom(
      final Bytes32 startKeyHash, final int limit) {
    return new TreeMap<>();
  }

  @Override
  public void writeTo(final RLPOutput out) {
    out.startList();
    out.writeLongScalar(nonce);
    out.writeUInt256Scalar(balance);
    out.writeBytes(codeHash.getBytes());
    out.writeLongScalar(codeSize);
    out.endList();
  }

  public long getCodeSizeValue() {
    return codeSize;
  }

  @Override
  public Optional<Long> getCodeSize() {
    return Optional.of(codeSize);
  }

  public void setCodeSize(final long codeSize) {
    this.codeSize = codeSize;
  }

  @Override
  public void setCode(final Bytes byteCode) {
    super.setCode(byteCode);
    this.codeSize = byteCode != null ? byteCode.size() : 0;
  }

  @Override
  public Bytes serializeAccount() {
    return org.hyperledger.besu.ethereum.rlp.RLP.encode(this::writeTo);
  }

  public BinTrieAccount copyIn(final PathBasedWorldView context) {
    return new BinTrieAccount(this, context, true);
  }

  /**
   * Deserialize a BinTrieAccount from RLP-encoded bytes.
   *
   * @param context the world view context
   * @param address the account address
   * @param encoded the RLP-encoded account data
   * @param mutable whether the account is mutable
   * @return the deserialized account
   */
  public static BinTrieAccount fromRlp(
      final PathBasedWorldView context,
      final Address address,
      final Bytes encoded,
      final boolean mutable) {
    final org.hyperledger.besu.ethereum.rlp.RLPInput in =
        org.hyperledger.besu.ethereum.rlp.RLP.input(encoded);
    in.enterList();
    final long nonce = in.readLongScalar();
    final Wei balance = Wei.of(in.readUInt256Scalar());
    final Hash codeHash = Hash.wrap(in.readBytes32());
    final long codeSize = in.readLongScalar();
    in.leaveList();
    return new BinTrieAccount(
        context,
        address,
        address.addressHash(),
        nonce,
        balance,
        codeHash,
        codeSize,
        mutable,
        context.codeCache());
  }

  /**
   * Throws an exception if the two accounts represent different stored states.
   *
   * @param source The BinTrie account to compare
   * @param account The State Trie account to compare
   * @param context a description to be added to the thrown exceptions
   * @throws IllegalStateException if the stored values differ
   */
  public static void assertCloseEnoughForDiffing(
      final BinTrieAccount source, final AccountValue account, final String context) {
    if (source == null) {
      throw new IllegalStateException(context + ": source is null but target isn't");
    } else {
      if (source.nonce != account.getNonce()) {
        throw new IllegalStateException(context + ": nonces differ");
      }
      if (!Objects.equals(source.balance, account.getBalance())) {
        throw new IllegalStateException(context + ": balances differ");
      }
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final BinTrieAccount that = (BinTrieAccount) o;
    return nonce == that.nonce
        && codeSize == that.codeSize
        && Objects.equals(balance, that.balance)
        && Objects.equals(codeHash, that.codeHash);
  }

  @Override
  public int hashCode() {
    return Objects.hash(nonce, balance, codeHash, codeSize);
  }
}
