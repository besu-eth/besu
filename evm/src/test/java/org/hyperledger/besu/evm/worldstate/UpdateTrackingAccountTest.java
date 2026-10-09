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
package org.hyperledger.besu.evm.worldstate;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.AccountStorageEntry;
import org.hyperledger.besu.evm.internal.CodeCache;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class UpdateTrackingAccountTest {

  // PUSH1 4, JUMP, STOP, JUMPDEST, STOP
  private static final Bytes CODE = Bytes.fromHexString("0x600456005b00");
  // PUSH1 5, JUMP, STOP, STOP, JUMPDEST, STOP
  private static final Bytes OTHER_CODE = Bytes.fromHexString("0x60055600005b00");

  @Test
  void keepsTheAnalysedCodeOfTheWrappedAccountAfterTheCacheEvictedIt() {
    final MapCodeCache cache = new MapCodeCache();
    final Code analysed = new Code(CODE, Hash.hash(CODE));
    assertThat(analysed.isJumpDestInvalid(4)).isFalse();
    final StubAccount wrapped = new StubAccount(analysed, cache);

    final UpdateTrackingAccount<Account> tracker = new UpdateTrackingAccount<>(wrapped);
    final UpdateTrackingAccount<Account> nested = new UpdateTrackingAccount<>(tracker);
    cache.entries.clear();

    assertThat(tracker.getOrCreateCachedCode()).isSameAs(analysed);
    assertThat(nested.getOrCreateCachedCode()).isSameAs(analysed);
  }

  @Test
  void doesNotReadTheCodeOfTheWrappedAccountAgainWithoutACodeCache() {
    final StubAccount wrapped = new StubAccount(new Code(CODE, Hash.hash(CODE)), null);
    final UpdateTrackingAccount<Account> tracker = new UpdateTrackingAccount<>(wrapped);
    final int codeReads = wrapped.codeReads;

    final Code code = tracker.getOrCreateCachedCode();

    assertThat(code.getBytes()).isEqualTo(CODE);
    assertThat(code.getCodeHash()).isEqualTo(Hash.hash(CODE));
    assertThat(wrapped.codeReads).isEqualTo(codeReads);
    assertThat(wrapped.cachedCodeReads).isZero();
  }

  @Test
  void keepsTheUpdatedCodeAfterTheCacheEvictedIt() {
    final MapCodeCache cache = new MapCodeCache();
    final UpdateTrackingAccount<Account> tracker =
        new UpdateTrackingAccount<>(new StubAccount(new Code(CODE, Hash.hash(CODE)), cache));
    tracker.setCode(OTHER_CODE);
    final Code updated = tracker.getOrCreateCachedCode();

    cache.entries.clear();

    assertThat(tracker.getOrCreateCachedCode()).isSameAs(updated);
  }

  @Test
  void returnsTheUpdatedCodeWithoutACodeCache() {
    final UpdateTrackingAccount<Account> tracker =
        new UpdateTrackingAccount<>(new StubAccount(new Code(CODE, Hash.hash(CODE)), null));

    tracker.setCode(OTHER_CODE);
    final Code updated = tracker.getOrCreateCachedCode();

    assertThat(updated.getBytes()).isEqualTo(OTHER_CODE);
    assertThat(updated.getCodeHash()).isEqualTo(Hash.hash(OTHER_CODE));
    assertThat(updated.isJumpDestInvalid(5)).isFalse();
  }

  @Test
  void returnsTheCachedInstanceOfTheUpdatedCode() {
    final MapCodeCache cache = new MapCodeCache();
    final Code cached = new Code(OTHER_CODE, Hash.hash(OTHER_CODE));
    cache.put(Hash.hash(OTHER_CODE), cached);
    final UpdateTrackingAccount<Account> tracker =
        new UpdateTrackingAccount<>(new StubAccount(new Code(CODE, Hash.hash(CODE)), cache));

    tracker.setCode(OTHER_CODE);

    assertThat(tracker.getOrCreateCachedCode()).isSameAs(cached);
  }

  @Test
  void returnsTheUpdatedCode() {
    final MapCodeCache cache = new MapCodeCache();
    final UpdateTrackingAccount<Account> tracker =
        new UpdateTrackingAccount<>(new StubAccount(new Code(CODE, Hash.hash(CODE)), cache));
    tracker.getOrCreateCachedCode();

    tracker.setCode(OTHER_CODE);
    final Code updated = tracker.getOrCreateCachedCode();

    assertThat(updated.getBytes()).isEqualTo(OTHER_CODE);
    assertThat(updated.getCodeHash()).isEqualTo(Hash.hash(OTHER_CODE));
    assertThat(updated.isJumpDestInvalid(4)).isTrue();
    assertThat(updated.isJumpDestInvalid(5)).isFalse();
    assertThat(cache.entries).containsEntry(Hash.hash(OTHER_CODE), updated);

    tracker.setCode(Bytes.EMPTY);
    assertThat(tracker.getOrCreateCachedCode().getSize()).isZero();
  }

  @Test
  void keepsItsSnapshotWhenTheWrappedAccountChangesItsCode() {
    final UpdateTrackingAccount<Account> parent =
        new UpdateTrackingAccount<>(
            new StubAccount(new Code(CODE, Hash.hash(CODE)), new MapCodeCache()));
    final UpdateTrackingAccount<Account> child = new UpdateTrackingAccount<>(parent);

    parent.setCode(OTHER_CODE);

    assertThat(child.getCode()).isEqualTo(CODE);
    assertThat(child.getOrCreateCachedCode().getBytes()).isEqualTo(CODE);
    assertThat(parent.getOrCreateCachedCode().getBytes()).isEqualTo(OTHER_CODE);
  }

  private static final class MapCodeCache implements CodeCache {
    private final Map<Hash, Code> entries = new HashMap<>();

    @Override
    public Code getIfPresent(final Hash codeHash) {
      return entries.get(codeHash);
    }

    @Override
    public void put(final Hash codeHash, final Code code) {
      entries.put(codeHash, code);
    }
  }

  private static final class StubAccount implements Account {
    private final Code code;
    private final CodeCache codeCache;
    private int codeReads;
    private int cachedCodeReads;

    private StubAccount(final Code code, final CodeCache codeCache) {
      this.code = code;
      this.codeCache = codeCache;
    }

    @Override
    public Address getAddress() {
      return Address.fromHexString("0x1234");
    }

    @Override
    public Hash getAddressHash() {
      return getAddress().addressHash();
    }

    @Override
    public long getNonce() {
      return 1;
    }

    @Override
    public Wei getBalance() {
      return Wei.ZERO;
    }

    @Override
    public Bytes getCode() {
      codeReads++;
      return code.getBytes();
    }

    @Override
    public Hash getCodeHash() {
      return code.getCodeHash();
    }

    @Override
    public Code getOrCreateCachedCode() {
      cachedCodeReads++;
      return code;
    }

    @Override
    public CodeCache getCodeCache() {
      return codeCache;
    }

    @Override
    public UInt256 getStorageValue(final UInt256 key) {
      return UInt256.ZERO;
    }

    @Override
    public UInt256 getOriginalStorageValue(final UInt256 key) {
      return UInt256.ZERO;
    }

    @Override
    public NavigableMap<Bytes32, AccountStorageEntry> storageEntriesFrom(
        final Bytes32 startKeyHash, final int limit) {
      return new TreeMap<>();
    }
  }
}
