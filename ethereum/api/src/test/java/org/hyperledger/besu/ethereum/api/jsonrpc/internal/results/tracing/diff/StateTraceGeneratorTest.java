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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.diff;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.tracing.TraceFrame;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StateTraceGeneratorTest {

  private StateTraceGenerator generator;

  @BeforeEach
  void setup() {
    generator = new StateTraceGenerator();
  }

  @Test
  void shouldReturnEmptyStateDiff_whenTraceHasNoFrames() {
    TransactionTrace tx = mock(TransactionTrace.class);
    when(tx.getTraceFrames()).thenReturn(List.of());

    assertThat(generator.generateStateDiff(tx)).isEmpty();
  }

  @Test
  void shouldIncludeChangedAccounts_whenGeneratingStateDiff() {
    Address A = Address.fromHexString("0xaaa1");

    Account pre = mockAccount(A, 10, 1, Map.of(UInt256.ZERO, UInt256.valueOf(5)));
    MutableAccount post = mockAccount(A, 15, 1, Map.of(UInt256.ZERO, UInt256.valueOf(7)));

    WorldUpdater prev = mock(WorldUpdater.class);
    when(prev.get(A)).thenReturn(pre);

    WorldUpdater tx = mockTxUpdaterWith(prev, List.of(post), List.of());
    TraceFrame f = mockFrame(mockWorldUpdater(tx));

    StateDiffTrace diff = generator.generateStateDiff(trace(f)).findFirst().orElseThrow();

    assertThat(diff).containsKey(A.getBytes().toHexString());
    AccountDiff a = diff.get(A.getBytes().toHexString());
    assertThat(a.getBalance()).isNotNull();
    assertThat(a.getStorage()).containsKey(UInt256.ZERO.toHexString());
  }

  @Test
  void shouldSkipUnchangedAccounts_whenGeneratingStateDiff() {
    Address A = Address.fromHexString("0xabc1");

    Account pre = mockAccount(A, 10, 1, Map.of(UInt256.ZERO, UInt256.valueOf(5)));
    MutableAccount post = mockAccount(A, 10, 1, Map.of(UInt256.ZERO, UInt256.valueOf(5)));

    WorldUpdater prev = mock(WorldUpdater.class);
    when(prev.get(A)).thenReturn(pre);

    WorldUpdater tx = mockTxUpdaterWith(prev, List.of(post), List.of());
    TraceFrame f = mockFrame(mockWorldUpdater(tx));

    StateDiffTrace diff = generator.generateStateDiff(trace(f)).findFirst().orElseThrow();

    assertThat(diff).isEmpty();
  }

  @Test
  void shouldIncludeOnlyNonZeroSlots_whenDiffMode() {
    Address A = Address.fromHexString("0xdead");

    MutableAccount post =
        mockAccount(A, 0, 0, Map.of(UInt256.ZERO, UInt256.ZERO, UInt256.ONE, UInt256.valueOf(999)));

    WorldUpdater prev = mock(WorldUpdater.class);
    when(prev.get(A)).thenReturn(null);

    WorldUpdater tx = mockTxUpdaterWith(prev, List.of(post), List.of());
    TraceFrame f = mockFrame(mockWorldUpdater(tx));

    StateDiffTrace diff = generator.generateStateDiff(trace(f)).findFirst().orElseThrow();

    AccountDiff a = diff.get(A.getBytes().toHexString());
    assertThat(a.getStorage()).containsKey(UInt256.ONE.toHexString());
    assertThat(a.getStorage()).doesNotContainKey(UInt256.ZERO.toHexString());
  }

  @Test
  void shouldRepresentDeletedAccountsWithFromOnly_whenAccountWasDeleted() {
    Address A = Address.fromHexString("0xd0d0");

    Account pre = mockAccount(A, 100, 5, Map.of());

    WorldUpdater prev = mock(WorldUpdater.class);
    when(prev.get(A)).thenReturn(pre);

    WorldUpdater tx = mockTxUpdaterWith(prev, List.of(), List.of(A));
    TraceFrame f = mockFrame(mockWorldUpdater(tx));

    StateDiffTrace diff = generator.generateStateDiff(trace(f)).findFirst().orElseThrow();

    AccountDiff a = diff.get(A.getBytes().toHexString());
    assertThat(a.getBalance().getFrom()).contains("0x64");
    assertThat(a.getBalance().getTo()).isEmpty();
  }

  private TraceFrame mockFrame(final WorldUpdater updater) {
    TraceFrame f = mock(TraceFrame.class);
    when(f.getWorldUpdater()).thenReturn(updater);
    return f;
  }

  private TransactionTrace trace(final TraceFrame... frames) {
    TransactionTrace tx = mock(TransactionTrace.class);
    when(tx.getTraceFrames()).thenReturn(List.of(frames));
    return tx;
  }

  private MutableAccount mockAccount(
      final Address addr,
      final long balance,
      final long nonce,
      final Map<UInt256, UInt256> updatedStorage) {

    MutableAccount a = mock(MutableAccount.class);
    when(a.getAddress()).thenReturn(addr);
    when(a.getBalance()).thenReturn(Wei.of(balance));
    when(a.getNonce()).thenReturn(nonce);
    when(a.getCode()).thenReturn(Bytes.EMPTY);
    when(a.getCodeHash()).thenReturn(Hash.ZERO);
    when(a.getUpdatedStorage()).thenReturn(updatedStorage);
    updatedStorage.forEach((k, v) -> when(a.getStorageValue(k)).thenReturn(v));

    return a;
  }

  private WorldUpdater mockTxUpdaterWith(
      final WorldUpdater parent, final List<MutableAccount> touched, final List<Address> deleted) {
    WorldUpdater u = mock(WorldUpdater.class);
    when(u.parentUpdater()).thenReturn(Optional.of(parent));
    doReturn(touched).when(u).getTouchedAccounts();
    when(u.getDeletedAccountAddresses()).thenReturn(deleted);
    return u;
  }

  private WorldUpdater mockWorldUpdater(final WorldUpdater leaf) {
    WorldUpdater mid = mock(WorldUpdater.class);
    WorldUpdater root = mock(WorldUpdater.class);
    when(mid.parentUpdater()).thenReturn(Optional.of(root));
    when(root.parentUpdater()).thenReturn(Optional.of(leaf));
    return mid;
  }
}
