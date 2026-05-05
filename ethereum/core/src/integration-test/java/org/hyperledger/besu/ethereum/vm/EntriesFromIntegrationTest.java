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
package org.hyperledger.besu.ethereum.vm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryWorldStateArchive;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.evm.account.AccountStorageEntry;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

public class EntriesFromIntegrationTest {

  @Test
  @SuppressWarnings("MathAbsoluteRandom")
  public void shouldCollectStateEntries() {
    final MutableWorldState worldState = createInMemoryWorldStateArchive().getWorldState();
    final WorldUpdater updater = worldState.updater();
    MutableAccount account = updater.getOrCreate(Address.fromHexString("0x56"));
    final Map<Bytes32, AccountStorageEntry> expectedValues = new TreeMap<>();
    final int nodeCount = 100_000;
    final Random random = new Random(42989428249L);

    // Create some storage entries in the committed, underlying account.
    for (int i = 0; i <= nodeCount; i++) {
      addExpectedValue(
          account,
          expectedValues,
          UInt256.valueOf(random.nextLong(Long.MAX_VALUE)),
          UInt256.valueOf(i * 10 + 1L));
    }
    updater.commit();

    // Add some changes on top that AbstractWorldUpdater.UpdateTrackingAccount will have to merge.
    account = worldState.updater().getOrCreate(Address.fromHexString("0x56"));
    for (int i = 0; i <= nodeCount; i++) {
      addExpectedValue(
          account,
          expectedValues,
          UInt256.valueOf(random.nextLong(Long.MAX_VALUE)),
          UInt256.valueOf(i * 10 + 1L));
    }

    final Map<Bytes32, AccountStorageEntry> values =
        account.storageEntriesFrom(Bytes32.ZERO, Integer.MAX_VALUE);
    assertThat(values).isEqualTo(expectedValues);
  }

  private void addExpectedValue(
      final MutableAccount account,
      final Map<Bytes32, AccountStorageEntry> expectedValues,
      final UInt256 key,
      final UInt256 value) {
    account.setStorageValue(key, value);
    expectedValues.put(
        Bytes32.wrap(Hash.hash(key).getBytes()), AccountStorageEntry.forKeyAndValue(key, value));
  }
}
