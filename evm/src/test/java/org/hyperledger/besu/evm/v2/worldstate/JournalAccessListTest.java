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
package org.hyperledger.besu.evm.v2.worldstate;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.AccessListEntry;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.EvmSpecVersion;
import org.hyperledger.besu.evm.fluent.EVMExecutor;
import org.hyperledger.besu.evm.fluent.EvmSpec;
import org.hyperledger.besu.evm.fluent.SimpleWorld;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.tracing.AccessListOperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.math.BigInteger;
import java.util.List;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

/** The access list a transaction on EVM v2 builds from its journal, against EVM v1's. */
class JournalAccessListTest {

  private static final Address CALLER = Address.fromHexString("0x1000");
  private static final Address REVERTING = Address.fromHexString("0x2000");
  private static final Address STOPPING = Address.fromHexString("0x3000");

  // SLOAD 1, SSTORE 2, then call the reverting and the stopping contract
  private static final Bytes CALLER_CODE =
      Bytes.fromHexString(
          "0x6001545060036002556000600060006000600073"
              + REVERTING.getBytes().toUnprefixedHexString()
              + "5af1506000600060006000600073"
              + STOPPING.getBytes().toUnprefixedHexString()
              + "5af15000");
  // SLOAD 5 and 6, then revert, which drops the warm-ups
  private static final Bytes REVERTING_CODE = Bytes.fromHexString("0x600554506006545060006000fd");
  // SLOAD 7
  private static final Bytes STOPPING_CODE = Bytes.fromHexString("0x6007545000");

  @Test
  void journalAccessListMatchesEvmV1() {
    final List<AccessListEntry> v1 = accessList(false, WorldUpdater::updater);
    final List<AccessListEntry> v2 =
        accessList(true, world -> new JournaledWorldUpdaterV2(world.updater()));

    assertThat(v1).extracting(AccessListEntry::address).containsExactlyInAnyOrder(CALLER, STOPPING);
    assertThat(v2).isEqualTo(v1);
  }

  @Test
  void accessListIncludesPrewarmedSlots() {
    final List<AccessListEntry> v2 =
        accessList(
            true,
            world -> {
              final JournaledWorldUpdaterV2 journal = new JournaledWorldUpdaterV2(world.updater());
              journal.warmUpStorage(REVERTING, Bytes32.fromHexStringLenient("0x09"));
              return journal;
            });

    assertThat(v2)
        .filteredOn(entry -> entry.address().equals(REVERTING))
        .flatExtracting(AccessListEntry::storageKeys)
        .containsExactly(Bytes32.fromHexStringLenient("0x09"));
  }

  private static List<AccessListEntry> accessList(
      final boolean evmV2, final Function<SimpleWorld, WorldUpdater> updater) {
    final SimpleWorld world = new SimpleWorld();
    world.createAccount(CALLER, 1, Wei.ZERO).setCode(CALLER_CODE);
    world.createAccount(REVERTING, 1, Wei.ZERO).setCode(REVERTING_CODE);
    world.createAccount(STOPPING, 1, Wei.ZERO).setCode(STOPPING_CODE);
    final AccessListOperationTracer tracer = AccessListOperationTracer.create();
    new EVMExecutor(
            EvmSpec.evmSpec(
                EvmSpecVersion.CANCUN,
                BigInteger.ONE,
                new EvmConfiguration(
                    32_000L, EvmConfiguration.WorldUpdaterMode.STACKED, true, evmV2)))
        .worldUpdater(updater.apply(world))
        .tracer(tracer)
        .gas(1_000_000L)
        .sender(Address.fromHexString("0x4000"))
        .receiver(CALLER)
        .contract(CALLER)
        .code(CALLER_CODE)
        .execute();
    return tracer.getAccessList();
  }
}
