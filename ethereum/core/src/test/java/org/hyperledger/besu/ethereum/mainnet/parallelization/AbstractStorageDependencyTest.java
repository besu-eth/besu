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
package org.hyperledger.besu.ethereum.mainnet.parallelization;

import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_1;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_1_KEYPAIR;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_2;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_2_KEYPAIR;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.CONTRACT_ADDRESS;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.MINING_BENEFICIARY_KEYPAIR;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.PARALLEL_TEST_CONTRACT;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;

import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for storage dependency scenarios that validate collision detection. These tests
 * MUST fail if collision detection is disabled because transactions would see stale state.
 *
 * <p>Uses the ParallelTestStorage contract at 0x...eeeee which has incrementSlot1() that reads slot
 * 0 and writes slot0 + 1. Initial slot 0 value = 10.
 */
public abstract class AbstractStorageDependencyTest
    extends AbstractParallelBlockProcessorIntegrationTest {

  private final Address parallelContract = Address.fromHexStringStrict(PARALLEL_TEST_CONTRACT);

  @Test
  @DisplayName("setSlot then increment from different sender must detect collision")
  void setSlotThenIncrementFromDifferentSender() {
    // Tx1: sender A sets slot 0 = 100
    // Tx2: sender B increments slot 0 (reads slot 0, writes slot 0 + 1)
    // Sequential result: slot 0 = 101 (set to 100, then 100 + 1)
    // Without collision detection: slot 0 = 11 (base=10, pre-computed 10+1, overwrites 100)
    final Transaction txSet =
        createContractCallTransaction(
            0, parallelContract, "setSlot1", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(100));
    final Transaction txIncrement =
        createContractCallTransaction(
            0, parallelContract, "incrementSlot1", ACCOUNT_GENESIS_2_KEYPAIR, Optional.empty());

    final ComparisonResult result = executeAndCompare(Wei.of(5), txSet, txIncrement);

    assertContractStorage(result.seqWorldState(), parallelContract, 0, 101);
    assertContractStorageMatches(
        result.seqWorldState(), result.parWorldState(), parallelContract, 0);
  }

  @Test
  @DisplayName("Two increments from different senders must detect collision")
  void twoIncrementsFromDifferentSenders() {
    // Tx1: sender A increments slot 0 (10 → 11)
    // Tx2: sender B increments slot 0 (11 → 12)
    // Sequential result: slot 0 = 12
    // Without collision detection: both see base=10, both write 11, last import wins → 11
    final Transaction txInc1 =
        createContractCallTransaction(
            0, parallelContract, "incrementSlot1", ACCOUNT_GENESIS_1_KEYPAIR, Optional.empty());
    final Transaction txInc2 =
        createContractCallTransaction(
            0, parallelContract, "incrementSlot1", ACCOUNT_GENESIS_2_KEYPAIR, Optional.empty());

    final ComparisonResult result = executeAndCompare(Wei.of(5), txInc1, txInc2);

    assertContractStorage(result.seqWorldState(), parallelContract, 0, 12);
    assertContractStorageMatches(
        result.seqWorldState(), result.parWorldState(), parallelContract, 0);
  }

  @Test
  @DisplayName("Increment then set from different sender must detect collision")
  void incrementThenSetFromDifferentSender() {
    // Tx1: sender A increments slot 0 (10 → 11)
    // Tx2: sender B sets slot 0 = 500
    // Sequential result: slot 0 = 500
    final Transaction txInc =
        createContractCallTransaction(
            0, parallelContract, "incrementSlot1", ACCOUNT_GENESIS_1_KEYPAIR, Optional.empty());
    final Transaction txSet =
        createContractCallTransaction(
            0, parallelContract, "setSlot1", ACCOUNT_GENESIS_2_KEYPAIR, Optional.of(500));

    final ComparisonResult result = executeAndCompare(Wei.of(5), txInc, txSet);

    assertContractStorage(result.seqWorldState(), parallelContract, 0, 500);
    assertContractStorageMatches(
        result.seqWorldState(), result.parWorldState(), parallelContract, 0);
    assertAccountsMatch(
        result.seqWorldState(),
        result.parWorldState(),
        Address.fromHexStringStrict(ACCOUNT_GENESIS_1));
    assertAccountsMatch(
        result.seqWorldState(),
        result.parWorldState(),
        Address.fromHexStringStrict(ACCOUNT_GENESIS_2));
  }

  @Test
  @DisplayName("Set slot + increment + set another slot must detect collision on shared slot")
  void setIncrementAndWriteOtherSlot() {
    // Tx1: sender A sets slot 0 = 200 on ParallelTestStorage
    // Tx2: sender B increments slot 0 (should read 200, write 201)
    // Tx3: sender A sets slot 1 = 42 on the other contract (no conflict)
    final Transaction txSet =
        createContractCallTransaction(
            0, parallelContract, "setSlot1", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(200));
    final Transaction txIncrement =
        createContractCallTransaction(
            0, parallelContract, "incrementSlot1", ACCOUNT_GENESIS_2_KEYPAIR, Optional.empty());
    final Address contractAddr = Address.fromHexStringStrict(CONTRACT_ADDRESS);
    final Transaction txOther =
        createContractCallTransaction(
            1, contractAddr, "setSlot2", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(42));

    final ComparisonResult result = executeAndCompare(Wei.of(5), txSet, txIncrement, txOther);

    assertContractStorage(result.seqWorldState(), parallelContract, 0, 201);
    assertContractStorageMatches(
        result.seqWorldState(), result.parWorldState(), parallelContract, 0);
    assertContractStorage(result.seqWorldState(), contractAddr, 1, 42);
    assertContractStorageMatches(result.seqWorldState(), result.parWorldState(), contractAddr, 1);
  }

  @Test
  @DisplayName("Three-way storage dependency chain produces correct sequential result")
  void threeWayStorageDependencyChain() {
    // Tx1: set slot 0 = 50
    // Tx2: increment slot 0 (should read 50, write 51)
    // Tx3: increment slot 0 (should read 51, write 52)
    final Transaction txSet =
        createContractCallTransaction(
            0, parallelContract, "setSlot1", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(50));
    final Transaction txInc1 =
        createContractCallTransaction(
            0, parallelContract, "incrementSlot1", ACCOUNT_GENESIS_2_KEYPAIR, Optional.empty());
    final Transaction txInc2 =
        createContractCallTransaction(
            0, parallelContract, "incrementSlot1", MINING_BENEFICIARY_KEYPAIR, Optional.empty());

    final ComparisonResult result = executeAndCompare(Wei.of(5), txSet, txInc1, txInc2);

    assertContractStorage(result.seqWorldState(), parallelContract, 0, 52);
    assertContractStorageMatches(
        result.seqWorldState(), result.parWorldState(), parallelContract, 0);
  }
}
