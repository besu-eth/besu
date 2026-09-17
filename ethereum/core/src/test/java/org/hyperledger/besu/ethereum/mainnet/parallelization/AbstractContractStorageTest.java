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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;

import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Integration tests for contract storage operations. */
public abstract class AbstractContractStorageTest
    extends AbstractParallelBlockProcessorIntegrationTest {

  private final Address contractAddr = Address.fromHexStringStrict(CONTRACT_ADDRESS);

  @Test
  @DisplayName("Writing different storage slots from the same sender produces matching state")
  void writeMultipleSlotsSameSender() {
    final Transaction txSetSlot1 =
        createContractCallTransaction(
            0, contractAddr, "setSlot1", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(100));
    final Transaction txSetSlot2 =
        createContractCallTransaction(
            1, contractAddr, "setSlot2", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(200));
    final Transaction txSetSlot3 =
        createContractCallTransaction(
            2, contractAddr, "setSlot3", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(300));

    final ComparisonResult result =
        executeAndCompare(Wei.of(5), txSetSlot1, txSetSlot2, txSetSlot3);

    assertContractStorage(result.seqWorldState(), contractAddr, 0, 100);
    assertContractStorage(result.seqWorldState(), contractAddr, 1, 200);
    assertContractStorage(result.seqWorldState(), contractAddr, 2, 300);

    assertContractStorageMatches(result.seqWorldState(), result.parWorldState(), contractAddr, 0);
    assertContractStorageMatches(result.seqWorldState(), result.parWorldState(), contractAddr, 1);
    assertContractStorageMatches(result.seqWorldState(), result.parWorldState(), contractAddr, 2);
    assertAccountsMatch(
        result.seqWorldState(),
        result.parWorldState(),
        Address.fromHexStringStrict(ACCOUNT_GENESIS_1));
  }

  @Test
  @DisplayName("Writing then reading the same storage slot produces matching state")
  void writeThenReadSameSlot() {
    final Transaction txSetSlot1 =
        createContractCallTransaction(
            0, contractAddr, "setSlot1", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(999));
    final Transaction txGetSlot1 =
        createContractCallTransaction(
            0, contractAddr, "getSlot1", ACCOUNT_GENESIS_2_KEYPAIR, Optional.empty());

    final ComparisonResult result = executeAndCompare(Wei.of(5), txSetSlot1, txGetSlot1);

    assertContractStorage(result.seqWorldState(), contractAddr, 0, 999);
    assertContractStorageMatches(result.seqWorldState(), result.parWorldState(), contractAddr, 0);
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
  @DisplayName("Reading then writing the same storage slot produces matching state")
  void readThenWriteSameSlot() {
    final Transaction txGetSlot1 =
        createContractCallTransaction(
            0, contractAddr, "getSlot1", ACCOUNT_GENESIS_1_KEYPAIR, Optional.empty());
    final Transaction txSetSlot1 =
        createContractCallTransaction(
            0, contractAddr, "setSlot1", ACCOUNT_GENESIS_2_KEYPAIR, Optional.of(777));

    final ComparisonResult result = executeAndCompare(Wei.of(5), txGetSlot1, txSetSlot1);

    assertContractStorage(result.seqWorldState(), contractAddr, 0, 777);
    assertContractStorageMatches(result.seqWorldState(), result.parWorldState(), contractAddr, 0);
  }

  @Test
  @DisplayName("Write-read-write across two senders produces matching state")
  void writeReadWriteAcrossSenders() {
    final Transaction txSetSlot1 =
        createContractCallTransaction(
            0, contractAddr, "setSlot1", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(100));
    final Transaction txGetSlot1 =
        createContractCallTransaction(
            0, contractAddr, "getSlot1", ACCOUNT_GENESIS_2_KEYPAIR, Optional.empty());
    final Transaction txSetSlot2 =
        createContractCallTransaction(
            1, contractAddr, "setSlot2", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(200));
    final Transaction txSetSlot3 =
        createContractCallTransaction(
            2, contractAddr, "setSlot3", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(300));

    final ComparisonResult result =
        executeAndCompare(Wei.of(5), txSetSlot1, txGetSlot1, txSetSlot2, txSetSlot3);

    assertContractStorage(result.seqWorldState(), contractAddr, 0, 100);
    assertContractStorage(result.seqWorldState(), contractAddr, 1, 200);
    assertContractStorage(result.seqWorldState(), contractAddr, 2, 300);

    for (int slot = 0; slot <= 2; slot++) {
      assertContractStorageMatches(
          result.seqWorldState(), result.parWorldState(), contractAddr, slot);
    }
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
  @DisplayName("Storage reads from different senders produce matching state")
  void readFromDifferentSenders() {
    final Transaction txGetSlot1A =
        createContractCallTransaction(
            0, contractAddr, "getSlot1", ACCOUNT_GENESIS_1_KEYPAIR, Optional.empty());
    final Transaction txGetSlot1B =
        createContractCallTransaction(
            0, contractAddr, "getSlot1", ACCOUNT_GENESIS_2_KEYPAIR, Optional.empty());

    final ComparisonResult result = executeAndCompare(Wei.of(5), txGetSlot1A, txGetSlot1B);

    assertContractStorageMatches(result.seqWorldState(), result.parWorldState(), contractAddr, 0);
    assertAccountsMatch(
        result.seqWorldState(),
        result.parWorldState(),
        Address.fromHexStringStrict(ACCOUNT_GENESIS_1));
    assertAccountsMatch(
        result.seqWorldState(),
        result.parWorldState(),
        Address.fromHexStringStrict(ACCOUNT_GENESIS_2));
  }
}
