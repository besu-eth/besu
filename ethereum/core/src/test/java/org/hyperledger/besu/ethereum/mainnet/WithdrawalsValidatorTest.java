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
package org.hyperledger.besu.ethereum.mainnet;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.GWei;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.Withdrawal;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.units.bigints.UInt64;
import org.junit.jupiter.api.Test;

public class WithdrawalsValidatorTest {
  private final BlockDataGenerator blockDataGenerator = new BlockDataGenerator();

  @Test
  public void validateProhibitedWithdrawals() {
    assertThat(
            new WithdrawalsValidator.ProhibitedWithdrawals().validateWithdrawals(Optional.empty()))
        .isTrue();
  }

  @Test
  public void validateProhibitedWithdrawalsRoot() {
    final Block block = blockDataGenerator.block();
    assertThat(new WithdrawalsValidator.ProhibitedWithdrawals().validateWithdrawalsRoot(block))
        .isTrue();
  }

  @Test
  public void invalidateProhibitedWithdrawals() {
    assertThat(
            new WithdrawalsValidator.ProhibitedWithdrawals()
                .validateWithdrawals(Optional.of(emptyList())))
        .isFalse();
  }

  @Test
  public void invalidateProhibitedWithdrawalsRoot() {
    final BlockDataGenerator.BlockOptions blockOptions =
        BlockDataGenerator.BlockOptions.create().setWithdrawalsRoot(Hash.EMPTY_LIST_HASH);
    final Block block = blockDataGenerator.block(blockOptions);
    assertThat(new WithdrawalsValidator.ProhibitedWithdrawals().validateWithdrawalsRoot(block))
        .isFalse();
  }

  @Test
  public void validateAllowedWithdrawals() {
    assertThat(
            new WithdrawalsValidator.AllowedWithdrawals()
                .validateWithdrawals(Optional.of(emptyList())))
        .isTrue();
  }

  @Test
  public void validateAllowedWithdrawalsRoot() {
    final BlockDataGenerator.BlockOptions blockOptions =
        BlockDataGenerator.BlockOptions.create()
            .setWithdrawals(Optional.of(Collections.emptyList()))
            .setWithdrawalsRoot(Hash.EMPTY_TRIE_HASH);
    final Block block = blockDataGenerator.block(blockOptions);
    assertThat(new WithdrawalsValidator.AllowedWithdrawals().validateWithdrawalsRoot(block))
        .isTrue();
  }

  @Test
  public void invalidateAllowedWithdrawals() {
    assertThat(new WithdrawalsValidator.AllowedWithdrawals().validateWithdrawals(Optional.empty()))
        .isFalse();
  }

  @Test
  public void invalidateAllowedWithdrawalsRoot() {
    final BlockDataGenerator.BlockOptions blockOptions =
        BlockDataGenerator.BlockOptions.create()
            .setWithdrawals(Optional.of(Collections.emptyList()))
            .setWithdrawalsRoot(Hash.ZERO); // this is invalid it should be empty trie hash
    final Block block = blockDataGenerator.block(blockOptions);
    assertThat(new WithdrawalsValidator.AllowedWithdrawals().validateWithdrawalsRoot(block))
        .isFalse();
  }

  @Test
  public void validateNotApplicableWithdrawalsWhenPresent() {
    assertThat(
            new WithdrawalsValidator.NotApplicableWithdrawals()
                .validateWithdrawals(Optional.of(emptyList())))
        .isTrue();
  }

  @Test
  public void validateNotApplicableWithdrawalsWhenAbsent() {
    assertThat(
            new WithdrawalsValidator.NotApplicableWithdrawals()
                .validateWithdrawals(Optional.empty()))
        .isTrue();
  }

  @Test
  public void validateNotApplicableWithdrawalsRootWhenPresent() {
    final BlockDataGenerator.BlockOptions blockOptions =
        BlockDataGenerator.BlockOptions.create()
            .setWithdrawals(Optional.of(Collections.emptyList()))
            .setWithdrawalsRoot(Hash.EMPTY_TRIE_HASH);
    final Block block = blockDataGenerator.block(blockOptions);
    assertThat(new WithdrawalsValidator.NotApplicableWithdrawals().validateWithdrawalsRoot(block))
        .isTrue();
  }

  @Test
  public void validateNotApplicableWithdrawalsRootWhenAbsent() {
    final Block block = blockDataGenerator.block();
    assertThat(new WithdrawalsValidator.NotApplicableWithdrawals().validateWithdrawalsRoot(block))
        .isTrue();
  }

  // Byzantine-proposer regression tests for GHSA-p4h2-gvh4-pv6j:
  // a proposer injecting non-empty withdrawals must be rejected before execution.

  @Test
  public void rejectsByzantineNonEmptyWithdrawalList() {
    final List<Withdrawal> injected =
        List.of(
            new Withdrawal(
                UInt64.valueOf(9001),
                UInt64.valueOf(313),
                Address.fromHexString("0x000000000000000000000000000000000000c0de"),
                GWei.of(11)));
    assertThat(
            new WithdrawalsValidator.NotApplicableWithdrawals()
                .validateWithdrawals(Optional.of(injected)))
        .isFalse();
  }

  @Test
  public void rejectsByzantineWithdrawalsRootMismatch() {
    // Body has empty list but header declares a wrong root (all-zero, as in the PoC).
    final BlockDataGenerator.BlockOptions blockOptions =
        BlockDataGenerator.BlockOptions.create()
            .setWithdrawals(Optional.of(Collections.emptyList()))
            .setWithdrawalsRoot(Hash.ZERO);
    final Block block = blockDataGenerator.block(blockOptions);
    assertThat(new WithdrawalsValidator.NotApplicableWithdrawals().validateWithdrawalsRoot(block))
        .isFalse();
  }

  @Test
  public void rejectsByzantineNonEmptyWithdrawalsWithCorrectRoot() {
    // Body has one real withdrawal and the header root correctly reflects it — still rejected
    // because the list is non-empty.
    final List<Withdrawal> injected =
        List.of(
            new Withdrawal(
                UInt64.valueOf(9001),
                UInt64.valueOf(313),
                Address.fromHexString("0x000000000000000000000000000000000000c0de"),
                GWei.of(11)));
    final BlockDataGenerator.BlockOptions blockOptions =
        BlockDataGenerator.BlockOptions.create().setWithdrawals(Optional.of(injected));
    final Block block = blockDataGenerator.block(blockOptions);
    assertThat(
            new WithdrawalsValidator.NotApplicableWithdrawals()
                .validateWithdrawals(block.getBody().getWithdrawals()))
        .isFalse();
  }

  @Test
  public void rejectsByzantineDuplicateWithdrawals() {
    final Withdrawal w =
        new Withdrawal(
            UInt64.valueOf(7),
            UInt64.valueOf(1),
            Address.fromHexString("0x000000000000000000000000000000000000dead"),
            GWei.of(7));
    assertThat(
            new WithdrawalsValidator.NotApplicableWithdrawals()
                .validateWithdrawals(Optional.of(List.of(w, w))))
        .isFalse();
  }
}
