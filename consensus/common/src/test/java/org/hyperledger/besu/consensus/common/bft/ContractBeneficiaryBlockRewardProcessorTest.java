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
package org.hyperledger.besu.consensus.common.bft;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.mainnet.BlockRewardProcessor;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Runs the processor against real EVM bytecode on an in-memory world state. */
public class ContractBeneficiaryBlockRewardProcessorTest {

  private static final Address CONTRACT =
      Address.fromHexString("0x000000000000000000000000000000000000c0de");
  private static final Address MINING_BENEFICIARY =
      Address.fromHexString("0x0000000000000000000000000000000000000fee");
  private static final Address A =
      Address.fromHexString("0x00000000000000000000000000000000000000a1");
  private static final Address B =
      Address.fromHexString("0x00000000000000000000000000000000000000b2");
  private static final Address C =
      Address.fromHexString("0x00000000000000000000000000000000000000c3");

  private static final String REVERT = "60006000fd";
  private static final String INVALID_OPCODE = "fe";
  private static final String OUT_OF_GAS_LOOP = "5b600056";
  private static final String WRITE_STORAGE = "6001600055" + "00";

  private final BlockHeader header =
      new BlockHeaderTestFixture().number(30_000_000L).timestamp(1_900_000_000L).buildHeader();

  private MainnetTransactionProcessor transactionProcessor;
  private MutableWorldState worldState;

  @BeforeEach
  public void setUp() {
    transactionProcessor =
        ProtocolScheduleFixture.MAINNET.getByBlockHeader(header).getTransactionProcessor();
    worldState = InMemoryKeyValueStorageProvider.createInMemoryWorldState();
  }

  @Test
  public void splitsTheRewardByWeight() {
    deploy(
        returning(
            BeneficiaryContractDecoderTest.encode(List.of(A, B, C), weights(5000, 3000, 2000))));

    reward(1000);

    assertThat(balance(A)).isEqualTo(Wei.of(500));
    assertThat(balance(B)).isEqualTo(Wei.of(300));
    assertThat(balance(C)).isEqualTo(Wei.of(200));
    assertThat(balance(MINING_BENEFICIARY)).isEqualTo(Wei.ZERO);
  }

  @Test
  public void roundingRemainderGoesToTheFirstBeneficiary() {
    deploy(
        returning(
            BeneficiaryContractDecoderTest.encode(List.of(A, B, C), weights(3333, 3333, 3334))));

    reward(1001);

    // floor(1001 * 3333 / 10000) = 333, floor(1001 * 3334 / 10000) = 333, remainder 2 to A
    assertThat(balance(A)).isEqualTo(Wei.of(335));
    assertThat(balance(B)).isEqualTo(Wei.of(333));
    assertThat(balance(C)).isEqualTo(Wei.of(333));
  }

  @Test
  public void totalPaidAlwaysEqualsTheReward() {
    deploy(
        returning(BeneficiaryContractDecoderTest.encode(List.of(A, B, C), weights(1, 4999, 5000))));

    reward(1_000_000_007L);

    assertThat(balance(A).add(balance(B)).add(balance(C))).isEqualTo(Wei.of(1_000_000_007L));
  }

  @Test
  public void duplicateAddressesAreCreditedAdditively() {
    deploy(
        returning(
            BeneficiaryContractDecoderTest.encode(List.of(A, B, A), weights(2500, 5000, 2500))));

    reward(1000);

    assertThat(balance(A)).isEqualTo(Wei.of(500));
    assertThat(balance(B)).isEqualTo(Wei.of(500));
  }

  @Test
  public void aZeroShareDoesNotCreateAnAccount() {
    deploy(returning(BeneficiaryContractDecoderTest.encode(List.of(A, B), weights(5000, 5000))));

    reward(1);

    // both shares round to zero, the remainder of 1 goes to the first beneficiary
    assertThat(balance(A)).isEqualTo(Wei.of(1));
    assertThat(worldState.get(B)).isNull();
  }

  @Test
  public void zeroWeightBeneficiaryGetsNothingAndIsNotCreated() {
    deploy(returning(BeneficiaryContractDecoderTest.encode(List.of(A, B), weights(0, 10_000))));

    reward(1000);

    // the remainder is zero, so the first beneficiary stays uncredited
    assertThat(worldState.get(A)).isNull();
    assertThat(balance(B)).isEqualTo(Wei.of(1000));
  }

  @Test
  public void callsGetBeneficiariesBySelector() {
    deploy(
        selectorGuarded(
            ContractBeneficiaryBlockRewardProcessor.GET_BENEFICIARIES_SELECTOR,
            BeneficiaryContractDecoderTest.encode(List.of(A, B), weights(5000, 5000))));

    reward(1000);

    assertThat(balance(A)).isEqualTo(Wei.of(500));
    assertThat(balance(B)).isEqualTo(Wei.of(500));
  }

  @Test
  public void selectorIsTheKeccakOfTheFunctionSignature() {
    assertThat(ContractBeneficiaryBlockRewardProcessor.GET_BENEFICIARIES_SELECTOR)
        .isEqualTo(
            org.hyperledger.besu.crypto.Hash.keccak256(
                    Bytes.wrap(
                        "getBeneficiaries()".getBytes(java.nio.charset.StandardCharsets.UTF_8)))
                .slice(0, 4));
  }

  // ---- fallbacks: every one of these pays the whole reward to the mining beneficiary ----

  @Test
  public void fallsBackWhenThereIsNoCodeAtTheAddress() {
    reward(1000);
    assertPaidToMiningBeneficiary(1000);
  }

  @Test
  public void fallsBackWhenTheContractReverts() {
    deploy(Bytes.fromHexString(REVERT));
    reward(1000);
    assertPaidToMiningBeneficiary(1000);
  }

  @Test
  public void fallsBackOnAnInvalidOpcode() {
    deploy(Bytes.fromHexString(INVALID_OPCODE));
    reward(1000);
    assertPaidToMiningBeneficiary(1000);
  }

  @Test
  public void fallsBackWhenTheCallRunsOutOfGas() {
    deploy(Bytes.fromHexString(OUT_OF_GAS_LOOP));
    reward(1000);
    assertPaidToMiningBeneficiary(1000);
  }

  @Test
  public void fallsBackWhenTheContractTriesToWriteState() {
    deploy(Bytes.fromHexString(WRITE_STORAGE));
    reward(1000);
    assertPaidToMiningBeneficiary(1000);
    assertThat(worldState.get(CONTRACT).getStorageValue(UInt256.ZERO)).isEqualTo(UInt256.ZERO);
  }

  @Test
  public void fallsBackWhenTheSelectorDoesNotMatch() {
    deploy(
        selectorGuarded(
            Bytes.fromHexString("0xdeadbeef"),
            BeneficiaryContractDecoderTest.encode(List.of(A), weights(10_000))));
    reward(1000);
    assertPaidToMiningBeneficiary(1000);
  }

  @Test
  public void fallsBackOnEmptyReturnData() {
    deploy(returning(Bytes.EMPTY));
    reward(1000);
    assertPaidToMiningBeneficiary(1000);
  }

  @Test
  public void fallsBackOnMalformedReturnData() {
    deploy(returning(Bytes.fromHexString("0x1234")));
    reward(1000);
    assertPaidToMiningBeneficiary(1000);
  }

  @Test
  public void fallsBackOnAnEmptyList() {
    deploy(returning(BeneficiaryContractDecoderTest.encode(List.of(), List.of())));
    reward(1000);
    assertPaidToMiningBeneficiary(1000);
  }

  @Test
  public void fallsBackOnMismatchedArrayLengths() {
    deploy(returning(BeneficiaryContractDecoderTest.encode(List.of(A, B), weights(10_000))));
    reward(1000);
    assertPaidToMiningBeneficiary(1000);
  }

  @Test
  public void fallsBackWhenWeightsSumToLessThanTotal() {
    deploy(returning(BeneficiaryContractDecoderTest.encode(List.of(A, B), weights(5000, 4000))));
    reward(1000);
    assertPaidToMiningBeneficiary(1000);
  }

  @Test
  public void fallsBackWhenWeightsSumToMoreThanTotal() {
    deploy(returning(BeneficiaryContractDecoderTest.encode(List.of(A, B), weights(5000, 6000))));
    reward(1000);
    assertPaidToMiningBeneficiary(1000);
  }

  @Test
  public void fallsBackWhenAnAddressIsZero() {
    deploy(
        returning(
            BeneficiaryContractDecoderTest.encode(List.of(Address.ZERO, B), weights(5000, 5000))));
    reward(1000);
    assertPaidToMiningBeneficiary(1000);
  }

  @Test
  public void fallsBackWhenThereAreTooManyBeneficiaries() {
    final int n = BeneficiaryContractDecoder.MAX_BENEFICIARIES + 1;
    final List<Address> addresses = new ArrayList<>();
    final List<Long> weights = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      addresses.add(Address.fromHexString(String.format("0x%040x", i + 1)));
      weights.add(i == 0 ? 10_000L - (n - 1) : 1L);
    }
    deploy(returning(BeneficiaryContractDecoderTest.encode(addresses, weights)));

    reward(1000);

    assertPaidToMiningBeneficiary(1000);
  }

  // ---- other behaviour ----

  @Test
  public void aBlockWithOmmersIsHandledByTheFallback() {
    deploy(returning(BeneficiaryContractDecoderTest.encode(List.of(A, B), weights(5000, 5000))));
    final BlockHeader tooOldOmmer = new BlockHeaderTestFixture().number(1).buildHeader();

    final boolean result =
        processor(1000)
            .rewardBeneficiaries(worldState, header, List.of(tooOldOmmer), MINING_BENEFICIARY);

    assertThat(result).isFalse();
    assertThat(worldState.get(MINING_BENEFICIARY)).isNull();
    assertThat(worldState.get(A)).isNull();
  }

  @Test
  public void rewardAmountsAreThoseOfTheFallbackProcessor() {
    final ContractBeneficiaryBlockRewardProcessor processor = processor(1000);

    assertThat(processor.getBlockReward()).isEqualTo(Wei.of(1000));
    assertThat(processor.getCoinbaseReward(0)).isEqualTo(Wei.of(1000));
  }

  @Test
  public void theCallDoesNotChangeStateOtherThanThePaidRewards() {
    deploy(returning(BeneficiaryContractDecoderTest.encode(List.of(A), weights(10_000))));
    final Wei contractBalanceBefore = balance(CONTRACT);

    reward(1000);

    assertThat(balance(CONTRACT)).isEqualTo(contractBalanceBefore);
    assertThat(worldState.get(Address.fromHexString("0xfffffffffffffffffffffffffffffffffffffffe")))
        .isNull();
  }

  // ---- helpers ----

  private void reward(final long amount) {
    assertThat(
            processor(amount)
                .rewardBeneficiaries(worldState, header, List.of(), MINING_BENEFICIARY))
        .isTrue();
  }

  private ContractBeneficiaryBlockRewardProcessor processor(final long amount) {
    return new ContractBeneficiaryBlockRewardProcessor(
        transactionProcessor, CONTRACT, BlockRewardProcessor.of(Wei.of(amount)));
  }

  private void assertPaidToMiningBeneficiary(final long amount) {
    assertThat(balance(MINING_BENEFICIARY)).isEqualTo(Wei.of(amount));
    assertThat(worldState.get(A)).isNull();
    assertThat(worldState.get(B)).isNull();
    assertThat(worldState.get(C)).isNull();
  }

  private void deploy(final Bytes code) {
    final WorldUpdater updater = worldState.updater();
    final MutableAccount contract = updater.getOrCreate(CONTRACT);
    contract.setCode(code);
    updater.commit();
  }

  private Wei balance(final Address address) {
    final Account account = worldState.get(address);
    return account == null ? Wei.ZERO : account.getBalance();
  }

  private static List<Long> weights(final long... values) {
    final List<Long> list = new ArrayList<>();
    for (final long v : values) {
      list.add(v);
    }
    return list;
  }

  private static final int RETURNING_PREFIX_SIZE = 14;

  /** Bytecode that ignores its input and returns {@code data}. */
  private static Bytes returning(final Bytes data) {
    return returningAt(data, 0);
  }

  /** The returning code, for when it starts at {@code base} within the contract. */
  private static Bytes returningAt(final Bytes data, final int base) {
    final int length = data.size();
    // PUSH2 len, PUSH1 dataOffset, PUSH1 0, CODECOPY, PUSH2 len, PUSH1 0, RETURN
    return Bytes.concatenate(
        Bytes.fromHexString("61"),
        Bytes.of((byte) (length >> 8), (byte) length),
        Bytes.fromHexString("60"),
        Bytes.of((byte) (base + RETURNING_PREFIX_SIZE)),
        Bytes.fromHexString("600039"),
        Bytes.fromHexString("61"),
        Bytes.of((byte) (length >> 8), (byte) length),
        Bytes.fromHexString("6000f3"),
        data);
  }

  /**
   * Bytecode that returns {@code data} only if the first four bytes of the input are {@code
   * selector}.
   */
  private static Bytes selectorGuarded(final Bytes selector, final Bytes data) {
    final int jumpDestination = 20;
    final Bytes guard =
        Bytes.concatenate(
            // PUSH1 0, CALLDATALOAD, PUSH1 224, SHR, PUSH4 selector, EQ, PUSH1 dest, JUMPI
            Bytes.fromHexString("600035" + "60e01c" + "63"),
            selector,
            Bytes.fromHexString("14" + "60"),
            Bytes.of((byte) jumpDestination),
            // JUMPI, then REVERT(0, 0) when it does not jump, then the JUMPDEST
            Bytes.fromHexString("57" + "60006000fd" + "5b"));
    return Bytes.concatenate(guard, returningAt(data, guard.size()));
  }
}
