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
package org.hyperledger.besu.ethereum.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.GWei;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.GasLimitCalculator;
import org.hyperledger.besu.ethereum.mainnet.BlockGasAccountingStrategy;
import org.hyperledger.besu.ethereum.mainnet.BlockGasUsedValidator;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.DefaultProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.PoWHasher;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.DefaultStateRootCommitterFactory;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.evm.precompile.PrecompileContractRegistry;

import java.math.BigInteger;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt64;
import org.bouncycastle.jcajce.provider.digest.Keccak;
import org.junit.jupiter.api.Test;

public class SyncBlockBodyTest {

  private static final Optional<BigInteger> CHAIN_ID = Optional.of(BigInteger.ONE);
  public static final DefaultProtocolSchedule DEFAULT_PROTOCOL_SCHEDULE =
      new DefaultProtocolSchedule(CHAIN_ID);

  @Test
  public void testGetBodyProvider() {
    final DefaultProtocolSchedule protocolSchedule = new DefaultProtocolSchedule(CHAIN_ID);
    protocolSchedule.putBlockNumberMilestone(0, getProtocolSpec());

    final Block block = getBlock();

    final BytesValueRLPOutput bytesValueRLPOutput = new BytesValueRLPOutput();
    block.getBody().writeWrappedBodyTo(bytesValueRLPOutput);
    final Bytes rlpBytes = bytesValueRLPOutput.encoded();

    final SyncBlockBody syncBlockBody =
        SyncBlockBody.readWrappedBodyFrom(
            new BytesValueRLPInput(rlpBytes, false), false, protocolSchedule);
    final BlockBody suppliedBlockBody = syncBlockBody.getBodySupplier().get();

    assertThat(suppliedBlockBody).isEqualTo(block.getBody());
  }

  @Test
  public void testTxRootWdrRootOmmersHash() {
    final Block block = getBlock();
    final BytesValueRLPOutput bytesValueRLPOutput = new BytesValueRLPOutput();
    block.getBody().writeWrappedBodyTo(bytesValueRLPOutput);
    final Bytes rlpBytes = bytesValueRLPOutput.encoded();
    final SyncBlockBody syncBlockBody =
        SyncBlockBody.readWrappedBodyFrom(
            new BytesValueRLPInput(rlpBytes, false), false, DEFAULT_PROTOCOL_SCHEDULE);

    assertThat(syncBlockBody.getTransactionsRoot())
        .isEqualTo(BodyValidation.transactionsRoot(block.getBody().getTransactions()));
    assertThat(syncBlockBody.getWithdrawalsRoot())
        .isEqualTo(BodyValidation.withdrawalsRoot(block.getBody().getWithdrawals().get()));
    assertThat(syncBlockBody.getOmmersHash())
        .isEqualTo(BodyValidation.ommersHash(block.getBody().getOmmers()));
  }

  @Test
  public void testEmptyWithEmptyWithdrawls() {
    final SyncBlockBody syncBlockBody =
        new SyncBlockBody(
            Bytes.EMPTY,
            Collections.emptyList(),
            Bytes.EMPTY,
            Collections.emptyList(),
            DEFAULT_PROTOCOL_SCHEDULE);

    final Keccak.Digest256 keccak = new Keccak.Digest256();

    assertThat(syncBlockBody.getTransactionCount()).isEqualTo(0);
    assertThat(syncBlockBody.getOmmersHash())
        .isEqualTo(Hash.wrap(Bytes32.wrap(keccak.digest(Bytes.EMPTY.toArray()))));
    assertThat(syncBlockBody.getTransactionsRoot()).isEqualTo(Hash.EMPTY_TRIE_HASH);
    assertThat(syncBlockBody.getWithdrawalsRoot()).isEqualTo(Hash.EMPTY_TRIE_HASH);
    assertThat(syncBlockBody.getEncodedTransactions()).isEqualTo(Collections.emptyList());
  }

  @Test
  public void testEmptyWithNullWithrawalsMethod() {
    final SyncBlockBody emptySBB =
        SyncBlockBody.emptyWithNullWithdrawals(DEFAULT_PROTOCOL_SCHEDULE);

    final Keccak.Digest256 keccak = new Keccak.Digest256();

    assertThat(emptySBB.getTransactionCount()).isEqualTo(0);
    assertThat(emptySBB.getOmmersHash())
        .isEqualTo(Hash.wrap(Bytes32.wrap(keccak.digest(Bytes.EMPTY.toArray()))));
    assertThat(emptySBB.getTransactionsRoot()).isEqualTo(Hash.EMPTY_TRIE_HASH);
    assertThat(emptySBB.getWithdrawalsRoot()).isNull();
    assertThat(emptySBB.getEncodedTransactions()).isEqualTo(Collections.emptyList());
  }

  @Test
  public void testEmptyWithEmptyWithdralsMethod() {
    final SyncBlockBody emptySBB =
        SyncBlockBody.emptyWithEmptyWithdrawals(DEFAULT_PROTOCOL_SCHEDULE);

    final Keccak.Digest256 keccak = new Keccak.Digest256();

    assertThat(emptySBB.getTransactionCount()).isEqualTo(0);
    assertThat(emptySBB.getOmmersHash())
        .isEqualTo(Hash.wrap(Bytes32.wrap(keccak.digest(Bytes.EMPTY.toArray()))));
    assertThat(emptySBB.getTransactionsRoot()).isEqualTo(Hash.EMPTY_TRIE_HASH);
    assertThat(emptySBB.getWithdrawalsRoot()).isEqualTo(Hash.EMPTY_TRIE_HASH);
    assertThat(emptySBB.getEncodedTransactions()).isEqualTo(Collections.emptyList());
  }

  @Test
  public void testEmptyIsSameAsBlockBody() {
    final BlockBody emptyBB = BlockBody.empty();
    final BytesValueRLPOutput bytesValueRLPOutput = new BytesValueRLPOutput();
    emptyBB.writeWrappedBodyTo(bytesValueRLPOutput);
    final Bytes emptyBBBytes = bytesValueRLPOutput.encoded();
    final SyncBlockBody syncBlockBody =
        SyncBlockBody.emptyWithNullWithdrawals(DEFAULT_PROTOCOL_SCHEDULE);
    assertThat(syncBlockBody.getRlp()).isEqualTo(emptyBBBytes);
  }

  @Test
  public void testEmptyIsSameAsBlockBody2() {
    final SyncBlockBody syncBlockBody =
        SyncBlockBody.emptyWithNullWithdrawals(DEFAULT_PROTOCOL_SCHEDULE);
    BytesValueRLPInput bytesValueRLPInput = new BytesValueRLPInput(syncBlockBody.getRlp(), false);
    final BlockBody emptyBB =
        BlockBody.readWrappedBodyFrom(bytesValueRLPInput, new MainnetBlockHeaderFunctions());
    assertThat(emptyBB).isEqualTo(BlockBody.empty());

    bytesValueRLPInput = new BytesValueRLPInput(syncBlockBody.getRlp(), false);
    final SyncBlockBody syncBlockBody2 =
        SyncBlockBody.readWrappedBodyFrom(bytesValueRLPInput, true, DEFAULT_PROTOCOL_SCHEDULE);
    assertThat(syncBlockBody2).isEqualTo(syncBlockBody);
  }

  @Test
  public void testEmptyBlockBodyFailsWhenAllowEmptyIsFalse() {
    final BytesValueRLPInput input =
        new BytesValueRLPInput(Bytes.fromHexString("0xc0"), false, true);
    assertThat(SyncBlockBody.readWrappedBodyFrom(input, true, DEFAULT_PROTOCOL_SCHEDULE))
        .isEqualTo(SyncBlockBody.emptyWithNullWithdrawals(DEFAULT_PROTOCOL_SCHEDULE));
  }

  private static Block getBlock() {
    BlockDataGenerator generator = new BlockDataGenerator();
    BlockDataGenerator.BlockOptions options = new BlockDataGenerator.BlockOptions();
    final Withdrawal withdrawal =
        new Withdrawal(UInt64.ONE, UInt64.ONE, Address.fromHexString("0x1"), GWei.ONE);
    options.setWithdrawals(Optional.of(List.of(withdrawal)));
    TransactionTestFixture transactionTestFixture = new TransactionTestFixture();
    final SignatureAlgorithm signatureAlgorithm = SignatureAlgorithmFactory.getInstance();
    options.addTransaction(
        transactionTestFixture.createTransaction(signatureAlgorithm.generateKeyPair()));
    options.addTransaction(
        transactionTestFixture.createTransaction(signatureAlgorithm.generateKeyPair()));
    final Block block = generator.block(options);
    return block;
  }

  private static ProtocolSpec getProtocolSpec() {
    return new ProtocolSpec(
        HardforkId.MainnetHardforkId.CANCUN,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        new MainnetBlockHeaderFunctions(),
        null,
        null,
        null,
        null,
        new PrecompileContractRegistry(),
        false,
        null,
        GasLimitCalculator.constant(),
        FeeMarket.legacy(),
        Optional.of(PoWHasher.ETHASH_LIGHT),
        null,
        Optional.empty(),
        null,
        Optional.empty(),
        null,
        true,
        Duration.ofSeconds(12),
        true,
        Optional.empty(),
        Optional.empty(),
        new DefaultStateRootCommitterFactory(),
        BlockGasAccountingStrategy.FRONTIER,
        BlockGasUsedValidator.FRONTIER);
  }
}
