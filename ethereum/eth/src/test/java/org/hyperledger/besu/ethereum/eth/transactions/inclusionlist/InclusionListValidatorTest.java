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
package org.hyperledger.besu.ethereum.eth.transactions.inclusionlist;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.eth.transactions.inclusionlist.InclusionListValidationResult.Status.UNSATISFIED;
import static org.hyperledger.besu.ethereum.eth.transactions.inclusionlist.InclusionListValidationResult.Status.VALID;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionEncoder;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class InclusionListValidatorTest {

  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();
  private static final KeyPair FUNDED_KEYS = SIGNATURE_ALGORITHM.generateKeyPair();
  private static final KeyPair UNFUNDED_KEYS = SIGNATURE_ALGORITHM.generateKeyPair();
  private static final Address RECIPIENT = Address.fromHexString("0x" + "ab".repeat(20));
  private static final BigInteger CHAIN_ID = BigInteger.ONE;
  // high enough that the blob base fee is above its minimum of 1 wei
  private static final BlobGas EXCESS_BLOB_GAS = BlobGas.of(10_000_000);

  private final InclusionListValidator validator = new InclusionListValidator();
  private ExecutionContextTestFixture fixture;
  private BlockHeader genesisHeader;
  private ProtocolSpec protocolSpec;
  private long blobGasPerBlob;
  private long maxBlobGasPerBlock;
  private Wei blobBaseFee;

  @BeforeEach
  public void setUp() {
    final String genesis =
        "{\"config\":{\"chainId\":1,\"cancunTime\":0,\"terminalTotalDifficulty\":0},"
            + "\"nonce\":\"0x42\",\"timestamp\":\"0x0\",\"extraData\":\"\","
            + "\"gasLimit\":\"0x1c9c380\",\"difficulty\":\"0x0\","
            + "\"mixHash\":\"0x0000000000000000000000000000000000000000000000000000000000000000\","
            + "\"coinbase\":\"0x0000000000000000000000000000000000000000\","
            + "\"baseFeePerGas\":\"0xA\","
            + "\"alloc\":{\""
            + Address.extract(FUNDED_KEYS.getPublicKey()).toHexString()
            + "\":{\"balance\":\"0xde0b6b3a7640000\"}}}";
    fixture = ExecutionContextTestFixture.builder(GenesisConfig.fromConfig(genesis)).build();
    genesisHeader = fixture.getBlockchain().getChainHeadHeader();
    protocolSpec = fixture.getProtocolSchedule().getByBlockHeader(genesisHeader);
    blobGasPerBlob = protocolSpec.getGasCalculator().blobGasCost(1);
    maxBlobGasPerBlock = protocolSpec.getGasLimitCalculator().currentBlobGasLimit();
    blobBaseFee = protocolSpec.getFeeMarket().blobGasPricePerGas(EXCESS_BLOB_GAS);
    assertThat(blobBaseFee).isGreaterThan(Wei.ONE);
  }

  @Test
  public void appendableEip1559TxIsUnsatisfied() {
    assertThat(validate(eip1559Tx(FUNDED_KEYS, 0))).isEqualTo(UNSATISFIED);
  }

  @Test
  public void eip1559TxWithInvalidSignatureIsSatisfied() {
    assertThat(validate(withInvalidSignature(eip1559Tx(FUNDED_KEYS, 0)))).isEqualTo(VALID);
  }

  @Test
  public void eip1559TxWithWrongNonceIsSatisfied() {
    assertThat(validate(eip1559Tx(FUNDED_KEYS, 5))).isEqualTo(VALID);
  }

  @Test
  public void blobTxPayingExactlyBlobBaseFeeIsUnsatisfied() {
    assertThat(validate(blobTx(FUNDED_KEYS, 0, blobBaseFee))).isEqualTo(UNSATISFIED);
  }

  @Test
  public void blobTxWithInvalidSignatureIsSatisfied() {
    assertThat(validate(withInvalidSignature(blobTx(FUNDED_KEYS, 0, blobBaseFee))))
        .isEqualTo(VALID);
  }

  @Test
  public void blobTxWithWrongNonceIsSatisfied() {
    assertThat(validate(blobTx(FUNDED_KEYS, 5, blobBaseFee))).isEqualTo(VALID);
  }

  @Test
  public void blobTxFromUnfundedSenderIsSatisfied() {
    assertThat(validate(blobTx(UNFUNDED_KEYS, 0, blobBaseFee))).isEqualTo(VALID);
  }

  @Test
  public void blobTxFillingRemainingBlobGasExactlyIsUnsatisfied() {
    final BlockHeader header = headerWithBlobGasUsed(maxBlobGasPerBlock - blobGasPerBlob);
    assertThat(validate(header, blobTx(FUNDED_KEYS, 0, blobBaseFee))).isEqualTo(UNSATISFIED);
  }

  @Test
  public void blobTxExceedingRemainingBlobGasIsSatisfied() {
    final BlockHeader header = headerWithBlobGasUsed(maxBlobGasPerBlock);
    assertThat(validate(header, blobTx(FUNDED_KEYS, 0, blobBaseFee))).isEqualTo(VALID);
  }

  @Test
  public void blobTxWithMaxFeePerBlobGasBelowBlobBaseFeeIsSatisfied() {
    assertThat(validate(blobTx(FUNDED_KEYS, 0, blobBaseFee.subtract(Wei.ONE)))).isEqualTo(VALID);
  }

  private InclusionListValidationResult.Status validate(final Transaction tx) {
    return validate(headerWithBlobGasUsed(0), tx);
  }

  private InclusionListValidationResult.Status validate(
      final BlockHeader header, final Transaction tx) {
    return validator
        .validate(protocolSpec, fixture.getProtocolContext(), header, 0L, List.of(encode(tx)))
        .getStatus();
  }

  private BlockHeader headerWithBlobGasUsed(final long blobGasUsed) {
    return BlockHeaderBuilder.fromHeader(genesisHeader)
        .blobGasUsed(blobGasUsed)
        .excessBlobGas(EXCESS_BLOB_GAS)
        .blockHeaderFunctions(new MainnetBlockHeaderFunctions())
        .buildBlockHeader();
  }

  private static Transaction blobTx(
      final KeyPair keys, final long nonce, final Wei maxFeePerBlobGas) {
    return new TransactionTestFixture()
        .type(TransactionType.BLOB)
        .chainId(Optional.of(CHAIN_ID))
        .nonce(nonce)
        .gasLimit(21_000)
        .to(Optional.of(RECIPIENT))
        .value(Wei.ONE)
        .maxFeePerGas(Optional.of(Wei.of(5000)))
        .maxPriorityFeePerGas(Optional.of(Wei.ONE))
        .maxFeePerBlobGas(Optional.of(maxFeePerBlobGas))
        .versionedHashes(
            Optional.of(List.of(new VersionedHash(VersionedHash.SHA256_VERSION_ID, Hash.ZERO))))
        .createTransaction(keys);
  }

  private static Transaction eip1559Tx(final KeyPair keys, final long nonce) {
    return new TransactionTestFixture()
        .type(TransactionType.EIP1559)
        .chainId(Optional.of(CHAIN_ID))
        .nonce(nonce)
        .gasLimit(21_000)
        .to(Optional.of(RECIPIENT))
        .value(Wei.ONE)
        .maxFeePerGas(Optional.of(Wei.of(5000)))
        .maxPriorityFeePerGas(Optional.of(Wei.ONE))
        .createTransaction(keys);
  }

  private static Transaction withInvalidSignature(final Transaction tx) {
    return Transaction.builder()
        .copiedFrom(tx)
        .signature(SIGNATURE_ALGORITHM.createSignature(BigInteger.ONE, BigInteger.ONE, (byte) 0))
        .build();
  }

  private static Bytes encode(final Transaction tx) {
    return TransactionEncoder.encodeOpaqueBytes(tx, EncodingContext.BLOCK_BODY);
  }
}
