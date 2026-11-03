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
package org.hyperledger.besu.ethereum.core;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.ChainId;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionDecoder;
import org.hyperledger.besu.ethereum.core.encoding.TransactionEncoder;

import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Measures the transaction operations that involve the chain id: decoding a transaction received
 * from the network and checking that it is for this chain, as the transaction validator does,
 * encoding it, computing the signing preimage and the legacy {@code v} value.
 *
 * <p>Run with {@code -prof gc} to see the allocation per operation.
 */
@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class ChainIdBenchmark {
  private static final Address TO =
      Address.fromHexString("0x000000000000000000000000000000000000beef");

  /** Mainnet, that has a chain id cached by BigInteger, and Hoodi, that does not. */
  @Param({"1", "560048"})
  public long chainIdValue;

  @Param({"FRONTIER", "EIP1559", "DELEGATE_CODE"})
  public TransactionType type;

  private ChainId networkChainId;
  private Bytes encoded;
  private Transaction transaction;

  @Setup
  public void setUp() {
    // done at startup by the node, with the chain id of the network
    networkChainId = ChainId.initNetworkChainId(ChainId.of(chainIdValue));

    // a fixed key, so that signatures, and their allocation, are the same in every run
    final SignatureAlgorithm signatureAlgorithm = SignatureAlgorithmFactory.getInstance();
    final KeyPair keys =
        signatureAlgorithm.createKeyPair(
            signatureAlgorithm.createPrivateKey(
                Bytes32.fromHexString(
                    "0x8f2a55949038a9610f50fb23b5883af3b4ecb3c3bb792cbcefbd1542c692be63")));
    final BigInteger chainId = BigInteger.valueOf(chainIdValue);
    final Transaction.Builder builder =
        Transaction.builder()
            .type(type)
            .nonce(1)
            .gasLimit(100_000)
            .to(TO)
            .value(Wei.of(1))
            .payload(Bytes.EMPTY)
            .chainId(chainId);
    switch (type) {
      case FRONTIER -> builder.gasPrice(Wei.of(10));
      case EIP1559 ->
          builder.maxPriorityFeePerGas(Wei.of(1)).maxFeePerGas(Wei.of(10)).accessList(List.of());
      case DELEGATE_CODE ->
          builder
              .maxPriorityFeePerGas(Wei.of(1))
              .maxFeePerGas(Wei.of(10))
              .accessList(List.of())
              .codeDelegations(
                  List.of(
                      org.hyperledger.besu.ethereum.core.CodeDelegation.builder()
                          .chainId(chainId)
                          .address(TO)
                          .nonce(1)
                          .signAndBuild(keys),
                      org.hyperledger.besu.ethereum.core.CodeDelegation.builder()
                          .chainId(BigInteger.ZERO)
                          .address(TO)
                          .nonce(2)
                          .signAndBuild(keys)));
      default -> throw new IllegalArgumentException("Unsupported type " + type);
    }
    encoded =
        TransactionEncoder.encodeOpaqueBytes(
            builder.signAndBuild(keys), EncodingContext.POOLED_TRANSACTION);
    transaction = TransactionDecoder.decodeOpaqueBytes(encoded, EncodingContext.POOLED_TRANSACTION);
  }

  @Benchmark
  public boolean decodeAndCheckChainId() {
    final Transaction decoded =
        TransactionDecoder.decodeOpaqueBytes(encoded, EncodingContext.POOLED_TRANSACTION);
    return decoded.getChainIdentifier().equals(networkChainId);
  }

  @Benchmark
  public boolean checkChainId() {
    return transaction.getChainIdentifier().equals(networkChainId);
  }

  @Benchmark
  public Bytes encode() {
    return TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.POOLED_TRANSACTION);
  }

  @Benchmark
  public Bytes encodedPreimage() {
    return transaction.encodedPreimage();
  }

  @Benchmark
  public BigInteger getV() {
    return transaction.getV();
  }
}
