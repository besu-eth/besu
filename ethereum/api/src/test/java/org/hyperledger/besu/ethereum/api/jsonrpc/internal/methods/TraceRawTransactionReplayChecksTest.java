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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.OsakaTargetingGasLimitCalculator;
import org.hyperledger.besu.evm.gascalculator.AmsterdamGasCalculator;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.math.BigInteger;
import java.util.List;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** trace_rawTransaction applies the checks block processing applies to a signed transaction. */
class TraceRawTransactionReplayChecksTest extends AbstractTraceRawTransactionTest {
  private static final BigInteger CHAIN_ID = BigInteger.valueOf(3503995874084926L);
  private static final long CAP =
      OsakaTargetingGasLimitCalculator.EIP_7825_TRANSACTION_GAS_LIMIT_CAP;
  // Public fixture keys: key 1 is a plain account with nonce 10, key 3 an account with code.
  private static final KeyPair PLAIN_SENDER = keyPair(1);
  private static final KeyPair CODE_SENDER = keyPair(3);
  private String genesis;

  @Override
  @BeforeEach
  public void setup() {
    // Each case starts the service on its own genesis.
  }

  @Override
  protected BlockchainSetupUtil getBlockchainSetupUtil(final DataStorageFormat format) {
    return createBlockchainSetupUtil(
        "trace/signed-transaction/" + genesis, "trace/signed-transaction/blocks.bin", format);
  }

  private static Stream<Arguments> transactions() {
    return Stream.of(
        Arguments.of(
            "EIP-7825 cap, at the cap",
            "genesis-osaka.json",
            raw(PLAIN_SENDER, 10, CAP, Bytes.EMPTY),
            null,
            null),
        Arguments.of(
            "EIP-7825 cap, above the cap",
            "genesis-osaka.json",
            raw(PLAIN_SENDER, 10, CAP + 1, Bytes.EMPTY),
            RpcErrorType.EXCEEDS_TRANSACTION_GAS_LIMIT,
            "Transaction gas limit must be at most 16777216"),
        Arguments.of(
            "EIP-3607 sender with code",
            "genesis-osaka.json",
            raw(CODE_SENDER, 0, 100_000L, Bytes.EMPTY),
            RpcErrorType.TX_SENDER_NOT_AUTHORIZED,
            "has deployed code and so is not authorized to send transactions"),
        // EIP-8037 raises the cap on the gas limit, and caps the larger of the intrinsic gas and
        // the calldata floor at the EIP-7825 value instead. The gas limit is twice the cap and the
        // floor lies halfway between them, so only the cap check can reject the transaction.
        Arguments.of(
            "EIP-8037 intrinsic gas cap",
            "genesis-amsterdam.json",
            raw(PLAIN_SENDER, 10, 2 * CAP, floorPayload(3 * CAP / 2)),
            RpcErrorType.INTRINSIC_GAS_EXCEEDS_LIMIT,
            "exceeds gas limit 16777216"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("transactions")
  void shouldApplyBlockProcessingChecks(
      final String name,
      final String genesis,
      final String raw,
      final RpcErrorType error,
      final String reason)
      throws Exception {
    this.genesis = genesis;
    startServiceWithEmptyChain(DataStorageFormat.BONSAI);
    final JsonNode response = rpc("trace_rawTransaction", List.of(raw, List.of("trace")));
    if (error == null) {
      assertThat(response.has("error")).isFalse();
      assertThat(response.path("result").path("output").asText()).isEqualTo(WORD_42);
    } else {
      assertThat(response.has("result")).isFalse();
      assertThat(response.path("error").path("code").asInt()).isEqualTo(error.getCode());
      assertThat(response.path("error").path("message").asText()).contains(reason);
    }
  }

  private static KeyPair keyPair(final int key) {
    final SignatureAlgorithm algorithm = SignatureAlgorithmFactory.getInstance();
    return algorithm.createKeyPair(algorithm.createPrivateKey(Bytes32.leftPad(Bytes.of(key))));
  }

  /** Calldata whose Amsterdam floor cost is the given gas, to within one byte's cost. */
  private static Bytes floorPayload(final long floor) {
    final AmsterdamGasCalculator gasCalculator = new AmsterdamGasCalculator();
    final long base =
        gasCalculator.transactionFloorCost(transaction(PLAIN_SENDER, 10, 2 * CAP, Bytes.EMPTY));
    final long perByte =
        gasCalculator.transactionFloorCost(transaction(PLAIN_SENDER, 10, 2 * CAP, Bytes.of(0)))
            - base;
    return Bytes.wrap(new byte[Math.toIntExact((floor - base) / perByte)]);
  }

  private static String raw(
      final KeyPair sender, final long nonce, final long gasLimit, final Bytes payload) {
    return transaction(sender, nonce, gasLimit, payload).encoded().toHexString();
  }

  private static Transaction transaction(
      final KeyPair sender, final long nonce, final long gasLimit, final Bytes payload) {
    return Transaction.builder()
        .type(TransactionType.FRONTIER)
        .chainId(CHAIN_ID)
        .nonce(nonce)
        .gasPrice(Wei.of(1_000_000_000L))
        .gasLimit(gasLimit)
        .to(Address.fromHexString(MARKER))
        .value(Wei.ZERO)
        .payload(payload)
        .signAndBuild(sender);
  }
}
