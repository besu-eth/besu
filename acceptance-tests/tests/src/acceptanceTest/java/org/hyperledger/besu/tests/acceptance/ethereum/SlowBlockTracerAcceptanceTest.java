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
package org.hyperledger.besu.tests.acceptance.ethereum;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.hyperledger.besu.crypto.SECP256K1;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.WaitUtils;
import org.hyperledger.besu.tests.acceptance.dsl.account.Account;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNodeRunner;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.web3j.protocol.core.methods.response.TransactionReceipt;

/**
 * End-to-end check that {@code --slow-block-threshold} reaches block processing and that a real
 * BAL-enabled import emits a well-formed slow-block document.
 *
 * <p>Exact counter values are asserted in the block-processor integration tests; what this covers
 * that they cannot is the CLI wiring and the process-wide storage decorator that produces the
 * cache-miss half of the numbers.
 */
public class SlowBlockTracerAcceptanceTest extends AcceptanceTestBase {

  private static final String GENESIS_FILE = "/dev/dev_amsterdam.json";
  private static final SECP256K1 SECP = new SECP256K1();

  private static final Bytes SENDER_PRIVATE_KEY =
      Bytes.fromHexString("3a4ff6d22d7502ef2452368165422861c01a0f72f851793b372b87888dc3c453");

  private final Account recipient = accounts.createAccount("recipient");

  private BesuNode besuNode;
  private AmsterdamAcceptanceTestHelper testHelper;

  @BeforeEach
  void setUp() throws IOException {
    // The threshold is a real CLI option, so only the process runner exercises this wiring.
    assumeTrue(BesuNodeRunner.isProcessBesuNodeRunner());

    besuNode =
        besu.createExecutionEngineGenesisNode(
            "besuNode", GENESIS_FILE, true, List.of("--slow-block-threshold=0", "--logging=INFO"));
    cluster.start(besuNode);
    testHelper = new AmsterdamAcceptanceTestHelper(besuNode, ethTransactions);
  }

  @AfterEach
  void tearDown() {
    if (besuNode != null) {
      besuNode.close();
    }
  }

  @Test
  public void logsAWellFormedSlowBlockDocumentForAnImportedBlock() throws IOException {
    // capture only from here so startup noise is excluded
    cluster.startConsoleCapture();

    final TransactionReceipt receipt = sendTransactionAndBuildBlock();
    assertThat(receipt.getStatus()).isEqualTo("0x1");

    WaitUtils.waitFor(60, () -> assertThat(cluster.getConsoleContents()).contains("Slow block"));

    final JsonNode json = lastSlowBlockDocumentFor(receipt.getBlockHash());

    assertThat(json.get("msg").asText()).isEqualTo("Slow block");
    assertThat(json.get("block").get("hash").asText()).isEqualTo(receipt.getBlockHash());
    assertThat(json.get("block").get("tx_count").asInt()).isEqualTo(1);
    assertThat(json.get("timing").get("total_ms").asDouble()).isPositive();

    final JsonNode reads = json.get("state_reads");
    final JsonNode cache = json.get("cache");
    assertCacheReconstructsTotal(cache.get("account"), reads.get("accounts").asLong());
    assertCacheReconstructsTotal(cache.get("storage"), reads.get("storage_slots").asLong());
    assertCacheReconstructsTotal(cache.get("code"), reads.get("code").asLong());

    // a real import reads state from disk, which is what the storage decorator counts
    assertThat(cache.get("account").get("misses").asLong()).isPositive();

    final JsonNode unique = json.get("unique");
    assertThat(unique.get("accounts").asLong()).isPositive();
    assertThat(unique.get("accounts").asLong()).isLessThanOrEqualTo(reads.get("accounts").asLong());

    assertThat(json.get("bal").get("replayed_txs").asInt()).isNotNegative();
  }

  private void assertCacheReconstructsTotal(final JsonNode cacheNode, final long totalReads) {
    assertThat(cacheNode.get("hits").asLong()).isNotNegative();
    assertThat(cacheNode.get("hits").asLong() + cacheNode.get("misses").asLong())
        .isEqualTo(totalReads);
  }

  /**
   * Several blocks are logged around a single transaction: empty proposals, proposal improvement
   * rounds on the block-creation thread, and finally the newPayload import. Only the last line for
   * the receipt's block, off the block-creation thread, is the canonical import.
   */
  private JsonNode lastSlowBlockDocumentFor(final String blockHash) {
    final String line =
        Arrays.stream(cluster.getConsoleContents().split("\n"))
            .filter(l -> l.contains("Slow block"))
            .filter(l -> l.contains(blockHash))
            .filter(l -> !l.contains("BlockCreation"))
            .reduce((first, second) -> second)
            .orElseThrow(
                () -> new AssertionError("No 'Slow block' line for block hash " + blockHash));

    final int start = line.indexOf('{');
    final int end = line.lastIndexOf('}');
    assertThat(start).as("JSON start brace").isNotNegative();
    assertThat(end).as("JSON end brace").isGreaterThan(start);
    try {
      return new ObjectMapper().readTree(line.substring(start, end + 1));
    } catch (final Exception e) {
      throw new AssertionError("Slow block log was not valid JSON: " + line, e);
    }
  }

  private TransactionReceipt sendTransactionAndBuildBlock() throws IOException {
    final Transaction tx =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .chainId(BigInteger.valueOf(20211))
            .nonce(0)
            .maxPriorityFeePerGas(Wei.of(1_000_000_000))
            .maxFeePerGas(Wei.fromHexString("0x02540BE400"))
            .gasLimit(300_000)
            .to(Address.fromHexStringStrict(recipient.getAddress()))
            .value(Wei.of(1_000_000_000_000_000L))
            .payload(Bytes.EMPTY)
            .signAndBuild(
                SECP.createKeyPair(
                    SECP.createPrivateKey(SENDER_PRIVATE_KEY.toUnsignedBigInteger())));

    final String txHash =
        besuNode.execute(ethTransactions.sendRawTransaction(tx.encoded().toHexString()));
    testHelper.buildNewBlock();

    final AtomicReference<Optional<TransactionReceipt>> holder =
        new AtomicReference<>(Optional.empty());
    WaitUtils.waitFor(
        60,
        () -> {
          holder.set(besuNode.execute(ethTransactions.getTransactionReceipt(txHash)));
          assertThat(holder.get()).isPresent();
        });
    return holder.get().orElseThrow();
  }
}
