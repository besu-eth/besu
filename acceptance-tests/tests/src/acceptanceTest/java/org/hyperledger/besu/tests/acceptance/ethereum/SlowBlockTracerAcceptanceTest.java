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
import org.hyperledger.besu.datatypes.GWei;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.WaitUtils;
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
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.TransactionReceipt;

/**
 * Acceptance test for SlowBlockTracer end-to-end wiring.
 *
 * <p>Sends one transaction to an exercise contract that triggers every counter tracked by
 * SlowBlockTracer (sload, sstore, create, call, storage reads/writes/deletes, account deletes),
 * then asserts every field in the JSON emitted by logSlowBlock. Uses --slow-block-threshold=0 so
 * every block is logged, and --bonsai-parallel-tx-processing-enabled=false so state-read/cache
 * counters are wired.
 */
public class SlowBlockTracerAcceptanceTest extends AcceptanceTestBase {
  private static final String GENESIS_FILE = "/dev/dev_amsterdam.json";
  private static final SECP256K1 SECP = new SECP256K1();

  // Exercise contract pre-deployed in genesis with storage slot 0 = 1
  private static final Address EXERCISE_CONTRACT =
      Address.fromHexStringStrict("0x510b000000000000000000000000000000000001");

  public static final Bytes SENDER_PRIVATE_KEY =
      Bytes.fromHexString("3a4ff6d22d7502ef2452368165422861c01a0f72f851793b372b87888dc3c453");

  private BesuNode besuNode;
  private AmsterdamAcceptanceTestHelper testHelper;

  @BeforeEach
  void setUp() throws IOException {
    // --slow-block-threshold wires the tracer via a system property set in BesuCommand which only
    // runs in process mode
    assumeTrue(BesuNodeRunner.isProcessBesuNodeRunner());

    besuNode =
        besu.createExecutionEngineGenesisNode(
            "besuNode",
            GENESIS_FILE,
            true,
            List.of(
                "--slow-block-threshold=0",
                "--bonsai-parallel-tx-processing-enabled=false",
                "--logging=INFO"));
    cluster.start(besuNode);
    testHelper = new AmsterdamAcceptanceTestHelper(besuNode, ethTransactions);
  }

  @AfterEach
  void tearDown() {
    besuNode.close();
  }

  @Test
  public void testSlowBlockTracerLogsExpectedFields() throws IOException {
    // Start console capture after startup so we only capture the block log, not startup noise
    cluster.startConsoleCapture();

    final Transaction tx =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .chainId(BigInteger.valueOf(20211))
            .nonce(0)
            .maxPriorityFeePerGas(Wei.of(1_000_000_000))
            .maxFeePerGas(GWei.of(10).getAsWei())
            .gasLimit(2_000_000)
            .to(EXERCISE_CONTRACT)
            .value(Wei.ZERO)
            .payload(Bytes.EMPTY)
            .signAndBuild(
                SECP.createKeyPair(
                    SECP.createPrivateKey(SENDER_PRIVATE_KEY.toUnsignedBigInteger())));

    final String txHash =
        besuNode.execute(ethTransactions.sendRawTransaction(tx.encoded().toHexString()));
    testHelper.buildNewBlock();

    final TransactionReceipt receipt = waitForReceipt(txHash);
    assertThat(receipt.getStatus()).isEqualTo("0x1");

    // Wait for the slow block JSON log line (stdout pipe is async)
    WaitUtils.waitFor(60, () -> assertThat(cluster.getConsoleContents()).contains("Slow block"));

    final String consoleContents = cluster.getConsoleContents();
    // Multiple "Slow block" lines are emitted: an initial empty-block proposal (different hash),
    // async proposal-improvement rounds on the BlockCreation thread, and finally the newPayload
    // import on the vert.x thread. Filter to the block that matches the receipt hash, drop the
    // BlockCreation proposal lines, and take the last remaining line (the newPayload import),
    // which is the canonical execution path.
    final String jsonLine =
        Arrays.stream(consoleContents.split("\n"))
            .filter(line -> line.contains("Slow block"))
            .filter(line -> line.contains(receipt.getBlockHash()))
            .filter(line -> !line.contains("BlockCreation"))
            .reduce((first, second) -> second)
            .orElseThrow(
                () ->
                    new AssertionError(
                        "No 'Slow block' line for block hash " + receipt.getBlockHash()));
    final int jsonStart = jsonLine.indexOf('{');
    final int jsonEnd = jsonLine.lastIndexOf('}');
    assertThat(jsonStart).as("JSON start brace").isGreaterThanOrEqualTo(0);
    assertThat(jsonEnd).as("JSON end brace").isGreaterThan(jsonStart);
    final JsonNode json = new ObjectMapper().readTree(jsonLine.substring(jsonStart, jsonEnd + 1));

    /* Example JSON to assert
     * {
     *   "level": "warn",
     *   "msg": "Slow block",
     *   "block": {
     *     "number": 1,
     *     "hash": "0xd6fc389e6926ecc7626d385af029ef36e7a01040942b8e2432b5c9068f111a45",
     *     "gas_used": 170230,
     *     "tx_count": 1
     *   },
     *   "timing": {
     *     "execution_ms": "2.64",
     *     "state_read_ms": "0.30",
     *     "state_hash_ms": "0.08",
     *     "commit_ms": "0.03",
     *     "total_ms": "2.76"
     *   },
     *   "throughput": {
     *     "mgas_per_sec": "61.78"
     *   },
     *   "unique": {
     *     "accounts": 4,
     *     "storage_slots": 2,
     *     "contracts": 3
     *   },
     *   "evm": {
     *     "sload": 1,
     *     "sstore": 2,
     *     "calls": 1,
     *     "creates": 1
     *   },
     *   "state_reads": {
     *     "accounts": 18,
     *     "storage_slots": 26,
     *     "code": 3,
     *     "code_bytes": 652
     *   },
     *   "state_writes": {
     *     "accounts": 7,
     *     "storage_slots": 8,
     *     "code": 1,
     *     "code_bytes": 1,
     *     "accounts_deleted": 1,
     *     "storage_slots_deleted": 3
     *   },
     *   "cache": {
     *     "account": {
     *       "hits": 8,
     *       "misses": 10,
     *       "hit_rate": 44.44
     *     },
     *     "storage": {
     *       "hits": 15,
     *       "misses": 11,
     *       "hit_rate": 57.69
     *     },
     *     "code": {
     *       "hits": 3,
     *       "misses": 0,
     *       "hit_rate": 100.0
     *     }
     *   }
     * }
     */

    // Top-level fields
    assertThat(json.get("level").asText()).isEqualTo("warn");
    assertThat(json.get("msg").asText()).isEqualTo("Slow block");

    // block. The tracer reports the block header's gasUsed, which under EIP-8037 (Amsterdam)
    // differs from the receipt's metered gas (max of regular and state gas), so compare against the
    // block header rather than the receipt.
    final EthBlock.Block canonicalBlock =
        besuNode.execute(
            ethTransactions.block(DefaultBlockParameter.valueOf(receipt.getBlockNumber())));
    final JsonNode block = json.get("block");
    assertThat(block.get("number").asLong()).isEqualTo(receipt.getBlockNumber().longValue());
    assertThat(block.get("hash").asText()).isEqualTo(receipt.getBlockHash());
    assertThat(block.get("gas_used").asLong()).isEqualTo(canonicalBlock.getGasUsed().longValue());
    assertThat(block.get("tx_count").asInt()).isEqualTo(1);

    // timing — stored as strings ("12.34"), all >= 0, total_ms > 0
    final JsonNode timing = json.get("timing");
    assertThat(Double.parseDouble(timing.get("execution_ms").asText())).isGreaterThanOrEqualTo(0);
    assertThat(Double.parseDouble(timing.get("state_read_ms").asText())).isGreaterThanOrEqualTo(0);
    assertThat(Double.parseDouble(timing.get("state_hash_ms").asText())).isGreaterThanOrEqualTo(0);
    assertThat(Double.parseDouble(timing.get("commit_ms").asText())).isGreaterThanOrEqualTo(0);
    assertThat(Double.parseDouble(timing.get("total_ms").asText())).isGreaterThan(0);

    // throughput — stored as string
    assertThat(Double.parseDouble(json.get("throughput").get("mgas_per_sec").asText()))
        .isGreaterThan(0);

    // unique — exercise contract + created contract + called empty account, sender too for accounts
    final JsonNode unique = json.get("unique");
    assertThat(unique.get("storage_slots").asInt()).isEqualTo(2); // slot0 + slot1
    assertThat(unique.get("contracts").asInt()).isEqualTo(3); // exercise, created, called
    assertThat(unique.get("accounts").asInt()).isEqualTo(4); // + sender

    // evm operation counts
    final JsonNode evm = json.get("evm");
    assertThat(evm.get("sload").asInt()).isEqualTo(1);
    assertThat(evm.get("sstore").asInt()).isEqualTo(2);
    assertThat(evm.get("calls").asInt()).isEqualTo(1);
    assertThat(evm.get("creates").asInt()).isEqualTo(1);

    // state_reads
    final JsonNode stateReads = json.get("state_reads");
    assertThat(stateReads.get("accounts").asInt()).isGreaterThan(0);
    assertThat(stateReads.get("storage_slots").asInt()).isGreaterThanOrEqualTo(1);
    assertThat(stateReads.get("code").asInt()).isGreaterThanOrEqualTo(1);
    assertThat(stateReads.get("code_bytes").asInt()).isGreaterThan(0);

    // state_writes — note storage_slots and storage_slots_deleted also include the EIP-4788 and
    // EIP-2935 system-contract writes that run at the start of every block, so assert the minimum
    // attributable to the exercise contract rather than an exact count.
    final JsonNode stateWrites = json.get("state_writes");
    assertThat(stateWrites.get("storage_slots").asInt())
        .isGreaterThanOrEqualTo(2); // slot0 cleared + slot1 set (+ system contracts)
    assertThat(stateWrites.get("code").asInt()).isEqualTo(1); // created contract's code
    assertThat(stateWrites.get("code_bytes").asInt()).isEqualTo(1); // 1-byte STOP code
    assertThat(stateWrites.get("accounts_deleted").asInt()).isEqualTo(1); // EIP-158 touch-clear
    assertThat(stateWrites.get("storage_slots_deleted").asInt())
        .isGreaterThanOrEqualTo(1); // slot0 1->0 (+ system contracts)
    assertThat(stateWrites.get("accounts").asInt())
        .isGreaterThanOrEqualTo(4); // sender + coinbase + exercise + created + deleted

    // cache: hits + misses == state_reads total, hit_rate in [0, 100]
    final JsonNode cache = json.get("cache");

    final JsonNode accountCache = cache.get("account");
    final int accountHits = accountCache.get("hits").asInt();
    final int accountMisses = accountCache.get("misses").asInt();
    assertThat(accountHits).isGreaterThanOrEqualTo(0);
    assertThat(accountMisses).isGreaterThanOrEqualTo(0);
    assertThat(accountHits + accountMisses).isEqualTo(stateReads.get("accounts").asInt());
    assertThat(accountCache.get("hit_rate").asDouble()).isBetween(0.0, 100.0);

    final JsonNode storageCache = cache.get("storage");
    final int storageHits = storageCache.get("hits").asInt();
    final int storageMisses = storageCache.get("misses").asInt();
    assertThat(storageHits).isGreaterThanOrEqualTo(0);
    assertThat(storageMisses).isGreaterThanOrEqualTo(0);
    assertThat(storageHits + storageMisses).isEqualTo(stateReads.get("storage_slots").asInt());
    assertThat(storageCache.get("hit_rate").asDouble()).isBetween(0.0, 100.0);

    final JsonNode codeCache = cache.get("code");
    final int codeHits = codeCache.get("hits").asInt();
    final int codeMisses = codeCache.get("misses").asInt();
    assertThat(codeHits).isGreaterThanOrEqualTo(0);
    assertThat(codeMisses).isGreaterThanOrEqualTo(0);
    assertThat(codeHits + codeMisses).isEqualTo(stateReads.get("code").asInt());
    assertThat(codeCache.get("hit_rate").asDouble()).isBetween(0.0, 100.0);
  }

  private TransactionReceipt waitForReceipt(final String txHash) {
    final AtomicReference<Optional<TransactionReceipt>> maybeReceiptHolder =
        new AtomicReference<>(Optional.empty());
    WaitUtils.waitFor(
        60,
        () -> {
          maybeReceiptHolder.set(besuNode.execute(ethTransactions.getTransactionReceipt(txHash)));
          assertThat(maybeReceiptHolder.get()).isPresent();
        });
    return maybeReceiptHolder.get().orElseThrow();
  }
}
