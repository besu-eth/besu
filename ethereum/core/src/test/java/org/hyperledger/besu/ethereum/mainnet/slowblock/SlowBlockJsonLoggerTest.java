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
package org.hyperledger.besu.ethereum.mainnet.slowblock;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;

import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class SlowBlockJsonLoggerTest {

  @Test
  void emitsTheDocumentedSlowBlockShape() throws JsonProcessingException {
    final JsonNode json = new ObjectMapper().readTree(SlowBlockJsonLogger.toJson(populated()));

    assertThat(json.get("level").asText()).isEqualTo("warn");
    assertThat(json.get("msg").asText()).isEqualTo("Slow block");

    final JsonNode block = json.get("block");
    assertThat(block.get("number").asLong()).isEqualTo(42);
    assertThat(block.get("hash").asText()).isEqualTo("0xabc");
    assertThat(block.get("gas_used").asLong()).isEqualTo(30_000_000);
    assertThat(block.get("tx_count").asInt()).isEqualTo(95);

    final JsonNode timing = json.get("timing");
    assertThat(timing.get("state_read_ms").asDouble()).isEqualTo(2.0);
    assertThat(timing.get("state_hash_ms").asDouble()).isEqualTo(3.0);
    assertThat(timing.get("commit_ms").asDouble()).isEqualTo(1.0);
    assertThat(timing.get("total_ms").asDouble()).isEqualTo(10.0);
    // phases deliberately do not sum to the total; execution is the import-thread remainder
    assertThat(timing.get("execution_ms").asDouble()).isEqualTo(6.0);

    assertThat(json.get("throughput").get("mgas_per_sec").asDouble()).isEqualTo(3000.0);

    final JsonNode stateReads = json.get("state_reads");
    assertThat(stateReads.get("accounts").asLong()).isEqualTo(100);
    assertThat(stateReads.get("storage_slots").asLong()).isEqualTo(500);
    assertThat(stateReads.get("code").asLong()).isEqualTo(7);
    assertThat(stateReads.get("code_bytes").asLong()).isEqualTo(1_600);

    final JsonNode stateWrites = json.get("state_writes");
    assertThat(stateWrites.get("accounts").asLong()).isEqualTo(170);
    assertThat(stateWrites.get("storage_slots").asLong()).isEqualTo(9_515);
    assertThat(stateWrites.get("code").asLong()).isEqualTo(1);
    assertThat(stateWrites.get("code_bytes").asLong()).isEqualTo(24);
    assertThat(stateWrites.get("accounts_deleted").asLong()).isEqualTo(2);
    assertThat(stateWrites.get("storage_slots_deleted").asLong()).isEqualTo(3);

    // hits + misses always reconstruct the total logical reads
    final JsonNode cache = json.get("cache");
    assertThat(cache.get("account").get("hits").asLong()).isEqualTo(70);
    assertThat(cache.get("account").get("misses").asLong()).isEqualTo(30);
    assertThat(cache.get("account").get("hit_rate").asDouble()).isEqualTo(70.0);
    assertThat(
            cache.get("storage").get("hits").asLong() + cache.get("storage").get("misses").asLong())
        .isEqualTo(stateReads.get("storage_slots").asLong());
    assertThat(cache.get("code").get("hits").asLong() + cache.get("code").get("misses").asLong())
        .isEqualTo(stateReads.get("code").asLong());

    final JsonNode unique = json.get("unique");
    assertThat(unique.get("accounts").asLong()).isEqualTo(150);
    assertThat(unique.get("storage_slots").asLong()).isEqualTo(9_510);
    assertThat(unique.get("contracts").asLong()).isEqualTo(1);

    final JsonNode evm = json.get("evm");
    assertThat(evm.get("sload").asLong()).isEqualTo(68);
    assertThat(evm.get("sstore").asLong()).isEqualTo(9_532);
    assertThat(evm.get("calls").asLong()).isEqualTo(22);
    assertThat(evm.get("creates").asLong()).isEqualTo(3);

    final JsonNode bal = json.get("bal");
    assertThat(bal.get("state_hash_background_ms").asDouble()).isEqualTo(8.0);
    assertThat(bal.get("tx_exec_background_ms").asDouble()).isEqualTo(40.0);
    assertThat(bal.get("tx_result_wait_ms").asDouble()).isEqualTo(0.5);
    assertThat(bal.get("replayed_txs").asInt()).isEqualTo(1);
    assertThat(bal.get("prefetch").get("duration_ms").asDouble()).isEqualTo(4.0);
    assertThat(bal.get("prefetch").get("accounts").asLong()).isEqualTo(120);
    assertThat(bal.get("prefetch").get("storage_slots").asLong()).isEqualTo(900);
  }

  @Test
  void hitRateIsZeroWhenNothingWasRead() throws JsonProcessingException {
    final SlowBlockMetrics metrics = new SlowBlockMetrics(0);
    final JsonNode json = new ObjectMapper().readTree(SlowBlockJsonLogger.toJson(metrics));

    assertThat(json.get("cache").get("account").get("hit_rate").asDouble()).isZero();
    assertThat(json.get("throughput").get("mgas_per_sec").asDouble()).isZero();
  }

  /** Fully populated without running a block, so every field has a known value. */
  private SlowBlockMetrics populated() {
    final SlowBlockMetrics metrics = new SlowBlockMetrics(0);
    metrics.startBlock(42, "0xabc", 30_000_000, 95);
    metrics.addStateReads(100, 500, 7, 1_600);
    metrics.setCacheMisses(30, 200, 2);
    metrics.addStateReadNanos(2_000_000);
    metrics.setStateHashWaitNanos(3_000_000);
    metrics.setCommitNanos(1_000_000);
    metrics.setBlockAccessListDerived(150, 9_510, 170, 9_515, 1, 24, 2, 3);
    metrics.addEvmCounts(68, 9_532, 22, 3);
    metrics.addContractsExecuted(
        List.of(Address.fromHexString("0x00000000000000000000000000000000000000aa")));
    metrics.setStateHashBackgroundNanos(8_000_000);
    metrics.addBackgroundExecutionNanos(40_000_000);
    metrics.addResultWaitNanos(500_000);
    metrics.incrementReplayedTxs();
    metrics.setPrefetch(4_000_000, 120, 900);
    metrics.endBlock(10_000_000L); // pinned so the rendered timings are deterministic
    return metrics;
  }
}
