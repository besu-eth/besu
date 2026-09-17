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

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPrivateKey;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.debug.TraceOptions;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DebugTraceBlockStreamerPrecompileTest {

  private static final String GENESIS_RESOURCE =
      "/org/hyperledger/besu/ethereum/api/jsonrpc/trace/chain-data/genesis-osaka.json";
  private static final KeyPair KEY_PAIR =
      SignatureAlgorithmFactory.getInstance()
          .createKeyPair(
              SECPPrivateKey.create(
                  Bytes32.fromHexString(
                      "c87509a1c067bbde78beb793e6fa76530b6382a4c0241e5e4a9ec0a0f44dc0d3"),
                  SignatureAlgorithm.ALGORITHM));
  // IDENTITY precompile — returns its input unchanged; no EVM opcodes are executed
  private static final Address IDENTITY_PRECOMPILE =
      Address.fromHexString("0x0000000000000000000000000000000000000004");

  private ExecutionContextTestFixture fixture;
  private BlockchainQueries blockchainQueries;
  private final ObjectMapper mapper = new ObjectMapper();

  @BeforeEach
  public void setUp() {
    final GenesisConfig genesisConfig = GenesisConfig.fromResource(GENESIS_RESOURCE);
    fixture =
        ExecutionContextTestFixture.builder(genesisConfig)
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .build();
    blockchainQueries =
        new BlockchainQueries(
            fixture.getProtocolSchedule(),
            fixture.getBlockchain(),
            fixture.getStateArchive(),
            MiningConfiguration.MINING_DISABLED);
  }

  /**
   * When the transaction target is a precompile, no EVM opcodes run and tracePrecompileCall is the
   * only tracer hook fired. The streaming path must emit a synthetic struct log so that the result
   * is non-empty (matching the accumulating path).
   */
  @Test
  public void streamingPathEmitsSyntheticFrameForDirectPrecompileCall() throws Exception {
    final Block block = buildPrecompileBlock(0);
    final DebugTraceBlockStreamer streamer =
        new DebugTraceBlockStreamer(
            block, TraceOptions.DEFAULT, fixture.getProtocolSchedule(), blockchainQueries);

    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    streamer.streamTo(out, mapper);

    final JsonNode structLogs = getStructLogs(out);
    assertThat(structLogs.isArray()).isTrue();
    assertThat(structLogs.size()).isEqualTo(1);
    assertThat(structLogs.get(0).get("op").asText()).isEqualTo("");
  }

  /**
   * Streaming and accumulating paths must produce structurally identical JSON for a direct
   * precompile call — not just the same struct log count but the same fields and values.
   */
  @Test
  public void streamingAndAccumulatingPathsMatchForPrecompileCall() throws Exception {
    final Block block = buildPrecompileBlock(0);
    final DebugTraceBlockStreamer streamer =
        new DebugTraceBlockStreamer(
            block, TraceOptions.DEFAULT, fixture.getProtocolSchedule(), blockchainQueries);

    // streaming path
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    streamer.streamTo(out, mapper);
    final JsonNode streamedRoot = mapper.readTree(out.toByteArray());

    // accumulating path
    final List<Object> accumulated = streamer.accumulateAll();
    final JsonNode accRoot = mapper.readTree(mapper.writeValueAsBytes(accumulated));

    assertThat(streamedRoot)
        .as("streaming and accumulating paths must produce identical JSON")
        .isEqualTo(accRoot);
  }

  // ── helpers ──────────────────────────────────────────────────────────────

  private Block buildPrecompileBlock(final int nonce) {
    final Transaction tx =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .nonce(nonce)
            .maxPriorityFeePerGas(Wei.of(5))
            .maxFeePerGas(Wei.of(7))
            .gasLimit(100_000L)
            .to(IDENTITY_PRECOMPILE)
            .value(Wei.ZERO)
            .payload(Bytes.of(1, 2, 3, 4))
            .chainId(BigInteger.valueOf(42))
            .signAndBuild(KEY_PAIR);

    final BlockHeader genesis = fixture.getBlockchain().getChainHeadHeader();
    final BlockHeader header =
        new BlockHeaderTestFixture()
            .number(genesis.getNumber() + 1L)
            .parentHash(genesis.getHash())
            .gasLimit(30_000_000L)
            .baseFeePerGas(Wei.of(7))
            .buildHeader();
    return new Block(header, new BlockBody(List.of(tx), Collections.emptyList(), Optional.empty()));
  }

  private JsonNode getStructLogs(final ByteArrayOutputStream out) throws Exception {
    final JsonNode root = mapper.readTree(out.toByteArray());
    return root.get(0).get("result").get("structLogs");
  }
}
