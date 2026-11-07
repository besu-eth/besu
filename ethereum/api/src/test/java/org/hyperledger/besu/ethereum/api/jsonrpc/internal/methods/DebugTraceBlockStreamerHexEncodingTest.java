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
import static org.assertj.core.api.Assertions.assertThatCode;

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
import org.hyperledger.besu.ethereum.debug.TracerType;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DebugTraceBlockStreamerHexEncodingTest {

  private static final String GENESIS_RESOURCE =
      "/org/hyperledger/besu/ethereum/api/jsonrpc/trace/chain-data/genesis-osaka.json";
  private static final KeyPair KEY_PAIR =
      SignatureAlgorithmFactory.getInstance()
          .createKeyPair(
              SECPPrivateKey.create(
                  Bytes32.fromHexString(
                      "c87509a1c067bbde78beb793e6fa76530b6382a4c0241e5e4a9ec0a0f44dc0d3"),
                  SignatureAlgorithm.ALGORITHM));
  // Contract that copies calldata to memory then REVERTs with it
  private static final Address REVERT_CONTRACT =
      Address.fromHexString("0x0032000000000000000000000000000000000000");

  // ABI encoding of Error("") — 68 bytes; first byte is 0x08 (has a leading-zero high nibble)
  private static final Bytes ABI_ERROR_EMPTY =
      Bytes.fromHexString(
          "0x08c379a0"
              + "0000000000000000000000000000000000000000000000000000000000000020"
              + "0000000000000000000000000000000000000000000000000000000000000000");

  // ABI encoding of Error("revert") — 4+32+32+32 = 100 bytes
  private static final Bytes ABI_ERROR_WITH_MESSAGE =
      Bytes.fromHexString(
          "0x08c379a0"
              + "0000000000000000000000000000000000000000000000000000000000000020"
              + "0000000000000000000000000000000000000000000000000000000000000006"
              + "7265766572740000000000000000000000000000000000000000000000000000");

  // Init code that loops PUSH0/POP until gas runs out:
  //   JUMPDEST PUSH0 POP PUSH1 0x00 JUMP
  // Each iteration pushes Bytes.EMPTY (0x0) onto the stack, generating thousands of struct logs.
  private static final Bytes PUSH0_INIT_CODE = Bytes.fromHexString("0x5b5f506000560000");

  // Init code with two sequential MSTOREs, each expanding memory by one word:
  //   PUSH1 0x00 PUSH1 0x00 MSTORE PUSH1 0x00 PUSH1 0x20 MSTORE STOP
  // structLogs[5] is the second MSTORE: one word of memory before it, two after.
  private static final Bytes DOUBLE_MSTORE_INIT_CODE =
      Bytes.fromHexString("0x6000600052600060205200");

  // Init code that sets slot 0 then restores it to its original zero, earning a refund:
  //   PUSH1 0x01 PUSH1 0x00 SSTORE PUSH1 0x00 PUSH1 0x00 SSTORE STOP
  // structLogs[5] is the second SSTORE: no refund before it, a refund after.
  private static final Bytes SSTORE_RESTORE_INIT_CODE =
      Bytes.fromHexString("0x6001600055600060005500");

  // Init code that writes slot 0, then reads unwritten slot 5 and written slot 0:
  //   PUSH1 0x01 PUSH1 0x00 SSTORE PUSH1 0x05 SLOAD POP PUSH1 0x00 SLOAD POP STOP
  // structLogs[2] is the SSTORE, structLogs[4] and [7] are the SLOADs.
  private static final Bytes SSTORE_SLOAD_INIT_CODE =
      Bytes.fromHexString("0x6001600055600554506000545000");

  // Init code that STATICCALLs the IDENTITY precompile twice:
  //   (PUSH1 0x00 PUSH1 0x00 PUSH1 0x20 PUSH1 0x00 PUSH1 0x04 GAS STATICCALL POP) x2 STOP
  // structLogs[14] is the second STATICCALL: return data of the first call before it, none after.
  private static final Bytes DOUBLE_STATICCALL_INIT_CODE =
      Bytes.fromHexString("0x600060006020600060045afa50600060006020600060045afa5000");

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
   * Return data must be written when enableReturnData is set. The init code STATICCALLs the
   * IDENTITY precompile, so the steps after the call have return data.
   */
  @Test
  public void streamingAndAccumulatingPathsMatchWithReturnDataEnabled() throws Exception {
    final TraceOptions withReturnData =
        new TraceOptions(
            TracerType.OPCODE_TRACER,
            OpCodeTracerConfigBuilder.createFrom(TraceOptions.DEFAULT.opCodeTracerConfig())
                .traceReturnData(true)
                .build(),
            java.util.Map.of());
    final Transaction tx =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .nonce(0)
            .maxPriorityFeePerGas(Wei.of(5))
            .maxFeePerGas(Wei.of(7))
            .gasLimit(200_000L)
            .value(Wei.ZERO)
            .payload(Bytes.fromHexString("0x602060006020600060045afa00"))
            .chainId(BigInteger.valueOf(42))
            .signAndBuild(KEY_PAIR);
    final DebugTraceBlockStreamer streamer =
        new DebugTraceBlockStreamer(
            buildBlock(tx), withReturnData, fixture.getProtocolSchedule(), blockchainQueries);

    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    streamer.streamTo(out, mapper, () -> true);
    final JsonNode streamedRoot = mapper.readTree(out.toByteArray());
    final JsonNode accRoot =
        mapper.readTree(mapper.writeValueAsBytes(streamer.accumulateAll(() -> true)));

    assertThat(accRoot.toString()).contains("\"returnData\"");
    assertThat(streamedRoot).isEqualTo(accRoot);
  }

  // ── revert reason tests ───────────────────────────────────────────

  /**
   * A 68-byte revert reason requires 138 chars (2 for "0x" + 136 hex digits). The old hexBuf was
   * only 130 bytes — this test verifies no ArrayIndexOutOfBoundsException is thrown.
   */
  @Test
  public void hexBufDoesNotOverflowFor68ByteRevertReason() {
    final Block block = buildBlockWithCalldata(ABI_ERROR_EMPTY, 0);
    final DebugTraceBlockStreamer streamer =
        new DebugTraceBlockStreamer(
            block, TraceOptions.DEFAULT, fixture.getProtocolSchedule(), blockchainQueries);

    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    assertThatCode(() -> streamer.streamTo(out, mapper, () -> true)).doesNotThrowAnyException();
  }

  /**
   * The first byte of an ABI Error selector is 0x08. Leading zeros are stripped (compact hex
   * format), matching the accumulating path's StructLog.toCompactHex behaviour.
   */
  @Test
  public void revertReasonUsesCompactHex() throws Exception {
    final Block block = buildBlockWithCalldata(ABI_ERROR_EMPTY, 0);
    final DebugTraceBlockStreamer streamer =
        new DebugTraceBlockStreamer(
            block, TraceOptions.DEFAULT, fixture.getProtocolSchedule(), blockchainQueries);

    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    streamer.streamTo(out, mapper, () -> true);

    final JsonNode structLogs = getStructLogs(out);
    final String reason = findRevertReason(structLogs);
    assertThat(reason).isNotNull();
    // 0x08 → high nibble stripped → "0x8c379a0..."
    assertThat(reason).startsWith("0x8c379a0");
  }

  /** Verifies the full hex encoding length for a 100-byte revert reason in compact format. */
  @Test
  public void revertReasonEncodedCorrectlyFor100BytePayload() throws Exception {
    final Block block = buildBlockWithCalldata(ABI_ERROR_WITH_MESSAGE, 0);
    final DebugTraceBlockStreamer streamer =
        new DebugTraceBlockStreamer(
            block, TraceOptions.DEFAULT, fixture.getProtocolSchedule(), blockchainQueries);

    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    streamer.streamTo(out, mapper, () -> true);

    final JsonNode structLogs = getStructLogs(out);
    final String reason = findRevertReason(structLogs);
    assertThat(reason).isNotNull();
    assertThat(reason).startsWith("0x8c379a0");
    // compact hex: "0x" + 1 (stripped high nibble of 0x08) + 99*2 = 201 chars
    assertThat(reason).hasSize(201);
  }

  /**
   * Streaming and accumulating paths must produce structurally identical JSON for a transaction
   * with a revert reason.
   */
  @Test
  public void streamingAndAccumulatingPathsMatchForRevertReason() throws Exception {
    final Block block = buildBlockWithCalldata(ABI_ERROR_WITH_MESSAGE, 0);
    final DebugTraceBlockStreamer streamer =
        new DebugTraceBlockStreamer(
            block, TraceOptions.DEFAULT, fixture.getProtocolSchedule(), blockchainQueries);

    // streaming path
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    streamer.streamTo(out, mapper, () -> true);
    final JsonNode streamedRoot = mapper.readTree(out.toByteArray());

    // accumulating path
    final List<Object> accumulated = streamer.accumulateAll(() -> true);
    final JsonNode accRoot = mapper.readTree(mapper.writeValueAsBytes(accumulated));

    assertThat(streamedRoot)
        .as("streaming and accumulating paths must produce identical JSON")
        .isEqualTo(accRoot);
  }

  // ── zero-stack / writeHex buffer boundary tests ───────────────────

  /**
   * Directly tests writeHex at the buffer boundary that triggers the off-by-one bug.
   *
   * <p>Uses reflection to position writePos at BUF_SIZE - 2, then calls writeHex with an empty
   * byte[]. Without the fix (maxLen = 2), writeHex does not flush and encodeTo writes 3 bytes past
   * the 2-byte headroom → ArrayIndexOutOfBoundsException. With the fix (maxLen = max(3, ...)),
   * writeHex flushes first, leaving room for all 3 bytes.
   */
  @Test
  public void writeHexAtBufferBoundaryWithEmptyBytes() throws Exception {
    final Block block = buildBlockWithPush0();
    final DebugTraceBlockStreamer streamer =
        new DebugTraceBlockStreamer(
            block, TraceOptions.DEFAULT, fixture.getProtocolSchedule(), blockchainQueries);

    // Use reflection to access private internals
    final Field rawOutField = DebugTraceBlockStreamer.class.getDeclaredField("rawOut");
    rawOutField.setAccessible(true);

    final Field writePosField = DebugTraceBlockStreamer.class.getDeclaredField("writePos");
    writePosField.setAccessible(true);

    final Field bufSizeField = DebugTraceBlockStreamer.class.getDeclaredField("BUF_SIZE");
    bufSizeField.setAccessible(true);
    final int bufSize = (int) bufSizeField.get(null);

    final Method writeHexMethod =
        DebugTraceBlockStreamer.class.getDeclaredMethod("writeHex", byte[].class, boolean.class);
    writeHexMethod.setAccessible(true);

    // Set up the streamer with a real output stream and position at the boundary
    rawOutField.set(streamer, new ByteArrayOutputStream());
    writePosField.set(streamer, bufSize - 2); // exactly 2 bytes of headroom

    // This call exercises the exact condition: empty byte[] with maxLen=2 vs 3
    assertThatCode(() -> writeHexMethod.invoke(streamer, new byte[0], true))
        .as("writeHex with empty byte[] at BUF_SIZE-2 must not throw")
        .doesNotThrowAnyException();
  }

  /**
   * Streaming a block trace with stack tracing enabled must produce valid JSON even when the stack
   * contains zero values (empty Bytes).
   */
  @Test
  public void streamingWithZeroStackValuesProducesValidJson() {
    final Block block = buildBlockWithPush0();
    final DebugTraceBlockStreamer streamer =
        new DebugTraceBlockStreamer(
            block, TraceOptions.DEFAULT, fixture.getProtocolSchedule(), blockchainQueries);

    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    assertThatCode(() -> streamer.streamTo(out, mapper, () -> true)).doesNotThrowAnyException();

    // Must be valid JSON
    assertThatCode(() -> mapper.readTree(out.toByteArray())).doesNotThrowAnyException();
  }

  /**
   * Streaming and accumulating paths must produce structurally identical JSON — not just the same
   * struct log count but the same fields and values for every entry.
   */
  @Test
  public void streamingAndAccumulatingPathsMatchForZeroStackValues() throws Exception {
    final Block block = buildBlockWithPush0();
    final DebugTraceBlockStreamer streamer =
        new DebugTraceBlockStreamer(
            block, TraceOptions.DEFAULT, fixture.getProtocolSchedule(), blockchainQueries);

    // streaming path
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    streamer.streamTo(out, mapper, () -> true);
    final JsonNode streamedRoot = mapper.readTree(out.toByteArray());

    // accumulating path
    final List<Object> accumulated = streamer.accumulateAll(() -> true);
    final JsonNode accRoot = mapper.readTree(mapper.writeValueAsBytes(accumulated));

    assertThat(streamedRoot)
        .as("streaming and accumulating paths must produce identical JSON")
        .isEqualTo(accRoot);
  }

  /** Stack arrays for PUSH0 operations must contain "0x0" entries. */
  @Test
  public void zeroStackEntriesAreEncodedAsHexZero() throws Exception {
    final Block block = buildBlockWithPush0();
    final DebugTraceBlockStreamer streamer =
        new DebugTraceBlockStreamer(
            block, TraceOptions.DEFAULT, fixture.getProtocolSchedule(), blockchainQueries);

    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    streamer.streamTo(out, mapper, () -> true);
    final JsonNode root = mapper.readTree(out.toByteArray());
    final JsonNode structLogs = root.get(0).get("result").get("structLogs");

    // Find a struct log whose stack contains "0x0" (from PUSH0)
    boolean foundZero = false;
    for (final JsonNode log : structLogs) {
      if (log.has("stack")) {
        for (final JsonNode item : log.get("stack")) {
          if ("0x0".equals(item.asText())) {
            foundZero = true;
            break;
          }
        }
      }
      if (foundZero) break;
    }
    assertThat(foundZero).as("Expected at least one 0x0 stack entry from PUSH0").isTrue();
  }

  /**
   * Memory words must be 32-byte zero-padded hex (66 chars each), not compact. Streaming and
   * accumulating paths must agree on the format when memory tracing is enabled.
   */
  @Test
  public void streamingAndAccumulatingPathsMatchWithMemoryEnabled() throws Exception {
    final TraceOptions withMemory =
        new TraceOptions(
            TracerType.OPCODE_TRACER,
            OpCodeTracerConfigBuilder.createFrom(TraceOptions.DEFAULT.opCodeTracerConfig())
                .traceMemory(true)
                .build(),
            java.util.Map.of());
    // Call the increment contract (uses MSTORE) with input=5
    final Address incrementContract =
        Address.fromHexString("0x0030000000000000000000000000000000000000");
    final Transaction tx =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .nonce(0)
            .maxPriorityFeePerGas(Wei.of(5))
            .maxFeePerGas(Wei.of(7))
            .gasLimit(100_000L)
            .to(incrementContract)
            .value(Wei.ZERO)
            .payload(Bytes32.leftPad(Bytes.of(5)))
            .chainId(BigInteger.valueOf(42))
            .signAndBuild(KEY_PAIR);
    final Block block = buildBlock(tx);
    final DebugTraceBlockStreamer streamer =
        new DebugTraceBlockStreamer(
            block, withMemory, fixture.getProtocolSchedule(), blockchainQueries);

    // streaming path
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    streamer.streamTo(out, mapper, () -> true);
    final JsonNode streamedRoot = mapper.readTree(out.toByteArray());

    // accumulating path
    final List<Object> accumulated = streamer.accumulateAll(() -> true);
    final JsonNode accRoot = mapper.readTree(mapper.writeValueAsBytes(accumulated));

    // Verify memory entries are actually present (MSTORE creates memory)
    final JsonNode structLogs = streamedRoot.get(0).get("result").get("structLogs");
    assertThat(structLogs.size()).as("trace must have struct logs").isGreaterThan(0);
    boolean hasMemory = false;
    for (final JsonNode log : structLogs) {
      if (log.has("memory")) {
        hasMemory = true;
        break;
      }
    }
    assertThat(hasMemory)
        .as(
            "trace with traceMemory=true must contain memory entries, got %d logs: %s",
            structLogs.size(), structLogs.get(0))
        .isTrue();

    assertThat(streamedRoot)
        .as("streaming and accumulating paths must produce identical JSON with memory enabled")
        .isEqualTo(accRoot);
  }

  /**
   * The {@code memory} field must hold the memory space <em>before</em> the operation executes, as
   * documented.
   */
  @Test
  public void memoryIsReportedAsOfBeforeTheOperationExecutes() throws Exception {
    final TraceOptions withMemory =
        new TraceOptions(
            TracerType.OPCODE_TRACER,
            OpCodeTracerConfigBuilder.createFrom(TraceOptions.DEFAULT.opCodeTracerConfig())
                .traceMemory(true)
                .build(),
            java.util.Map.of());
    final DebugTraceBlockStreamer streamer =
        new DebugTraceBlockStreamer(
            buildBlock(contractCreation(DOUBLE_MSTORE_INIT_CODE)),
            withMemory,
            fixture.getProtocolSchedule(),
            blockchainQueries);

    for (final JsonNode structLogs : streamedAndAccumulatedStructLogs(streamer)) {
      assertThat(structLogs.get(2).get("op").asText()).isEqualTo("MSTORE");
      assertThat(structLogs.get(2).has("memory")).isFalse();

      assertThat(structLogs.get(5).get("op").asText()).isEqualTo("MSTORE");
      assertThat(structLogs.get(5).get("memory")).hasSize(1);

      assertThat(structLogs.get(6).get("op").asText()).isEqualTo("STOP");
      assertThat(structLogs.get(6).get("memory")).hasSize(2);
    }
  }

  @Test
  public void refundIsReportedAsOfBeforeTheOperationExecutes() throws Exception {
    final DebugTraceBlockStreamer streamer =
        new DebugTraceBlockStreamer(
            buildBlock(contractCreation(SSTORE_RESTORE_INIT_CODE)),
            TraceOptions.DEFAULT,
            fixture.getProtocolSchedule(),
            blockchainQueries);

    for (final JsonNode structLogs : streamedAndAccumulatedStructLogs(streamer)) {
      assertThat(structLogs.get(5).get("op").asText()).isEqualTo("SSTORE");
      assertThat(structLogs.get(5).path("refund").asLong(0)).isZero();

      assertThat(structLogs.get(6).get("op").asText()).isEqualTo("STOP");
      assertThat(structLogs.get(6).path("refund").asLong(0)).isPositive();
    }
  }

  @Test
  public void storageIsReportedOnlyForTheSlotTouchedBySloadAndSstore() throws Exception {
    final DebugTraceBlockStreamer streamer =
        new DebugTraceBlockStreamer(
            buildBlock(contractCreation(SSTORE_SLOAD_INIT_CODE)),
            TraceOptions.DEFAULT,
            fixture.getProtocolSchedule(),
            blockchainQueries);

    final List<JsonNode> structLogs = streamedAndAccumulatedStructLogs(streamer);
    assertThat(structLogs.get(0)).isEqualTo(structLogs.get(1));

    final JsonNode streamed = structLogs.get(0);
    final String zero = UInt256.ZERO.toHexString();
    final String one = UInt256.ONE.toHexString();
    final String five = UInt256.valueOf(5).toHexString();
    for (int i = 0; i < streamed.size(); i++) {
      assertThat(streamed.get(i).has("storage"))
          .as("step %d", i)
          .isEqualTo(i == 2 || i == 4 || i == 7);
    }
    assertThat(streamed.get(2).get("op").asText()).isEqualTo("SSTORE");
    assertThat(streamed.get(2).get("storage").get(zero).asText()).isEqualTo(one);
    assertThat(streamed.get(4).get("op").asText()).isEqualTo("SLOAD");
    assertThat(streamed.get(4).get("storage").get(five).asText()).isEqualTo(zero);
    assertThat(streamed.get(7).get("op").asText()).isEqualTo("SLOAD");
    assertThat(streamed.get(7).get("storage").get(zero).asText()).isEqualTo(one);
  }

  @Test
  public void returnDataIsReportedAsOfBeforeTheOperationExecutes() throws Exception {
    final TraceOptions withReturnData =
        new TraceOptions(
            TracerType.OPCODE_TRACER,
            OpCodeTracerConfigBuilder.createFrom(TraceOptions.DEFAULT.opCodeTracerConfig())
                .traceReturnData(true)
                .build(),
            java.util.Map.of());
    final DebugTraceBlockStreamer streamer =
        new DebugTraceBlockStreamer(
            buildBlock(contractCreation(DOUBLE_STATICCALL_INIT_CODE)),
            withReturnData,
            fixture.getProtocolSchedule(),
            blockchainQueries);

    for (final JsonNode structLogs : streamedAndAccumulatedStructLogs(streamer)) {
      assertThat(structLogs.get(6).get("op").asText()).isEqualTo("STATICCALL");
      assertThat(structLogs.get(6).has("returnData")).isFalse();

      assertThat(structLogs.get(7).get("op").asText()).isEqualTo("POP");
      assertThat(structLogs.get(7).has("returnData")).isTrue();

      assertThat(structLogs.get(14).get("op").asText()).isEqualTo("STATICCALL");
      assertThat(structLogs.get(14).has("returnData")).isTrue();
    }
  }

  // ── helpers ──────────────────────────────────────────────────────────────

  private Block buildBlockWithCalldata(final Bytes calldata, final int nonce) {
    final Transaction tx =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .nonce(nonce)
            .maxPriorityFeePerGas(Wei.of(5))
            .maxFeePerGas(Wei.of(7))
            .gasLimit(200_000L)
            .to(REVERT_CONTRACT)
            .value(Wei.ZERO)
            .payload(calldata)
            .chainId(BigInteger.valueOf(42))
            .signAndBuild(KEY_PAIR);

    return buildBlock(tx);
  }

  private Block buildBlockWithPush0() {
    // Contract creation tx whose init code uses PUSH0 opcodes
    final Transaction tx =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .nonce(0)
            .maxPriorityFeePerGas(Wei.of(5))
            .maxFeePerGas(Wei.of(7))
            .gasLimit(200_000L)
            .value(Wei.ZERO)
            .payload(PUSH0_INIT_CODE)
            .chainId(BigInteger.valueOf(42))
            .signAndBuild(KEY_PAIR);

    return buildBlock(tx);
  }

  private Transaction contractCreation(final Bytes initCode) {
    return Transaction.builder()
        .type(TransactionType.EIP1559)
        .nonce(0)
        .maxPriorityFeePerGas(Wei.of(5))
        .maxFeePerGas(Wei.of(7))
        .gasLimit(200_000L)
        .value(Wei.ZERO)
        .payload(initCode)
        .chainId(BigInteger.valueOf(42))
        .signAndBuild(KEY_PAIR);
  }

  private List<JsonNode> streamedAndAccumulatedStructLogs(final DebugTraceBlockStreamer streamer)
      throws Exception {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    streamer.streamTo(out, mapper, () -> true);
    final JsonNode streamed = mapper.readTree(out.toByteArray());
    final JsonNode accumulated =
        mapper.readTree(mapper.writeValueAsBytes(streamer.accumulateAll(() -> true)));
    return List.of(
        streamed.get(0).get("result").get("structLogs"),
        accumulated.get(0).get("result").get("structLogs"));
  }

  private Block buildBlock(final Transaction tx) {
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

  /** Returns the "reason" field from the first struct log that has one, or null. */
  private String findRevertReason(final JsonNode structLogs) {
    for (final JsonNode log : structLogs) {
      if (log.has("reason")) {
        return log.get("reason").asText();
      }
    }
    return null;
  }
}
