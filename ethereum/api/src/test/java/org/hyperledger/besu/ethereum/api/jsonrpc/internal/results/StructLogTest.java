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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.evm.tracing.TraceFrame;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class StructLogTest {

  @Mock private TraceFrame traceFrame;

  private StructLog structLog;
  private ObjectMapper objectMapper;

  @BeforeEach
  public void setUp() {
    objectMapper = new ObjectMapper();
  }

  @Test
  public void constructorShouldInitializeFields() throws Exception {
    // Given
    final String op = "PUSH1";
    final int depth = 0;
    final long gas = 100L;
    final OptionalLong gasCost = OptionalLong.of(10L);
    final String[] memory =
        new String[] {"0x01", "0x02", "0x0003", "0x04", "0x00000005", "0x000000"};
    final String[] stack = new String[] {"0x00000000000000", "0x01", "0x000002", "0x03"};
    final Map<String, String> storage =
        Map.of(
            "0x01", "0x2233333",
            "0x02", "0x4455667");
    final String reason = "0x0001";

    // Mock TraceFrame behaviors
    when(traceFrame.getDepth()).thenReturn(depth);
    when(traceFrame.getGasRemaining()).thenReturn(gas);
    when(traceFrame.getGasCost()).thenReturn(gasCost);
    when(traceFrame.getMemory())
        .thenReturn(
            Optional.of(
                Arrays.stream(memory)
                    .map(Bytes::fromHexString) // Convert each string to Bytes
                    .toArray(Bytes[]::new)));
    when(traceFrame.getOpcode()).thenReturn(op);
    when(traceFrame.getPc()).thenReturn(1);
    when(traceFrame.getStack())
        .thenReturn(
            Optional.of(
                Arrays.stream(stack)
                    .map(Bytes::fromHexString) // Convert each string to Bytes
                    .toArray(Bytes[]::new)));
    Map<UInt256, UInt256> storageMap = new HashMap<>();
    for (Map.Entry<String, String> entry : storage.entrySet()) {
      storageMap.put(
          UInt256.fromHexString(entry.getKey()), UInt256.fromHexString(entry.getValue()));
    }
    when(traceFrame.getStorage()).thenReturn(Optional.of(storageMap));
    when(traceFrame.getRevertReason()).thenReturn(Optional.of(Bytes.fromHexString(reason)));

    // When
    structLog = new StructLog(traceFrame);

    // Then
    assertThat(structLog.depth()).isEqualTo(depth + 1);
    assertThat(structLog.gas()).isEqualTo(gas);
    assertThat(structLog.gasCost()).isEqualTo(gasCost.getAsLong());
    assertThat(structLog.memory())
        .isEqualTo(
            new String[] {
              "0x0000000000000000000000000000000000000000000000000000000000000001",
              "0x0000000000000000000000000000000000000000000000000000000000000002",
              "0x0000000000000000000000000000000000000000000000000000000000000003",
              "0x0000000000000000000000000000000000000000000000000000000000000004",
              "0x0000000000000000000000000000000000000000000000000000000000000005",
              "0x0000000000000000000000000000000000000000000000000000000000000000"
            });
    assertThat(structLog.op()).isEqualTo(op);
    assertThat(structLog.pc()).isEqualTo(1);
    assertThat(structLog.stack()).isEqualTo(new String[] {"0x0", "0x1", "0x2", "0x3"});
    assertThat(structLog.storage())
        .isEqualTo(
            Map.of(
                "0x0000000000000000000000000000000000000000000000000000000000000001",
                "0x0000000000000000000000000000000000000000000000000000000002233333",
                "0x0000000000000000000000000000000000000000000000000000000000000002",
                "0x0000000000000000000000000000000000000000000000000000000004455667"));
    assertThat(structLog.reason()).isEqualTo("0x1");
  }

  @Test
  public void shouldGenerateJsonCorrectly() throws Exception {
    final String op = "PUSH1";
    final int depth = 0;
    final long gas = 100L;
    final OptionalLong gasCost = OptionalLong.of(10L);
    final String[] memory =
        new String[] {"0x01", "0x02", "0x0023", "0x04", "0x000000e5", "0x000000"};
    final String[] stack = new String[] {"0x00000000000000", "0x01", "0x000002", "0x03"};
    final Map<String, String> storage =
        Map.of(
            "0x01", "0x2233333",
            "0x02", "0x4455667");
    final String reason = "0x53756e696665642066756e6473";

    // Mock TraceFrame behaviors
    when(traceFrame.getDepth()).thenReturn(depth);
    when(traceFrame.getGasRemaining()).thenReturn(gas);
    when(traceFrame.getGasCost()).thenReturn(gasCost);
    when(traceFrame.getMemory())
        .thenReturn(
            Optional.of(
                Arrays.stream(memory)
                    .map(Bytes::fromHexString) // Convert each string to Bytes
                    .toArray(Bytes[]::new)));
    when(traceFrame.getOpcode()).thenReturn(op);
    when(traceFrame.getPc()).thenReturn(1);
    when(traceFrame.getStack())
        .thenReturn(
            Optional.of(
                Arrays.stream(stack)
                    .map(Bytes::fromHexString) // Convert each string to Bytes
                    .toArray(Bytes[]::new)));
    Map<UInt256, UInt256> storageMap = new HashMap<>();
    for (Map.Entry<String, String> entry : storage.entrySet()) {
      storageMap.put(
          UInt256.fromHexString(entry.getKey()), UInt256.fromHexString(entry.getValue()));
    }
    when(traceFrame.getStorage()).thenReturn(Optional.of(storageMap));
    when(traceFrame.getRevertReason()).thenReturn(Optional.of(Bytes.fromHexString(reason)));

    // When
    structLog = new StructLog(traceFrame);

    // Use Jackson to serialize the StructLog object to a JSON string
    String json = objectMapper.writeValueAsString(structLog);

    // Define the expected JSON output
    String expectedJson =
        "{"
            + "\"pc\":1,"
            + "\"op\":\"PUSH1\","
            + "\"gas\":100,"
            + "\"gasCost\":10,"
            + "\"depth\":1,"
            + "\"stack\":[\"0x0\",\"0x1\",\"0x2\",\"0x3\"],"
            + "\"memory\":[\"0x0000000000000000000000000000000000000000000000000000000000000001\",\"0x0000000000000000000000000000000000000000000000000000000000000002\",\"0x0000000000000000000000000000000000000000000000000000000000000023\",\"0x0000000000000000000000000000000000000000000000000000000000000004\",\"0x00000000000000000000000000000000000000000000000000000000000000e5\",\"0x0000000000000000000000000000000000000000000000000000000000000000\"],"
            + "\"storage\":{\"0x0000000000000000000000000000000000000000000000000000000000000001\":\"0x0000000000000000000000000000000000000000000000000000000002233333\",\"0x0000000000000000000000000000000000000000000000000000000000000002\":\"0x0000000000000000000000000000000000000000000000000000000004455667\"},"
            + "\"reason\":\"0x53756e696665642066756e6473\""
            + "}";

    // Then
    assertThat(json).isEqualTo(expectedJson); // Verify the generated JSON matches the expected JSON
  }

  @Test
  public void testToCompactHexEmptyWithPrefix() {
    Bytes emptyBytes = Bytes.EMPTY;
    String result = StructLog.toCompactHex(emptyBytes, true);
    assertEquals("0x0", result, "Expected '0x0' for an empty byte array with prefix");
  }

  @Test
  public void testToCompactHexEmptyWithoutPrefix() {
    Bytes emptyBytes = Bytes.EMPTY;
    String result = StructLog.toCompactHex(emptyBytes, false);
    assertEquals("0", result, "Expected '0' for an empty byte array without prefix");
  }

  @Test
  public void testToCompactHexSingleByteWithPrefix() {
    Bytes bytes = Bytes.fromHexString("0x01");
    String result = StructLog.toCompactHex(bytes, true);
    assertEquals("0x1", result, "Expected '0x1' for the byte 0x01 with prefix");
  }

  @Test
  public void testToCompactHexSingleByteWithoutPrefix() {
    Bytes bytes = Bytes.fromHexString("0x01");
    String result = StructLog.toCompactHex(bytes, false);
    assertEquals("1", result, "Expected '1' for the byte 0x01 without prefix");
  }

  @Test
  public void testToCompactHexMultipleBytesWithPrefix() {
    Bytes bytes = Bytes.fromHexString("0x010203");
    String result = StructLog.toCompactHex(bytes, true);
    assertEquals("0x10203", result, "Expected '0x10203' for the byte array 0x010203 with prefix");
  }

  @Test
  public void testToCompactHexMultipleBytesWithoutPrefix() {
    Bytes bytes = Bytes.fromHexString("0x010203");
    String result = StructLog.toCompactHex(bytes, false);
    assertEquals("10203", result, "Expected '10203' for the byte array 0x010203 without prefix");
  }

  @Test
  public void testToCompactHexWithLeadingZeros() {
    Bytes bytes = Bytes.fromHexString("0x0001");
    String result = StructLog.toCompactHex(bytes, true);
    assertEquals(
        "0x1",
        result,
        "Expected '0x1' for the byte array 0x0001 with prefix (leading zeros removed)");
  }

  @Test
  public void testToCompactHexWithLargeData() {
    Bytes bytes = Bytes.fromHexString("0x0102030405060708090a");
    String result = StructLog.toCompactHex(bytes, true);
    assertEquals("0x102030405060708090a", result, "Expected correct hex output for large data");
  }
}
