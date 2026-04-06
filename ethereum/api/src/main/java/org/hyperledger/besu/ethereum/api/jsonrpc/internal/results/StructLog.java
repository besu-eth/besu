/*
 * Copyright ConsenSys AG.
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

import org.hyperledger.besu.evm.tracing.TraceFrame;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

@JsonPropertyOrder({"pc", "op", "gas", "gasCost", "depth", "refund", "stack", "memory", "storage"})
public class StructLog {

  private static final char[] hexChars = "0123456789abcdef".toCharArray();
  private final int depth;
  private final long gas;
  private final long gasCost;
  private final long refund;
  private final String[] memory;
  private final String op;
  private final int pc;
  private final String[] stack;
  private final Object storage;
  private final String reason;

  public StructLog(final TraceFrame traceFrame) {
    depth = traceFrame.getDepth() + 1;
    gas = traceFrame.getGasRemaining();
    gasCost = traceFrame.getGasCost().orElse(0L);
    refund = traceFrame.getGasRefund();
    memory =
        traceFrame
            .getMemory()
            .map(a -> Arrays.stream(a).map(StructLog::toBytes32Hex).toArray(String[]::new))
            .orElse(null);
    op = traceFrame.getOpcode();
    pc = traceFrame.getPc();
    stack =
        traceFrame
            .getStack()
            .map(
                a ->
                    Arrays.stream(a).map(bytes -> toCompactHex(bytes, true)).toArray(String[]::new))
            .orElse(null);

    storage = traceFrame.getStorage().map(StructLog::formatStorage).orElse(null);
    reason = traceFrame.getRevertReason().map(bytes -> toCompactHex(bytes, true)).orElse(null);
  }

  private static Map<String, String> formatStorage(final Map<UInt256, UInt256> storage) {
    final Map<String, String> formattedStorage = new TreeMap<>();
    storage.forEach((key, value) -> formattedStorage.put(toBytes32Hex(key), toBytes32Hex(value)));
    return formattedStorage;
  }

  /**
   * Encodes bytes as a 0x-prefixed, zero-padded 32-byte hex string (always 66 chars). Used for
   * memory words and storage keys/values per the execution-apis opcode tracer spec.
   */
  public static String toBytes32Hex(final Bytes abytes) {
    final byte[] raw = abytes.toArrayUnsafe();
    final StringBuilder sb = new StringBuilder(66);
    sb.append("0x");
    for (int i = raw.length; i < 32; i++) {
      sb.append("00");
    }
    for (final byte b : raw) {
      sb.append(hexChars[(b >> 4) & 0xF]);
      sb.append(hexChars[b & 0xF]);
    }
    return sb.toString();
  }

  @JsonGetter("depth")
  public int depth() {
    return depth;
  }

  @JsonGetter("gas")
  public long gas() {
    return gas;
  }

  @JsonGetter("gasCost")
  public long gasCost() {
    return gasCost;
  }

  @JsonGetter("refund")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public long refund() {
    return refund;
  }

  @JsonGetter("memory")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String[] memory() {
    return memory;
  }

  @JsonGetter("op")
  public String op() {
    return op;
  }

  @JsonGetter("pc")
  public int pc() {
    return pc;
  }

  @JsonGetter("stack")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String[] stack() {
    return stack;
  }

  @JsonGetter("storage")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Object storage() {
    return storage;
  }

  @JsonGetter("reason")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String reason() {
    return reason;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final StructLog structLog = (StructLog) o;
    return depth == structLog.depth
        && gas == structLog.gas
        && gasCost == structLog.gasCost
        && pc == structLog.pc
        && Arrays.equals(memory, structLog.memory)
        && Objects.equals(op, structLog.op)
        && Arrays.equals(stack, structLog.stack)
        && Objects.equals(storage, structLog.storage);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(depth, gas, gasCost, op, pc, storage);
    result = 31 * result + Arrays.hashCode(memory);
    result = 31 * result + Arrays.hashCode(stack);
    return result;
  }

  public static String toCompactHex(final Bytes abytes, final boolean prefix) {
    if (abytes.isEmpty()) {
      if (prefix) return "0x0";
      else return "0";
    }

    byte[] bytes = abytes.toArrayUnsafe();
    final int size = bytes.length;
    final StringBuilder result = new StringBuilder(prefix ? (size * 2) + 2 : size * 2);

    if (prefix) {
      result.append("0x");
    }

    boolean leadingZero = true;

    for (int i = 0; i < size; i++) {
      byte b = bytes[i];

      int highNibble = (b >> 4) & 0xF;
      if (!leadingZero || highNibble != 0) {
        result.append(hexChars[highNibble]);
        leadingZero = false;
      }

      int lowNibble = b & 0xF;
      if (!leadingZero || lowNibble != 0 || i == size - 1) {
        result.append(hexChars[lowNibble]);
        leadingZero = false;
      }
    }

    return result.toString();
  }
}
