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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.precompile.PrecompileContractRegistry;
import org.hyperledger.besu.evm.precompile.PrecompiledContract;

import java.util.Map;
import java.util.TreeMap;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FourByteTracerTest {

  private PrecompileContractRegistry registry;
  private FourByteTracer tracer;

  @BeforeEach
  void setUp() {
    registry = mock(PrecompileContractRegistry.class);
    tracer = new FourByteTracer(registry);
  }

  @Test
  void countsRootAndNestedCallsAndAggregatesDuplicates() {
    final MessageFrame rootFrame =
        mockFrame(
            MessageFrame.Type.MESSAGE_CALL,
            Address.fromHexString("0x1111"),
            Bytes.concatenate(Bytes.fromHexString("0xa9059cbb"), Bytes.repeat((byte) 0x11, 64)));

    final MessageFrame childFrame1 =
        mockFrame(
            MessageFrame.Type.MESSAGE_CALL,
            Address.fromHexString("0x2222"),
            Bytes.concatenate(Bytes.fromHexString("0x70a08231"), Bytes.repeat((byte) 0x22, 32)));

    final MessageFrame childFrame2 =
        mockFrame(
            MessageFrame.Type.MESSAGE_CALL,
            Address.fromHexString("0x3333"),
            Bytes.concatenate(Bytes.fromHexString("0x70a08231"), Bytes.repeat((byte) 0x33, 32)));

    tracer.traceContextEnter(rootFrame);
    tracer.traceContextEnter(childFrame1);
    tracer.traceContextEnter(childFrame2);

    final FourByteTracerResult result = tracer.buildResult();
    final Map<String, Integer> counts = result.selectorCounts();

    assertThat(counts).containsExactly(entry("0x70a08231-32", 2), entry("0xa9059cbb-64", 1));
    assertThat(counts.keySet()).containsExactly("0x70a08231-32", "0xa9059cbb-64");
  }

  @Test
  void skipsCreateFramesAndShortInput() {
    final MessageFrame createFrame =
        mockFrame(
            MessageFrame.Type.CONTRACT_CREATION,
            Address.fromHexString("0x1111"),
            Bytes.repeat((byte) 0xaa, 100));

    final MessageFrame shortFrame =
        mockFrame(
            MessageFrame.Type.MESSAGE_CALL,
            Address.fromHexString("0x2222"),
            Bytes.fromHexString("0x123456"));

    tracer.traceContextEnter(createFrame);
    tracer.traceContextEnter(shortFrame);

    assertThat(tracer.buildResult().selectorCounts()).isEmpty();
  }

  @Test
  void skipsPrecompileFrames() {
    final Address precompileAddress = Address.fromHexString("0x04");
    when(registry.get(precompileAddress)).thenReturn(mock(PrecompiledContract.class));

    final MessageFrame frame =
        mockFrame(
            MessageFrame.Type.MESSAGE_CALL,
            precompileAddress,
            Bytes.concatenate(Bytes.fromHexString("0x12345678"), Bytes.repeat((byte) 0x00, 32)));

    tracer.traceContextEnter(frame);

    assertThat(tracer.buildResult().selectorCounts()).isEmpty();
  }

  @Test
  void resetsBetweenTransactions() {
    final MessageFrame frame =
        mockFrame(
            MessageFrame.Type.MESSAGE_CALL,
            Address.fromHexString("0x1111"),
            Bytes.concatenate(Bytes.fromHexString("0x12345678"), Bytes.repeat((byte) 0x00, 32)));

    tracer.traceContextEnter(frame);

    final FourByteTracerResult firstResult = tracer.buildResult();
    assertThat(firstResult.selectorCounts()).hasSize(1);

    tracer.traceStartTransaction(null, mock(Transaction.class));

    final FourByteTracerResult secondResult = tracer.buildResult();
    assertThat(secondResult.selectorCounts()).isEmpty();
    assertThat(firstResult.selectorCounts()).hasSize(1);
  }

  @Test
  void returnsIndependentTreeMapCopy() {
    final MessageFrame frame =
        mockFrame(
            MessageFrame.Type.MESSAGE_CALL,
            Address.fromHexString("0x1111"),
            Bytes.concatenate(Bytes.fromHexString("0x12345678"), Bytes.repeat((byte) 0x00, 32)));

    tracer.traceContextEnter(frame);

    final FourByteTracerResult result = tracer.buildResult();
    assertThat(result.selectorCounts()).isInstanceOf(TreeMap.class);

    result.selectorCounts().clear();
    assertThat(tracer.buildResult().selectorCounts()).hasSize(1);
  }

  private static MessageFrame mockFrame(
      final MessageFrame.Type type, final Address contractAddress, final Bytes inputData) {
    final MessageFrame frame = mock(MessageFrame.class);
    when(frame.getType()).thenReturn(type);
    when(frame.getContractAddress()).thenReturn(contractAddress);
    when(frame.getInputData()).thenReturn(inputData);
    return frame;
  }
}
