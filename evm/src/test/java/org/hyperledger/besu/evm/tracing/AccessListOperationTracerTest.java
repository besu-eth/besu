/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.evm.tracing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.AccessListEntry;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;

import java.util.List;
import java.util.Set;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class AccessListOperationTracerTest {

  private static final Address TARGET =
      Address.fromHexString("0x00000000000000000000000000000000deadbe02");
  private static final Address STORAGE_ADDRESS =
      Address.fromHexString("0x000000000000000000000000000000000000aaaa");

  private MessageFrame frame;
  private Table<Address, Bytes32, Boolean> warmedUpStorage;

  @BeforeEach
  void setUp() {
    frame = mock(MessageFrame.class);
    warmedUpStorage = HashBasedTable.create();
    when(frame.getWarmedUpStorage()).thenReturn(warmedUpStorage);
  }

  private void mockOperation(final int opcode, final int stackSize) {
    final Operation operation = mock(Operation.class);
    when(operation.getOpcode()).thenReturn(opcode);
    when(frame.getCurrentOperation()).thenReturn(operation);
    when(frame.stackSize()).thenReturn(stackSize);
  }

  @ParameterizedTest
  @ValueSource(ints = {0x31, 0x3B, 0x3C, 0x3F, 0xFF})
  void accountTouchingOpcodeTargetIsIncludedWithEmptyStorageKeys(final int opcode) {
    final AccessListOperationTracer tracer = AccessListOperationTracer.create();
    mockOperation(opcode, 1);
    when(frame.getStackItem(0)).thenReturn(Bytes32.leftPad(TARGET.getBytes()));

    tracer.tracePreExecution(frame);
    tracer.tracePostExecution(frame, mock(OperationResult.class));

    assertThat(tracer.getAccessList()).containsExactly(new AccessListEntry(TARGET, List.of()));
  }

  @ParameterizedTest
  @ValueSource(ints = {0xF1, 0xF2, 0xF4, 0xFA})
  void callOpcodeTargetIsIncludedWithEmptyStorageKeys(final int opcode) {
    final AccessListOperationTracer tracer = AccessListOperationTracer.create();
    mockOperation(opcode, 7);
    when(frame.getStackItem(1)).thenReturn(Bytes32.leftPad(TARGET.getBytes()));

    tracer.tracePreExecution(frame);
    tracer.tracePostExecution(frame, mock(OperationResult.class));

    assertThat(tracer.getAccessList()).containsExactly(new AccessListEntry(TARGET, List.of()));
  }

  @ParameterizedTest
  @ValueSource(ints = {0xF1, 0xF2, 0xF4, 0xFA})
  void callOpcodeWithUnderflowingStackIsNotCaptured(final int opcode) {
    final AccessListOperationTracer tracer = AccessListOperationTracer.create();
    mockOperation(opcode, 4);

    tracer.tracePreExecution(frame);
    tracer.tracePostExecution(frame, mock(OperationResult.class));

    assertThat(tracer.getAccessList()).isEmpty();
  }

  @Test
  void accountTouchingOpcodeWithEmptyStackIsNotCaptured() {
    final AccessListOperationTracer tracer = AccessListOperationTracer.create();
    mockOperation(0x31, 0);

    tracer.tracePreExecution(frame);
    tracer.tracePostExecution(frame, mock(OperationResult.class));

    assertThat(tracer.getAccessList()).isEmpty();
  }

  @Test
  void nonCapturingOpcodeAddsNothing() {
    final AccessListOperationTracer tracer = AccessListOperationTracer.create();
    mockOperation(0x01, 2);

    tracer.tracePreExecution(frame);
    tracer.tracePostExecution(frame, mock(OperationResult.class));

    assertThat(tracer.getAccessList()).isEmpty();
  }

  @Test
  void excludedAddressIsNotIncluded() {
    final AccessListOperationTracer tracer = AccessListOperationTracer.create(Set.of(TARGET));
    mockOperation(0x31, 1);
    when(frame.getStackItem(0)).thenReturn(Bytes32.leftPad(TARGET.getBytes()));

    tracer.tracePreExecution(frame);
    tracer.tracePostExecution(frame, mock(OperationResult.class));

    assertThat(tracer.getAccessList()).isEmpty();
  }

  @Test
  void touchedAddressWithWarmedStorageIsListedOnceWithItsKeys() {
    final AccessListOperationTracer tracer = AccessListOperationTracer.create();
    final Bytes32 slot = Bytes32.leftPad(Bytes32.fromHexString("0x01"));
    warmedUpStorage.put(TARGET, slot, Boolean.TRUE);
    mockOperation(0x31, 1);
    when(frame.getStackItem(0)).thenReturn(Bytes32.leftPad(TARGET.getBytes()));

    tracer.tracePreExecution(frame);
    tracer.tracePostExecution(frame, mock(OperationResult.class));

    assertThat(tracer.getAccessList()).containsExactly(new AccessListEntry(TARGET, List.of(slot)));
  }

  @Test
  void storageOnlyEntriesAreUnchanged() {
    final AccessListOperationTracer tracer = AccessListOperationTracer.create();
    final Bytes32 slot = Bytes32.leftPad(Bytes32.fromHexString("0x02"));
    warmedUpStorage.put(STORAGE_ADDRESS, slot, Boolean.TRUE);
    mockOperation(0x54, 1);
    when(frame.getStackItem(0)).thenReturn(slot);

    tracer.tracePreExecution(frame);
    tracer.tracePostExecution(frame, mock(OperationResult.class));

    assertThat(tracer.getAccessList())
        .containsExactly(new AccessListEntry(STORAGE_ADDRESS, List.of(slot)));
  }
}
