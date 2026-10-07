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
package org.hyperledger.besu.evm.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.evm.processor.AbstractMessageProcessorTest.ContextTracer.TRACE_TYPE.CONTEXT_ENTER;
import static org.hyperledger.besu.evm.processor.AbstractMessageProcessorTest.ContextTracer.TRACE_TYPE.CONTEXT_EXIT;
import static org.hyperledger.besu.evm.processor.AbstractMessageProcessorTest.ContextTracer.TRACE_TYPE.CONTEXT_RE_ENTER;
import static org.hyperledger.besu.evm.processor.AbstractMessageProcessorTest.ContextTracer.TRACE_TYPE.POST_EXECUTION;
import static org.hyperledger.besu.evm.processor.AbstractMessageProcessorTest.ContextTracer.TRACE_TYPE.PRE_EXECUTION;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.EvmSpecVersion;
import org.hyperledger.besu.evm.fluent.EVMExecutor;
import org.hyperledger.besu.evm.fluent.EvmSpec;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.toy.ToyWorld;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
abstract class AbstractMessageProcessorTest<T extends AbstractMessageProcessor> {
  @Mock MessageFrame messageFrame;
  @Mock OperationTracer operationTracer;
  @Mock Deque<MessageFrame> messageFrameStack;

  protected abstract T getAbstractMessageProcessor();

  @Test
  void shouldTraceExitForContext() {
    when(messageFrame.getWorldUpdater()).thenReturn(new ToyWorld());
    when(messageFrame.getState()).thenReturn(MessageFrame.State.COMPLETED_SUCCESS);
    when(messageFrame.getMessageFrameStack()).thenReturn(messageFrameStack);

    getAbstractMessageProcessor().process(messageFrame, operationTracer);

    // As the only MessageFrame state will be COMPLETED_SUCCESS, only a contextExit is expected
    verify(operationTracer, times(1)).traceContextExit(messageFrame);
  }

  @Test
  void shouldTraceExitEvenIfContextFailed() {
    when(messageFrame.getState()).thenReturn(MessageFrame.State.COMPLETED_FAILED);
    when(messageFrame.getMessageFrameStack()).thenReturn(messageFrameStack);

    getAbstractMessageProcessor().process(messageFrame, operationTracer);

    // As the only MessageFrame state will be COMPLETED_FAILED, only a contextExit is expected
    verify(operationTracer, times(1)).traceContextExit(messageFrame);
  }

  @Test
  void shouldTraceContextEnterExitForEip3155Test() {
    final EVMExecutor executor = new EVMExecutor(EvmSpec.evmSpec(EvmSpecVersion.SHANGHAI));
    final ContextTracer contextTracer = new ContextTracer();

    executor.tracer(contextTracer);
    executor.gas(10_000_000_000L);

    /*
     The byte code below is taken from https://eips.ethereum.org/EIPS/eip-3155

     It produces the following trace:

       0: {"pc":0,"op":96,"gas":"0x2540be400","gasCost":"0x3","memory":"0x","memSize":0,"stack":[],"returnStack":[],"returnData":"0x","depth":1,"refund":0,"opName":"PUSH1","error":""}
       1: {"pc":2,"op":128,"gas":"0x2540be3fd","gasCost":"0x3","memory":"0x","memSize":0,"stack":["0x40"],"returnStack":[],"returnData":"0x","depth":1,"refund":0,"opName":"DUP1","error":""}
       2: {"pc":3,"op":83,"gas":"0x2540be3fa","gasCost":"0xc","memory":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":["0x40","0x40"],"returnStack":[],"returnData":"0x","depth":1,"refund":0,"opName":"MSTORE8","error":""}
       3: {"pc":4,"op":96,"gas":"0x2540be3ee","gasCost":"0x3","memory":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":[],"returnStack":[],"returnData":"0x","depth":1,"refund":0,"opName":"PUSH1","error":""}
       4: {"pc":6,"op":96,"gas":"0x2540be3eb","gasCost":"0x3","memory":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":["0x40"],"returnStack":[],"returnData":"0x","depth":1,"refund":0,"opName":"PUSH1","error":""}
       5: {"pc":8,"op":85,"gas":"0x2540be3e8","gasCost":"0x4e20","memory":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":["0x40","0x40"],"returnStack":[],"returnData":"0x","depth":1,"refund":0,"opName":"SSTORE","error":""}
       6: {"pc":9,"op":96,"gas":"0x2540b95c8","gasCost":"0x3","memory":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":[],"returnStack":[],"returnData":"0x","depth":1,"refund":0,"opName":"PUSH1","error":""}
       7: {"pc":11,"op":96,"gas":"0x2540b95c5","gasCost":"0x3","memory":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":["0x40"],"returnStack":[],"returnData":"0x","depth":1,"refund":0,"opName":"PUSH1","error":""}
       8: {"pc":13,"op":96,"gas":"0x2540b95c2","gasCost":"0x3","memory":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":["0x40","0x0"],"returnStack":[],"returnData":"0x","depth":1,"refund":0,"opName":"PUSH1","error":""}
       9: {"pc":15,"op":96,"gas":"0x2540b95bf","gasCost":"0x3","memory":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":["0x40","0x0","0x40"],"returnStack":[],"returnData":"0x","depth":1,"refund":0,"opName":"PUSH1","error":""}
      10: {"pc":17,"op":96,"gas":"0x2540b95bc","gasCost":"0x3","memory":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":["0x40","0x0","0x40","0x0"],"returnStack":[],"returnData":"0x","depth":1,"refund":0,"opName":"PUSH1","error":""}
      11: {"pc":19,"op":90,"gas":"0x2540b95b9","gasCost":"0x2","memory":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":["0x40","0x0","0x40","0x0","0x2"],"returnStack":[],"returnData":"0x","depth":1,"refund":0,"opName":"GAS","error":""}
      12: {"pc":20,"op":250,"gas":"0x2540b95b7","gasCost":"0x24abb676c","memory":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":["0x40","0x0","0x40","0x0","0x2","0x2540b95b7"],"returnStack":[],"returnData":"0x","depth":1,"refund":0,"opName":"STATICCALL","error":""}
      13: {"pc":21,"op":96,"gas":"0x2540b92a7","gasCost":"0x3","memory":"0xf5a5fd42d16a20302798ef6ed309979b43003d2320d9f0e8ea9831a92759fb4b00000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":["0x1"],"returnStack":[],"returnData":"0xf5a5fd42d16a20302798ef6ed309979b43003d2320d9f0e8ea9831a92759fb4b","depth":1,"refund":0,"opName":"PUSH1","error":""}
      14: {"pc":23,"op":243,"gas":"0x2540b92a4","gasCost":"0x0","memory":"0xf5a5fd42d16a20302798ef6ed309979b43003d2320d9f0e8ea9831a92759fb4b00000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":["0x1","0x40"],"returnStack":[],"returnData":"0xf5a5fd42d16a20302798ef6ed309979b43003d2320d9f0e8ea9831a92759fb4b","depth":1,"refund":0,"opName":"RETURN","error":""}
      15: {"stateRoot":"2eef130ec61805516c1f050720b520619787704a5dd826a39aeefb850f83acfd", "output":"40","gasUsed":"0x515c","time":350855}
    */
    final Bytes codeBytes =
        Bytes.fromHexString("0x604080536040604055604060006040600060025afa6040f3");
    executor.execute(codeBytes, Bytes.EMPTY, Wei.ZERO, Address.ZERO);

    final List<ContextTracer.TRACE_TYPE> expectedTraces =
        Arrays.asList(
            CONTEXT_ENTER, // Entry in root context
            PRE_EXECUTION, // PUSH1
            POST_EXECUTION, // PUSH1
            PRE_EXECUTION, // DUP1
            POST_EXECUTION, // DUP1
            PRE_EXECUTION, // MSTORE8
            POST_EXECUTION, // MSTORE8
            PRE_EXECUTION, // PUSH1
            POST_EXECUTION, // PUSH1
            PRE_EXECUTION, // PUSH1
            POST_EXECUTION, // PUSH1
            PRE_EXECUTION, // SSTORE
            POST_EXECUTION, // SSTORE
            PRE_EXECUTION, // PUSH1
            POST_EXECUTION, // PUSH1
            PRE_EXECUTION, // PUSH1
            POST_EXECUTION, // PUSH1
            PRE_EXECUTION, // PUSH1
            POST_EXECUTION, // PUSH1
            PRE_EXECUTION, // PUSH1
            POST_EXECUTION, // PUSH1
            PRE_EXECUTION, // PUSH1
            POST_EXECUTION, // PUSH1
            PRE_EXECUTION, // GAS
            POST_EXECUTION, // GAS
            PRE_EXECUTION, // STATICCALL
            POST_EXECUTION, // STATICCALL
            CONTEXT_ENTER, // STATICCALL
            CONTEXT_EXIT, // STATICCALL
            CONTEXT_RE_ENTER, // Re-entry in root context post-STATICALL
            PRE_EXECUTION, // PUSH1
            POST_EXECUTION, // PUSH1
            PRE_EXECUTION, // RETURN
            POST_EXECUTION, // RETURN
            CONTEXT_EXIT // Exiting root context
            );

    assertThat(contextTracer.traceHistory()).isEqualTo(expectedTraces);
  }

  static class ContextTracer implements OperationTracer {
    enum TRACE_TYPE {
      PRE_EXECUTION,
      POST_EXECUTION,
      CONTEXT_ENTER,
      CONTEXT_RE_ENTER,
      CONTEXT_EXIT
    }

    private final List<TRACE_TYPE> traceHistory = new ArrayList<>();

    @Override
    public void tracePreExecution(final MessageFrame frame) {
      traceHistory.add(PRE_EXECUTION);
    }

    @Override
    public void tracePostExecution(
        final MessageFrame frame, final Operation.OperationResult operationResult) {
      traceHistory.add(POST_EXECUTION);
    }

    @Override
    public void traceContextEnter(final MessageFrame frame) {
      traceHistory.add(TRACE_TYPE.CONTEXT_ENTER);
    }

    @Override
    public void traceContextReEnter(final MessageFrame frame) {
      traceHistory.add(CONTEXT_RE_ENTER);
    }

    @Override
    public void traceContextExit(final MessageFrame frame) {
      traceHistory.add(TRACE_TYPE.CONTEXT_EXIT);
    }

    public List<TRACE_TYPE> traceHistory() {
      return traceHistory;
    }
  }
}
