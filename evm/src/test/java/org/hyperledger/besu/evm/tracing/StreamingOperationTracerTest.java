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
import static org.assertj.core.api.InstanceOfAssertFactories.STRING;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.EvmSpecVersion;
import org.hyperledger.besu.evm.fluent.EVMExecutor;
import org.hyperledger.besu.evm.fluent.EvmSpec;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder.OpCodeTracerConfig;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class StreamingOperationTracerTest {

  @Test
  void eip3155ModifiedTestCaseStrictTypes() {

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(baos);
    var executor = new EVMExecutor(EvmSpec.evmSpec(EvmSpecVersion.ISTANBUL));
    StreamingOperationTracer tracer =
        new StreamingOperationTracer(
            out,
            OpCodeTracerConfigBuilder.createFrom(OpCodeTracerConfig.DEFAULT)
                .traceMemory(true)
                .traceReturnData(true)
                .traceStorage(false)
                .eip3155Strict(true)
                .build());
    executor.tracer(tracer);
    executor.gas(10_000_000_000L);

    var codeBytes = Bytes.fromHexString("0x604080536040604055604060006040600060025afa6040f3");
    executor.execute(codeBytes, Bytes.EMPTY, Wei.ZERO, Address.ZERO);

    // differences from the EIP-3155 test case
    //  (a) the test case was written when EIP-2315 was part of the pending hard fork, so
    //      returnStack was a valid field.  It no longer appears in any traces.
    //  (b) the summary line is omitted
    //  (c) pc:3 is in error, the size of the memory before the first MSTORE8 is zero.
    //  (d) if memory is zero length, it is not included even if `showMemory` is true
    //  (e) if return data is zero length or null, it is not included even if `showReturnData` is
    // true
    //  (f) if error is zero length or null it is not included.
    assertThat(baos)
        .hasToString(
            """
                        {"pc":0,"op":96,"gas":"0x2540be400","gasCost":"0x3","memSize":0,"stack":[],"depth":1,"refund":0,"opName":"PUSH1"}
                        {"pc":2,"op":128,"gas":"0x2540be3fd","gasCost":"0x3","memSize":0,"stack":["0x40"],"depth":1,"refund":0,"opName":"DUP1"}
                        {"pc":3,"op":83,"gas":"0x2540be3fa","gasCost":"0xc","memSize":0,"stack":["0x40","0x40"],"depth":1,"refund":0,"opName":"MSTORE8"}
                        {"pc":4,"op":96,"gas":"0x2540be3ee","gasCost":"0x3","memory":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":[],"depth":1,"refund":0,"opName":"PUSH1"}
                        {"pc":6,"op":96,"gas":"0x2540be3eb","gasCost":"0x3","memory":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":["0x40"],"depth":1,"refund":0,"opName":"PUSH1"}
                        {"pc":8,"op":85,"gas":"0x2540be3e8","gasCost":"0x4e20","memory":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":["0x40","0x40"],"depth":1,"refund":0,"opName":"SSTORE"}
                        {"pc":9,"op":96,"gas":"0x2540b95c8","gasCost":"0x3","memory":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":[],"depth":1,"refund":0,"opName":"PUSH1"}
                        {"pc":11,"op":96,"gas":"0x2540b95c5","gasCost":"0x3","memory":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":["0x40"],"depth":1,"refund":0,"opName":"PUSH1"}
                        {"pc":13,"op":96,"gas":"0x2540b95c2","gasCost":"0x3","memory":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":["0x40","0x0"],"depth":1,"refund":0,"opName":"PUSH1"}
                        {"pc":15,"op":96,"gas":"0x2540b95bf","gasCost":"0x3","memory":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":["0x40","0x0","0x40"],"depth":1,"refund":0,"opName":"PUSH1"}
                        {"pc":17,"op":96,"gas":"0x2540b95bc","gasCost":"0x3","memory":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":["0x40","0x0","0x40","0x0"],"depth":1,"refund":0,"opName":"PUSH1"}
                        {"pc":19,"op":90,"gas":"0x2540b95b9","gasCost":"0x2","memory":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":["0x40","0x0","0x40","0x0","0x2"],"depth":1,"refund":0,"opName":"GAS"}
                        {"pc":20,"op":250,"gas":"0x2540b95b7","gasCost":"0x24abb676c","memory":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":["0x40","0x0","0x40","0x0","0x2","0x2540b95b7"],"depth":1,"refund":0,"opName":"STATICCALL"}
                        {"pc":21,"op":96,"gas":"0x2540b92a7","gasCost":"0x3","memory":"0xf5a5fd42d16a20302798ef6ed309979b43003d2320d9f0e8ea9831a92759fb4b00000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":["0x1"],"returnData":"0xf5a5fd42d16a20302798ef6ed309979b43003d2320d9f0e8ea9831a92759fb4b","depth":1,"refund":0,"opName":"PUSH1"}
                        {"pc":23,"op":243,"gas":"0x2540b92a4","gasCost":"0x0","memory":"0xf5a5fd42d16a20302798ef6ed309979b43003d2320d9f0e8ea9831a92759fb4b00000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":["0x1","0x40"],"returnData":"0xf5a5fd42d16a20302798ef6ed309979b43003d2320d9f0e8ea9831a92759fb4b","depth":1,"refund":0,"opName":"RETURN"}
                        """);
  }

  @Test
  void eip3155ModifiedTestCase() {

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(baos);
    var executor = new EVMExecutor(EvmSpec.evmSpec(EvmSpecVersion.ISTANBUL));
    StreamingOperationTracer tracer =
        new StreamingOperationTracer(
            out,
            OpCodeTracerConfigBuilder.createFrom(OpCodeTracerConfig.DEFAULT)
                .traceMemory(true)
                .traceStack(true)
                .traceReturnData(true)
                .traceStorage(false)
                .build());
    executor.tracer(tracer);
    executor.gas(10_000_000_000L);

    var codeBytes = Bytes.fromHexString("0x604080536040604055604060006040600060025afa6040f3");
    executor.execute(codeBytes, Bytes.EMPTY, Wei.ZERO, Address.ZERO);

    // differences from the EIP-3155 test case
    //  (a) the test case was written when EIP-2315 was part of the pending hard fork, so
    //      returnStack was a valid field.  It no longer appears in any traces.
    //  (b) the summary line is omitted
    //  (c) pc:3 is in error, the size of the memory before the first MSTORE8 is zero.
    //  (d) if memory is zero length, it is not included even if `showMemory` is true
    //  (e) if return data is zero length or null, it is not included even if `showReturnData` is
    // true
    //  (f) if error is zero length or null it is not included.
    assertThat(baos)
        .hasToString(
            """
                                {"pc":0,"op":"0x60","gas":10000000000,"gasCost":3,"memSize":0,"stack":[],"depth":1,"refund":0,"opName":"PUSH1"}
                                {"pc":2,"op":"0x80","gas":9999999997,"gasCost":3,"memSize":0,"stack":["0x40"],"depth":1,"refund":0,"opName":"DUP1"}
                                {"pc":3,"op":"0x53","gas":9999999994,"gasCost":12,"memSize":0,"stack":["0x40","0x40"],"depth":1,"refund":0,"opName":"MSTORE8"}
                                {"pc":4,"op":"0x60","gas":9999999982,"gasCost":3,"memory":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":[],"depth":1,"refund":0,"opName":"PUSH1"}
                                {"pc":6,"op":"0x60","gas":9999999979,"gasCost":3,"memory":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":["0x40"],"depth":1,"refund":0,"opName":"PUSH1"}
                                {"pc":8,"op":"0x55","gas":9999999976,"gasCost":20000,"memory":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":["0x40","0x40"],"depth":1,"refund":0,"opName":"SSTORE"}
                                {"pc":9,"op":"0x60","gas":9999979976,"gasCost":3,"memory":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":[],"depth":1,"refund":0,"opName":"PUSH1"}
                                {"pc":11,"op":"0x60","gas":9999979973,"gasCost":3,"memory":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":["0x40"],"depth":1,"refund":0,"opName":"PUSH1"}
                                {"pc":13,"op":"0x60","gas":9999979970,"gasCost":3,"memory":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":["0x40","0x0"],"depth":1,"refund":0,"opName":"PUSH1"}
                                {"pc":15,"op":"0x60","gas":9999979967,"gasCost":3,"memory":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":["0x40","0x0","0x40"],"depth":1,"refund":0,"opName":"PUSH1"}
                                {"pc":17,"op":"0x60","gas":9999979964,"gasCost":3,"memory":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":["0x40","0x0","0x40","0x0"],"depth":1,"refund":0,"opName":"PUSH1"}
                                {"pc":19,"op":"0x5a","gas":9999979961,"gasCost":2,"memory":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":["0x40","0x0","0x40","0x0","0x2"],"depth":1,"refund":0,"opName":"GAS"}
                                {"pc":20,"op":"0xfa","gas":9999979959,"gasCost":9843730284,"memory":"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":["0x40","0x0","0x40","0x0","0x2","0x2540b95b7"],"depth":1,"refund":0,"opName":"STATICCALL"}
                                {"pc":21,"op":"0x60","gas":9999979175,"gasCost":3,"memory":"0xf5a5fd42d16a20302798ef6ed309979b43003d2320d9f0e8ea9831a92759fb4b00000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":["0x1"],"returnData":"0xf5a5fd42d16a20302798ef6ed309979b43003d2320d9f0e8ea9831a92759fb4b","depth":1,"refund":0,"opName":"PUSH1"}
                                {"pc":23,"op":"0xf3","gas":9999979172,"gasCost":0,"memory":"0xf5a5fd42d16a20302798ef6ed309979b43003d2320d9f0e8ea9831a92759fb4b00000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000","memSize":96,"stack":["0x1","0x40"],"returnData":"0xf5a5fd42d16a20302798ef6ed309979b43003d2320d9f0e8ea9831a92759fb4b","depth":1,"refund":0,"opName":"RETURN"}
                                """);
  }

  @Test
  void stopsTracingAtTheStepLimit() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(baos);
    var executor = new EVMExecutor(EvmSpec.evmSpec(EvmSpecVersion.ISTANBUL));
    StreamingOperationTracer tracer =
        new StreamingOperationTracer(
            out, OpCodeTracerConfigBuilder.createFrom(OpCodeTracerConfig.DEFAULT).limit(3).build());
    executor.tracer(tracer);
    executor.gas(10_000_000_000L);

    var codeBytes = Bytes.fromHexString("0x604080536040604055604060006040600060025afa6040f3");
    executor.execute(codeBytes, Bytes.EMPTY, Wei.ZERO, Address.ZERO);

    assertThat(baos.toString(StandardCharsets.UTF_8).lines()).hasSize(3);
  }

  @Test
  void stepLimitCountsOnlyTracedOpcodes() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(baos);
    var executor = new EVMExecutor(EvmSpec.evmSpec(EvmSpecVersion.ISTANBUL));
    StreamingOperationTracer tracer =
        new StreamingOperationTracer(
            out,
            OpCodeTracerConfigBuilder.createFrom(OpCodeTracerConfig.DEFAULT)
                .traceOpcodes(Set.of("sstore"))
                .limit(1)
                .build());
    executor.tracer(tracer);
    executor.gas(10_000_000_000L);

    var codeBytes = Bytes.fromHexString("0x604080536040604055604060006040600060025afa6040f3");
    executor.execute(codeBytes, Bytes.EMPTY, Wei.ZERO, Address.ZERO);

    assertThat(baos.toString(StandardCharsets.UTF_8).lines())
        .singleElement(STRING)
        .contains("\"opName\":\"SSTORE\"");
  }

  @Test
  void updatedStorageTestCase() {

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(baos);
    var executor = new EVMExecutor(EvmSpec.evmSpec(EvmSpecVersion.ISTANBUL));
    StreamingOperationTracer tracer =
        new StreamingOperationTracer(
            out,
            OpCodeTracerConfigBuilder.createFrom(OpCodeTracerConfig.DEFAULT)
                .traceMemory(false)
                .traceStack(false)
                .traceReturnData(false)
                .traceStorage(true)
                .build());
    executor.tracer(tracer);
    executor.gas(10_000_000_000L);

    var codeBytes = Bytes.fromHexString("0x604080536040604055604060006040600060025afa6040f3");
    executor.execute(codeBytes, Bytes.EMPTY, Wei.ZERO, Address.ZERO);

    assertThat(baos)
        .hasToString(
            """
                        {"pc":0,"op":"0x60","gas":10000000000,"gasCost":3,"memSize":0,"depth":1,"refund":0,"opName":"PUSH1"}
                        {"pc":2,"op":"0x80","gas":9999999997,"gasCost":3,"memSize":0,"depth":1,"refund":0,"opName":"DUP1"}
                        {"pc":3,"op":"0x53","gas":9999999994,"gasCost":12,"memSize":0,"depth":1,"refund":0,"opName":"MSTORE8"}
                        {"pc":4,"op":"0x60","gas":9999999982,"gasCost":3,"memSize":96,"depth":1,"refund":0,"opName":"PUSH1"}
                        {"pc":6,"op":"0x60","gas":9999999979,"gasCost":3,"memSize":96,"depth":1,"refund":0,"opName":"PUSH1"}
                        {"pc":8,"op":"0x55","gas":9999999976,"gasCost":20000,"memSize":96,"depth":1,"refund":0,"opName":"SSTORE"}
                        {"pc":9,"op":"0x60","gas":9999979976,"gasCost":3,"memSize":96,"depth":1,"refund":0,"opName":"PUSH1","storage":{"0x40":"0x40"}}
                        {"pc":11,"op":"0x60","gas":9999979973,"gasCost":3,"memSize":96,"depth":1,"refund":0,"opName":"PUSH1","storage":{"0x40":"0x40"}}
                        {"pc":13,"op":"0x60","gas":9999979970,"gasCost":3,"memSize":96,"depth":1,"refund":0,"opName":"PUSH1","storage":{"0x40":"0x40"}}
                        {"pc":15,"op":"0x60","gas":9999979967,"gasCost":3,"memSize":96,"depth":1,"refund":0,"opName":"PUSH1","storage":{"0x40":"0x40"}}
                        {"pc":17,"op":"0x60","gas":9999979964,"gasCost":3,"memSize":96,"depth":1,"refund":0,"opName":"PUSH1","storage":{"0x40":"0x40"}}
                        {"pc":19,"op":"0x5a","gas":9999979961,"gasCost":2,"memSize":96,"depth":1,"refund":0,"opName":"GAS","storage":{"0x40":"0x40"}}
                        {"pc":20,"op":"0xfa","gas":9999979959,"gasCost":9843730284,"memSize":96,"depth":1,"refund":0,"opName":"STATICCALL","storage":{"0x40":"0x40"}}
                        {"pc":21,"op":"0x60","gas":9999979175,"gasCost":3,"memSize":96,"depth":1,"refund":0,"opName":"PUSH1","storage":{"0x40":"0x40"}}
                        {"pc":23,"op":"0xf3","gas":9999979172,"gasCost":0,"memSize":96,"depth":1,"refund":0,"opName":"RETURN","storage":{"0x40":"0x40"}}
                        """);
  }
}
