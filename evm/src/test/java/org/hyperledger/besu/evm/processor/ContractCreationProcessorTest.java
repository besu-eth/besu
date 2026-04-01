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
package org.hyperledger.besu.evm.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.evm.frame.MessageFrame.State.COMPLETED_FAILED;
import static org.hyperledger.besu.evm.frame.MessageFrame.State.COMPLETED_SUCCESS;

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.EvmSpecVersion;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.contractvalidation.MaxCodeSizeRule;
import org.hyperledger.besu.evm.contractvalidation.PrefixCodeRule;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.testutils.TestMessageFrameBuilder;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.util.Collections;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@Nested
@ExtendWith(MockitoExtension.class)
class ContractCreationProcessorTest
    extends AbstractMessageProcessorTest<ContractCreationProcessor> {

  EVM evm = MainnetEVMs.futureEips(EvmConfiguration.DEFAULT);

  private ContractCreationProcessor processor;

  @Test
  void shouldThrowAnExceptionWhenCodeContractFormatInvalidPreEOF() {
    processor =
        new ContractCreationProcessor(evm, true, Collections.singletonList(PrefixCodeRule.of()), 1);
    final Bytes contractCode = Bytes.fromHexString("EF01010101010101");
    final MessageFrame messageFrame = new TestMessageFrameBuilder().build();
    messageFrame.setOutputData(contractCode);
    messageFrame.setGasRemaining(10600L);

    processor.codeSuccess(messageFrame, OperationTracer.NO_TRACING);
    // At depth 0, validation failures use COMPLETED_FAILED to preserve state gas reservoir
    assertThat(messageFrame.getState()).isEqualTo(COMPLETED_FAILED);
    // Gas should be cleared — failCodeDepositWithoutRollback burns all remaining gas
    assertThat(messageFrame.getRemainingGas()).isEqualTo(0L);
  }

  @Test
  void shouldNotThrowAnExceptionWhenCodeContractIsValid() {
    processor =
        new ContractCreationProcessor(evm, true, Collections.singletonList(PrefixCodeRule.of()), 1);
    final Bytes contractCode = Bytes.fromHexString("0101010101010101");
    final MessageFrame messageFrame = new TestMessageFrameBuilder().build();
    messageFrame.setOutputData(contractCode);
    messageFrame.setGasRemaining(10600L);

    processor.codeSuccess(messageFrame, OperationTracer.NO_TRACING);
    assertThat(messageFrame.getState()).isEqualTo(COMPLETED_SUCCESS);
  }

  @Test
  void shouldNotThrowAnExceptionWhenPrefixCodeRuleNotAdded() {
    processor = new ContractCreationProcessor(evm, true, Collections.emptyList(), 1);
    final Bytes contractCode = Bytes.fromHexString("0F01010101010101");
    final MessageFrame messageFrame = new TestMessageFrameBuilder().build();
    messageFrame.setOutputData(contractCode);
    messageFrame.setGasRemaining(10600L);

    processor.codeSuccess(messageFrame, OperationTracer.NO_TRACING);
    assertThat(messageFrame.getState()).isEqualTo(COMPLETED_SUCCESS);
  }

  @Test
  void shouldThrowAnExceptionWhenCodeContractTooLarge() {
    processor =
        new ContractCreationProcessor(
            evm,
            true,
            Collections.singletonList(
                MaxCodeSizeRule.from(EvmSpecVersion.SPURIOUS_DRAGON, EvmConfiguration.DEFAULT)),
            1);
    final Bytes contractCode =
        Bytes.fromHexString("00".repeat(EvmSpecVersion.SPURIOUS_DRAGON.getMaxCodeSize() + 1));
    final MessageFrame messageFrame = new TestMessageFrameBuilder().build();
    messageFrame.setOutputData(contractCode);
    messageFrame.setGasRemaining(10_000_000L);

    processor.codeSuccess(messageFrame, OperationTracer.NO_TRACING);
    // At depth 0, validation failures use COMPLETED_FAILED to preserve state gas reservoir
    assertThat(messageFrame.getState()).isEqualTo(COMPLETED_FAILED);
    // Gas should be cleared — failCodeDepositWithoutRollback burns all remaining gas
    assertThat(messageFrame.getRemainingGas()).isEqualTo(0L);
  }

  @Test
  void shouldNotThrowAnExceptionWhenCodeContractTooLarge() {
    processor =
        new ContractCreationProcessor(
            evm,
            true,
            Collections.singletonList(
                MaxCodeSizeRule.from(EvmSpecVersion.SPURIOUS_DRAGON, EvmConfiguration.DEFAULT)),
            1);
    final Bytes contractCode =
        Bytes.fromHexString("00".repeat(EvmSpecVersion.SPURIOUS_DRAGON.getMaxCodeSize()));
    final MessageFrame messageFrame = new TestMessageFrameBuilder().build();
    messageFrame.setOutputData(contractCode);
    messageFrame.setGasRemaining(5_000_000L);

    processor.codeSuccess(messageFrame, OperationTracer.NO_TRACING);
    assertThat(messageFrame.getState()).isEqualTo(COMPLETED_SUCCESS);
  }

  @Test
  void shouldRejectDeployedCodeAboveAmsterdamLimit() {
    processor =
        new ContractCreationProcessor(
            evm,
            true,
            Collections.singletonList(
                MaxCodeSizeRule.from(EvmSpecVersion.AMSTERDAM, EvmConfiguration.DEFAULT)),
            1);
    final Bytes contractCode =
        Bytes.fromHexString("00".repeat(EvmSpecVersion.AMSTERDAM.getMaxCodeSize() + 1));
    final MessageFrame messageFrame = new TestMessageFrameBuilder().build();
    messageFrame.setOutputData(contractCode);
    messageFrame.setGasRemaining(10_000_000L);

    processor.codeSuccess(messageFrame, OperationTracer.NO_TRACING);
    // At depth 0, validation failures use COMPLETED_FAILED to preserve state gas reservoir
    assertThat(messageFrame.getState()).isEqualTo(COMPLETED_FAILED);
    // Gas should be cleared — failCodeDepositWithoutRollback burns all remaining gas
    assertThat(messageFrame.getRemainingGas()).isEqualTo(0L);
  }

  @Test
  void shouldAcceptDeployedCodeAtAmsterdamLimit() {
    processor =
        new ContractCreationProcessor(
            evm,
            true,
            Collections.singletonList(
                MaxCodeSizeRule.from(EvmSpecVersion.AMSTERDAM, EvmConfiguration.DEFAULT)),
            1);
    final Bytes contractCode =
        Bytes.fromHexString("00".repeat(EvmSpecVersion.AMSTERDAM.getMaxCodeSize()));
    final MessageFrame messageFrame = new TestMessageFrameBuilder().build();
    messageFrame.setOutputData(contractCode);
    messageFrame.setGasRemaining(10_000_000L);

    processor.codeSuccess(messageFrame, OperationTracer.NO_TRACING);
    assertThat(messageFrame.getState()).isEqualTo(COMPLETED_SUCCESS);
  }

  @Test
  void shouldAcceptDeployedCodeBetweenOldAndNewAmsterdamLimit() {
    processor =
        new ContractCreationProcessor(
            evm,
            true,
            Collections.singletonList(
                MaxCodeSizeRule.from(EvmSpecVersion.AMSTERDAM, EvmConfiguration.DEFAULT)),
            1);
    final Bytes contractCode = Bytes.fromHexString("00".repeat(0x6001));
    final MessageFrame messageFrame = new TestMessageFrameBuilder().build();
    messageFrame.setOutputData(contractCode);
    messageFrame.setGasRemaining(10_000_000L);

    processor.codeSuccess(messageFrame, OperationTracer.NO_TRACING);
    assertThat(messageFrame.getState()).isEqualTo(COMPLETED_SUCCESS);
  }

  @Test
  void shouldNotThrowAnExceptionWhenCodeSizeRuleNotAdded() {
    processor = new ContractCreationProcessor(evm, true, Collections.emptyList(), 1);
    final Bytes contractCode = Bytes.fromHexString("00".repeat(24 * 1024 + 1));
    final MessageFrame messageFrame = new TestMessageFrameBuilder().build();
    messageFrame.setOutputData(contractCode);
    messageFrame.setGasRemaining(5_000_000L);

    processor.codeSuccess(messageFrame, OperationTracer.NO_TRACING);
    assertThat(messageFrame.getState()).isEqualTo(COMPLETED_SUCCESS);
  }

  @Override
  protected ContractCreationProcessor getAbstractMessageProcessor() {
    return new ContractCreationProcessor(evm, true, Collections.emptyList(), 1);
  }
}
