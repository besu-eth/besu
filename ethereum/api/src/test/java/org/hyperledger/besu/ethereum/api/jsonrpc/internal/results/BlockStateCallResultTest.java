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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.transaction.BlockSimulationResult;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulatorResult;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

public class BlockStateCallResultTest {

  private static final Bytes ERROR_STRING_REVERT_DATA =
      Bytes.fromHexString(
          "0x08c379a0"
              + "0000000000000000000000000000000000000000000000000000000000000020"
              + "0000000000000000000000000000000000000000000000000000000000000019"
              + "416c7761797320726576657274696e6720636f6e747261637400000000000000");

  @Test
  public void revertWithReasonReturnsLowercaseMessageWithReason() {
    final JsonRpcError error =
        callError(failed(Optional.of(ERROR_STRING_REVERT_DATA), Optional.empty()));

    assertThat(error.getCode()).isEqualTo(3);
    assertThat(error.getMessage()).isEqualTo("execution reverted: Always reverting contract");
    assertThat(error.getData()).isEqualTo(ERROR_STRING_REVERT_DATA.toHexString());
  }

  @Test
  public void revertWithoutReasonReturnsLowercaseMessage() {
    final JsonRpcError error = callError(failed(Optional.of(Bytes.EMPTY), Optional.empty()));

    assertThat(error.getCode()).isEqualTo(3);
    assertThat(error.getMessage()).isEqualTo("execution reverted");
    assertThat(error.getData()).isEqualTo("0x");
  }

  @Test
  public void revertWithCustomErrorReturnsBareMessageWithData() {
    final Bytes customError =
        Bytes.fromHexString(
            "0xdeadbeef0000000000000000000000000000000000000000000000000000000000000001");
    final JsonRpcError error = callError(failed(Optional.of(customError), Optional.empty()));

    assertThat(error.getCode()).isEqualTo(3);
    assertThat(error.getMessage()).isEqualTo("execution reverted");
    assertThat(error.getData()).isEqualTo(customError.toHexString());
  }

  @Test
  public void revertWithPanicReturnsMessageWithPanicReason() {
    final Bytes panic =
        Bytes.fromHexString(
            "0x4e487b710000000000000000000000000000000000000000000000000000000000000011");
    final JsonRpcError error = callError(failed(Optional.of(panic), Optional.empty()));

    assertThat(error.getCode()).isEqualTo(3);
    assertThat(error.getMessage())
        .isEqualTo("execution reverted: arithmetic underflow or overflow");
    assertThat(error.getData()).isEqualTo(panic.toHexString());
  }

  @Test
  public void revertWithUnknownPanicCodeReturnsMessageWithCode() {
    final Bytes panic =
        Bytes.fromHexString(
            "0x4e487b7100000000000000000000000000000000000000000000000000000000000000ff");
    final JsonRpcError error = callError(failed(Optional.of(panic), Optional.empty()));

    assertThat(error.getCode()).isEqualTo(3);
    assertThat(error.getMessage()).isEqualTo("execution reverted: unknown panic code: 0xff");
    assertThat(error.getData()).isEqualTo(panic.toHexString());
  }

  @Test
  public void revertWithTruncatedPanicReturnsBareMessageWithData() {
    final Bytes panic = Bytes.fromHexString("0x4e487b710011");
    final JsonRpcError error = callError(failed(Optional.of(panic), Optional.empty()));

    assertThat(error.getCode()).isEqualTo(3);
    assertThat(error.getMessage()).isEqualTo("execution reverted");
    assertThat(error.getData()).isEqualTo(panic.toHexString());
  }

  @Test
  public void revertWithMalformedErrorStringReturnsBareMessageWithData() {
    final Bytes malformed = Bytes.fromHexString("0x08c379a00000");
    final JsonRpcError error = callError(failed(Optional.of(malformed), Optional.empty()));

    assertThat(error.getCode()).isEqualTo(3);
    assertThat(error.getMessage()).isEqualTo("execution reverted");
    assertThat(error.getData()).isEqualTo(malformed.toHexString());
  }

  @Test
  public void haltWithRevertReasonReturnsVmExecutionError() {
    final JsonRpcError error =
        callError(
            failed(
                Optional.of(Bytes.wrap("error".getBytes(UTF_8))),
                Optional.of(ExceptionalHaltReason.PRECOMPILE_ERROR)));

    assertThat(error.getCode()).isEqualTo(-32015);
    assertThat(error.getMessage()).isEqualTo("vm execution error: Precompile error");
    assertThat(error.getData()).isNull();
  }

  @Test
  public void outOfGasReturnsLowercaseMessage() {
    final JsonRpcError error =
        callError(failed(Optional.empty(), Optional.of(ExceptionalHaltReason.INSUFFICIENT_GAS)));

    assertThat(error.getCode()).isEqualTo(-32015);
    assertThat(error.getMessage()).isEqualTo("out of gas");
    assertThat(error.getData()).isNull();
  }

  @Test
  public void otherHaltReturnsVmExecutionErrorMessage() {
    final JsonRpcError error =
        callError(failed(Optional.empty(), Optional.of(ExceptionalHaltReason.INVALID_OPERATION)));

    assertThat(error.getCode()).isEqualTo(-32015);
    assertThat(error.getMessage()).isEqualTo("vm execution error: Bad instruction");
    assertThat(error.getData()).isNull();
  }

  private static TransactionProcessingResult failed(
      final Optional<Bytes> revertReason, final Optional<ExceptionalHaltReason> haltReason) {
    return TransactionProcessingResult.failed(
        21000, 0, ValidationResult.valid(), revertReason, haltReason, Optional.empty());
  }

  private static JsonRpcError callError(final TransactionProcessingResult result) {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockSimulationResult simulationResult = mock(BlockSimulationResult.class);
    when(simulationResult.getBlock()).thenReturn(gen.block());
    when(simulationResult.getLogsWithMetadata()).thenReturn(List.of());
    when(simulationResult.getSerializedTrieLog()).thenReturn(Optional.empty());
    when(simulationResult.getTransactionSimulations())
        .thenReturn(List.of(new TransactionSimulatorResult(gen.transaction(), result)));

    return BlockStateCallResult.create(simulationResult, false)
        .getTransactionProcessingResults()
        .get(0)
        .getError();
  }
}
