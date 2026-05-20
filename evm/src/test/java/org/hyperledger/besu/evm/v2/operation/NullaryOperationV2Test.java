package org.hyperledger.besu.evm.v2.operation;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.testutils.FakeBlockValues;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

abstract class NullaryOperationV2Test {

  protected final Operation operation;

  public NullaryOperationV2Test(final Operation operation) {
    this.operation = operation;
  }

  @Test
  void shouldHaltOnInsufficientGas() {
    final MessageFrame frame =
            new TestMessageFrameBuilderV2().initialGas(1).build();
    final Operation.OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
  }

  @Test
  void shouldHaltOnStackOverflow() {
    Wei baseFee = Wei.of(5L);
    final MessageFrame frame = new TestMessageFrameBuilderV2()
            // required for BaseFeeOperation
            .blockValues(new FakeBlockValues(Optional.of(baseFee)))
            .build();
    frame.setTopV2(MessageFrame.DEFAULT_MAX_STACK_SIZE);
    final Operation.OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.TOO_MANY_STACK_ITEMS);
  }

  @Test
  void shouldHaltOnInsufficientGasEvenStackOverflow() {
    final MessageFrame frame = new TestMessageFrameBuilderV2().initialGas(1L).build();
    frame.setTopV2(MessageFrame.DEFAULT_MAX_STACK_SIZE);
    final Operation.OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
  }
}