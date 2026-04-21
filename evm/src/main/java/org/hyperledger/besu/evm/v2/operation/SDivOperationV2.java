package org.hyperledger.besu.evm.v2.operation;

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

/** The SDiv operation. */
public class SDivOperationV2 extends AbstractFixedCostOperationV2 {

  private static final OperationResult sdivSuccess = new OperationResult(5, null);

  /**
   * Instantiates a new SDiv operation.
   *
   * @param gasCalculator the gas calculator
   */
  public SDivOperationV2(final GasCalculator gasCalculator) {
    super(0x05, "SDIV", 2, 1, gasCalculator, gasCalculator.getLowTierGasCost());
  }

  @Override
  public OperationResult executeFixedCostOperation(
    final MessageFrame frame, final EVM evm) {
    return staticOperation(frame);
  }

  /**
   * Performs SDiv operation.
   *
   * @param frame the frame
   * @return the operation result
   */
  public static OperationResult staticOperation(final MessageFrame frame) {
    if (!frame.stackHasItemsV2(2)) {
      return UNDERFLOW_RESPONSE;
    }
    long[] stack = frame.stackDataV2();
    int top = frame.stackTopV2();
    final int numOffset = (--top) << 2;
    final UInt256 num =
      new UInt256(
        stack[numOffset],
        stack[numOffset + 1],
        stack[numOffset + 2],
        stack[numOffset + 3]);
    final int denomOffset = (--top) << 2;
    final UInt256 denom =
      new UInt256(
        stack[denomOffset],
        stack[denomOffset + 1],
        stack[denomOffset + 2],
        stack[denomOffset + 3]);
    final UInt256 result = num.signedDiv(denom);
    stack[denomOffset] = result.u3();
    stack[denomOffset + 1] = result.u2();
    stack[denomOffset + 2] = result.u1();
    stack[denomOffset + 3] = result.u0();
    frame.setTopV2(++top);
    return sdivSuccess;
  }
}
