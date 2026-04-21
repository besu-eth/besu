package org.hyperledger.besu.evm.v2.operation;

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

/** The SMod operation. */
public class SModOperationV2 extends AbstractFixedCostOperationV2 {

  private static final OperationResult smodSuccess = new OperationResult(5, null);

  /**
   * Instantiates a new SMod operation.
   *
   * @param gasCalculator the gas calculator
   */
  public SModOperationV2(final GasCalculator gasCalculator) {
    super(0x07, "SMOD", 2, 1, gasCalculator, gasCalculator.getLowTierGasCost());
  }

  @Override
  public OperationResult executeFixedCostOperation(
    final MessageFrame frame, final EVM evm) {
    return staticOperation(frame);
  }

  /**
   * Performs SMod operation.
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
    final int modulusOffset = (--top) << 2;
    final UInt256 modulus =
      new UInt256(
        stack[modulusOffset],
        stack[modulusOffset + 1],
        stack[modulusOffset + 2],
        stack[modulusOffset + 3]);
    final UInt256 result = num.signedMod(modulus);
    stack[modulusOffset] = result.u3();
    stack[modulusOffset + 1] = result.u2();
    stack[modulusOffset + 2] = result.u1();
    stack[modulusOffset + 3] = result.u0();
    frame.setTopV2(++top);
    return smodSuccess;
  }
}
