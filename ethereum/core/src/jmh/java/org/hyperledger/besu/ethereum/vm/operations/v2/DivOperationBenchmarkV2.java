package org.hyperledger.besu.ethereum.vm.operations.v2;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.operation.DivOperationV2;
import org.openjdk.jmh.annotations.Param;

public class DivOperationBenchmarkV2 extends BinaryArithmeticOperationBenchmarkV2 {
  @Param({
    "DIV_32_32",
    "DIV_64_32",
    "DIV_64_64",
    "DIV_128_32",
    "DIV_128_64",
    "DIV_128_128",
    "DIV_192_32",
    "DIV_192_64",
    "DIV_192_128",
    "DIV_192_192",
    "DIV_256_32",
    "DIV_256_64",
    "DIV_256_128",
    "DIV_256_192",
    "DIV_0_256",
    "DIV_64_256",
    "DIV_128_256",
    "DIV_192_256",
    "DIV_256_256",
    "DIV_RANDOM_RANDOM"
  })
  private String caseName;

  @Override
  protected Operation.OperationResult invoke(final MessageFrame frame) {
    return DivOperationV2.staticOperation(frame);
  }

  @Override
  protected String caseName() {
    return caseName;
  }

  @Override
  protected String opCode() {
    return "DIV";
  }
}
