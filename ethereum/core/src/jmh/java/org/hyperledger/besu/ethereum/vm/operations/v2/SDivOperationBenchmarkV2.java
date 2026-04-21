package org.hyperledger.besu.ethereum.vm.operations.v2;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.operation.SDivOperationV2;
import org.openjdk.jmh.annotations.Param;

public class SDivOperationBenchmarkV2 extends SignedBinaryArithmeticOperationBenchmarkV2 {
  @Param({
    "SDIV_32_32",
    "SDIV_64_32",
    "SDIV_64_64",
    "SDIV_128_32",
    "SDIV_128_64",
    "SDIV_128_128",
    "SDIV_192_32",
    "SDIV_192_64",
    "SDIV_192_128",
    "SDIV_192_192",
    "SDIV_256_32",
    "SDIV_256_64",
    "SDIV_256_128",
    "SDIV_256_192",
    "SDIV_256_256",
    "SDIV_0_256",
    "SDIV_64_256",
    "SDIV_128_256",
    "SDIV_192_256",
    "SDIV_RANDOM_RANDOM"
  })
  private String caseName;

  @Override
  protected String opCode() {
    return "SDIV";
  }

  @Override
  protected String caseName() {
    return caseName;
  }

  @Override
  protected Operation.OperationResult invoke(final MessageFrame frame) {
    return SDivOperationV2.staticOperation(frame);
  }
}
