package org.hyperledger.besu.ethereum.vm.operations.v2;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.operation.ModOperationV2;
import org.openjdk.jmh.annotations.Param;

public class ModOperationBenchmarkV2 extends BinaryArithmeticOperationBenchmarkV2 {
  @Param({
    "MOD_32_32",
    "MOD_64_32",
    "MOD_64_64",
    "MOD_128_32",
    "MOD_128_64",
    "MOD_128_128",
    "MOD_192_32",
    "MOD_192_64",
    "MOD_192_128",
    "MOD_192_192",
    "MOD_256_32",
    "MOD_256_64",
    "MOD_256_128",
    "MOD_256_192",
    "MOD_256_256",
    "MOD_64_128",
    "MOD_192_256",
    "MOD_RANDOM_RANDOM"
  })
  private String caseName;

  @Override
  protected Operation.OperationResult invoke(final MessageFrame frame) {
    return ModOperationV2.staticOperation(frame);
  }

  @Override
  protected String caseName() {
    return caseName;
  }

  @Override
  protected String opCode() {
    return "MOD";
  }
}
