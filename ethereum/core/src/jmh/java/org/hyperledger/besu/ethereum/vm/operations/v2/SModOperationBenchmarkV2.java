package org.hyperledger.besu.ethereum.vm.operations.v2;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.operation.SModOperationV2;
import org.openjdk.jmh.annotations.Param;

public class SModOperationBenchmarkV2 extends SignedBinaryArithmeticOperationBenchmarkV2 {
  @Param({
    "SMOD_32_32",
    "SMOD_64_32",
    "SMOD_64_64",
    "SMOD_128_32",
    "SMOD_128_64",
    "SMOD_128_128",
    "SMOD_192_32",
    "SMOD_192_64",
    "SMOD_192_128",
    "SMOD_192_192",
    "SMOD_256_32",
    "SMOD_256_64",
    "SMOD_256_128",
    "SMOD_256_192",
    "SMOD_256_256",
    "SMOD_64_128",
    "SMOD_192_256",
    "SMOD_128_0",
    "SMOD_RANDOM_RANDOM"
  })
  private String caseName;

  @Override
  protected String opCode() {
    return "SMOD";
  }

  @Override
  protected String caseName() {
    return caseName;
  }

  @Override
  protected Operation.OperationResult invoke(final MessageFrame frame) {
    return SModOperationV2.staticOperation(frame);
  }
}
