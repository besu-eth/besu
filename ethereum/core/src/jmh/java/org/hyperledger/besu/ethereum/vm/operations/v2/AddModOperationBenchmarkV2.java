package org.hyperledger.besu.ethereum.vm.operations.v2;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;
import org.hyperledger.besu.evm.v2.operation.AddModOperationV2;
import org.openjdk.jmh.annotations.Param;

public class AddModOperationBenchmarkV2  extends TernaryArithmeticBenchmarkV2 {
  @Param({
    "ADDMOD_32_32_32",
    "ADDMOD_64_32_32",
    "ADDMOD_64_64_32",
    "ADDMOD_64_64_64",
    "ADDMOD_128_32_32",
    "ADDMOD_128_64_32",
    "ADDMOD_128_64_64",
    "ADDMOD_128_128_32",
    "ADDMOD_128_128_64",
    "ADDMOD_128_128_128",
    "ADDMOD_192_32_32",
    "ADDMOD_192_64_32",
    "ADDMOD_192_64_64",
    "ADDMOD_192_128_32",
    "ADDMOD_192_128_64",
    "ADDMOD_192_128_128",
    "ADDMOD_192_192_32",
    "ADDMOD_192_192_64",
    "ADDMOD_192_192_128",
    "ADDMOD_192_192_192",
    "ADDMOD_256_32_32",
    "ADDMOD_256_64_32",
    "ADDMOD_256_64_64",
    "ADDMOD_256_64_128",
    "ADDMOD_256_64_192",
    "ADDMOD_256_128_32",
    "ADDMOD_256_128_64",
    "ADDMOD_256_128_128",
    "ADDMOD_256_192_32",
    "ADDMOD_256_192_64",
    "ADDMOD_256_192_128",
    "ADDMOD_256_192_192",
    "ADDMOD_256_256_32",
    "ADDMOD_256_256_64",
    "ADDMOD_256_256_128",
    "ADDMOD_256_256_192",
    "ADDMOD_256_256_256",
    "ADDMOD_64_64_128",
    "ADDMOD_192_192_256",
    "ADDMOD_128_256_0",
    "ADDMOD_RANDOM_RANDOM_RANDOM"
  })
  private String caseName;

  @Override
  protected OperationResult invoke(final MessageFrame frame) {
    return AddModOperationV2.staticOperation(frame);
  }

  @Override
  protected String opCode() {
    return "ADDMOD";
  }

  @Override
  protected String caseName() {
    return caseName;
  }
}
