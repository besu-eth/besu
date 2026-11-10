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
package org.hyperledger.besu.evm.precompile;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.nativelib.gnark.LibGnarkEIP196;

import java.util.Optional;

import jakarta.validation.constraints.NotNull;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Abstract AltBn precompiled contract. */
public abstract class AbstractAltBnPrecompiledContract extends AbstractPrecompiledContract {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractAltBnPrecompiledContract.class);

  /** The constant useNative. */
  // use the native library implementation, if it is available
  static boolean useNative;

  static {
    maybeEnableNative();
  }

  /**
   * Attempt to enable the native library for AltBn contracts
   *
   * @return true if the native library was enabled.
   */
  public static boolean maybeEnableNative() {
    try {
      useNative = LibGnarkEIP196.ENABLED;
    } catch (UnsatisfiedLinkError | NoClassDefFoundError ule) {
      LOG.info("altbn128 native precompile not available: {}", ule.getMessage());
      useNative = false;
    }
    return useNative;
  }

  /** Disable native. */
  public static void disableNative() {
    useNative = false;
  }

  /**
   * Is native boolean.
   *
   * @return the boolean
   */
  public static boolean isNative() {
    return useNative;
  }

  private final byte operationId;
  private final int inputLimit;
  private final int outputLength;

  /**
   * Instantiates a new Abstract alt bn precompiled contract.
   *
   * @param name the name
   * @param gasCalculator the gas calculator
   * @param operationId the operation id
   * @param inputLen the input len
   * @param outputLen the output len
   */
  AbstractAltBnPrecompiledContract(
      final String name,
      final GasCalculator gasCalculator,
      final byte operationId,
      final int inputLen,
      final int outputLen) {
    super(name, gasCalculator);
    this.operationId = operationId;
    this.inputLimit = inputLen + 1;
    this.outputLength = outputLen;

    if (!LibGnarkEIP196.ENABLED) {
      LOG.info("Native alt bn128 not available");
    }
  }

  /**
   * Compute native precompile contract result.
   *
   * @param input the input
   * @param messageFrame the message frame
   * @return the precompile contract result
   */
  @NotNull
  public PrecompileContractResult computeNative(
      final @NotNull Bytes input, final MessageFrame messageFrame) {
    final byte[] result = new byte[LibGnarkEIP196.EIP196_PREALLOCATE_FOR_RESULT_BYTES];

    final int inputSize = Math.min(inputLimit, input.size());
    final int errorNo =
        LibGnarkEIP196.eip196_perform_operation(
            operationId, input.slice(0, inputSize).toArrayUnsafe(), inputSize, result);

    if (errorNo == LibGnarkEIP196.EIP196_ERR_CODE_SUCCESS) {
      return PrecompileContractResult.success(Bytes.wrap(result, 0, outputLength));
    } else {
      messageFrame.setRevertReason(Bytes.wrap("error".getBytes(UTF_8)));
      return PrecompileContractResult.halt(
          null, Optional.of(ExceptionalHaltReason.PRECOMPILE_ERROR));
    }
  }
}
