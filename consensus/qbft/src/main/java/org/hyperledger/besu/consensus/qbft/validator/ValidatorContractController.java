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
package org.hyperledger.besu.consensus.qbft.validator;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.transaction.CallParameter;
import org.hyperledger.besu.ethereum.transaction.ImmutableCallParameter;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulatorResult;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.tuweni.bytes.Bytes;
import org.web3j.abi.FunctionEncoder;
import org.web3j.abi.FunctionReturnDecoder;
import org.web3j.abi.TypeReference;
import org.web3j.abi.datatypes.DynamicArray;
import org.web3j.abi.datatypes.Function;
import org.web3j.abi.datatypes.Type;

/** The Validator contract controller. */
public class ValidatorContractController {
  /** The constant GET_VALIDATORS. */
  public static final String GET_VALIDATORS = "getValidators";

  /** The constant CONTRACT_ERROR_MSG. */
  public static final String CONTRACT_ERROR_MSG =
      "Failed validator smart contract call: unable to execute call at block ";

  private final TransactionSimulator transactionSimulator;
  private final Function getValidatorsFunction;

  /**
   * Instantiates a new Validator contract controller.
   *
   * @param transactionSimulator the transaction simulator
   */
  public ValidatorContractController(final TransactionSimulator transactionSimulator) {
    this.transactionSimulator = transactionSimulator;

    try {
      this.getValidatorsFunction =
          new Function(
              GET_VALIDATORS,
              List.of(),
              List.of(new TypeReference<DynamicArray<org.web3j.abi.datatypes.Address>>() {}));
    } catch (final Exception e) {
      throw new RuntimeException("Error creating smart contract function", e);
    }
  }

  /**
   * Gets validators.
   *
   * @param blockNumber the block number
   * @param contractAddress the contract address
   * @return the validators
   */
  public Collection<Address> getValidators(final long blockNumber, final Address contractAddress) {
    return callFunction(blockNumber, getValidatorsFunction, contractAddress)
        .map(this::parseGetValidatorsResult)
        .orElseThrow(() -> new IllegalStateException(CONTRACT_ERROR_MSG + blockNumber));
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private Collection<Address> parseGetValidatorsResult(final TransactionSimulatorResult result) {
    final List<Type> resultDecoding = decodeResult(result, getValidatorsFunction);
    final List<org.web3j.abi.datatypes.Address> addresses =
        (List<org.web3j.abi.datatypes.Address>) resultDecoding.get(0).getValue();
    return addresses.stream()
        .map(a -> Address.fromHexString(a.getValue()))
        .collect(Collectors.toList());
  }

  private Optional<TransactionSimulatorResult> callFunction(
      final long blockNumber, final Function function, final Address contractAddress) {
    final Bytes payload = Bytes.fromHexString(FunctionEncoder.encode(function));
    final CallParameter callParams =
        ImmutableCallParameter.builder().to(contractAddress).input(payload).build();
    final TransactionValidationParams transactionValidationParams =
        TransactionValidationParams.transactionSimulatorAllowExceedingBalance();
    return transactionSimulator.process(
        callParams, transactionValidationParams, OperationTracer.NO_TRACING, blockNumber);
  }

  @SuppressWarnings("rawtypes")
  private List<Type> decodeResult(
      final TransactionSimulatorResult result, final Function function) {
    if (result.isSuccessful()) {
      final List<Type> decodedList =
          FunctionReturnDecoder.decode(
              result.result().getOutput().toHexString(), function.getOutputParameters());

      if (decodedList.isEmpty()) {
        throw new IllegalStateException(
            "Unexpected empty result from validator smart contract call");
      }

      return decodedList;
    } else {
      throw new IllegalStateException(
          "Failed validator smart contract call: " + result.getValidationResult());
    }
  }
}
