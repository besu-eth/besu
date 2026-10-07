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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.consensus.qbft.validator.ValidatorContractController.GET_VALIDATORS;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.JsonQbftConfigOptions;
import org.hyperledger.besu.consensus.qbft.MutableQbftConfigOptions;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.ImmutableTransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.transaction.CallParameter;
import org.hyperledger.besu.ethereum.transaction.ImmutableCallParameter;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulatorResult;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.web3j.abi.FunctionEncoder;
import org.web3j.abi.TypeReference;
import org.web3j.abi.datatypes.DynamicArray;
import org.web3j.abi.datatypes.Function;

public class ValidatorContractControllerTest {
  private static final String GET_VALIDATORS_FUNCTION_RESULT =
      "00000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000001000000000000000000000000eac51e3fe1afc9894f0dfeab8ceb471899b932df";
  private static final Address VALIDATOR_ADDRESS =
      Address.fromHexString("0xeac51e3fe1afc9894f0dfeab8ceb471899b932df");
  private static final Address CONTRACT_ADDRESS = Address.fromHexString("1");
  private static final TransactionValidationParams ALLOW_EXCEEDING_BALANCE_VALIDATION_PARAMS =
      ImmutableTransactionValidationParams.builder()
          .from(TransactionValidationParams.transactionSimulator())
          .isAllowExceedingBalance(true)
          .build();

  private final TransactionSimulator transactionSimulator =
      Mockito.mock(TransactionSimulator.class);

  private final Transaction transaction = Mockito.mock(Transaction.class);
  private CallParameter callParameter;

  @BeforeEach
  public void setup() {
    final Function getValidatorsFunction =
        new Function(
            GET_VALIDATORS,
            List.of(),
            List.of(new TypeReference<DynamicArray<org.web3j.abi.datatypes.Address>>() {}));
    final Bytes payload = Bytes.fromHexString(FunctionEncoder.encode(getValidatorsFunction));
    callParameter = ImmutableCallParameter.builder().to(CONTRACT_ADDRESS).input(payload).build();
    final MutableQbftConfigOptions qbftConfigOptions =
        new MutableQbftConfigOptions(JsonQbftConfigOptions.DEFAULT);
    qbftConfigOptions.setValidatorContractAddress(
        Optional.of(CONTRACT_ADDRESS.getBytes().toHexString()));
  }

  @Test
  public void decodesGetValidatorsResultFromContractCall() {
    final TransactionSimulatorResult result =
        new TransactionSimulatorResult(
            transaction,
            TransactionProcessingResult.successful(
                List.of(),
                0,
                0,
                Bytes.fromHexString(GET_VALIDATORS_FUNCTION_RESULT),
                Optional.empty(),
                ValidationResult.valid()));

    when(transactionSimulator.process(
            callParameter,
            ALLOW_EXCEEDING_BALANCE_VALIDATION_PARAMS,
            OperationTracer.NO_TRACING,
            1))
        .thenReturn(Optional.of(result));

    final ValidatorContractController validatorContractController =
        new ValidatorContractController(transactionSimulator);
    final Collection<Address> validators =
        validatorContractController.getValidators(1, CONTRACT_ADDRESS);
    assertThat(validators).containsExactly(VALIDATOR_ADDRESS);
  }

  @Test
  public void throwErrorIfInvalidSimulationResult() {
    final TransactionSimulatorResult result =
        new TransactionSimulatorResult(
            transaction,
            TransactionProcessingResult.invalid(
                ValidationResult.invalid(TransactionInvalidReason.INTERNAL_ERROR)));

    when(transactionSimulator.process(
            callParameter,
            ALLOW_EXCEEDING_BALANCE_VALIDATION_PARAMS,
            OperationTracer.NO_TRACING,
            1))
        .thenReturn(Optional.of(result));

    final ValidatorContractController validatorContractController =
        new ValidatorContractController(transactionSimulator);
    Assertions.assertThatThrownBy(
            () -> validatorContractController.getValidators(1, CONTRACT_ADDRESS))
        .hasMessageContaining("Failed validator smart contract call");
  }

  @Test
  public void throwErrorIfFailedSimulationResult() {
    final TransactionSimulatorResult result =
        new TransactionSimulatorResult(
            transaction,
            TransactionProcessingResult.failed(
                0,
                0,
                ValidationResult.invalid(TransactionInvalidReason.INTERNAL_ERROR),
                Optional.empty(),
                Optional.empty(),
                Optional.empty()));

    when(transactionSimulator.process(
            callParameter,
            ALLOW_EXCEEDING_BALANCE_VALIDATION_PARAMS,
            OperationTracer.NO_TRACING,
            1))
        .thenReturn(Optional.of(result));

    final ValidatorContractController validatorContractController =
        new ValidatorContractController(transactionSimulator);
    Assertions.assertThatThrownBy(
            () -> validatorContractController.getValidators(1, CONTRACT_ADDRESS))
        .hasMessageContaining("Failed validator smart contract call");
  }

  @Test
  public void throwErrorIfUnexpectedSuccessfulEmptySimulationResult() {
    final TransactionSimulatorResult result =
        new TransactionSimulatorResult(
            transaction,
            TransactionProcessingResult.successful(
                List.of(), 0, 0, Bytes.EMPTY, Optional.empty(), ValidationResult.valid()));

    when(transactionSimulator.process(
            callParameter,
            ALLOW_EXCEEDING_BALANCE_VALIDATION_PARAMS,
            OperationTracer.NO_TRACING,
            1))
        .thenReturn(Optional.of(result));

    final ValidatorContractController validatorContractController =
        new ValidatorContractController(transactionSimulator);

    Assertions.assertThatThrownBy(
            () -> validatorContractController.getValidators(1, CONTRACT_ADDRESS))
        .hasMessage("Unexpected empty result from validator smart contract call");
  }

  @Test
  public void throwErrorIfEmptySimulationResult() {
    when(transactionSimulator.process(
            callParameter,
            ALLOW_EXCEEDING_BALANCE_VALIDATION_PARAMS,
            OperationTracer.NO_TRACING,
            1))
        .thenReturn(Optional.empty());

    final ValidatorContractController validatorContractController =
        new ValidatorContractController(transactionSimulator);
    Assertions.assertThatThrownBy(
            () -> validatorContractController.getValidators(1, CONTRACT_ADDRESS))
        .hasMessage("Failed validator smart contract call: unable to execute call at block 1");
  }
}
