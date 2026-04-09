/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.mainnet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.feemarket.CoinbaseFeePriceCalculator;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.AmsterdamGasCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.processor.ContractCreationProcessor;
import org.hyperledger.besu.evm.processor.MessageCallProcessor;
import org.hyperledger.besu.evm.worldstate.CodeDelegationService;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.math.BigInteger;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class Eip8037GasLimitTest {

  private static final int MAX_STACK_SIZE = 1024;

  private final GasCalculator gasCalculator = new AmsterdamGasCalculator();

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private TransactionValidatorFactory transactionValidatorFactory;

  @Mock private ContractCreationProcessor contractCreationProcessor;
  @Mock private MessageCallProcessor messageCallProcessor;

  @Mock private WorldUpdater worldState;
  @Mock private ProcessableBlockHeader blockHeader;
  @Mock private Transaction transaction;
  @Mock private BlockHashLookup blockHashLookup;

  @Mock private MutableAccount senderAccount;

  private MainnetTransactionProcessor createProcessor() {
    return MainnetTransactionProcessor.builder()
        .gasCalculator(gasCalculator)
        .transactionValidatorFactory(transactionValidatorFactory)
        .contractCreationProcessor(contractCreationProcessor)
        .messageCallProcessor(messageCallProcessor)
        .clearEmptyAccounts(false)
        .warmCoinbase(false)
        .maxStackSize(MAX_STACK_SIZE)
        .feeMarket(FeeMarket.legacy())
        .coinbaseFeePriceCalculator(CoinbaseFeePriceCalculator.frontier())
        .codeDelegationProcessor(
            new CodeDelegationProcessor(
                Optional.of(BigInteger.ONE), BigInteger.TEN, new CodeDelegationService()))
        .build();
  }

  private void setupCommonMocks(final long gasLimit) {
    final Address senderAddress =
        Address.fromHexString("0x5555555555555555555555555555555555555555");
    final Address toAddress = Address.fromHexString("0x2222222222222222222222222222222222222222");

    when(transaction.getType()).thenReturn(TransactionType.EIP1559);
    when(transaction.getPayload()).thenReturn(Bytes.EMPTY);
    when(transaction.getSender()).thenReturn(senderAddress);
    when(transaction.getValue()).thenReturn(Wei.ZERO);
    when(transaction.getTo()).thenReturn(Optional.of(toAddress));
    when(transaction.getGasLimit()).thenReturn(gasLimit);

    when(transactionValidatorFactory.get().validate(any(), any(), any(), any()))
        .thenReturn(ValidationResult.valid());
    when(transactionValidatorFactory.get().validateForSender(any(), any(), any()))
        .thenReturn(ValidationResult.valid());
    when(worldState.getOrCreateSenderAccount(any())).thenReturn(senderAccount);
    when(worldState.updater()).thenReturn(worldState);
  }

  @Test
  void exceptionalHaltPreservesStateGasReservoirForRefund() {
    // EIP-8037: On exceptional halt of the initial frame, the state_gas_reservoir must be
    // preserved for transaction-level refund. This test simulates a child frame having refunded
    // state gas to the parent's reservoir before the parent runs out of regular gas. The
    // refunded reservoir must not be lost: the total gas used should be
    // txGasLimit - preserved_reservoir, not the full txGasLimit.
    setupCommonMocks(20_000_000L);

    // txGasLimit=20M, intrinsic≈21k → gasAvailable≈19,979,000.
    // regularBudget = TX_MAX_GAS_LIMIT (16,777,216) - intrinsic ≈ 16,756,216.
    // gas_left initial = 16,756,216; reservoir initial = 19,979,000 - 16,756,216 = 3,222,784.
    // We simulate: child SSTORE spilled 37,568 into child gas_left, then child halted and the
    // spill was restored to the reservoir. The initial frame then runs out of regular gas.
    final long childRefund = 37_568L;

    doAnswer(
            invocation -> {
              final MessageFrame frame = invocation.getArgument(0);
              // Simulate a child frame halt having added state gas back to the reservoir.
              frame.incrementStateGasReservoir(childRefund);
              // Now simulate the initial frame running out of regular gas.
              frame.setExceptionalHaltReason(Optional.of(ExceptionalHaltReason.INSUFFICIENT_GAS));
              frame.setGasRemaining(0);
              frame.getMessageFrameStack().pop();
              return null;
            })
        .when(messageCallProcessor)
        .process(any(), any());

    final TransactionProcessingResult result =
        createProcessor()
            .processTransaction(
                worldState,
                blockHeader,
                transaction,
                Address.fromHexString("0x4242424242424242424242424242424242424242"),
                blockHashLookup,
                ImmutableTransactionValidationParams.builder().build(),
                Wei.ZERO);

    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getValidationResult().getInvalidReason())
        .isEqualTo(TransactionInvalidReason.EXECUTION_HALTED);
    // The reservoir (initial budget overflow 3,222,784 + childRefund 37,568) must be preserved
    // for refund. Total gas used = 20,000,000 - (3,222,784 + 37,568) = 16,739,648.
    assertThat(result.getEstimateGasUsedByTransaction())
        .isEqualTo(20_000_000L - 3_222_784L - childRefund);
  }

  @Test
  void regularGasWithinTxMaxGasLimitSucceeds() {
    setupCommonMocks(20_000_000L);

    doAnswer(
            invocation -> {
              final MessageFrame frame = invocation.getArgument(0);
              frame.setState(MessageFrame.State.COMPLETED_SUCCESS);
              // totalConsumed = 20M - 5M = 15M
              // regularConsumed = 15M - 0 = 15M < TX_MAX_GAS_LIMIT (16,777,216) -> passes
              frame.setGasRemaining(5_000_000L);
              frame.getMessageFrameStack().pop();
              return null;
            })
        .when(messageCallProcessor)
        .process(any(), any());

    final TransactionProcessingResult result =
        createProcessor()
            .processTransaction(
                worldState,
                blockHeader,
                transaction,
                Address.fromHexString("0x4242424242424242424242424242424242424242"),
                blockHashLookup,
                ImmutableTransactionValidationParams.builder().build(),
                Wei.ZERO);

    assertThat(result.isSuccessful()).isTrue();
  }

  @Test
  void stateGasCanPushTotalBeyondTxMaxGasLimitWithoutRevert() {
    // Total gas > TX_MAX_GAS_LIMIT but regular gas portion is within limit
    setupCommonMocks(20_000_000L);

    doAnswer(
            invocation -> {
              final MessageFrame frame = invocation.getArgument(0);
              frame.setState(MessageFrame.State.COMPLETED_SUCCESS);
              // totalConsumed = 20M - 1M = 19M (exceeds TX_MAX_GAS_LIMIT)
              // stateGas = 5M
              // regularConsumed = 19M - 5M = 14M < TX_MAX_GAS_LIMIT -> passes
              frame.setGasRemaining(1_000_000L);
              frame.incrementStateGasUsed(5_000_000L);
              frame.getMessageFrameStack().pop();
              return null;
            })
        .when(messageCallProcessor)
        .process(any(), any());

    final TransactionProcessingResult result =
        createProcessor()
            .processTransaction(
                worldState,
                blockHeader,
                transaction,
                Address.fromHexString("0x4242424242424242424242424242424242424242"),
                blockHashLookup,
                ImmutableTransactionValidationParams.builder().build(),
                Wei.ZERO);

    // Total gas exceeds TX_MAX_GAS_LIMIT but only regular gas is checked
    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStateGasUsed()).isEqualTo(5_000_000L);
  }
}
