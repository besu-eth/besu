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
package org.hyperledger.besu.ethereum.mainnet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.feemarket.CoinbaseFeePriceCalculator;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.gascalculator.LondonGasCalculator;
import org.hyperledger.besu.evm.processor.ContractCreationProcessor;
import org.hyperledger.besu.evm.processor.MessageCallProcessor;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.CodeDelegationService;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.evm.worldstate.WorldView;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MainnetTransactionProcessorTest {

  private static final int MAX_STACK_SIZE = 1024;

  private final GasCalculator gasCalculator = new LondonGasCalculator();

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private TransactionValidatorFactory transactionValidatorFactory;

  @Mock private ContractCreationProcessor contractCreationProcessor;
  @Mock private MessageCallProcessor messageCallProcessor;

  @Mock private WorldUpdater worldState;
  @Mock private ProcessableBlockHeader blockHeader;
  @Mock private Transaction transaction;
  @Mock private BlockHashLookup blockHashLookup;

  @Mock private MutableAccount senderAccount;
  @Mock private MutableAccount receiverAccount;

  MainnetTransactionProcessor createTransactionProcessor(final boolean warmCoinbase) {
    return MainnetTransactionProcessor.builder()
        .gasCalculator(gasCalculator)
        .transactionValidatorFactory(transactionValidatorFactory)
        .contractCreationProcessor(contractCreationProcessor)
        .messageCallProcessor(messageCallProcessor)
        .clearEmptyAccounts(false)
        .warmCoinbase(warmCoinbase)
        .maxStackSize(MAX_STACK_SIZE)
        .feeMarket(FeeMarket.legacy())
        .coinbaseFeePriceCalculator(CoinbaseFeePriceCalculator.frontier())
        .codeDelegationProcessor(
            new CodeDelegationProcessor(
                Optional.of(BigInteger.ONE), BigInteger.TEN, new CodeDelegationService()))
        .build();
  }

  @Test
  void shouldWarmCoinbaseIfRequested() {
    Optional<Address> toAddress =
        Optional.of(Address.fromHexString("0x2222222222222222222222222222222222222222"));
    when(transaction.getTo()).thenReturn(toAddress);
    Address senderAddress = Address.fromHexString("0x5555555555555555555555555555555555555555");
    Address coinbaseAddress = Address.fromHexString("0x4242424242424242424242424242424242424242");

    when(transaction.getType()).thenReturn(TransactionType.EIP1559);
    when(transaction.getPayload()).thenReturn(Bytes.EMPTY);
    when(transaction.getGasLimit()).thenReturn(100_000L);
    when(transaction.getSender()).thenReturn(senderAddress);
    when(transaction.getValue()).thenReturn(Wei.ZERO);
    when(transactionValidatorFactory.get().validate(any(), any(), any(), any()))
        .thenReturn(ValidationResult.valid());
    when(transactionValidatorFactory.get().validateForSender(any(), any(), any()))
        .thenReturn(ValidationResult.valid());
    when(worldState.getOrCreateSenderAccount(any())).thenReturn(senderAccount);
    when(worldState.updater()).thenReturn(worldState);

    AtomicBoolean coinbaseWarmed = new AtomicBoolean(false);
    doAnswer(
            invocation -> {
              MessageFrame messageFrame = invocation.getArgument(0);
              coinbaseWarmed.set(messageFrame.warmUpAddress(coinbaseAddress));
              messageFrame.getMessageFrameStack().pop();
              return null;
            })
        .when(messageCallProcessor)
        .process(any(), any());

    var transactionProcessor = createTransactionProcessor(true);
    transactionProcessor.processTransaction(
        worldState,
        blockHeader,
        transaction,
        coinbaseAddress,
        blockHashLookup,
        ImmutableTransactionValidationParams.builder().build(),
        Wei.ZERO);

    assertThat(coinbaseWarmed).isTrue();

    transactionProcessor = createTransactionProcessor(false);
    transactionProcessor.processTransaction(
        worldState,
        blockHeader,
        transaction,
        coinbaseAddress,
        blockHashLookup,
        ImmutableTransactionValidationParams.builder().build(),
        Wei.ZERO);

    assertThat(coinbaseWarmed).isFalse();
  }

  @ParameterizedTest
  @MethodSource("provideExceptionsForTransactionProcessing")
  /*
   This test is to ensure that the OperationTracer.traceEndTxCalled is called even if the transaction processing fails.
   @param exception will either be of class RuntimeException or MerkleTrieException
  */
  void shouldTraceEndTxOnFailingTransaction(final Exception exception) {
    Optional<Address> toAddress =
        Optional.of(Address.fromHexString("0x2222222222222222222222222222222222222222"));
    Address senderAddress = Address.fromHexString("0x5555555555555555555555555555555555555555");
    Address coinbaseAddress = Address.fromHexString("0x4242424242424242424242424242424242424242");

    when(transaction.getType()).thenReturn(TransactionType.EIP1559);
    when(transaction.getTo()).thenReturn(toAddress);
    when(transaction.getPayload()).thenReturn(Bytes.EMPTY);
    when(transaction.getGasLimit()).thenReturn(100_000L);
    when(transaction.getSender()).thenReturn(senderAddress);
    when(transaction.getValue()).thenReturn(Wei.ZERO);
    when(transactionValidatorFactory.get().validate(any(), any(), any(), any()))
        .thenReturn(ValidationResult.valid());
    when(transactionValidatorFactory.get().validateForSender(any(), any(), any()))
        .thenReturn(ValidationResult.valid());
    when(worldState.getOrCreateSenderAccount(senderAddress)).thenReturn(senderAccount);
    when(worldState.get(toAddress.get())).thenReturn(receiverAccount);
    when(worldState.updater()).thenReturn(worldState);
    // throw exception when processing the transaction
    doAnswer(
            invocation -> {
              throw exception;
            })
        .when(messageCallProcessor)
        .process(any(), any());

    final TraceEndTxTracer tracer = new TraceEndTxTracer();
    var transactionProcessor = createTransactionProcessor(true);
    try {
      transactionProcessor.processTransaction(
          worldState,
          blockHeader,
          transaction,
          coinbaseAddress,
          tracer,
          blockHashLookup,
          ImmutableTransactionValidationParams.builder().build(),
          Wei.ZERO);
    } catch (final MerkleTrieException e) {
      // the MerkleTrieException is thrown again in MainnetTransactionProcessor, we ignore it here
    }

    assertThat(tracer.traceEndTxCalled).isTrue();
  }

  // those two exceptions can be thrown while processing a transaction
  private static Stream<Arguments> provideExceptionsForTransactionProcessing() {
    return Stream.of(
        Arguments.of(new MerkleTrieException("")), Arguments.of(new RuntimeException()));
  }

  static class TraceEndTxTracer implements OperationTracer {
    boolean traceEndTxCalled = false;

    @Override
    public void traceEndTransaction(
        final WorldView worldView,
        final org.hyperledger.besu.datatypes.Transaction tx,
        final boolean status,
        final Bytes output,
        final List<Log> logs,
        final long gasUsed,
        final Set<Address> selfDestructs,
        final long timeNs) {
      this.traceEndTxCalled = true;
    }
  }

  @Test
  void shouldCallTransactionValidatorWithExpectedTransactionValidationParams() {
    final ArgumentCaptor<TransactionValidationParams> txValidationParamCaptor =
        transactionValidationParamCaptor();

    final TransactionValidationParams expectedValidationParams =
        ImmutableTransactionValidationParams.builder().build();

    var transactionProcessor = createTransactionProcessor(false);

    transactionProcessor.processTransaction(
        worldState,
        blockHeader,
        transaction,
        Address.fromHexString("1"),
        blockHashLookup,
        ImmutableTransactionValidationParams.builder().build(),
        Wei.ZERO);

    assertThat(txValidationParamCaptor.getValue())
        .usingRecursiveComparison()
        .isEqualTo(expectedValidationParams);
  }

  private ArgumentCaptor<TransactionValidationParams> transactionValidationParamCaptor() {
    final ArgumentCaptor<TransactionValidationParams> txValidationParamCaptor =
        ArgumentCaptor.forClass(TransactionValidationParams.class);
    when(transactionValidatorFactory.get().validate(any(), any(), any(), any()))
        .thenReturn(ValidationResult.valid());
    // returning invalid transaction to halt method execution
    when(transactionValidatorFactory
            .get()
            .validateForSender(any(), any(), txValidationParamCaptor.capture()))
        .thenReturn(ValidationResult.invalid(TransactionInvalidReason.NONCE_TOO_HIGH));
    return txValidationParamCaptor;
  }
}
