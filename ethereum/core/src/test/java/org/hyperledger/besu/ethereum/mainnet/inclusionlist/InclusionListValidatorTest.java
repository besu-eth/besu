/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.mainnet.inclusionlist;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.GasLimitCalculator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.mainnet.BlockGasAccountingStrategy;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionValidator;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidator;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidatorFactory;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.gascalculator.StateGasCostCalculator;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InclusionListValidatorTest {

  private static final long BLOCK_GAS_LIMIT = 30_000_000L;
  private static final long TX_EXECUTION_GAS_LIMIT = 16_777_216L;
  private static final long TX_GAS_LIMIT = 100_000L;

  private final ProtocolSpec protocolSpec = mock(ProtocolSpec.class);
  private final MainnetTransactionProcessor transactionProcessor =
      mock(MainnetTransactionProcessor.class);
  private final TransactionValidator transactionValidator = mock(TransactionValidator.class);
  private final TransactionValidatorFactory transactionValidatorFactory =
      mock(TransactionValidatorFactory.class);
  private final WorldUpdater worldUpdater = mock(WorldUpdater.class);
  private final BlockHeader blockHeader =
      new BlockHeaderTestFixture().gasLimit(BLOCK_GAS_LIMIT).baseFeePerGas(Wei.ONE).buildHeader();
  private final Transaction ilTx =
      new TransactionTestFixture()
          .gasLimit(TX_GAS_LIMIT)
          .createTransaction(SignatureAlgorithmFactory.getInstance().generateKeyPair());

  @BeforeEach
  void setUp() {
    when(protocolSpec.getBlockGasAccountingStrategy())
        .thenReturn(BlockGasAccountingStrategy.AMSTERDAM);

    final GasCalculator gasCalculator = mock(GasCalculator.class);
    final StateGasCostCalculator stateGasCostCalculator = mock(StateGasCostCalculator.class);
    when(stateGasCostCalculator.transactionExecutionGasLimit()).thenReturn(TX_EXECUTION_GAS_LIMIT);
    when(gasCalculator.stateGasCostCalculator()).thenReturn(stateGasCostCalculator);
    when(protocolSpec.getGasCalculator()).thenReturn(gasCalculator);

    // the transaction is valid against the post-execution state
    when(transactionValidator.validate(any(), any(), any(), any()))
        .thenReturn(ValidationResult.<TransactionInvalidReason>valid());
    when(transactionValidator.validateForSender(any(), any(), any()))
        .thenReturn(ValidationResult.<TransactionInvalidReason>valid());
    when(transactionValidatorFactory.get()).thenReturn(transactionValidator);
    when(protocolSpec.getTransactionValidatorFactory()).thenReturn(transactionValidatorFactory);

    when(protocolSpec.getTransactionProcessor()).thenReturn(transactionProcessor);
  }

  @Test
  void satisfiedWithoutAnyWorkWhenAllInclusionListTransactionsAreInTheBlock() {
    // the case of every block until an inclusion list is passed by the engine API
    assertThat(
            InclusionListValidator.validate(
                    protocolSpec, worldUpdater, blockHeader, 0, 0, List.of())
                .isValid())
        .isTrue();

    verify(protocolSpec, never()).getTransactionValidatorFactory();
    verifyNoInteractions(worldUpdater);
  }

  @Test
  void unsatisfiedWhenTransactionFitsAndIsValid() {
    assertThat(validate(0, 0).getStatus())
        .isEqualTo(InclusionListValidationResult.Status.UNSATISFIED);
    // the nonce and balance are checked against the post-execution state, without executing it
    verify(transactionValidator).validateForSender(any(), any(), any());
    verifyNotExecuted();
  }

  @Test
  void satisfiedWhenTransactionIsNotValid() {
    when(transactionValidator.validate(any(), any(), any(), any()))
        .thenReturn(
            ValidationResult.invalid(TransactionInvalidReason.INTRINSIC_GAS_EXCEEDS_GAS_LIMIT));

    assertThat(validate(0, 0).isValid()).isTrue();
    verifyNotExecuted();
  }

  @Test
  void satisfiedWhenTransactionIsNotValidForTheSender() {
    when(transactionValidator.validateForSender(any(), any(), any()))
        .thenReturn(ValidationResult.invalid(TransactionInvalidReason.NONCE_TOO_LOW));

    assertThat(validate(0, 0).isValid()).isTrue();
    verifyNotExecuted();
  }

  @Test
  void unsatisfiedWhenTransactionFitsExactlyInBothDimensions() {
    final long used = BLOCK_GAS_LIMIT - TX_GAS_LIMIT;
    assertThat(validate(used, used).getStatus())
        .isEqualTo(InclusionListValidationResult.Status.UNSATISFIED);
  }

  @Test
  void satisfiedWhenExecutionGasLeftIsNotEnough() {
    assertThat(validate(BLOCK_GAS_LIMIT - TX_GAS_LIMIT + 1, 0).isValid()).isTrue();
    verify(transactionValidator, never()).validateForSender(any(), any(), any());
  }

  @Test
  void satisfiedWhenStateGasLeftIsNotEnough() {
    // EIP-8037: the transaction could use its whole gas limit as state gas
    assertThat(validate(0, BLOCK_GAS_LIMIT - TX_GAS_LIMIT + 1).isValid()).isTrue();
    verify(transactionValidator, never()).validateForSender(any(), any(), any());
  }

  @Test
  void satisfiedWhenTheSenderOfTheTransactionCannotBeRecovered() {
    // r = 5 is a valid scalar but not the x-coordinate of any curve point
    final Transaction unrecoverableTx =
        Transaction.builder()
            .copiedFrom(ilTx)
            .sender(null)
            .signature(
                SignatureAlgorithmFactory.getInstance()
                    .createSignature(BigInteger.valueOf(5), BigInteger.ONE, (byte) 0))
            .build();
    final TransactionValidator mainnetTransactionValidator =
        new MainnetTransactionValidator(
            mock(GasCalculator.class),
            mock(GasLimitCalculator.class),
            FeeMarket.london(0L),
            true,
            unrecoverableTx.getChainId(),
            Set.of(unrecoverableTx.getType()),
            Set.of(BlobType.KZG_PROOF),
            Integer.MAX_VALUE);
    when(transactionValidatorFactory.get()).thenReturn(mainnetTransactionValidator);

    assertThat(
            InclusionListValidator.validate(
                    protocolSpec, worldUpdater, blockHeader, 0, 0, Set.of(unrecoverableTx))
                .isValid())
        .isTrue();
    // skipped by the signature check, before its sender is needed
    assertThat(
            mainnetTransactionValidator
                .validate(
                    unrecoverableTx,
                    blockHeader.getBaseFee(),
                    Optional.of(Wei.ZERO),
                    TransactionValidationParams.processingBlock())
                .getInvalidReason())
        .isEqualTo(TransactionInvalidReason.INVALID_SIGNATURE);
    verifyNoInteractions(worldUpdater);
  }

  private InclusionListValidationResult validate(
      final long cumulativeExecutionGasUsed, final long cumulativeStateGasUsed) {
    return InclusionListValidator.validate(
        protocolSpec,
        worldUpdater,
        blockHeader,
        cumulativeExecutionGasUsed,
        cumulativeStateGasUsed,
        Set.of(ilTx));
  }

  private void verifyNotExecuted() {
    verify(transactionProcessor, never())
        .processTransaction(
            any(WorldUpdater.class),
            any(ProcessableBlockHeader.class),
            any(Transaction.class),
            any(Address.class),
            any(OperationTracer.class),
            any(BlockHashLookup.class),
            any(TransactionValidationParams.class),
            any(Wei.class));
  }
}
