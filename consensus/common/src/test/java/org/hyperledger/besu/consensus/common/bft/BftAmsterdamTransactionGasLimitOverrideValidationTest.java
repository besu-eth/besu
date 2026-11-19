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
package org.hyperledger.besu.consensus.common.bft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams.processingBlockParams;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.BlobType;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.GasLimitCalculator;
import org.hyperledger.besu.ethereum.core.ChainId;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.mainnet.AmsterdamTargetingGasLimitCalculator;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionValidator;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.mainnet.feemarket.BaseFeeMarket;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import java.math.BigInteger;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Covers #11492: a BFT {@code transactiongaslimit} override must not drop Amsterdam's EIP-8037
 * caps.
 */
public class BftAmsterdamTransactionGasLimitOverrideValidationTest {

  private final KeyPair senderKeys = SignatureAlgorithmFactory.getInstance().generateKeyPair();
  private final GasCalculator gasCalculator = mock(GasCalculator.class);
  private final BaseFeeMarket feeMarket = FeeMarket.london(0L);

  @BeforeEach
  void setUp() {
    when(gasCalculator.transactionIntrinsicGasCost(any(), anyLong())).thenReturn(50L);
    when(gasCalculator.transactionFloorCost(any())).thenReturn(0L);
    when(gasCalculator.accessListGasCost(any())).thenReturn(0L);
    when(gasCalculator.delegateCodeGasCost(anyInt())).thenReturn(0L);
  }

  @Test
  void zeroOverrideKeepsAmsterdamTxGasCap() {
    final MainnetTransactionValidator validator = amsterdamValidatorWithBftOverride(0L);
    final Transaction transaction =
        eip1559Transaction(
            AmsterdamTargetingGasLimitCalculator.EIP_8037_TRANSACTION_TOTAL_GAS_LIMIT_CAP + 1);

    final ValidationResult<TransactionInvalidReason> result =
        validator.validate(
            transaction, Optional.of(Wei.of(150000L)), Optional.empty(), processingBlockParams);

    assertThat(result.isValid()).isFalse();
    assertThat(result.getInvalidReason())
        .isEqualTo(TransactionInvalidReason.EXCEEDS_TRANSACTION_GAS_LIMIT);
    assertThat(result.getErrorMessage())
        .isEqualTo(
            "Transaction gas limit must be at most "
                + AmsterdamTargetingGasLimitCalculator.EIP_8037_TRANSACTION_TOTAL_GAS_LIMIT_CAP);
  }

  @Test
  void zeroOverrideKeepsAmsterdamIntrinsicGasCap() {
    when(gasCalculator.transactionIntrinsicGasCost(any(), anyLong())).thenReturn(16_777_217L);
    final MainnetTransactionValidator validator = amsterdamValidatorWithBftOverride(0L);
    final Transaction transaction = eip1559Transaction(30_000_000L);

    final ValidationResult<TransactionInvalidReason> result =
        validator.validate(
            transaction, Optional.of(Wei.of(150000L)), Optional.empty(), processingBlockParams);

    assertThat(result.isValid()).isFalse();
    assertThat(result.getInvalidReason())
        .isEqualTo(TransactionInvalidReason.INTRINSIC_GAS_EXCEEDS_GAS_LIMIT);
  }

  @Test
  void explicitOverrideStillEnforcesAmsterdamIntrinsicGasCap() {
    when(gasCalculator.transactionIntrinsicGasCost(any(), anyLong())).thenReturn(16_777_217L);
    final MainnetTransactionValidator validator = amsterdamValidatorWithBftOverride(1_000_000L);
    final Transaction transaction = eip1559Transaction(1_000_000L);

    final ValidationResult<TransactionInvalidReason> result =
        validator.validate(
            transaction, Optional.of(Wei.of(150000L)), Optional.empty(), processingBlockParams);

    assertThat(result.isValid()).isFalse();
    assertThat(result.getInvalidReason())
        .isEqualTo(TransactionInvalidReason.INTRINSIC_GAS_EXCEEDS_GAS_LIMIT);
  }

  private MainnetTransactionValidator amsterdamValidatorWithBftOverride(final long bftOverride) {
    final GasLimitCalculator amsterdam =
        new AmsterdamTargetingGasLimitCalculator(
            0L, feeMarket, gasCalculator, 6, 3, OptionalInt.of(6), OptionalInt.empty());
    final GasLimitCalculator gasLimitCalculator =
        new TransactionGasLimitOverrideGasLimitCalculator(amsterdam, bftOverride);
    return new MainnetTransactionValidator(
        gasCalculator,
        gasLimitCalculator,
        feeMarket,
        false,
        ChainId.fromOptional(Optional.of(BigInteger.ONE)),
        Set.of(TransactionType.FRONTIER, TransactionType.EIP1559),
        Set.of(BlobType.KZG_PROOF),
        Integer.MAX_VALUE);
  }

  private Transaction eip1559Transaction(final long gasLimit) {
    return new TransactionTestFixture()
        .maxPriorityFeePerGas(Optional.of(Wei.of(1)))
        .maxFeePerGas(Optional.of(Wei.of(150000L)))
        .type(TransactionType.EIP1559)
        .chainId(Optional.of(BigInteger.ONE))
        .gasLimit(gasLimit)
        .createTransaction(senderKeys);
  }
}
