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

import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.GasLimitCalculator;
import org.hyperledger.besu.ethereum.core.Frame;
import org.hyperledger.besu.ethereum.core.PermissionTransactionFilter;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TransactionValidationFactoryTest {
  private static final Optional<BigInteger> CHAIN_ID = Optional.of(BigInteger.ONE);
  @Mock GasCalculator gasCalculator;
  @Mock GasLimitCalculator gasLimitCalculator;
  @Mock PermissionTransactionFilter permissionTransactionFilter;

  @Test
  public void alwaysTheSameInstanceIsReturnedOnceCreated() {
    final TransactionValidatorFactory transactionValidatorFactory =
        new TransactionValidatorFactory(gasCalculator, gasLimitCalculator, false, CHAIN_ID);
    assertThat(transactionValidatorFactory.get()).isSameAs(transactionValidatorFactory.get());
  }

  @Test
  public void alwaysTheSameInstanceIsReturnedOnceCreatedWithPermissionFilter() {
    final TransactionValidatorFactory transactionValidatorFactory =
        new TransactionValidatorFactory(gasCalculator, gasLimitCalculator, false, CHAIN_ID);
    transactionValidatorFactory.setPermissionTransactionFilter(permissionTransactionFilter);
    assertThat(transactionValidatorFactory.get()).isSameAs(transactionValidatorFactory.get());
  }

  @Test
  public void withAcceptedTransactionTypeAddsTheTypeAndLeavesTheOriginalUnchanged() {
    final TransactionValidatorFactory parent =
        new TransactionValidatorFactory(
            gasCalculator,
            GasLimitCalculator.constant(),
            FeeMarket.london(0L),
            true,
            CHAIN_ID,
            Set.of(TransactionType.EIP1559),
            Integer.MAX_VALUE);

    final TransactionValidatorFactory derived =
        parent.withAcceptedTransactionType(TransactionType.FRAME);

    assertThat(derived).isNotSameAs(parent);
    assertThat(validate(derived, frameTransaction()).isValid()).isTrue();
    final ValidationResult<TransactionInvalidReason> onParent =
        validate(parent, frameTransaction());
    assertThat(onParent.getInvalidReason())
        .isEqualTo(TransactionInvalidReason.INVALID_TRANSACTION_FORMAT);
    assertThat(onParent.getErrorMessage())
        .isEqualTo("Transaction type FRAME is invalid, accepted transaction types are [EIP1559]");
  }

  @Test
  public void derivedFactoriesListTheParentTypesFollowedByTheAddedOnes() {
    final TransactionValidatorFactory derived =
        new TransactionValidatorFactory(
                gasCalculator,
                GasLimitCalculator.constant(),
                FeeMarket.london(0L),
                true,
                CHAIN_ID,
                Set.of(TransactionType.EIP1559),
                Integer.MAX_VALUE)
            .withAcceptedTransactionType(TransactionType.FRAME)
            .withAcceptedTransactionType(TransactionType.DELEGATE_CODE);
    final Transaction blob =
        new TransactionTestFixture()
            .type(TransactionType.BLOB)
            .chainId(CHAIN_ID)
            .to(Optional.of(Address.fromHexString("0x2222222222222222222222222222222222222222")))
            .versionedHashes(
                Optional.of(List.of(new VersionedHash(VersionedHash.SHA256_VERSION_ID, Hash.ZERO))))
            .createTransaction(SignatureAlgorithmFactory.getInstance().generateKeyPair());

    final ValidationResult<TransactionInvalidReason> result = validate(derived, blob);

    assertThat(result.getErrorMessage())
        .isEqualTo(
            "Transaction type BLOB is invalid, accepted transaction types are "
                + "[EIP1559, FRAME, DELEGATE_CODE]");
  }

  private ValidationResult<TransactionInvalidReason> validate(
      final TransactionValidatorFactory factory, final Transaction transaction) {
    return factory
        .get()
        .validate(
            transaction,
            Optional.of(Wei.ZERO),
            Optional.empty(),
            TransactionValidationParams.processingBlock());
  }

  private static Transaction frameTransaction() {
    return Transaction.builder()
        .type(TransactionType.FRAME)
        .chainId(BigInteger.ONE)
        .nonce(0)
        .sender(Address.fromHexString("0x1111111111111111111111111111111111111111"))
        .frames(
            List.of(
                new Frame(
                    Frame.MODE_VERIFY,
                    Frame.APPROVE_SCOPE_MASK,
                    Optional.empty(),
                    50_000L,
                    0L,
                    Wei.ZERO,
                    Bytes.EMPTY)))
        .frameSignatures(List.of())
        .maxPriorityFeePerGas(Wei.ZERO)
        .maxFeePerGas(Wei.ZERO)
        .build();
  }
}
