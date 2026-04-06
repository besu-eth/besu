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
import static org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams.transactionPoolParams;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.GasLimitCalculator;
import org.hyperledger.besu.ethereum.core.PermissionTransactionFilter;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.evm.account.Account;

import java.math.BigInteger;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class PermissionTransactionValidatorTest extends MainnetTransactionValidatorTest {

  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();
  private static final KeyPair senderKeys = SIGNATURE_ALGORITHM.generateKeyPair();

  private final Transaction basicTransaction =
      new TransactionTestFixture()
          .chainId(Optional.of(BigInteger.ONE))
          .createTransaction(senderKeys);

  @Test
  public void shouldRejectTransactionIfAccountIsNotPermitted() {
    final TransactionValidator baseValidator =
        createTransactionValidator(
            gasCalculator, GasLimitCalculator.constant(), false, Optional.empty());
    final TransactionValidator validator =
        new PermissionTransactionValidator(baseValidator, transactionFilter(false));

    assertThat(
            validator.validateForSender(
                basicTransaction, accountWithNonce(0), transactionPoolParams))
        .isEqualTo(ValidationResult.invalid(TransactionInvalidReason.TX_SENDER_NOT_AUTHORIZED));
  }

  @Test
  public void shouldAcceptValidTransactionIfAccountIsPermitted() {
    final TransactionValidator baseValidator =
        createTransactionValidator(
            gasCalculator, GasLimitCalculator.constant(), false, Optional.empty());
    final TransactionValidator validator =
        new PermissionTransactionValidator(baseValidator, transactionFilter(true));

    assertThat(
            validator.validateForSender(
                basicTransaction, accountWithNonce(0), transactionPoolParams))
        .isEqualTo(ValidationResult.valid());
  }

  @Test
  public void shouldNotCheckAccountPermissionIfValidationParamsCheckPermissionsFalse() {
    final PermissionTransactionFilter permissionTransactionFilter =
        mock(PermissionTransactionFilter.class);

    final TransactionValidator baseValidator =
        createTransactionValidator(
            gasCalculator, GasLimitCalculator.constant(), false, Optional.empty());
    final TransactionValidator validator =
        new PermissionTransactionValidator(baseValidator, permissionTransactionFilter);

    final TransactionValidationParams validationParams =
        ImmutableTransactionValidationParams.builder().checkLocalPermissions(false).build();

    validator.validateForSender(basicTransaction, accountWithNonce(0), validationParams);

    assertThat(validator.validateForSender(basicTransaction, accountWithNonce(0), validationParams))
        .isEqualTo(ValidationResult.valid());

    verifyNoInteractions(permissionTransactionFilter);
  }

  private Account accountWithNonce(final long nonce) {
    return account(basicTransaction.getUpfrontCost(0L), nonce);
  }

  private Account account(final Wei balance, final long nonce) {
    final Account account = mock(Account.class);
    when(account.getBalance()).thenReturn(balance);
    when(account.getNonce()).thenReturn(nonce);
    return account;
  }

  private PermissionTransactionFilter transactionFilter(final boolean permitted) {
    final PermissionTransactionFilter permissionTransactionFilter =
        mock(PermissionTransactionFilter.class);
    when(permissionTransactionFilter.permitted(any(Transaction.class), anyBoolean()))
        .thenReturn(permitted);
    return permissionTransactionFilter;
  }
}
