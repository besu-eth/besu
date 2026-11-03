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

import org.hyperledger.besu.datatypes.ChainId;
import org.hyperledger.besu.ethereum.GasLimitCalculator;
import org.hyperledger.besu.ethereum.core.PermissionTransactionFilter;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TransactionValidationFactoryTest {
  private static final ChainId CHAIN_ID = ChainId.of(1);
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
}
