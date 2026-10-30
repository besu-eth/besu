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
package org.hyperledger.besu.ethereum.mainnet;

import org.hyperledger.besu.ethereum.mainnet.feemarket.BaseFeeMarket;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import java.util.OptionalInt;

/**
 * EIP-8037: Amsterdam relaxes the EIP-7825 cap on {@code tx.gas} itself to {@link
 * #EIP_8037_TRANSACTION_TOTAL_GAS_LIMIT_CAP} and instead caps {@code max(intrinsic_execution,
 * calldata_floor)} at the former cap value.
 */
public class AmsterdamTargetingGasLimitCalculator extends OsakaTargetingGasLimitCalculator {

  /**
   * The EIP-8037 cap on {@code tx.gas} (2^32-1), covering both execution and state gas. Execution
   * gas alone stays bounded by {@link #EIP_7825_TRANSACTION_GAS_LIMIT_CAP}.
   */
  public static final long EIP_8037_TRANSACTION_TOTAL_GAS_LIMIT_CAP = 4_294_967_295L;

  public AmsterdamTargetingGasLimitCalculator(
      final long londonForkBlock,
      final BaseFeeMarket feeMarket,
      final GasCalculator gasCalculator,
      final int maxBlobsPerBlock,
      final int targetBlobsPerBlock,
      final OptionalInt maxBlobsPerTransaction,
      final OptionalInt userMaxBlobsPerBlock) {
    super(
        londonForkBlock,
        feeMarket,
        gasCalculator,
        maxBlobsPerBlock,
        targetBlobsPerBlock,
        maxBlobsPerTransaction,
        userMaxBlobsPerBlock,
        EIP_8037_TRANSACTION_TOTAL_GAS_LIMIT_CAP);
  }

  @Override
  public long transactionIntrinsicGasLimitCap() {
    return EIP_7825_TRANSACTION_GAS_LIMIT_CAP;
  }
}
