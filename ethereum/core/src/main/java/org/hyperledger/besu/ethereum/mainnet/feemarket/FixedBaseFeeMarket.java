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
package org.hyperledger.besu.ethereum.mainnet.feemarket;

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.Optional;
import java.util.function.Supplier;

public class FixedBaseFeeMarket extends LondonFeeMarket {

  FixedBaseFeeMarket(final long londonForkBlockNumber, final Wei fixedBaseFee) {
    super(londonForkBlockNumber, Optional.of(fixedBaseFee));
  }

  @Override
  public Wei computeBaseFee(
      final long blockNumber,
      final long timestamp,
      final long slotNumber,
      final BlockHeader parentBlockHeader) {
    return baseFeeInitialValue;
  }

  @Override
  public Wei predictNextBaseFee(
      final BlockHeader headBlockHeader,
      final Supplier<Optional<BlockHeader>> headParentBlockHeaderSupplier) {
    return baseFeeInitialValue;
  }

  @Override
  public ValidationMode baseFeeValidationMode(final long blockNumber) {
    return ValidationMode.NONE;
  }
}
