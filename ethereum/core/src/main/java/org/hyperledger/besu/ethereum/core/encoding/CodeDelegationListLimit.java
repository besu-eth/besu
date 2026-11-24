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
package org.hyperledger.besu.ethereum.core.encoding;

import org.hyperledger.besu.ethereum.GasLimitCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

/**
 * The longest EIP-7702 authorization list a transaction can validly carry, given its gas limit.
 *
 * <p>Decoding an authorization list builds several objects per entry: a 27-byte entry on the wire
 * becomes about 340 bytes of heap. The decoder therefore checks the length of the list against this
 * limit before decoding any entry. The length comes from the RLP item headers, which allocates
 * nothing, and a list that is too long fails with an {@link
 * org.hyperledger.besu.ethereum.rlp.RLPException}.
 */
@FunctionalInterface
public interface CodeDelegationListLimit {

  /** No limit: the list is bounded only by the size of the message that carries it. */
  CodeDelegationListLimit UNLIMITED = gasLimit -> Integer.MAX_VALUE;

  /**
   * The longest authorization list a transaction with this gas limit can validly carry.
   *
   * @param gasLimit the gas limit of the transaction, read as an unsigned value
   * @return the maximum number of authorizations
   */
  int maxCodeDelegations(long gasLimit);

  /**
   * The more permissive of this limit and another one, for a fork transition, where a transaction
   * may be valid under either fork's rules.
   *
   * @param other the other limit
   * @return a limit that allows the longer list of the two
   */
  default CodeDelegationListLimit or(final CodeDelegationListLimit other) {
    return gasLimit -> Math.max(maxCodeDelegations(gasLimit), other.maxCodeDelegations(gasLimit));
  }

  /**
   * The limit that follows from the gas rules of a fork.
   *
   * <p>Each authorization adds {@link GasCalculator#delegateCodeGasCost(int)} to the intrinsic gas
   * of the transaction, and the intrinsic gas can exceed neither the gas limit of the transaction,
   * nor the gas limit of the block, nor the fork's caps on transaction gas and intrinsic gas. So a
   * list longer than that gas divided by the cost of one authorization can never be valid. The cost
   * is read from the fork's gas calculator, so a fork that reprices authorizations moves the limit
   * with it. This relies on the cost growing linearly with the length of the list, as it does in
   * every fork so far.
   *
   * <p>A fork that charges nothing for an authorization predates EIP-7702 and accepts no
   * authorization list at all.
   *
   * @param gasCalculator the gas calculator of the fork
   * @param gasLimitCalculator the gas limit calculator of the fork, for its transaction gas caps
   * @param blockGasLimit the gas limit of the block the transaction would go into
   * @return the limit for that fork
   */
  static CodeDelegationListLimit forFork(
      final GasCalculator gasCalculator,
      final GasLimitCalculator gasLimitCalculator,
      final long blockGasLimit) {
    final long costPerAuthorization = gasCalculator.delegateCodeGasCost(1);
    if (costPerAuthorization <= 0) {
      return gasLimit -> 0;
    }
    // The caps use Long.MAX_VALUE for "no cap", which an unsigned minimum leaves out on its own.
    final long maxGas =
        unsignedMin(
            blockGasLimit,
            unsignedMin(
                gasLimitCalculator.transactionGasLimitCap(),
                gasLimitCalculator.transactionIntrinsicGasLimitCap()));
    return gasLimit ->
        (int)
            Math.min(
                Integer.MAX_VALUE,
                Long.divideUnsigned(unsignedMin(gasLimit, maxGas), costPerAuthorization));
  }

  private static long unsignedMin(final long a, final long b) {
    return Long.compareUnsigned(a, b) <= 0 ? a : b;
  }
}
