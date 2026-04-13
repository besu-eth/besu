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
package org.hyperledger.besu.ethereum.eth.transactions.inclusionlist;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;

/**
 * Interface for validating inclusion list constraints per EIP-7805. Implementations determine
 * whether a payload satisfies the required inclusion list transactions using the 3-step conditional
 * validation algorithm.
 */
public interface InclusionListValidator {

  /**
   * Validates that the given payload satisfies the inclusion list constraints using the EIP-7805
   * 3-step conditional algorithm with full execution context.
   *
   * <p>For each IL transaction T not present in the payload:
   *
   * <ol>
   *   <li>Skip if T is present in the block (handled by payloadTransactions set lookup)
   *   <li>Skip if T.gas &gt; gas_left (transaction cannot fit in remaining gas)
   *   <li>Validate T against post-execution state (nonce and balance of T.origin)
   * </ol>
   *
   * Returns INCLUSION_LIST_UNSATISFIED only if a transaction passes all three checks.
   *
   * @param payloadTransactions the transactions included in the execution payload (encoded bytes)
   * @param inclusionListTransactions the transactions required by the inclusion list (encoded
   *     bytes)
   * @param context the post-execution context with gas_left and account state
   * @return validation result with status VALID, INVALID, or UNSATISFIED
   */
  InclusionListValidationResult validate(
      List<Bytes> payloadTransactions,
      List<Bytes> inclusionListTransactions,
      InclusionListValidationContext context);

  /**
   * Validates inclusion list constraints using a simple presence check (no gas or state context).
   * This is a backward-compatible convenience method that calls the full 3-arg validate with an
   * unlimited context (unlimited gas, empty account state).
   *
   * @param payloadTransactions the transactions included in the execution payload (encoded bytes)
   * @param inclusionListTransactions the transactions required by the inclusion list (encoded
   *     bytes)
   * @return validation result with status VALID, INVALID, or UNSATISFIED
   */
  default InclusionListValidationResult validate(
      final List<Bytes> payloadTransactions, final List<Bytes> inclusionListTransactions) {
    return validate(
        payloadTransactions, inclusionListTransactions, InclusionListValidationContext.unlimited());
  }
}
