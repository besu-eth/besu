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

/**
 * Consolidated configuration for EIP-7805 inclusion list behavior.
 *
 * @param validationMode the validation mode (strict or lenient)
 * @param selector the transaction selector implementation
 */
public record InclusionListConfiguration(
    InclusionListValidationMode validationMode, InclusionListTransactionSelector selector) {

  /** Maximum total bytes allowed for an inclusion list per EIP-7805 (2^13 = 8192 bytes). */
  public static final int MAX_BYTES_PER_INCLUSION_LIST = 8192;

  /** Default configuration: strict validation, default selector. */
  public static final InclusionListConfiguration DEFAULT =
      new InclusionListConfiguration(
          InclusionListValidationMode.STRICT, new DefaultInclusionListSelector());

  /**
   * Creates the appropriate validator for the configured validation mode.
   *
   * @return a new validator instance
   */
  public InclusionListValidator createValidator() {
    return validationMode.createValidator();
  }
}
