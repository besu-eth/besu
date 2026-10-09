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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results;

/**
 * The outcome of the validations that can only be done on a successfully processed block, and that
 * are specific to a version of the new payload method.
 */
public class PayloadPostExecutionValidationResultV1 {
  /** The result for a block that passed all the post-execution validations. */
  public static final PayloadPostExecutionValidationResultV1 SUCCESS =
      new PayloadPostExecutionValidationResultV1(true);

  private final boolean inclusionListSatisfied;

  /**
   * Instantiates a new Payload post execution validation result.
   *
   * @param inclusionListSatisfied true if the block satisfies the inclusion list (EIP-7805)
   */
  public PayloadPostExecutionValidationResultV1(final boolean inclusionListSatisfied) {
    this.inclusionListSatisfied = inclusionListSatisfied;
  }

  /**
   * Is the inclusion list satisfied.
   *
   * @return true if the block satisfies the inclusion list
   */
  public boolean isInclusionListSatisfied() {
    return inclusionListSatisfied;
  }
}
