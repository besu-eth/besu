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

public enum BodyValidationMode {
  /** No Validation. data must be pre-validated */
  NONE,

  /**
   * Skip receipts and transactions root validation. From Amsterdam on, the gasUsed of a block with
   * transactions is only validated when the block gas from processing is supplied.
   */
  LIGHT,

  /** Validate transactions, state, and receipts root */
  ROOT_ONLY,

  /** Fully validate the body */
  FULL;
}
