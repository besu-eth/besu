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
package org.hyperledger.besu.plugin.services.txvalidator;

import org.hyperledger.besu.plugin.Unstable;

/** Interface for a factory that creates transaction validators for txpool usage */
@Unstable
public interface PluginTransactionPoolValidatorFactory {

  /**
   * Create a transaction validator for txpool usage.
   *
   * <p>May be called for every transaction offered to the transaction pool, not just once per
   * registration, so implementations should be cheap to run and must not rely on state accumulating
   * in the returned validator, which is discarded after validating a single transaction.
   *
   * @return the transaction validator
   */
  PluginTransactionPoolValidator createTransactionValidator();
}
