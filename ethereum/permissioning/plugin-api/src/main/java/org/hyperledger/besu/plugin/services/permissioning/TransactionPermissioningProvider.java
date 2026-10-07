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
package org.hyperledger.besu.plugin.services.permissioning;

import org.hyperledger.besu.datatypes.Transaction;

/**
 * Allows you to register a provider that will decide if a transaction is permitted. <br>
 * <br>
 * A simple implementation can look like:
 *
 * <pre>{@code
 * context
 *    .getService(PermissioningService.class)
 *    .get()
 *    .registerTransactionPermissioningProvider((tx) -> {
 *        // Your logic here
 *        return true;
 *    });
 * }</pre>
 */
@FunctionalInterface
public interface TransactionPermissioningProvider {
  /**
   * Can be used to filter transactions according to an arbitrary criteria. Registered providers are
   * consulted as part of transaction validation, so rejecting a transaction keeps it out of the
   * transaction pool and causes it to fail block-level transaction validation.
   *
   * @param transaction the transaction to check
   * @return whether the transaction is permitted
   */
  boolean isPermitted(final Transaction transaction);
}
