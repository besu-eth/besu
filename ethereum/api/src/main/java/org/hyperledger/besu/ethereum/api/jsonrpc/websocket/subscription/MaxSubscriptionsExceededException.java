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
package org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription;

/**
 * Thrown when a new subscription would exceed the configured maximum number of concurrently active
 * subscriptions. Callers should map this to a JSON-RPC error rather than propagating it as an
 * internal error.
 */
public class MaxSubscriptionsExceededException extends RuntimeException {

  /**
   * Constructs the exception with the configured limit that was reached.
   *
   * @param maxActiveSubscriptions the configured maximum number of active subscriptions
   */
  public MaxSubscriptionsExceededException(final long maxActiveSubscriptions) {
    super("Maximum number of active subscriptions (" + maxActiveSubscriptions + ") exceeded");
  }
}
