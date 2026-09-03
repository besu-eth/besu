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
package org.hyperledger.besu.plugin.services;

/**
 * A live subscription to a Besu event, returned by every {@code subscribe*} method on a service.
 * Besu implements it; plugins only call {@link #close()}.
 *
 * <p>The handle is bound to the one listener it was returned for, so closing it can never affect
 * another subscription. Plugins hold the handle instead of a numeric id and need no matching remove
 * method: {@code stop()} is typically {@code subscriptions.forEach(Subscription::close)}.
 */
@FunctionalInterface
public interface Subscription {

  /**
   * Stops delivery to the listener this subscription was returned for. Idempotent: a second call is
   * a no-op. Safe to call from any thread, including from inside the listener itself.
   */
  void close();
}
