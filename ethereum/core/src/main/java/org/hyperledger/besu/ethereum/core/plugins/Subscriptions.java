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
package org.hyperledger.besu.ethereum.core.plugins;

import org.hyperledger.besu.plugin.services.Subscription;

import java.util.concurrent.atomic.AtomicBoolean;

/** Builds the {@link Subscription} handles the plugin services hand out. */
public final class Subscriptions {

  private Subscriptions() {}

  /**
   * Builds a {@link Subscription} whose {@link Subscription#close()} runs the given unsubscribe
   * action exactly once. Later calls are no-ops, and concurrent calls run the action once.
   *
   * @param unsubscribe the action that removes the listener from its internal source
   * @return the handle to return to the plugin
   */
  public static Subscription unsubscribeOnClose(final Runnable unsubscribe) {
    final AtomicBoolean closed = new AtomicBoolean();
    return () -> {
      if (closed.compareAndSet(false, true)) {
        unsubscribe.run();
      }
    };
  }
}
