/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.permissioning.node;

import org.hyperledger.besu.ethereum.p2p.discovery.NodeIdentifier;

import java.util.Optional;

/** A permissioning provider that only answers under some conditions */
public interface ContextualNodePermissioningProvider {

  /**
   * Returns whether a connection is permitted if the conditions allow for a decision to be made
   *
   * @param sourceEnode The node originating the connection
   * @param destinationEnode The node receiving the connection
   * @return True if permitted, false if not, Empty if an answer is not possible under current
   *     conditions
   */
  Optional<Boolean> isPermitted(
      final NodeIdentifier sourceEnode, final NodeIdentifier destinationEnode);

  /**
   * Subscribe a callback that will be invoked whenever this provider has changed such that it would
   * deliver different answers
   *
   * @param callback The runnable to invoke when the provider has updated
   * @return the subscription id that can be used to unsubscribe
   */
  long subscribeToUpdates(final Runnable callback);

  /**
   * Unsubscribe an existing subscription
   *
   * @param id the id of the subscriber to unsubscribe
   * @return true if the subscriber id was found, false if not
   */
  boolean unsubscribeFromUpdates(final long id);
}
