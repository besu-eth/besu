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
package org.hyperledger.besu.consensus.merge;

/**
 * Listener for fork choices the Engine API applies or syncs to. A fork choice it rejects is not
 * passed on. One for an unknown head cannot be checked before it is synced, so it is passed on when
 * the sync to it starts. The event is fired before the chain head is moved.
 */
public interface VerifiedForkchoiceListener {

  /**
   * On new verified forkchoice event.
   *
   * @param event the event
   */
  void onNewVerifiedForkchoice(final ForkchoiceEvent event);
}
