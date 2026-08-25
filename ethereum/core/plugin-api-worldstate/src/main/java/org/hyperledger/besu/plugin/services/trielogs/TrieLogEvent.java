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
package org.hyperledger.besu.plugin.services.trielogs;

/** A TrieLog event. */
public interface TrieLogEvent {
  /** The type of the event. */
  enum Type {
    /** TrieLog added event type */
    ADDED
  }

  /**
   * The type of the event.
   *
   * @return the type of the event
   */
  TrieLogEvent.Type getType();

  /**
   * The TrieLog layer.
   *
   * @return the TrieLog layer
   */
  TrieLog layer();

  /** Observer interface for TrieLog events. */
  interface TrieLogObserver {

    /**
     * Called when a TrieLog is added.
     *
     * @param event the TrieLog event
     */
    void onTrieLogAdded(TrieLogEvent event);
  }
}
