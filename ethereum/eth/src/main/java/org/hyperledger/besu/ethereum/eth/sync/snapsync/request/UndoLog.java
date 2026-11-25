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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.request;

import java.util.ArrayList;
import java.util.List;

/**
 * Records how persisting a batch of requests changed their state, such as a node counted in its
 * parent. When the batch's commit fails, undoing it lets the retried requests persist and count
 * again as if the batch never ran.
 */
public class UndoLog {

  private final List<Runnable> undoActions = new ArrayList<>();

  /**
   * Adds the action that undoes a change made while persisting the batch.
   *
   * @param undoAction the action to run when the batch is undone
   */
  public void add(final Runnable undoAction) {
    undoActions.add(undoAction);
  }

  /** Runs the undo actions, the latest first. */
  public void undo() {
    for (int i = undoActions.size() - 1; i >= 0; i--) {
      undoActions.get(i).run();
    }
    undoActions.clear();
  }
}
