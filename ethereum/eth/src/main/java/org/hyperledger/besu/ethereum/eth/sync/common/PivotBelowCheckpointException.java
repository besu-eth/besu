/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.eth.sync.common;

/**
 * Signals that the selected pivot block is at or below the trusted checkpoint, so there is nothing
 * we could validate against the checkpoint and no bodies to download above it. This normally means
 * the consensus client has not caught up to the checkpoint the operator configured yet.
 *
 * <p>Recoverable by waiting: {@code SnapSyncDownloader.handleFailure} re-pivots after a delay,
 * rather than immediately, because the pivot selector reuses its previous pivot until the chain
 * head has advanced far enough, so an immediate retry would spin on the same block.
 */
public class PivotBelowCheckpointException extends RuntimeException {

  /**
   * Creates a new PivotBelowCheckpointException.
   *
   * @param message a human-readable description of the pivot/checkpoint mismatch
   */
  public PivotBelowCheckpointException(final String message) {
    super(message);
  }
}
