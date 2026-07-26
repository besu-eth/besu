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
 * Signals that the trusted body checkpoint appears to have been reorganized. Thrown when the header
 * downloaded at the checkpoint height does not match the trusted checkpoint, or when anchor
 * recovery walks below the checkpoint without reconnecting to the canonical chain above it.
 *
 * <p>This is a fatal, non-recoverable condition: the configured/trusted checkpoint is not on the
 * pivot's chain. It is recognised as non-retryable by {@code SnapSyncChainDownloader.shouldRetry}
 * and stops the sync (rather than re-pivoting) in {@code SnapSyncDownloader.handleFailure}, since
 * re-pivoting cannot fix a checkpoint that is no longer canonical.
 */
public class CheckpointReorgException extends RuntimeException {

  /**
   * Creates a new CheckpointReorgException.
   *
   * @param message a human-readable description of the checkpoint reorg condition
   */
  public CheckpointReorgException(final String message) {
    super(message);
  }
}
