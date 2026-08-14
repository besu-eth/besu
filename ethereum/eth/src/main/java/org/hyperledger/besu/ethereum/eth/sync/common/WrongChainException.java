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
 * Signals that the pivot does not descend from the chain we trust, so no amount of downloading from
 * the current pivot can produce a valid chain. Thrown when:
 *
 * <ul>
 *   <li>{@code BackwardHeaderDriver} reaches the floor block — genesis, or the trusted checkpoint —
 *       without the downloaded headers linking to it, either at the anchor boundary or after anchor
 *       recovery walked below it;
 *   <li>the header downloaded at the trusted checkpoint height does not match the checkpoint,
 *       either during the walk ({@code BackwardHeaderDriver.verifyCheckpointLinkage}) or afterwards
 *       ({@code SnapSyncChainDownloader.verifyCheckpointHeaderMatches});
 *   <li>a rolled-back pivot has no stored header below it to restart Stage 1 from ({@code
 *       SnapSyncChainDownloader.stage1RestartAnchor}).
 * </ul>
 *
 * <p>Recognised by {@code SnapSyncChainDownloader.shouldRetry} as non-retryable, so the cycle is
 * not retried from the saved state; the failure propagates instead to {@code
 * SnapSyncDownloader.handleFailure}, which re-pivots to a fresh block. A genuinely wrong chain — a
 * mis-configured checkpoint hash, say — therefore shows up as repeated re-pivots in the logs rather
 * than as a hard stop.
 */
public class WrongChainException extends RuntimeException {

  /**
   * Creates a new WrongChainException.
   *
   * @param message a human-readable description of the wrong-chain condition
   */
  public WrongChainException(final String message) {
    super(message);
  }
}
