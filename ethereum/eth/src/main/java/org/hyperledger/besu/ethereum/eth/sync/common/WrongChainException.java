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
 * Signals that the backward header download has encountered a chain whose genesis differs from the
 * locally-stored genesis. Thrown by {@code BackwardHeaderDriver} when anchor recovery walks down to
 * the genesis floor without finding a matching canonical ancestor.
 *
 * <p>Recognised by {@code SnapSyncChainDownloader.shouldRetry} as non-retryable: the overall sync
 * future completes exceptionally and the operator is expected to intervene rather than letting the
 * downloader loop on the wrong chain.
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
