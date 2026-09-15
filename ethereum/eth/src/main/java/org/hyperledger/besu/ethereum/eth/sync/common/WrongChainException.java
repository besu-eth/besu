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
 * Signals that the pivot does not descend from the chain we trust — the downloaded headers do not
 * link to the trust anchor (genesis or the trusted checkpoint) — so no amount of downloading from
 * the current pivot can produce a valid chain.
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
