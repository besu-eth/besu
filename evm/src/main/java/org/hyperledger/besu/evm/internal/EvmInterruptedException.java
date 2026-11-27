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
package org.hyperledger.besu.evm.internal;

/**
 * Thrown by {@link EvmKillSwitch#poll()} out of an EVM interpreter loop when the switch is armed
 * and the executing thread has been interrupted. It aborts the execution of the current
 * transaction; the partially updated {@link org.hyperledger.besu.evm.frame.MessageFrame} must be
 * discarded, as block building, parallel import and RPC simulations already do for their
 * cancellation paths.
 */
public class EvmInterruptedException extends RuntimeException {

  /**
   * Create an EvmInterruptedException.
   *
   * @param message a description of the execution that was interrupted
   */
  public EvmInterruptedException(final String message) {
    super(message);
  }
}
