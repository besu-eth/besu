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
package org.hyperledger.besu.evm.log;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.Set;
import java.util.function.Consumer;

/**
 * Strategy interface for emitting ETH transfer logs.
 *
 * <p>This allows different implementations for different protocol versions:
 *
 * <ul>
 *   <li>{@link #NOOP} - No-op implementation for pre-Amsterdam forks
 *   <li>{@link EIP7708TransferLogEmitter} - EIP-7708 compliant implementation for Amsterdam+
 * </ul>
 */
public interface TransferLogEmitter {

  /**
   * Emit a transfer log for an ETH value transfer.
   *
   * @param frame the message frame to add the log to
   * @param from the sender address
   * @param to the recipient address
   * @param value the amount transferred in Wei
   */
  void emitTransferLog(MessageFrame frame, Address from, Address to, Wei value);

  /**
   * Emit a log for a SELFDESTRUCT operation.
   *
   * <p>The behavior depends on the originator and beneficiary:
   *
   * <ul>
   *   <li>If originator equals beneficiary: emits a Burn log (LOG2)
   *   <li>Otherwise: emits a Transfer log (LOG3)
   * </ul>
   *
   * @param frame the message frame to add the log to
   * @param originator the address of the contract being selfdestructed
   * @param beneficiary the address receiving the balance
   * @param value the amount being transferred in Wei
   */
  default void emitSelfDestructLog(
      final MessageFrame frame,
      final Address originator,
      final Address beneficiary,
      final Wei value) {
    // No-op by default (pre-Amsterdam)
  }

  /**
   * Emit Burn logs for accounts being closed at the end of a transaction.
   *
   * <p>For each selfdestructed account with a nonzero balance, a Burn log (LOG2) is emitted. Logs
   * are emitted in lexicographical order by address.
   *
   * @param worldState the world state to query account balances
   * @param selfDestructs the set of addresses marked for selfdestruct
   * @param logConsumer consumer to receive the generated logs
   */
  default void emitClosureLogs(
      final WorldUpdater worldState,
      final Set<Address> selfDestructs,
      final Consumer<Log> logConsumer) {
    // No-op by default (pre-Amsterdam)
  }

  /** No-op implementation. Used for pre-Amsterdam forks. */
  TransferLogEmitter NOOP = (frame, from, to, value) -> {};
}
