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
package org.hyperledger.besu.evm.worldstate;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.evm.account.Account;

import java.nio.charset.StandardCharsets;

import org.apache.tuweni.bytes.Bytes;

/**
 * The calls of a transaction that runs ahead of its execution only to load the state it reads.
 *
 * <p>When such a run calls an account that is not in memory, the account reads as a placeholder
 * while it loads, and the call returns what the last call with the same function selector returned,
 * so the run goes on to the next call instead of waiting for the disk.
 */
public interface RunaheadCalls {

  /** The code hash of a placeholder account. */
  Hash PLACEHOLDER_CODE_HASH =
      Hash.hash(Bytes.wrap("besu run ahead placeholder".getBytes(StandardCharsets.UTF_8)));

  /** The run ahead on the current thread, if any. */
  ThreadLocal<RunaheadCalls> CURRENT = new ThreadLocal<>();

  /** Holder for the switch, so that executions without a run ahead skip the thread local. */
  final class Switch {
    private static volatile boolean enabled = false;

    private Switch() {}

    /** Lets call operations look for a run ahead on their thread. */
    public static void enable() {
      enabled = true;
    }
  }

  /**
   * The run ahead on the current thread.
   *
   * @return the run ahead, or null when the thread executes a transaction for real
   */
  static RunaheadCalls current() {
    return Switch.enabled ? CURRENT.get() : null;
  }

  /**
   * Whether the account is a placeholder for one that is still loading.
   *
   * @param account the account
   * @return true for a placeholder
   */
  static boolean isPlaceholder(final Account account) {
    return account != null && PLACEHOLDER_CODE_HASH.equals(account.getCodeHash());
  }

  /**
   * Marks the account lookups that follow as lookups of a call's target, which may return a
   * placeholder.
   *
   * @param lookingUp whether a call target is being looked up
   */
  void lookingUpCallTarget(boolean lookingUp);

  /**
   * Whether a call's target is being looked up.
   *
   * @return true while a call operation looks up its target
   */
  boolean isLookingUpCallTarget();

  /**
   * What a call to a placeholder returns.
   *
   * @param input the call's input
   * @return the output of the last call with the same function selector
   */
  Bytes predictedReturn(Bytes input);

  /**
   * Remembers what a call returned.
   *
   * @param input the call's input
   * @param output the call's output
   */
  void recordReturn(Bytes input, Bytes output);
}
