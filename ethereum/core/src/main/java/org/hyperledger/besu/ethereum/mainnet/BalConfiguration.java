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
package org.hyperledger.besu.ethereum.mainnet;

import java.time.Duration;

import org.immutables.value.Value;

/** Configuration options for Block Access List (BAL) processing. */
@Value.Immutable
public interface BalConfiguration {

  BalConfiguration DEFAULT = ImmutableBalConfiguration.builder().build();

  /** Returns whether the BAL-computed state root should be trusted without verification. */
  @Value.Default
  default boolean isBalStateRootTrusted() {
    return false;
  }

  /** Returns whether BAL perfect parallelization is enabled. */
  @Value.Default
  default boolean isPerfectParallelizationEnabled() {
    return true;
  }

  /**
   * Returns whether mismatches between BAL and synchronously computed state roots should only log
   * an error instead of throwing an exception.
   */
  @Value.Default
  default boolean isBalLenientOnStateRootMismatch() {
    return true;
  }

  /** Returns whether the BALs should be logged when a constructed and block's BALs mismatch. */
  @Value.Default
  default boolean shouldLogBalsOnMismatch() {
    return false;
  }

  /** Returns the timeout to use when waiting for the BAL-computed state root. */
  @Value.Default
  default Duration getBalStateRootTimeout() {
    return Duration.ofSeconds(1);
  }

  /** Returns the timeout to use when waiting for BAL transaction processing results. */
  @Value.Default
  default Duration getBalProcessingTimeout() {
    return Duration.ofSeconds(1);
  }
}
