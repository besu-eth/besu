/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.plugin.services.metrics;

/**
 * A counter is a metric to track counts of events or running totals etc. The value of the counter
 * can only increase.
 */
public interface Counter {

  /** Increment the counter by 1. */
  void inc();

  /**
   * Increment the counter by a specified amount.
   *
   * @param amount The amount to increment the counter by. Must be greater than or equal to 0.
   */
  void inc(long amount);
}
