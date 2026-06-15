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
package org.hyperledger.besu.evm.tracing;

/** Startup-resolved enable/threshold for {@link SlowBlockTracer}. */
public final class SlowBlockTracerConfig {
  private SlowBlockTracerConfig() {}

  /**
   * System property carrying the threshold in ms. Referenced as a compile-time String constant so
   * that callers (e.g. BesuCommand) never trigger this class's initialisation.
   */
  public static final String THRESHOLD_PROPERTY = "besu.slowBlockThresholdMs";

  /** -1 = disabled, 0 = log all blocks, &gt;0 = log blocks whose total_ms &ge; value. */
  public static final long THRESHOLD_MS = Long.getLong(THRESHOLD_PROPERTY, -1L);

  /** static final → JIT constant-folds the disabled branch in processBlock. */
  public static final boolean ENABLED = THRESHOLD_MS >= 0;
}
