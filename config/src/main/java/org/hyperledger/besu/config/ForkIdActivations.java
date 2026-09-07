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
package org.hyperledger.besu.config;

import java.util.List;
import java.util.Objects;

/**
 * EIP-2124 fork activations declared out of band of the genesis config keys: the block numbers and
 * timestamps of consensus changes peers must agree on, merged into the fork schedule the config
 * advertises. Block-number and timestamp activations are kept as two type-partitioned lists, as
 * EIP-2124 / EIP-6122 require; the two kinds are never compared against each other.
 *
 * @param blockNumbers the block-number fork activations
 * @param timestamps the timestamp fork activations
 */
public record ForkIdActivations(List<Long> blockNumbers, List<Long> timestamps) {

  private static final ForkIdActivations EMPTY = new ForkIdActivations(List.of(), List.of());

  /**
   * Canonical constructor; validates and canonicalizes each activation domain so semantic equality
   * does not depend on provider order or duplicate entries.
   *
   * @param blockNumbers the block-number fork activations
   * @param timestamps the timestamp fork activations
   */
  public ForkIdActivations(final List<Long> blockNumbers, final List<Long> timestamps) {
    this.blockNumbers = canonicalize(blockNumbers, "block numbers");
    this.timestamps = canonicalize(timestamps, "timestamps");
  }

  /**
   * No fork activations.
   *
   * @return the empty fork activations
   */
  public static ForkIdActivations empty() {
    return EMPTY;
  }

  private static List<Long> canonicalize(final List<Long> activations, final String domain) {
    Objects.requireNonNull(activations, domain + " must not be null");
    for (final Long activation : activations) {
      Objects.requireNonNull(activation, domain + " must not contain null");
      if (activation < 0) {
        throw new IllegalArgumentException(domain + " must not contain negative values");
      }
    }
    return activations.stream().distinct().sorted().toList();
  }
}
