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

import java.util.List;
import java.util.Objects;

/**
 * The fork activations a {@link ProtocolScheduleCustomization} contributes on top of the forks the
 * genesis config declares: the block numbers and timestamps of consensus changes that peers must
 * agree on. The node's single {@code ForkIdManager} computes the EIP-2124 fork ID from the declared
 * forks plus these, so the Status handshake, the ENR and {@code eth_config} all advertise them. The
 * genesis config itself never carries them. Block-number and timestamp activations are kept as two
 * type-partitioned lists, as EIP-2124 / EIP-6122 require; the two kinds are never compared against
 * each other.
 *
 * @param blockNumbers the block-number fork activations
 * @param timestamps the timestamp fork activations
 */
public record ContributedForkActivations(List<Long> blockNumbers, List<Long> timestamps) {

  private static final ContributedForkActivations EMPTY =
      new ContributedForkActivations(List.of(), List.of());

  /**
   * Canonical constructor; validates and canonicalizes each activation domain so semantic equality
   * does not depend on provider order or duplicate entries.
   *
   * @param blockNumbers the block-number fork activations
   * @param timestamps the timestamp fork activations
   */
  public ContributedForkActivations(final List<Long> blockNumbers, final List<Long> timestamps) {
    this.blockNumbers = canonicalize(blockNumbers, "block numbers");
    this.timestamps = canonicalize(timestamps, "timestamps");
  }

  /**
   * No fork activations.
   *
   * @return the empty fork activations
   */
  public static ContributedForkActivations empty() {
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
