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

import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.plugin.Unstable;

import java.util.Optional;

/**
 * Produces a protocol-schedule contribution for a supported chain.
 *
 * <p>This extension point deliberately lives with the internal Ethereum implementation rather than
 * in {@code besu-plugin-api}: a protocol-spec modifier necessarily depends on internal Besu
 * execution components. It is marked unstable so plugins do not mistake those components for a
 * compatibility-guaranteed API. Registration itself is coordinated by {@link
 * ProtocolScheduleService}, which ensures that the customizer is evaluated at most once.
 */
@Unstable
@FunctionalInterface
public interface ProtocolScheduleCustomizer {

  /**
   * Resolves this customizer for a chain.
   *
   * <p>The method must be deterministic and side-effect free. Return empty when the customizer does
   * not support the supplied chain. Besu rejects startup if more than one registered customizer
   * supports the same chain.
   *
   * @param config the uncustomized genesis configuration
   * @return this customizer's complete contribution, or empty when it does not apply
   */
  Optional<ProtocolScheduleCustomization> customize(GenesisConfigOptions config);
}
