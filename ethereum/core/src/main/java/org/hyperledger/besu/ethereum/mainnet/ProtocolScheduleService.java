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

import org.hyperledger.besu.plugin.Unstable;
import org.hyperledger.besu.plugin.services.BesuService;

/** Besu-owned registry for unstable protocol-schedule customizers. */
@Unstable
public interface ProtocolScheduleService extends BesuService {

  /**
   * Registers a customizer during {@code BesuPlugin.register}.
   *
   * @param customizer the customizer to register
   * @throws IllegalStateException after the plugin-registration phase has ended
   */
  void registerProtocolScheduleCustomizer(ProtocolScheduleCustomizer customizer);
}
