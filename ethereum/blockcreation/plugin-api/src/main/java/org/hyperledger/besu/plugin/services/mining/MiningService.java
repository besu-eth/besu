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
package org.hyperledger.besu.plugin.services.mining;

import org.hyperledger.besu.plugin.services.BesuService;

/**
 * The MiningService interface provides methods to start and stop the mining process.
 *
 * @deprecated no known plugin uses this service, so it is scheduled for removal from the plugin
 *     API. It will be removed in a future release.
 */
@Deprecated(forRemoval = true)
public interface MiningService extends BesuService {

  /** Starts the mining process. */
  void start();

  /** Stops the mining process. */
  void stop();
}
