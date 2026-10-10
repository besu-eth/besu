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
package org.hyperledger.besu.plugin;

import org.hyperledger.besu.plugin.services.BesuService;

/**
 * Marks a Besu-provided service that is usable once the node is running, that is from {@link
 * BesuPlugin#afterMainLoop(RunningContext)} onwards.
 *
 * <p>The components behind these services (the peer-to-peer network, the synchronizer, the mining
 * coordinator) only start with the main loop, so they are not offered any earlier.
 */
public interface RunningService extends BesuService {}
