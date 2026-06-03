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
package org.hyperledger.besu.consensus.merge;

import org.hyperledger.besu.ethereum.core.BlockHeader;

/** Listener notified when a new payload arrives from the consensus layer. */
public interface NewPayloadListener {

  /**
   * Called for each {@code engine_newPayload} request received from the consensus layer. The header
   * has had its block hash verified against the payload contents but has not been validated against
   * the local chain — callers must treat it as untrusted.
   *
   * @param header the header reconstructed from the payload
   */
  void onNewPayload(BlockHeader header);
}
