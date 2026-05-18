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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes32;

public sealed class NewPayloadRequestParametersV2 extends NewPayloadRequestParametersV1
    permits NewPayloadRequestParametersV3 {
  private final Optional<List<String>> expectedBlobVersionedHashes;
  private final Optional<String> parentBeaconBlockRootParameter;
  private final Optional<Bytes32> parentBeaconBlockRoot;

  public NewPayloadRequestParametersV2(
      final NewPayloadRequestParametersV1 requestParameters,
      final Optional<List<String>> expectedBlobVersionedHashes,
      final Optional<String> parentBeaconBlockRootParameter) {
    super(requestParameters.payloadParameter());
    this.expectedBlobVersionedHashes = expectedBlobVersionedHashes;
    this.parentBeaconBlockRootParameter = parentBeaconBlockRootParameter;
    this.parentBeaconBlockRoot = parentBeaconBlockRootParameter.map(Bytes32::fromHexString);
  }

  public Optional<List<String>> expectedBlobVersionedHashes() {
    return expectedBlobVersionedHashes;
  }

  public Optional<String> parentBeaconBlockRootParameter() {
    return parentBeaconBlockRootParameter;
  }

  public Optional<Bytes32> parentBeaconBlockRoot() {
    return parentBeaconBlockRoot;
  }
}
