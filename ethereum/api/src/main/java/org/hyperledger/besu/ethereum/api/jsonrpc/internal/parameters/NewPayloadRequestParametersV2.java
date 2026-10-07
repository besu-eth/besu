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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters;

import org.hyperledger.besu.datatypes.VersionedHash;

import java.util.List;

import org.apache.tuweni.bytes.Bytes32;

public sealed class NewPayloadRequestParametersV2<EP extends ExecutionPayloadV3>
    extends NewPayloadRequestParametersV1<EP> permits NewPayloadRequestParametersV3 {
  private final List<VersionedHash> expectedBlobVersionedHashes;
  private final Bytes32 parentBeaconBlockRoot;

  public NewPayloadRequestParametersV2(
      final NewPayloadRequestParametersV1<? extends EP> requestParametersV1,
      final List<VersionedHash> expectedBlobVersionedHashes,
      final Bytes32 parentBeaconBlockRoot) {
    super(requestParametersV1.payloadParameter());
    this.expectedBlobVersionedHashes = expectedBlobVersionedHashes;
    this.parentBeaconBlockRoot = parentBeaconBlockRoot;
  }

  public NewPayloadRequestParametersV2(
      final NewPayloadRequestParametersV2<? extends EP> requestParameters) {
    this(
        requestParameters,
        requestParameters.expectedBlobVersionedHashes(),
        requestParameters.parentBeaconBlockRoot());
  }

  public List<VersionedHash> expectedBlobVersionedHashes() {
    return expectedBlobVersionedHashes;
  }

  public Bytes32 parentBeaconBlockRoot() {
    return parentBeaconBlockRoot;
  }
}
