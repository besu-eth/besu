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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results;

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV3;
import org.hyperledger.besu.ethereum.core.Request;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonGetter;

public sealed class EngineGetPayloadResultV5 extends EngineGetPayloadResultV4
    permits EngineGetPayloadResultV6 {

  public EngineGetPayloadResultV5(
      final ExecutionPayloadV3 executionPayload,
      final Wei blockValue,
      final BlobsBundleV2 blobsBundle,
      final List<Request> executionRequests) {
    super(executionPayload, blockValue, blobsBundle, executionRequests);
  }

  @Override
  @JsonGetter(value = "blobsBundle")
  public BlobsBundleV2 getBlobsBundle() {
    return (BlobsBundleV2) blobsBundle;
  }
}
