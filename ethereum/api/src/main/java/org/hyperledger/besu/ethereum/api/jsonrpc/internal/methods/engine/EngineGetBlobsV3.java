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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlobAndProofV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlobAndProofV2;

import java.util.Collections;
import java.util.List;

/**
 * Implementation of engine_getBlobsV3 API method.
 *
 * <p>This method combines the partial response capability of V1 with the blob type support and
 * result format of V2. It returns an array matching the input order, with null entries for missing
 * or unsupported blobs, and supports KZG_CELL_PROOFS blob types introduced in Osaka.
 *
 * <p>Specification:
 *
 * <ul>
 *   <li>Returns partial responses with null entries for missing blobs
 *   <li>Supports at least 128 blob versioned hashes per request
 *   <li>Uses BlobAndProofV2 result format with cell proofs
 *   <li>Only supports KZG_CELL_PROOFS blob type (rejects KZG_PROOF)
 * </ul>
 */
public final class EngineGetBlobsV3<BAP extends BlobAndProofV2> extends EngineGetBlobsV2<BAP> {

  public EngineGetBlobsV3(
      final ConstructorArguments constructorArguments,
      final HardforkId minSupportedFork,
      final HardforkId firstUnsupportedFork) {
    super(constructorArguments, minSupportedFork, firstUnsupportedFork);
  }

  @Override
  public String getName() {
    return RpcMethod.ENGINE_GET_BLOBS_V3.getMethodName();
  }

  @Override
  protected List<BlobAndProofV1> getEmptyResult(final VersionedHash[] versionedHashes) {
    return Collections.nCopies(versionedHashes.length, null);
  }

  @Override
  protected List<BAP> getBlobResult(final VersionedHash[] versionedHashes) {
    return getResultPartialMode(versionedHashes);
  }
}
