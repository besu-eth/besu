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

import org.hyperledger.besu.datatypes.BlobType;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlobAndProofV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlobAndProofV2;
import org.hyperledger.besu.ethereum.core.kzg.BlobProofBundle;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.List;

import io.vertx.core.Vertx;
import org.jspecify.annotations.Nullable;

public sealed class EngineGetBlobsV2<BAP extends BlobAndProofV2> extends EngineGetBlobsV1<BAP>
    permits EngineGetBlobsV3 {

  public EngineGetBlobsV2(
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final Vertx vertx,
      final EngineCallListener engineCallListener,
      final TransactionPool transactionPool,
      final MetricsSystem metricsSystem,
      final HardforkId minSupportedFork,
      final HardforkId firstUnsupportedFork) {
    super(
        protocolSchedule,
        protocolContext,
        vertx,
        engineCallListener,
        transactionPool,
        metricsSystem,
        minSupportedFork,
        firstUnsupportedFork);
  }

  @Override
  public String getName() {
    return RpcMethod.ENGINE_GET_BLOBS_V2.getMethodName();
  }

  @Override
  protected List<BlobAndProofV1> getEmptyResult(final VersionedHash[] versionedHashes) {
    return null;
  }

  @Override
  protected List<BAP> getBlobResult(final VersionedHash[] versionedHashes) {
    return getResultFullMode(versionedHashes);
  }

  private @Nullable List<BAP> getResultFullMode(final VersionedHash[] versionedHashes) {
    final FetchedBlobsData fetchedBlobsData = fetchedBlobsData(versionedHashes);
    if (fetchedBlobsData.partial()) {
      return null;
    }
    return fetchedBlobsData.blobProofBundles().stream()
        .map(this::getVersionSpecificBlobAndProofResult)
        .toList();
  }

  @Override
  protected boolean isSupportedBlob(final BlobProofBundle blobProofBundle) {
    return blobProofBundle.getBlobType() == BlobType.KZG_PROOF;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected BAP getVersionSpecificBlobAndProofResult(final BlobProofBundle blobProofBundle) {
    return (BAP) new BlobAndProofV2(blobProofBundle);
  }
}
