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

import org.hyperledger.besu.datatypes.BlobType;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.kzg.BlobsWithCommitments;
import org.hyperledger.besu.ethereum.core.kzg.CKZG4844Helper;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BlobsBundleV2 extends BlobsBundleV1 {
  private static final Logger LOG = LoggerFactory.getLogger(BlobsBundleV2.class);

  public BlobsBundleV2(final List<Transaction> transactions) {
    super(transactions, BlobsBundleV2::convertBlobType);
  }

  private static BlobsWithCommitments convertBlobType(final BlobsWithCommitments bwc) {
    if (bwc.getBlobType() == BlobType.KZG_PROOF) {
      LOG.warn(
          "BlobsWithCommitments {} has a blob type of KZG_PROOF. Converting to KZG_CELL_PROOFS.",
          bwc.getVersionedHashes());
      return CKZG4844Helper.convertToVersion1(bwc);
    }
    return bwc;
  }
}
