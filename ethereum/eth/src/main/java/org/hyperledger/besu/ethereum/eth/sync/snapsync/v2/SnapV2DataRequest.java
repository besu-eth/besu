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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.v2;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.RequestType;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;

import org.apache.tuweni.bytes.Bytes32;

/** Base class for snap/2 requests. Requests are bound to the pivot they were created for. */
public abstract class SnapV2DataRequest extends SnapDataRequest {

  private final BlockHeader pivotBlockHeader;
  private final Bytes32 accountRangeStart;

  protected SnapV2DataRequest(
      final RequestType requestType,
      final BlockHeader pivotBlockHeader,
      final Bytes32 accountRangeStart) {
    super(requestType, pivotBlockHeader.getStateRoot());
    this.pivotBlockHeader = pivotBlockHeader;
    this.accountRangeStart = accountRangeStart;
  }

  public BlockHeader getPivotBlockHeader() {
    return pivotBlockHeader;
  }

  public Bytes32 getRangeStart() {
    return accountRangeStart;
  }

  @Override
  public boolean isExpired(final SnapSyncProcessState snapSyncState) {
    return snapSyncState.getPivotBlockHash().filter(pivotBlockHeader.getHash()::equals).isEmpty();
  }
}
