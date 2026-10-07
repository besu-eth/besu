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

import org.hyperledger.besu.datatypes.Hash;

import java.util.Map;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes32;

/**
 * Outcome of {@link SnapV2ReorgHealer#recoverFromReorg}.
 *
 * @param deletedAccounts accounts removed locally because they do not exist at the new pivot
 *     (created only on the orphaned fork); queued storage requests for these must be dropped
 * @param correctedStorageRoots canonical storage roots at the new pivot for every account whose
 *     local record was rewritten during recovery, plus the patched pending accounts; used to
 *     retarget queued storage requests
 */
public record ReorgRecoveryResult(
    Set<Hash> deletedAccounts, Map<Hash, Bytes32> correctedStorageRoots) {}
