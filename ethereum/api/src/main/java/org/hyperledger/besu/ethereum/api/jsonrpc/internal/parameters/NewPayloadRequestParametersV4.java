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

import org.hyperledger.besu.ethereum.mainnet.inclusionlist.InclusionListTransactions;

public final class NewPayloadRequestParametersV4<EP extends ExecutionPayloadV3>
    extends NewPayloadRequestParametersV3<EP> {
  private final InclusionListTransactions inclusionListTransactions;

  public NewPayloadRequestParametersV4(
      final NewPayloadRequestParametersV3<? extends EP> requestParameters,
      final InclusionListTransactions inclusionListTransactions) {
    super(requestParameters, requestParameters.executionRequests());
    this.inclusionListTransactions = inclusionListTransactions;
  }

  public InclusionListTransactions inclusionListTransactions() {
    return inclusionListTransactions;
  }
}
