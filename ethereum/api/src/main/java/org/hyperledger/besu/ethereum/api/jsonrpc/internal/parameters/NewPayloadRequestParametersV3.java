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

import org.hyperledger.besu.datatypes.RequestType;
import org.hyperledger.besu.ethereum.core.Request;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;

public final class NewPayloadRequestParametersV3<EP extends ExecutionPayloadV3>
    extends NewPayloadRequestParametersV2<EP> {
  private final List<Request> executionRequests;

  public NewPayloadRequestParametersV3(
      final NewPayloadRequestParametersV2<? extends EP> requestParameters,
      final List<String> hexExecutionRequests) {
    super(requestParameters);
    this.executionRequests =
        hexExecutionRequests.stream()
            .map(
                s -> {
                  final Bytes request = Bytes.fromHexString(s);
                  final Bytes requestData = request.slice(1);
                  if (requestData.isEmpty()) {
                    throw new IllegalArgumentException("Request data cannot be empty");
                  }
                  return new Request(RequestType.of(request.get(0)), requestData);
                })
            .toList();
  }

  public NewPayloadRequestParametersV3(
      final NewPayloadRequestParametersV3<? extends EP> requestParameters) {
    super(requestParameters);
    this.executionRequests = requestParameters.executionRequests();
  }

  public List<Request> executionRequests() {
    return executionRequests;
  }
}
