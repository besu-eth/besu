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

import java.util.Optional;

public sealed class ForkchoiceUpdatedRequestParametersV1<PA extends PayloadAttributesV1>
    permits ForkchoiceUpdatedRequestParametersV2 {
  private final ForkchoiceStateV1 forkchoiceState;
  private final PA payloadAttributes;

  public ForkchoiceUpdatedRequestParametersV1(
      final ForkchoiceStateV1 forkchoiceState, final Optional<PA> payloadAttributes) {
    this.forkchoiceState = forkchoiceState;
    this.payloadAttributes = payloadAttributes.orElse(null);
  }

  public ForkchoiceUpdatedRequestParametersV1(
      final ForkchoiceUpdatedRequestParametersV1<? extends PA> requestParameters) {
    this.forkchoiceState = requestParameters.forkchoiceState();
    this.payloadAttributes = requestParameters.payloadAttributes().orElse(null);
  }

  public ForkchoiceStateV1 forkchoiceState() {
    return forkchoiceState;
  }

  public Optional<PA> payloadAttributes() {
    return Optional.ofNullable(payloadAttributes);
  }
}
