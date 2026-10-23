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
package org.hyperledger.besu.ethereum.api.jsonrpc;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;

import java.util.Arrays;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class JsonRpcErrorConverterTest {

  @ParameterizedTest
  @EnumSource(
      value = TransactionInvalidReason.class,
      names = {
        "INVALID_FRAMES",
        "FRAME_VERIFICATION_FAILED",
        "FRAME_SENDER_NOT_APPROVED",
        "FRAME_PAYMENT_NOT_APPROVED"
      })
  void frameValidationReasonsMapToFrameValidationFailed(final TransactionInvalidReason reason) {
    assertThat(JsonRpcErrorConverter.convertTransactionInvalidReason(reason))
        .isEqualTo(RpcErrorType.FRAME_VALIDATION_FAILED);
  }

  @ParameterizedTest
  @EnumSource(
      value = TransactionInvalidReason.class,
      mode = EnumSource.Mode.EXCLUDE,
      names = {
        "INVALID_FRAMES",
        "FRAME_VERIFICATION_FAILED",
        "FRAME_SENDER_NOT_APPROVED",
        "FRAME_PAYMENT_NOT_APPROVED"
      })
  void noOtherReasonMapsToFrameValidationFailed(final TransactionInvalidReason reason) {
    assertThat(JsonRpcErrorConverter.convertTransactionInvalidReason(reason))
        .isNotEqualTo(RpcErrorType.FRAME_VALIDATION_FAILED);
  }

  @Test
  void frameValidationFailedIsATransactionRejection() {
    assertThat(RpcErrorType.FRAME_VALIDATION_FAILED.getCode()).isEqualTo(-32000);
    assertThat(RpcErrorType.FRAME_VALIDATION_FAILED.getMessage())
        .isEqualTo("Frame transaction validation failed");
  }

  @Test
  void frameValidationFailedResolvesFromItsResponse() {
    assertThat(errorTypeOf(new JsonRpcError(RpcErrorType.FRAME_VALIDATION_FAILED)))
        .isEqualTo(RpcErrorType.FRAME_VALIDATION_FAILED);
  }

  @Test
  void frameValidationResultKeepsItsDetailAndResolvesByPrefix() {
    final JsonRpcError error =
        JsonRpcError.from(
            ValidationResult.invalid(
                TransactionInvalidReason.FRAME_SENDER_NOT_APPROVED, "sender was not approved"));

    assertThat(error.getCode()).isEqualTo(-32000);
    assertThat(error.getMessage())
        .isEqualTo("Frame transaction validation failed (sender was not approved)");
    assertThat(errorTypeOf(error)).isEqualTo(RpcErrorType.FRAME_VALIDATION_FAILED);
  }

  /**
   * JsonRpcErrorResponse resolves its type as the first constant, in ordinal order, with the same
   * code whose message prefixes the response's message. A new constant can only change how an
   * existing message resolves by becoming that first match, so no other constant may resolve to it.
   */
  @Test
  void frameValidationFailedDoesNotCaptureExistingErrorMessages() {
    Arrays.stream(RpcErrorType.values())
        .filter(type -> type != RpcErrorType.FRAME_VALIDATION_FAILED)
        .forEach(
            type ->
                assertThat(errorTypeOf(new JsonRpcError(type)))
                    .as("error type resolved for %s", type)
                    .isNotEqualTo(RpcErrorType.FRAME_VALIDATION_FAILED));
  }

  private static RpcErrorType errorTypeOf(final JsonRpcError error) {
    return new JsonRpcErrorResponse(null, error).getErrorType();
  }
}
