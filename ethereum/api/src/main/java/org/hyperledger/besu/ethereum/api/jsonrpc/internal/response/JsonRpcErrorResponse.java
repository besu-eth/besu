/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.response;

import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.base.MoreObjects;
import org.apache.tuweni.bytes.Bytes;
import org.web3j.abi.FunctionReturnDecoder;
import org.web3j.abi.TypeReference;
import org.web3j.abi.datatypes.AbiTypes;
import org.web3j.abi.datatypes.Type;
import org.web3j.abi.datatypes.Utf8String;

@JsonPropertyOrder({"jsonrpc", "id", "error"})
public class JsonRpcErrorResponse implements JsonRpcResponse {

  private final Object id;
  private final JsonRpcError error;
  @JsonIgnore private final RpcErrorType errorType;

  // Encoding of "Error(string)" to check for at the start of the revert reason
  static final String errorMethodABI = "0x08c379a0";

  public JsonRpcErrorResponse(final Object id, final JsonRpcError error) {
    this.id = id;
    this.error = error;
    this.errorType = findErrorType(error.getCode(), error.getMessage());
  }

  public JsonRpcErrorResponse(final Object id, final RpcErrorType error) {
    this(id, new JsonRpcError(error));
  }

  public JsonRpcErrorResponse(
      final Object id, final ValidationResult<RpcErrorType> validationResult) {
    this(
        id,
        new JsonRpcError(validationResult.getInvalidReason(), validationResult.getErrorMessage()));
  }

  @JsonGetter("id")
  public Object getId() {
    return id;
  }

  @JsonGetter("error")
  public JsonRpcError getError() {
    return error;
  }

  @Override
  @JsonIgnore
  public RpcResponseType getType() {
    return RpcResponseType.ERROR;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final JsonRpcErrorResponse that = (JsonRpcErrorResponse) o;
    return Objects.equals(id, that.id) && Objects.equals(error, that.error);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, error);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("id", id).add("error", error).toString();
  }

  @JsonIgnore
  public RpcErrorType getErrorType() {
    return errorType;
  }

  private RpcErrorType findErrorType(final int code, final String message) {
    return Arrays.stream(RpcErrorType.values())
        .filter(e -> e.getCode() == code && message.startsWith(e.getMessage()))
        .findFirst()
        .orElse(RpcErrorType.UNKNOWN);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static Optional<String> decodeRevertReason(final Bytes revertReason) {
    if (revertReason.toHexString().startsWith(errorMethodABI)) {
      // Remove the "Error(string)" prefix
      final String encodedReasonText =
          revertReason.toHexString().substring(errorMethodABI.length());

      try {
        List<TypeReference<Type>> revertReasonTypes =
            Collections.singletonList(
                TypeReference.create((Class<Type>) AbiTypes.getType("string")));
        List<Type> decoded = FunctionReturnDecoder.decode(encodedReasonText, revertReasonTypes);

        // Expect a single decoded string
        if (decoded.size() == 1 && (decoded.get(0) instanceof Utf8String decodedRevertReason)) {
          return Optional.of(decodedRevertReason.getValue());
        }
      } catch (StringIndexOutOfBoundsException exception) {
        return Optional.of("ABI decode error");
      }
    }
    return Optional.empty();
  }
}
