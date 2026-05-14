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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcRequestException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;

@JsonIgnoreProperties(ignoreUnknown = true)
public class JsonRpcRequest {

  private final JsonRpcParameter parameterAccessor = new JsonRpcParameter();

  private JsonRpcRequestId id;
  private final String method;
  private final Object[] params;
  private final String version;
  private boolean isNotification = true;

  @JsonCreator
  public JsonRpcRequest(
      @JsonProperty("jsonrpc") final String version,
      @JsonProperty("method") final String method,
      @JsonProperty("params") final Object[] params) {
    this.version = version;
    this.method = method;
    this.params = params;
    if (method == null) {
      throw new InvalidJsonRpcRequestException(
          "Field 'method' is required", RpcErrorType.INVALID_METHOD_PARAMS);
    }
  }

  @JsonGetter("id")
  public Object getId() {
    return id == null ? null : id.getValue();
  }

  @JsonGetter("method")
  public String getMethod() {
    return method;
  }

  @JsonGetter("jsonrpc")
  public String getVersion() {
    return version;
  }

  @JsonInclude(Include.NON_NULL)
  @JsonGetter("params")
  public Object[] getParams() {
    return params;
  }

  @JsonIgnore
  public boolean isNotification() {
    return isNotification;
  }

  @JsonIgnore
  public int getParamLength() {
    return hasParams() ? params.length : 0;
  }

  @JsonIgnore
  public boolean hasParams() {

    // Null Object: "params":null
    if (params == null) {
      return false;
    }

    // Null Array: "params":[null]
    if (params.length == 0 || params[0] == null) {
      return false;
    }

    return true;
  }

  @JsonSetter("id")
  protected void setId(final JsonRpcRequestId id) {
    // If an id is explicitly set, it is not a notification
    isNotification = false;
    this.id = id;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final JsonRpcRequest that = (JsonRpcRequest) o;
    return isNotification == that.isNotification
        && Objects.equals(id, that.id)
        && Objects.equals(method, that.method)
        && Arrays.equals(params, that.params)
        && Objects.equals(version, that.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, method, Arrays.hashCode(params), version, isNotification);
  }

  public <T> T getRequiredParameter(final int index, final Class<T> paramClass)
      throws JsonRpcParameterException {
    return parameterAccessor.required(
        params, index, paramClass, JsonRpcParameter.Configuration.DEFAULT);
  }

  public <T> T getRequiredParameter(
      final int index,
      final Class<T> paramClass,
      final JsonRpcParameter.Configuration configuration)
      throws JsonRpcParameterException {
    return parameterAccessor.required(params, index, paramClass, configuration);
  }

  public <T> Optional<T> getOptionalParameter(final int index, final Class<T> paramClass)
      throws JsonRpcParameterException {
    return parameterAccessor.optional(
        params, index, paramClass, JsonRpcParameter.Configuration.DEFAULT);
  }

  public <T> Optional<T> getOptionalParameter(
      final int index,
      final Class<T> paramClass,
      final JsonRpcParameter.Configuration configuration)
      throws JsonRpcParameterException {
    return parameterAccessor.optional(params, index, paramClass, configuration);
  }

  public <T> Optional<List<T>> getOptionalList(final int index, final Class<T> paramClass)
      throws JsonRpcParameterException {
    return parameterAccessor.optionalList(
        params, index, paramClass, JsonRpcParameter.Configuration.DEFAULT);
  }

  public <T> Optional<List<T>> getOptionalList(
      final int index,
      final Class<T> paramClass,
      final JsonRpcParameter.Configuration configuration)
      throws JsonRpcParameterException {
    return parameterAccessor.optionalList(params, index, paramClass, configuration);
  }

  @Override
  public String toString() {
    return "JsonRpcRequest{"
        + "id="
        + id
        + ", method='"
        + method
        + '\''
        + ", params="
        + Arrays.toString(params)
        + ", version='"
        + version
        + '\''
        + ", isNotification="
        + isNotification
        + '}';
  }
}
