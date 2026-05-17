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

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import io.vertx.ext.auth.User;

public class JsonRpcRequestContext {

  private final JsonRpcRequest jsonRpcRequest;
  private final Optional<User> user;
  private final Supplier<Boolean> alive;

  public JsonRpcRequestContext(final JsonRpcRequest jsonRpcRequest) {
    this(jsonRpcRequest, () -> true);
  }

  public JsonRpcRequestContext(final JsonRpcRequest jsonRpcRequest, final Supplier<Boolean> alive) {
    this(jsonRpcRequest, Optional.empty(), alive);
  }

  public JsonRpcRequestContext(final JsonRpcRequest jsonRpcRequest, final User user) {
    this(jsonRpcRequest, Optional.of(user), () -> true);
  }

  public JsonRpcRequestContext(
      final JsonRpcRequest jsonRpcRequest, final User user, final Supplier<Boolean> alive) {
    this(jsonRpcRequest, Optional.of(user), alive);
  }

  public JsonRpcRequestContext(
      final JsonRpcRequest jsonRpcRequest,
      final Optional<User> user,
      final Supplier<Boolean> alive) {
    this.jsonRpcRequest = jsonRpcRequest;
    this.user = user;
    this.alive = alive;
  }

  public JsonRpcRequest getRequest() {
    return jsonRpcRequest;
  }

  public Optional<User> getUser() {
    return user;
  }

  public <T> T getRequiredParameter(final int index, final Class<T> paramClass)
      throws JsonRpcParameterException {
    return jsonRpcRequest.getRequiredParameter(
        index, paramClass, JsonRpcParameter.Configuration.DEFAULT);
  }

  public <T> T getRequiredParameter(
      final int index,
      final Class<T> paramClass,
      final JsonRpcParameter.Configuration configuration)
      throws JsonRpcParameterException {
    return jsonRpcRequest.getRequiredParameter(index, paramClass, configuration);
  }

  public <T> Optional<T> getOptionalParameter(final int index, final Class<T> paramClass)
      throws JsonRpcParameterException {
    return jsonRpcRequest.getOptionalParameter(
        index, paramClass, JsonRpcParameter.Configuration.DEFAULT);
  }

  public <T> Optional<T> getOptionalParameter(
      final int index,
      final Class<T> paramClass,
      final JsonRpcParameter.Configuration configuration)
      throws JsonRpcParameterException {
    return jsonRpcRequest.getOptionalParameter(index, paramClass, configuration);
  }

  public <T> Optional<List<T>> getOptionalList(final int index, final Class<T> listOf)
      throws JsonRpcParameterException {
    return jsonRpcRequest.getOptionalList(index, listOf, JsonRpcParameter.Configuration.DEFAULT);
  }

  public <T> Optional<List<T>> getOptionalList(
      final int index, final Class<T> listOf, final JsonRpcParameter.Configuration configuration)
      throws JsonRpcParameterException {
    return jsonRpcRequest.getOptionalList(index, listOf, configuration);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final JsonRpcRequestContext that = (JsonRpcRequestContext) o;
    return Objects.equals(jsonRpcRequest, that.jsonRpcRequest) && Objects.equals(user, that.user);
  }

  @Override
  public int hashCode() {
    return Objects.hash(jsonRpcRequest, user);
  }

  public boolean isAlive() {
    return alive.get();
  }
}
