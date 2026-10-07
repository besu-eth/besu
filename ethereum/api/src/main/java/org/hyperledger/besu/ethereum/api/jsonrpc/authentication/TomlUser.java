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
package org.hyperledger.besu.ethereum.api.jsonrpc.authentication;

import java.util.List;
import java.util.Optional;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authorization.Authorizations;

public class TomlUser implements User {

  private final String username;
  private final String password;
  private final List<String> groups;
  private final List<String> permissions;
  private final List<String> roles;
  private final Optional<String> privacyPublicKey;

  TomlUser(
      final String username,
      final String password,
      final List<String> groups,
      final List<String> permissions,
      final List<String> roles,
      final Optional<String> privacyPublicKey) {
    this.username = username;
    this.password = password;
    this.groups = groups;
    this.permissions = permissions;
    this.roles = roles;
    this.privacyPublicKey = privacyPublicKey;
  }

  @Override
  public JsonObject attributes() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean expired() {
    return false;
  }

  @Override
  public boolean expired(final int leeway) {
    return false;
  }

  @Override
  public boolean containsKey(final String key) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Authorizations authorizations() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public JsonObject principal() {
    final JsonObject principle =
        new JsonObject()
            .put("username", username)
            .put("password", password)
            .put("groups", groups)
            .put("permissions", permissions)
            .put("roles", roles);
    privacyPublicKey.ifPresent(pk -> principle.put("privacyPublicKey", pk));
    return principle;
  }

  @Override
  public User merge(final User other) {
    throw new UnsupportedOperationException("Not implemented");
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public List<String> getGroups() {
    return groups;
  }

  public List<String> getPermissions() {
    return permissions;
  }

  public List<String> getRoles() {
    return roles;
  }

  public Optional<String> getPrivacyPublicKey() {
    return privacyPublicKey;
  }
}
