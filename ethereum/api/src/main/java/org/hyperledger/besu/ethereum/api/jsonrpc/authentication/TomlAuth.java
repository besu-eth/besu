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

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authentication.AuthenticationProvider;
import io.vertx.ext.auth.authentication.CredentialValidationException;
import io.vertx.ext.auth.authentication.Credentials;
import io.vertx.ext.auth.authentication.UsernamePasswordCredentials;
import org.apache.tuweni.toml.Toml;
import org.apache.tuweni.toml.TomlParseResult;
import org.apache.tuweni.toml.TomlTable;
import org.springframework.security.crypto.bcrypt.BCrypt;

public class TomlAuth implements AuthenticationProvider {

  public static final String PRIVACY_PUBLIC_KEY = "privacyPublicKey";
  private final Vertx vertx;
  private final TomlAuthOptions options;

  public TomlAuth(final Vertx vertx, final TomlAuthOptions options) {
    this.vertx = vertx;
    this.options = options;
  }

  @Override
  public Future<User> authenticate(final Credentials credentials) {
    if (!(credentials instanceof final UsernamePasswordCredentials usernamePasswordCredentials)) {
      return Future.failedFuture(new CredentialValidationException("Invalid credentials type"));
    }
    try {
      usernamePasswordCredentials.checkValid(null);
    } catch (final RuntimeException e) {
      return Future.failedFuture(e);
    }

    final String username = usernamePasswordCredentials.getUsername();
    final String password = usernamePasswordCredentials.getPassword();
    return vertx.executeBlocking(() -> authenticateBlocking(username, password), false);
  }

  // Declares IOException so a config/IO problem (e.g. a missing or unreadable toml file) fails
  // the Future with its own specific, diagnosable type rather than being folded into
  // CredentialValidationException alongside actual bad-credentials failures.
  private TomlUser authenticateBlocking(final String username, final String password)
      throws IOException {
    final TomlParseResult parseResult = Toml.parse(options.getTomlPath());

    final TomlTable userData = parseResult.getTableOrEmpty("Users." + username);
    if (userData.isEmpty()) {
      throw new CredentialValidationException("User not found");
    }

    final TomlUser tomlUser = readTomlUserFromTable(username, userData);
    if (tomlUser.getPassword().isEmpty()) {
      throw new CredentialValidationException("No password set for user");
    }

    if (!checkPasswordHash(password, tomlUser.getPassword())) {
      throw new CredentialValidationException("Invalid password");
    }

    return tomlUser;
  }

  private TomlUser readTomlUserFromTable(final String username, final TomlTable userData) {
    final String saltedAndHashedPassword = userData.getString("password", () -> "");
    final List<String> groups =
        userData.getArrayOrEmpty("groups").toList().stream()
            .map(Object::toString)
            .collect(Collectors.toList());
    final List<String> permissions =
        userData.getArrayOrEmpty("permissions").toList().stream()
            .map(Object::toString)
            .collect(Collectors.toList());
    final List<String> roles =
        userData.getArrayOrEmpty("roles").toList().stream()
            .map(Object::toString)
            .collect(Collectors.toList());
    final Optional<String> privacyPublicKey =
        Optional.ofNullable(userData.getString(PRIVACY_PUBLIC_KEY));

    return new TomlUser(
        username, saltedAndHashedPassword, groups, permissions, roles, privacyPublicKey);
  }

  private boolean checkPasswordHash(final String password, final String passwordHash) {
    return BCrypt.checkpw(password, passwordHash);
  }
}
