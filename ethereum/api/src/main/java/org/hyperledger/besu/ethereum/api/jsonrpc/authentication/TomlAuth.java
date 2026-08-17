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

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authentication.AuthenticationProvider;
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
  public void authenticate(
      final JsonObject authInfo, final Handler<AsyncResult<User>> resultHandler) {
    final String username = authInfo.getString("username");
    if (username == null) {
      resultHandler.handle(Future.failedFuture("No username provided"));
      return;
    }

    final String password = authInfo.getString("password");
    if (password == null) {
      resultHandler.handle(Future.failedFuture("No password provided"));
      return;
    }

    vertx.executeBlocking(
        f -> {
          TomlParseResult parseResult;
          try {
            parseResult = Toml.parse(options.getTomlPath());
          } catch (IOException e) {
            f.fail(e);
            return;
          }

          final TomlTable userData = parseResult.getTableOrEmpty("Users." + username);
          if (userData.isEmpty()) {
            f.fail("User not found");
            return;
          }

          final TomlUser tomlUser = readTomlUserFromTable(username, userData);
          if (tomlUser.getPassword().isEmpty()) {
            f.fail("No password set for user");
            return;
          }

          checkPasswordHash(
              password,
              tomlUser.getPassword(),
              rs -> {
                if (rs.succeeded()) {
                  f.complete(tomlUser);
                } else {
                  f.fail(rs.cause());
                }
              });
        },
        false,
        res -> {
          if (res.succeeded()) {
            resultHandler.handle(Future.succeededFuture((User) res.result()));
          } else {
            resultHandler.handle(Future.failedFuture(res.cause()));
          }
        });
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

  private void checkPasswordHash(
      final String password,
      final String passwordHash,
      final Handler<AsyncResult<Void>> resultHandler) {
    boolean passwordMatches = BCrypt.checkpw(password, passwordHash);
    if (passwordMatches) {
      resultHandler.handle(Future.succeededFuture());
    } else {
      resultHandler.handle(Future.failedFuture("Invalid password"));
    }
  }
}
