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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URISyntaxException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.authentication.UsernamePasswordCredentials;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class TomlAuthTest {

  private Vertx vertx;
  private UsernamePasswordCredentials validAuthInfo;
  private TomlAuth tomlAuth;

  @BeforeEach
  public void before() throws URISyntaxException {
    vertx = Vertx.vertx();

    tomlAuth =
        new TomlAuth(
            vertx, new TomlAuthOptions().setTomlPath(getTomlPath("authentication/auth.toml")));
    validAuthInfo = new UsernamePasswordCredentials("userA", "pegasys");
  }

  @Test
  public void authInfoWithoutUsernameShouldFailAuthentication(final VertxTestContext testContext) {
    UsernamePasswordCredentials authInfo = new UsernamePasswordCredentials(null, "foo");

    tomlAuth
        .authenticate(authInfo)
        .onComplete(
            testContext.failing(
                th ->
                    testContext.verify(
                        () -> {
                          assertEquals("username cannot be null", th.getMessage());
                          testContext.completeNow();
                        })));
  }

  @Test
  public void authInfoWithoutPasswordShouldFailAuthentication(final VertxTestContext testContext) {
    UsernamePasswordCredentials authInfo = new UsernamePasswordCredentials("foo", null);

    tomlAuth
        .authenticate(authInfo)
        .onComplete(
            testContext.failing(
                th ->
                    testContext.verify(
                        () -> {
                          assertEquals("password cannot be null", th.getMessage());
                          testContext.completeNow();
                        })));
  }

  @Test
  public void parseFailureWithIOExceptionShouldFailAuthentication(
      final VertxTestContext testContext) {
    tomlAuth = new TomlAuth(vertx, new TomlAuthOptions().setTomlPath("invalid_path"));

    tomlAuth
        .authenticate(validAuthInfo)
        .onComplete(
            testContext.failing(
                th ->
                    testContext.verify(
                        () -> {
                          assertEquals(NoSuchFileException.class, th.getClass());
                          testContext.completeNow();
                        })));
  }

  @Test
  public void authInfoWithAbsentUserShouldFailAuthentication(final VertxTestContext testContext) {
    UsernamePasswordCredentials authInfo = new UsernamePasswordCredentials("foo", "foo");

    tomlAuth
        .authenticate(authInfo)
        .onComplete(
            testContext.failing(
                th ->
                    testContext.verify(
                        () -> {
                          assertEquals("User not found", th.getMessage());
                          testContext.completeNow();
                        })));
  }

  @Test
  public void userWithoutPasswordSetShouldFailAuthentication(final VertxTestContext testContext) {
    UsernamePasswordCredentials authInfo = new UsernamePasswordCredentials("noPasswordUser", "foo");

    tomlAuth
        .authenticate(authInfo)
        .onComplete(
            testContext.failing(
                th ->
                    testContext.verify(
                        () -> {
                          assertEquals("No password set for user", th.getMessage());
                          testContext.completeNow();
                        })));
  }

  @Test
  public void passwordMismatchShouldFailAuthentication(final VertxTestContext testContext) {
    UsernamePasswordCredentials authInfo = new UsernamePasswordCredentials("userA", "foo");

    tomlAuth
        .authenticate(authInfo)
        .onComplete(
            testContext.failing(
                th ->
                    testContext.verify(
                        () -> {
                          assertEquals("Invalid password", th.getMessage());
                          testContext.completeNow();
                        })));
  }

  @Test
  public void validPasswordWithAllValuesShouldAuthenticateAndCreateUserSuccessfully(
      final VertxTestContext testContext) {
    JsonObject expectedPrincipal =
        new JsonObject()
            .put("username", "userA")
            .put("password", "$2a$10$l3GA7K8g6rJ/Yv.YFSygCuI9byngpEzxgWS9qEg5emYDZomQW7fGC")
            .put("groups", new JsonArray().add("admin"))
            .put("permissions", new JsonArray().add("eth:*").add("perm:*"))
            .put("roles", new JsonArray().add("net"))
            .put("privacyPublicKey", "A1aVtMxLCUHmBVHXoZzzBgPbW/wj5axDpW9X8l91SGo=");

    UsernamePasswordCredentials authInfo = new UsernamePasswordCredentials("userA", "pegasys");

    tomlAuth
        .authenticate(authInfo)
        .onComplete(
            testContext.succeeding(
                res ->
                    testContext.verify(
                        () -> {
                          assertEquals(expectedPrincipal, res.principal());
                          testContext.completeNow();
                        })));
  }

  @Test
  public void validPasswordWithOptionalValuesShouldAuthenticateAndCreateUserSuccessfully(
      final VertxTestContext testContext) {
    JsonObject expectedPrincipal =
        new JsonObject()
            .put("username", "userB")
            .put("password", "$2a$10$l3GA7K8g6rJ/Yv.YFSygCuI9byngpEzxgWS9qEg5emYDZomQW7fGC")
            .put("groups", new JsonArray())
            .put("permissions", new JsonArray().add("net:*"))
            .put("roles", new JsonArray());

    UsernamePasswordCredentials authInfo = new UsernamePasswordCredentials("userB", "pegasys");

    tomlAuth
        .authenticate(authInfo)
        .onComplete(
            testContext.succeeding(
                res ->
                    testContext.verify(
                        () -> {
                          assertEquals(expectedPrincipal, res.principal());
                          testContext.completeNow();
                        })));
  }

  private String getTomlPath(final String tomlFileName) throws URISyntaxException {
    return Paths.get(ClassLoader.getSystemResource(tomlFileName).toURI())
        .toAbsolutePath()
        .toString();
  }
}
