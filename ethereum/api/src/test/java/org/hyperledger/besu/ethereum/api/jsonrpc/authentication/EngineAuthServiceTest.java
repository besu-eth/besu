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
package org.hyperledger.besu.ethereum.api.jsonrpc.authentication;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Optional;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.jwt.JWTAuth;
import org.junit.jupiter.api.Test;

public class EngineAuthServiceTest {

  @Test
  public void createsEphemeralByDefault() throws IOException {
    Vertx vertx = mock(Vertx.class);
    Path dataDir = Files.createTempDirectory("besuUnitTest");
    EngineAuthService auth = new EngineAuthService(vertx, Optional.empty(), dataDir);
    assertThat(auth).isNotNull();
    assertThat(dataDir.toFile()).exists();
    assertThat(dataDir.toFile()).isDirectory();
    boolean defaultFileFound =
        Arrays.stream(dataDir.toFile().listFiles())
            .anyMatch(
                file -> {
                  return file.getName().equals("jwt.hex");
                });
    assertThat(defaultFileFound).isTrue();
  }

  @Test
  public void usesSpecified() throws IOException, URISyntaxException {
    Vertx vertx = mock(Vertx.class);
    final Path userKey =
        Paths.get(ClassLoader.getSystemResource("authentication/ee-jwt-secret.hex").toURI());
    Path dataDir = Files.createTempDirectory("besuUnitTest");
    EngineAuthService auth = new EngineAuthService(vertx, Optional.of(userKey.toFile()), dataDir);
    assertThat(auth).isNotNull();
    JWTAuth jwtAuth = auth.getJwtAuthProvider();
    String token =
        jwtAuth.generateToken(new JsonObject().put("iat", System.currentTimeMillis() / 1000));

    Handler<Optional<User>> authHandler =
        new Handler<Optional<User>>() {
          @Override
          public void handle(final Optional<User> event) {
            assertThat(event).isPresent();
          }
        };
    auth.authenticate(token, authHandler);
  }

  @Test
  public void throwsOnShortKey() throws IOException, URISyntaxException {
    Vertx vertx = mock(Vertx.class);
    final Path userKey =
        Paths.get(
            ClassLoader.getSystemResource("authentication/ee-jwt-secret-too-short.hex").toURI());
    Path dataDir = Files.createTempDirectory("besuUnitTest");
    final Optional<File> signingKey = Optional.of(userKey.toFile());
    assertThatThrownBy(() -> new EngineAuthService(vertx, signingKey, dataDir))
        .isInstanceOf(UnsecurableEngineApiException.class);
  }

  @Test
  public void throwsKeyFileMissing() throws IOException, URISyntaxException {
    Vertx vertx = mock(Vertx.class);
    final Path userKey = Paths.get("no-such-file.hex");
    Path dataDir = Files.createTempDirectory("besuUnitTest");
    final Optional<File> signingKey = Optional.of(userKey.toFile());
    assertThatThrownBy(() -> new EngineAuthService(vertx, signingKey, dataDir))
        .isInstanceOf(UnsecurableEngineApiException.class);
  }

  @Test
  public void denyExpired() throws IOException, URISyntaxException {
    Vertx vertx = mock(Vertx.class);
    final Path userKey =
        Paths.get(ClassLoader.getSystemResource("authentication/ee-jwt-secret.hex").toURI());
    Path dataDir = Files.createTempDirectory("besuUnitTest");
    EngineAuthService auth = new EngineAuthService(vertx, Optional.of(userKey.toFile()), dataDir);
    assertThat(auth).isNotNull();
    JWTAuth jwtAuth = auth.getJwtAuthProvider();
    String token =
        jwtAuth.generateToken(
            new JsonObject().put("iat", (System.currentTimeMillis() / 1000) - 61));

    Handler<Optional<User>> authHandler = event -> assertThat(event).isEmpty();
    auth.authenticate(token, authHandler);
  }

  @Test
  public void cachesFreshTokenAndAcceptsOnSubsequentCall() throws IOException, URISyntaxException {
    Vertx vertx = mock(Vertx.class);
    final Path userKey =
        Paths.get(ClassLoader.getSystemResource("authentication/ee-jwt-secret.hex").toURI());
    Path dataDir = Files.createTempDirectory("besuUnitTest");
    EngineAuthService auth = new EngineAuthService(vertx, Optional.of(userKey.toFile()), dataDir);
    JWTAuth jwtAuth = auth.getJwtAuthProvider();
    String token =
        jwtAuth.generateToken(new JsonObject().put("iat", System.currentTimeMillis() / 1000));

    // First call: slow path — full parse + HMAC verify, result cached
    auth.authenticate(token, event -> assertThat(event).isPresent());

    // Second call: fast path — same token string, should be accepted from cache
    auth.authenticate(token, event -> assertThat(event).isPresent());
  }

  @Test
  public void rejectsStaleCachedToken() throws IOException, URISyntaxException {
    Vertx vertx = mock(Vertx.class);
    final Path userKey =
        Paths.get(ClassLoader.getSystemResource("authentication/ee-jwt-secret.hex").toURI());
    Path dataDir = Files.createTempDirectory("besuUnitTest");
    EngineAuthService auth = new EngineAuthService(vertx, Optional.of(userKey.toFile()), dataDir);
    JWTAuth jwtAuth = auth.getJwtAuthProvider();

    // Token with iat 61 seconds ago: slow path rejects it (issuedRecently returns false),
    // so it is never cached. A subsequent call with the same token also goes through the
    // slow path and is rejected again.
    String staleToken =
        jwtAuth.generateToken(
            new JsonObject().put("iat", (System.currentTimeMillis() / 1000) - 61));
    auth.authenticate(staleToken, event -> assertThat(event).isEmpty());
    auth.authenticate(staleToken, event -> assertThat(event).isEmpty());
  }
}
