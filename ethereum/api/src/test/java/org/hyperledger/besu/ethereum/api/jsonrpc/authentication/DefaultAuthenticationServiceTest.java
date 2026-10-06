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
package org.hyperledger.besu.ethereum.api.jsonrpc.authentication;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.WebSocketConfiguration;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.JWTOptions;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class DefaultAuthenticationServiceTest {

  private static final Collection<String> NO_AUTH_METHODS =
      Arrays.asList(RpcMethod.NET_SERVICES.getMethodName());

  private Vertx vertx;
  private WebSocketConfiguration configuration;
  private DefaultAuthenticationService authenticationService;
  private JWTAuth jwtAuth;

  @BeforeEach
  public void beforeEach() throws URISyntaxException {
    vertx = Vertx.vertx();

    final String authTomlPath =
        Paths.get(ClassLoader.getSystemResource("JsonRpcHttpService/auth.toml").toURI())
            .toAbsolutePath()
            .toString();

    configuration = WebSocketConfiguration.createDefault();
    configuration.setAuthenticationEnabled(true);
    configuration.setAuthenticationCredentialsFile(authTomlPath);
    configuration.setRpcApisNoAuth(new ArrayList<>(NO_AUTH_METHODS));

    final Optional<AuthenticationService> authService =
        DefaultAuthenticationService.create(vertx, configuration);
    assertThat(authService).isPresent();
    authenticationService = (DefaultAuthenticationService) authService.get();
    jwtAuth = authenticationService.getJwtAuthProvider();
  }

  @AfterEach
  public void afterEach() {
    vertx.close();
  }

  @Test
  public void synchronousPermissionCheckingAllowsAccessWithCorrectPermission() {
    // Create a user with the required permission
    final JWTOptions jwtOptions = new JWTOptions().setExpiresInMinutes(5).setAlgorithm("RS256");
    final JsonObject jwtContents =
        new JsonObject()
            .put("permissions", Arrays.asList("eth:blockNumber"))
            .put("username", "user");
    final String token = jwtAuth.generateToken(jwtContents, jwtOptions);

    // Create a mock JsonRpcMethod that requires the eth:blockNumber permission
    final JsonRpcMethod method = mock(JsonRpcMethod.class);
    when(method.getName()).thenReturn("eth_blockNumber");
    when(method.getPermissions()).thenReturn(Collections.singletonList("eth:blockNumber"));

    final User[] userHolder = new User[1];
    authenticationService.authenticate(token, user -> userHolder[0] = user.orElse(null));

    assertThat(userHolder[0]).isNotNull();

    final boolean isPermitted =
        authenticationService.isPermitted(Optional.of(userHolder[0]), method, NO_AUTH_METHODS);

    // User should be permitted
    assertThat(isPermitted).isTrue();
  }

  @Test
  public void synchronousPermissionCheckingDeniesAccessWithIncorrectPermission() {
    // Create a user with a different permission
    final JWTOptions jwtOptions = new JWTOptions().setExpiresInMinutes(5).setAlgorithm("RS256");
    final JsonObject jwtContents =
        new JsonObject().put("permissions", Arrays.asList("web3:*")).put("username", "user");
    final String token = jwtAuth.generateToken(jwtContents, jwtOptions);

    // Create a mock JsonRpcMethod that requires the eth:blockNumber permission
    final JsonRpcMethod method = mock(JsonRpcMethod.class);
    when(method.getName()).thenReturn("eth_blockNumber");
    when(method.getPermissions()).thenReturn(Collections.singletonList("eth:blockNumber"));

    final User[] userHolder = new User[1];
    authenticationService.authenticate(token, user -> userHolder[0] = user.orElse(null));

    assertThat(userHolder[0]).isNotNull();

    final boolean isPermitted =
        authenticationService.isPermitted(Optional.of(userHolder[0]), method, NO_AUTH_METHODS);

    // User should not be permitted
    assertThat(isPermitted).isFalse();
  }

  @Test
  public void synchronousPermissionCheckingAllowsAccessForNoAuthMethods() {
    // Create a user with no permissions
    final JWTOptions jwtOptions = new JWTOptions().setExpiresInMinutes(5).setAlgorithm("RS256");
    final JsonObject jwtContents =
        new JsonObject().put("permissions", Collections.emptyList()).put("username", "user");
    final String token = jwtAuth.generateToken(jwtContents, jwtOptions);

    // Create a mock JsonRpcMethod that is in the NO_AUTH_METHODS list
    final JsonRpcMethod method = mock(JsonRpcMethod.class);
    when(method.getName()).thenReturn(RpcMethod.NET_SERVICES.getMethodName());
    when(method.getPermissions()).thenReturn(Collections.singletonList("net:*"));

    final User[] userHolder = new User[1];
    authenticationService.authenticate(token, user -> userHolder[0] = user.orElse(null));

    assertThat(userHolder[0]).isNotNull();

    // Check if the user is permitted to access the method
    final boolean isPermitted =
        authenticationService.isPermitted(Optional.of(userHolder[0]), method, NO_AUTH_METHODS);

    // User should be permitted because the method is in NO_AUTH_METHODS
    assertThat(isPermitted).isTrue();
  }

  @Test
  public void synchronousPermissionCheckingHandlesExceptions() {
    // Create a user with the required permission
    final JWTOptions jwtOptions = new JWTOptions().setExpiresInMinutes(5).setAlgorithm("RS256");
    final JsonObject jwtContents =
        new JsonObject()
            .put("permissions", Arrays.asList("eth:blockNumber"))
            .put("username", "user");
    final String token = jwtAuth.generateToken(jwtContents, jwtOptions);

    // Create a mock JsonRpcMethod that requires a permission that will cause an exception
    final JsonRpcMethod method = mock(JsonRpcMethod.class);
    when(method.getName()).thenReturn("eth_blockNumber");
    when(method.getPermissions())
        .thenReturn(Collections.singletonList("invalid:permission:format"));

    final User[] userHolder = new User[1];
    authenticationService.authenticate(token, user -> userHolder[0] = user.orElse(null));

    assertThat(userHolder[0]).isNotNull();

    final boolean isPermitted =
        authenticationService.isPermitted(Optional.of(userHolder[0]), method, NO_AUTH_METHODS);

    // User should not be permitted due to exception in permission checking
    assertThat(isPermitted).isFalse();
  }

  @Test
  @Timeout(10)
  public void authenticateWithNoExpiryClaimFailsClosedInsteadOfHanging() {
    // JWTOptions with no expiresInMinutes/expiresInSeconds omits the "exp" claim entirely, which
    // validateExpiryExists() rejects. Under Vert.x 5, that rejection happens inside a nested
    // Future#onComplete callback: if it isn't guarded there, Vert.x's FutureBase#signalComplete
    // swallows the thrown exception and the handler below is never invoked at all (a hang), rather
    // than being called with Optional.empty() as it should be.
    final JWTOptions jwtOptionsWithNoExpiry = new JWTOptions().setAlgorithm("RS256");
    final JsonObject jwtContents =
        new JsonObject()
            .put("permissions", Arrays.asList("eth:blockNumber"))
            .put("username", "user");
    final String token = jwtAuth.generateToken(jwtContents, jwtOptionsWithNoExpiry);

    final AtomicBoolean handlerInvoked = new AtomicBoolean(false);
    final User[] userHolder = new User[1];
    authenticationService.authenticate(
        token,
        user -> {
          handlerInvoked.set(true);
          userHolder[0] = user.orElse(null);
        });

    assertThat(handlerInvoked).as("authentication handler should always be invoked").isTrue();
    assertThat(userHolder[0]).as("JWT without an expiry claim must fail authentication").isNull();
  }
}
