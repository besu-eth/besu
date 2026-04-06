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

import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.WebSocketConfiguration;

import java.io.File;
import java.util.Collection;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.JWTOptions;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authentication.AuthenticationProvider;
import io.vertx.ext.auth.authentication.Credentials;
import io.vertx.ext.auth.authentication.TokenCredentials;
import io.vertx.ext.auth.authentication.UsernamePasswordCredentials;
import io.vertx.ext.auth.authorization.PermissionBasedAuthorization;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.RoutingContext;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Provides authentication handlers for use in the http and websocket services */
public class DefaultAuthenticationService implements AuthenticationService {

  public static final String USERNAME = "username";
  public static final String PASSWORD = "password";
  private final JWTAuth jwtAuthProvider;
  @VisibleForTesting public final JWTAuthOptions jwtAuthOptions;
  private final Optional<AuthenticationProvider> credentialAuthProvider;
  private static final JWTAuthOptionsFactory jwtAuthOptionsFactory = new JWTAuthOptionsFactory();
  private static final Logger LOG = LoggerFactory.getLogger(DefaultAuthenticationService.class);

  public DefaultAuthenticationService(
      final JWTAuth jwtAuthProvider,
      final JWTAuthOptions jwtAuthOptions,
      final Optional<AuthenticationProvider> credentialAuthProvider) {
    this.jwtAuthProvider = jwtAuthProvider;
    this.jwtAuthOptions = jwtAuthOptions;
    this.credentialAuthProvider = credentialAuthProvider;
  }

  /**
   * Creates a ready for use set of authentication providers if authentication is enabled
   *
   * @param vertx The vertx instance that will be providing requests that this set of authentication
   *     providers will be handling
   * @param config The {{@link JsonRpcConfiguration}} that describes this rpc setup
   * @return Optionally an authentication service. If empty then authentication isn't to be enabled
   *     on this service
   */
  public static Optional<AuthenticationService> create(
      final Vertx vertx, final JsonRpcConfiguration config) {
    return create(
        vertx,
        config.isAuthenticationEnabled(),
        config.getAuthenticationCredentialsFile(),
        config.getAuthenticationPublicKeyFile(),
        config.getAuthenticationAlgorithm());
  }

  /**
   * Creates a ready for use set of authentication providers if authentication is enabled
   *
   * @param vertx The vertx instance that will be providing requests that this set of authentication
   *     providers will be handling
   * @param config The {{@link WebSocketConfiguration}} that describes this rpc setup
   * @return Optionally an authentication service. If empty then authentication isn't to be enabled
   *     on this service
   */
  public static Optional<AuthenticationService> create(
      final Vertx vertx, final WebSocketConfiguration config) {
    return create(
        vertx,
        config.isAuthenticationEnabled(),
        config.getAuthenticationCredentialsFile(),
        config.getAuthenticationPublicKeyFile(),
        config.getAuthenticationAlgorithm());
  }

  private static Optional<AuthenticationService> create(
      final Vertx vertx,
      final boolean authenticationEnabled,
      final String authenticationCredentialsFile,
      final File authenticationPublicKeyFile,
      final JwtAlgorithm authenticationAlgorithm) {
    if (!authenticationEnabled) {
      return Optional.empty();
    }

    final JWTAuthOptions jwtAuthOptions;
    if (authenticationPublicKeyFile == null) {
      jwtAuthOptions = jwtAuthOptionsFactory.createWithGeneratedKeyPair();
    } else {
      jwtAuthOptions =
          authenticationAlgorithm == null
              ? jwtAuthOptionsFactory.createForExternalPublicKey(authenticationPublicKeyFile)
              : jwtAuthOptionsFactory.createForExternalPublicKeyWithAlgorithm(
                  authenticationPublicKeyFile, authenticationAlgorithm);
    }
    final Optional<AuthenticationProvider> credentialAuthProvider =
        makeCredentialAuthProvider(vertx, authenticationEnabled, authenticationCredentialsFile);

    return Optional.of(
        new DefaultAuthenticationService(
            JWTAuth.create(vertx, jwtAuthOptions), jwtAuthOptions, credentialAuthProvider));
  }

  private static Optional<AuthenticationProvider> makeCredentialAuthProvider(
      final Vertx vertx,
      final boolean authenticationEnabled,
      @Nullable final String authenticationCredentialsFile) {
    if (authenticationEnabled && authenticationCredentialsFile != null) {
      return Optional.of(
          new TomlAuthOptions().setTomlPath(authenticationCredentialsFile).createProvider(vertx));
    } else {
      return Optional.empty();
    }
  }

  /**
   * Static route for terminating login requests when Authentication is disabled
   *
   * @param routingContext The vertx routing context for this request
   */
  public static void handleDisabledLogin(final RoutingContext routingContext) {
    routingContext
        .response()
        .setStatusCode(HttpResponseStatus.BAD_REQUEST.code())
        .setStatusMessage("Authentication not enabled")
        .end();
  }

  /**
   * Handles a login request and checks the provided credentials against our credential auth
   * provider
   *
   * @param routingContext Routing context associated with this request
   */
  @Override
  public void handleLogin(final RoutingContext routingContext) {
    if (credentialAuthProvider.isPresent()) {
      login(routingContext, credentialAuthProvider.get());
    } else {
      handleDisabledLogin(routingContext);
    }
  }

  private void login(
      final RoutingContext routingContext, final AuthenticationProvider credentialAuthProvider) {
    final JsonObject requestBody = routingContext.body().asJsonObject();

    if (requestBody == null
        || requestBody.getValue(USERNAME) == null
        || requestBody.getValue(PASSWORD) == null) {
      routingContext
          .response()
          .setStatusCode(HttpResponseStatus.BAD_REQUEST.code())
          .setStatusMessage(HttpResponseStatus.BAD_REQUEST.reasonPhrase())
          .end("Authentication failed: username and password are required.");
      return;
    }

    // Check user
    final JsonObject authParams = new JsonObject();
    authParams.put(USERNAME, requestBody.getValue(USERNAME));
    authParams.put(PASSWORD, requestBody.getValue(PASSWORD));
    final Credentials credentials = new UsernamePasswordCredentials(authParams);

    credentialAuthProvider.authenticate(
        credentials,
        r -> {
          if (r.failed()) {
            routingContext
                .response()
                .setStatusCode(HttpResponseStatus.UNAUTHORIZED.code())
                .setStatusMessage(HttpResponseStatus.UNAUTHORIZED.reasonPhrase())
                .end("Authentication failed: the username or password is incorrect.");
          } else {
            final User user = r.result();

            final JWTOptions options =
                new JWTOptions().setExpiresInMinutes(5).setAlgorithm("RS256");
            final JsonObject jwtContents =
                new JsonObject()
                    .put("permissions", user.principal().getValue("permissions"))
                    .put(USERNAME, user.principal().getValue(USERNAME));
            final String privacyPublicKey = user.principal().getString("privacyPublicKey");
            if (privacyPublicKey != null) {
              jwtContents.put("privacyPublicKey", privacyPublicKey);
            }

            final String token = jwtAuthProvider.generateToken(jwtContents, options);

            final JsonObject responseBody = new JsonObject().put("token", token);
            final HttpServerResponse response = routingContext.response();
            if (!response.closed()) {
              response.setStatusCode(200);
              response.putHeader("Content-Type", "application/json");
              response.end(responseBody.encode());
            }
          }
        });
  }

  @Override
  public JWTAuth getJwtAuthProvider() {
    return jwtAuthProvider;
  }

  @Override
  public void authenticate(final String token, final Handler<Optional<User>> handler) {
    try {
      getJwtAuthProvider()
          .authenticate(
              new TokenCredentials(new JsonObject().put("token", token)),
              r -> {
                if (r.succeeded()) {
                  final Optional<User> user = Optional.ofNullable(r.result());
                  validateExpiryExists(user);
                  handler.handle(user);
                } else {
                  LOG.debug("Invalid JWT token {}", r.cause().toString());
                  handler.handle(Optional.empty());
                }
              });

    } catch (Exception e) {
      LOG.debug("exception validating JWT ", e);
      handler.handle(Optional.empty());
    }
  }

  @Override
  public boolean isPermitted(
      final Optional<User> optionalUser,
      final JsonRpcMethod jsonRpcMethod,
      final Collection<String> noAuthMethods) {
    // if the method is configured as a no auth method we skip permission check
    if (noAuthMethods.stream().anyMatch(m -> m.equals(jsonRpcMethod.getName()))) {
      return true;
    }

    if (optionalUser.isPresent()) {
      User user = optionalUser.get();
      for (String perm : jsonRpcMethod.getPermissions()) {
        try {
          // Create a permission-based authorization for the required permission
          PermissionBasedAuthorization authorization = PermissionBasedAuthorization.create(perm);
          if (authorization.match(user)) {
            LOG.trace(
                "user {} authorized : {} via permission {}", user, jsonRpcMethod.getName(), perm);
            // exit if a matching permission was found, no need to keep checking
            return true;
          }
        } catch (Exception e) {
          LOG.debug("Error checking authorization for permission {}: {}", perm, e.getMessage());
        }
      }
    }

    LOG.trace("user NOT authorized : {}", jsonRpcMethod.getName());
    return false;
  }

  private void validateExpiryExists(final Optional<User> user) {
    if (!user.map(User::attributes).map(a -> a.containsKey("exp")).orElse(false)) {
      throw new IllegalStateException("Invalid JWT doesn't have expiry");
    }
  }
}
