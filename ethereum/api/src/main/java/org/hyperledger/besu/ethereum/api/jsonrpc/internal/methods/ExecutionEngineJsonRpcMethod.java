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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import org.hyperledger.besu.consensus.merge.MergeContext;
import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineCallListener;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.ForkSupportHelper;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.JsonMappingException;
import io.vertx.core.Vertx;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ExecutionEngineJsonRpcMethod implements JsonRpcMethod {
  public enum EngineStatus {
    VALID,
    INVALID,
    SYNCING,
    ACCEPTED,
    INVALID_BLOCK_HASH;
  }

  // Fields used by migrated series (currently only engine_forkchoiceUpdatedV* — see the package
  // README's migration status table). Not-yet-migrated series keep using the TRANSITIONAL SHIM
  // constructors below instead of this record.
  @Value.Builder
  public record ConstructorArguments(
      ProtocolSchedule protocolSchedule,
      ProtocolContext protocolContext,
      Vertx vertx,
      EngineCallListener engineCallListener,
      MergeMiningCoordinator mergeCoordinator) {}

  private static final Logger LOG = LoggerFactory.getLogger(ExecutionEngineJsonRpcMethod.class);
  public static final long ENGINE_API_LOGGING_THRESHOLD = 60000L;
  // Must be <= the engine HTTP timeout so Thread A is released before the HTTP timer writes a
  // response. Uses the same default (30s) as JsonRpcConfiguration.DEFAULT_HTTP_TIMEOUT_SEC.
  private static final long ENGINE_API_RESPONSE_TIMEOUT_MS = 30_000L;
  private final Vertx syncVertx;
  protected final Optional<MergeContext> mergeContextOptional;
  protected final Supplier<MergeContext> mergeContext;
  protected final ProtocolSchedule protocolSchedule;

  // TRANSITIONAL SHIM (remove in cleanup PR): not-yet-migrated engine methods reference the
  // protocol schedule as an Optional under this name; new methods use the non-optional field above.
  protected final Optional<ProtocolSchedule> maybeProtocolSchedule;

  protected final ProtocolContext protocolContext;
  protected final EngineCallListener engineCallListener;

  private final Optional<Long> minForkTimestamp;
  private final Optional<Long> maxForkTimestamp;

  private final HardforkId minSupportedFork;
  private final HardforkId firstUnsupportedFork;

  // TRANSITIONAL SHIM (remove in cleanup PR): old constructor signature used by not-yet-migrated
  // engine methods (vertx-first, optional protocol schedule).
  protected ExecutionEngineJsonRpcMethod(
      final Vertx vertx,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final EngineCallListener engineCallListener) {
    this(protocolSchedule, protocolContext, vertx, engineCallListener, null, null);
  }

  // TRANSITIONAL SHIM (remove in cleanup PR): old constructor signature for methods that have no
  // protocol schedule.
  protected ExecutionEngineJsonRpcMethod(
      final Vertx vertx,
      final ProtocolContext protocolContext,
      final EngineCallListener engineCallListener) {
    this(null, protocolContext, vertx, engineCallListener, null, null);
  }

  protected ExecutionEngineJsonRpcMethod(
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final Vertx vertx,
      final EngineCallListener engineCallListener) {
    this(protocolSchedule, protocolContext, vertx, engineCallListener, null, null);
  }

  protected ExecutionEngineJsonRpcMethod(
      final ConstructorArguments constructorArguments,
      final HardforkId minSupportedFork,
      final HardforkId firstUnsupportedFork) {
    this(
        constructorArguments.protocolSchedule(),
        constructorArguments.protocolContext(),
        constructorArguments.vertx(),
        constructorArguments.engineCallListener(),
        minSupportedFork,
        firstUnsupportedFork);
  }

  protected ExecutionEngineJsonRpcMethod(
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final Vertx vertx,
      final EngineCallListener engineCallListener,
      final HardforkId minSupportedFork,
      final HardforkId firstUnsupportedFork) {
    this.syncVertx = vertx;
    this.protocolSchedule = protocolSchedule;
    this.maybeProtocolSchedule = Optional.ofNullable(protocolSchedule);
    this.protocolContext = protocolContext;
    this.mergeContextOptional = protocolContext.safeConsensusContext(MergeContext.class);
    this.mergeContext = mergeContextOptional::orElseThrow;
    this.engineCallListener = engineCallListener;
    this.minSupportedFork = minSupportedFork;
    this.firstUnsupportedFork = firstUnsupportedFork;
    this.minForkTimestamp =
        minSupportedFork != null
            ? protocolSchedule.milestoneFor(minSupportedFork)
            : Optional.empty();
    this.maxForkTimestamp =
        firstUnsupportedFork != null
            ? protocolSchedule.milestoneFor(firstUnsupportedFork)
            : Optional.empty();
  }

  @Override
  public final JsonRpcResponse response(final JsonRpcRequestContext request) {

    final CompletableFuture<JsonRpcResponse> cf = new CompletableFuture<>();

    syncVertx.<JsonRpcResponse>executeBlocking(
        z -> {
          logger()
              .trace(
                  "execution engine JSON-RPC request {} {}",
                  this.getName(),
                  request.getRequest().getParams());
          z.tryComplete(syncResponse(request));
        },
        true,
        resp ->
            cf.complete(
                resp.otherwise(
                        t -> {
                          if (logger().isDebugEnabled()) {
                            logger()
                                .atDebug()
                                .setMessage("failed to exec consensus method {}")
                                .addArgument(this.getName())
                                .setCause(t)
                                .log();
                          } else {
                            logger()
                                .atError()
                                .setMessage("failed to exec consensus method {}, error: {}")
                                .addArgument(this.getName())
                                .addArgument(t.getMessage())
                                .log();
                          }
                          return new JsonRpcErrorResponse(
                              request.getRequest().getId(), RpcErrorType.INVALID_REQUEST);
                        })
                    .result()));
    try {
      return cf.get(ENGINE_API_RESPONSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      logger()
          .debug(
              "Timeout waiting for engine API response for {}, releasing worker thread",
              this.getName());
      return new JsonRpcErrorResponse(request.getRequest().getId(), RpcErrorType.TIMEOUT_ERROR);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger().error("Failed to get execution engine response", e);
      return new JsonRpcErrorResponse(request.getRequest().getId(), RpcErrorType.TIMEOUT_ERROR);
    } catch (ExecutionException e) {
      logger().error("Failed to get execution engine response", e);
      return new JsonRpcErrorResponse(request.getRequest().getId(), RpcErrorType.INTERNAL_ERROR);
    }
  }

  /**
   * Returns the SLF4J logger for this engine method.
   *
   * <p>The default implementation returns a logger bound to {@link ExecutionEngineJsonRpcMethod},
   * which is correct for most subclasses whose logic lives entirely in their own class. Subclasses
   * that share implementation code across a version hierarchy (such as the {@code
   * engine_forkchoiceUpdated} V1–V4 sealed hierarchy, where all logic lives in V1 but instances may
   * be V2/V3/V4) should override this method so that log lines name the actual running version:
   *
   * <pre>{@code
   * private static final Logger LOG = LoggerFactory.getLogger(EngineForkchoiceUpdatedV3.class);
   *
   * @Override
   * protected Logger logger() { return LOG; }
   * }</pre>
   */
  protected Logger logger() {
    return LOG;
  }

  public abstract JsonRpcResponse syncResponse(final JsonRpcRequestContext request);

  public EngineCallListener getEngineCallListener() {
    return engineCallListener;
  }

  // TRANSITIONAL: not 'final' yet (restored in cleanup PR) so not-yet-migrated engine methods can
  // still override it; new methods inherit this implementation.
  protected ValidationResult<RpcErrorType> validateForkSupported(final long blockTimestamp) {
    return ForkSupportHelper.validateForkSupported(
        minSupportedFork, minForkTimestamp, firstUnsupportedFork, maxForkTimestamp, blockTimestamp);
  }

  protected static <T> Optional<T> extractCauseByType(
      final Throwable throwable, final Class<T> type) {
    Throwable cause = throwable;
    while (cause != null) {
      if (type.isAssignableFrom(cause.getClass())) {
        return Optional.of(type.cast(cause));
      }
      cause = cause.getCause();
    }
    return Optional.empty();
  }

  protected static Optional<String> extractJsonPath(final JsonMappingException fieldEx) {

    if (fieldEx.getPath().isEmpty()) {
      return Optional.empty();
    }
    return Optional.ofNullable(fieldEx.getPath().getFirst().getFieldName());
  }
}
