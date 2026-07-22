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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlockResultFactory;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.JsonMappingException;
import io.vertx.core.Vertx;
import org.immutables.value.Value;
import org.jspecify.annotations.Nullable;
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

  // Only protocolSchedule/protocolContext/vertx/engineCallListener are used by every engine
  // method (via this base class); the rest are consumed by specific subclass families, so they
  // are nullable here to keep single-family construction (production and tests) from having to
  // populate fields it will never read.
  @Value.Builder
  public record ConstructorArguments(
      ProtocolSchedule protocolSchedule,
      ProtocolContext protocolContext,
      Vertx vertx,
      EngineCallListener engineCallListener,
      @Nullable MergeMiningCoordinator mergeCoordinator,
      @Nullable BlockResultFactory blockResultFactory,
      @Nullable TransactionPool transactionPool,
      @Nullable EthPeers ethPeers,
      @Nullable MetricsSystem metricsSystem,
      int maxRequestBlocks) {}

  private static final Logger LOG = LoggerFactory.getLogger(ExecutionEngineJsonRpcMethod.class);
  public static final long ENGINE_API_LOGGING_THRESHOLD = 60000L;
  // Must be <= the engine HTTP timeout so Thread A is released before the HTTP timer writes a
  // response. Uses the same default (30s) as JsonRpcConfiguration.DEFAULT_HTTP_TIMEOUT_SEC.
  private static final long ENGINE_API_RESPONSE_TIMEOUT_MS = 30_000L;
  private final Vertx syncVertx;
  protected final Optional<MergeContext> mergeContextOptional;
  protected final Supplier<MergeContext> mergeContext;
  protected final ProtocolSchedule protocolSchedule;
  protected final ProtocolContext protocolContext;
  protected final EngineCallListener engineCallListener;

  private final Optional<Long> minForkTimestamp;
  private final Optional<Long> maxForkTimestamp;

  private final HardforkId minSupportedFork;
  private final HardforkId firstUnsupportedFork;

  protected ExecutionEngineJsonRpcMethod(
      final ConstructorArguments constructorArguments,
      final HardforkId minSupportedFork,
      final HardforkId firstUnsupportedFork) {
    this.syncVertx = constructorArguments.vertx;
    this.protocolSchedule = constructorArguments.protocolSchedule;
    this.protocolContext = constructorArguments.protocolContext;
    this.mergeContextOptional = protocolContext.safeConsensusContext(MergeContext.class);
    this.mergeContext = mergeContextOptional::orElseThrow;
    this.engineCallListener = constructorArguments.engineCallListener;
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

  protected final ValidationResult<RpcErrorType> validateForkSupported(final long blockTimestamp) {
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
