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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.calltrace;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.CallTracerResult;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.debug.TraceOptions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldView;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;

/**
 * An OperationTracer that collects call frames during transaction execution and produces a flat
 * list of trace frames matching Geth's flatCallTracer.
 *
 * <p>One instance traces exactly one transaction and is not thread-safe; {@link
 * #traceStartTransaction} resets all per-transaction state so an instance may be reused
 * sequentially, matching {@code DebugOperationTracer.reset()} semantics.
 */
public class FlatCallTracer implements OperationTracer {

  private static final String EXECUTION_REVERTED = "execution reverted";

  private static final Map<String, String> PARITY_ERRORS =
      Map.of(
          "execution reverted", "Reverted",
          "Code is too large", "Out of gas",
          "Precompile error", "Built-in failed",
          "precompile failed", "Built-in failed",
          "precompiled failed", "Built-in failed");

  record Context(
      String blockHash,
      long blockNumber,
      String txHash,
      int txPosition,
      boolean convertParityErrors,
      boolean includePrecompiles,
      Set<Address> precompiles) {}

  private final CallTracer inner = new CallTracer(false);
  private final boolean convertParityErrors;
  private final boolean includePrecompiles;
  private final Set<Address> precompileAddresses;

  /**
   * Instantiates a new Flat call tracer.
   *
   * @param traceOptions the trace options containing the tracer configuration
   * @param protocolSpec the protocol spec for the current block
   */
  public FlatCallTracer(final TraceOptions traceOptions, final ProtocolSpec protocolSpec) {
    this.convertParityErrors =
        Boolean.TRUE.equals(traceOptions.tracerConfig().getOrDefault("convertParityErrors", false));
    this.includePrecompiles =
        Boolean.TRUE.equals(traceOptions.tracerConfig().getOrDefault("includePrecompiles", false));
    this.precompileAddresses =
        protocolSpec != null && protocolSpec.getPrecompileContractRegistry() != null
            ? protocolSpec.getPrecompileContractRegistry().getPrecompileAddresses()
            : Set.of();
  }

  @Override
  public void traceStartTransaction(final WorldView worldView, final Transaction transaction) {
    inner.traceStartTransaction(worldView, transaction);
  }

  @Override
  public void tracePreExecution(final MessageFrame frame) {
    inner.tracePreExecution(frame);
  }

  @Override
  public void tracePostExecution(final MessageFrame frame, final Operation.OperationResult result) {
    inner.tracePostExecution(frame, result);
  }

  @Override
  public void tracePrecompileCall(
      final MessageFrame frame, final long gasRequirement, final Bytes output) {
    inner.tracePrecompileCall(frame, gasRequirement, output);
  }

  @Override
  public void traceContextEnter(final MessageFrame frame) {
    inner.traceContextEnter(frame);
  }

  @Override
  public void traceContextExit(final MessageFrame frame) {
    inner.traceContextExit(frame);
  }

  /**
   * Builds the flat call trace result for the given transaction trace.
   *
   * @param trace the transaction trace
   * @return the list of flat call trace frames
   */
  public List<FlatCallTracerResult> buildResult(final TransactionTrace trace) {
    final CallTracerResult root = inner.buildResult(trace.getTransaction(), trace.getResult());
    final String blockHash;
    final long blockNumber;
    final String txHash;
    final int txPosition;

    if (trace.getBlock().isPresent()) {
      final Block block = trace.getBlock().get();
      blockHash = block.getHash().getBytes().toHexString();
      blockNumber = block.getHeader().getNumber();
      txHash = trace.getTransaction().getHash().getBytes().toHexString();
      final int rawPos = block.getBody().getTransactions().indexOf(trace.getTransaction());
      txPosition = Math.max(0, rawPos);
    } else {
      blockHash = null;
      blockNumber = 0L;
      txHash = null;
      txPosition = 0;
    }

    final Context ctx =
        new Context(
            blockHash,
            blockNumber,
            txHash,
            txPosition,
            convertParityErrors,
            includePrecompiles,
            precompileAddresses);

    final List<FlatCallTracerResult> out = new ArrayList<>();
    flatten(root, List.of(), ctx, out);
    return out;
  }

  static void flatten(
      final CallTracerResult node,
      final List<Integer> traceAddress,
      final Context ctx,
      final List<FlatCallTracerResult> out) {
    final List<CallTracerResult> children;
    if (ctx.includePrecompiles()) {
      children = node.getCalls() != null ? node.getCalls() : List.of();
    } else {
      children =
          node.getCalls() != null
              ? node.getCalls().stream()
                  .filter(c -> !isPrunedPrecompileCall(c, ctx.precompiles()))
                  .toList()
              : List.of();
    }

    final String type;
    final FlatCallTracerResult.Action action;
    FlatCallTracerResult.Result result;
    String error = null;

    final String rawType = node.getType();
    if ("CREATE".equalsIgnoreCase(rawType) || "CREATE2".equalsIgnoreCase(rawType)) {
      type = "create";
      action =
          new FlatCallTracerResult.Action(
              null,
              null,
              null,
              rawType.toLowerCase(Locale.ROOT),
              node.getFrom(),
              node.getGas(),
              orEmptyHex(node.getInput()),
              null,
              null,
              null,
              orZeroHex(node.getValue()));
      result =
          new FlatCallTracerResult.Result(
              node.getTo(), orEmptyHex(node.getOutput()), node.getGasUsed(), null);
    } else if ("SELFDESTRUCT".equalsIgnoreCase(rawType)) {
      type = "suicide";
      action =
          new FlatCallTracerResult.Action(
              node.getFrom(),
              orZeroHex(node.getValue()),
              null,
              null,
              null,
              null,
              null,
              null,
              node.getTo(),
              null,
              null);
      result = null;
    } else if ("CALL".equalsIgnoreCase(rawType)
        || "CALLCODE".equalsIgnoreCase(rawType)
        || "DELEGATECALL".equalsIgnoreCase(rawType)
        || "STATICCALL".equalsIgnoreCase(rawType)) {
      type = "call";
      action =
          new FlatCallTracerResult.Action(
              null,
              null,
              rawType.toLowerCase(Locale.ROOT),
              null,
              node.getFrom(),
              node.getGas(),
              null,
              orEmptyHex(node.getInput()),
              null,
              node.getTo(),
              orZeroHex(node.getValue()));
      result =
          new FlatCallTracerResult.Result(
              null, null, node.getGasUsed(), orEmptyHex(node.getOutput()));
    } else {
      throw new IllegalStateException("unrecognized call frame type: " + rawType);
    }

    if (!"suicide".equals(type)) {
      final String rawError = node.getError();
      if (rawError != null) {
        error = ctx.convertParityErrors() ? toParityError(rawError) : rawError;
        if (!EXECUTION_REVERTED.equals(rawError)) {
          result = null;
        }
      }
    }

    out.add(
        new FlatCallTracerResult(
            action,
            ctx.blockHash(),
            ctx.blockNumber(),
            error,
            result,
            children.size(),
            traceAddress,
            ctx.txHash(),
            ctx.txPosition(),
            type));

    for (int i = 0; i < children.size(); i++) {
      final List<Integer> childAddress = new ArrayList<>(traceAddress.size() + 1);
      childAddress.addAll(traceAddress);
      childAddress.add(i);
      flatten(children.get(i), childAddress, ctx, out);
    }
  }

  private static boolean isPrunedPrecompileCall(
      final CallTracerResult call, final Set<Address> precompiles) {
    if (("CALL".equalsIgnoreCase(call.getType()) || "STATICCALL".equalsIgnoreCase(call.getType()))
        && call.getTo() != null) {
      try {
        return precompiles.contains(Address.fromHexString(call.getTo()));
      } catch (final IllegalArgumentException e) {
        return false;
      }
    }
    return false;
  }

  static String toParityError(final String besuError) {
    if (besuError == null) {
      return null;
    }
    final String mapped = PARITY_ERRORS.get(besuError);
    if (mapped != null) {
      return mapped;
    }
    final String lower = besuError.toLowerCase(Locale.ROOT);
    if (lower.startsWith("invalid input length")) {
      return "Built-in failed";
    }
    if (lower.startsWith("invalid opcode:")) {
      return "Bad instruction";
    }
    if (lower.startsWith("out of gas:")) {
      return "Out of gas";
    }
    if (lower.startsWith("stack underflow")) {
      return "Stack underflow";
    }
    return besuError;
  }

  private static String orEmptyHex(final String s) {
    return s == null ? "0x" : s;
  }

  private static String orZeroHex(final String s) {
    return s == null ? "0x0" : s;
  }
}
