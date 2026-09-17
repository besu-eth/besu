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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.flat;

import static org.hyperledger.besu.evm.internal.Words.toAddress;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.Quantity;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.Trace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.TracingUtils;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.operation.ReturnOperation;
import org.hyperledger.besu.evm.operation.RevertOperation;
import org.hyperledger.besu.evm.tracing.TraceFrame;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Streams;
import com.google.common.util.concurrent.Atomics;
import org.apache.tuweni.bytes.Bytes;

public class FlatTraceGenerator {

  private static final String ZERO_ADDRESS_STRING = Address.ZERO.getBytes().toHexString();

  private static final int EIP_150_DIVISOR = 64;

  /**
   * Generates a stream of {@link Trace} from the passed {@link TransactionTrace} data.
   *
   * @param protocolSchedule the current {@link ProtocolSchedule} to use
   * @param transactionTrace the {@link TransactionTrace} to use
   * @param block the {@link Block} to use
   * @param traceCounter the current trace counter value
   * @param consumer to use to add additional contextual information to the trace
   * @return a stream of generated traces {@link Trace}
   */
  public static Stream<Trace> generateFromTransactionTrace(
      final ProtocolSchedule protocolSchedule,
      final TransactionTrace transactionTrace,
      final Block block,
      final AtomicInteger traceCounter,
      final Consumer<FlatTrace.Builder> consumer) {

    final FlatTrace.Builder firstFlatTraceBuilder = FlatTrace.freshBuilder(transactionTrace);

    final Transaction tx = transactionTrace.getTransaction();

    final Optional<String> smartContractCode =
        tx.getInit().map(__ -> transactionTrace.getResult().getOutput().toString());
    final Optional<String> smartContractAddress =
        smartContractCode.map(
            __ -> Address.contractAddress(tx.getSender(), tx.getNonce()).getBytes().toHexString());
    final Optional<Bytes> revertReason = transactionTrace.getResult().getRevertReason();

    // set code field in result node
    smartContractCode.ifPresent(firstFlatTraceBuilder.getResultBuilder()::code);
    revertReason.ifPresent(r -> firstFlatTraceBuilder.revertReason(r.toHexString()));

    // set init field if transaction is a smart contract deployment
    tx.getInit().map(Bytes::toHexString).ifPresent(firstFlatTraceBuilder.getActionBuilder()::init);

    // set to, input and callType fields if not a smart contract
    if (tx.getTo().isPresent()) {
      final Bytes payload = tx.getPayload();
      firstFlatTraceBuilder
          .getActionBuilder()
          .to(tx.getTo().map(a -> a.getBytes().toHexString()).orElse(null))
          .callType("call")
          .input(payload == null ? "0x" : payload.toHexString());

      if (!transactionTrace.getTraceFrames().isEmpty()
          && hasRevertInSubCall(transactionTrace, transactionTrace.getTraceFrames().get(0))) {
        firstFlatTraceBuilder.error(Optional.of("Reverted"));
      }

    } else {
      firstFlatTraceBuilder
          .type("create")
          .getResultBuilder()
          .address(smartContractAddress.orElse(null));
    }

    if (!transactionTrace.getTraceFrames().isEmpty()) {
      final OptionalLong precompiledGasCost =
          transactionTrace.getTraceFrames().get(0).getPrecompiledGasCost();
      if (precompiledGasCost.isPresent()) {
        firstFlatTraceBuilder
            .getResultBuilder()
            .gasUsed("0x" + Long.toHexString(precompiledGasCost.getAsLong()));
      }
    }

    final List<FlatTrace.Builder> flatTraces = new ArrayList<>();

    // stack of previous contexts
    final Deque<FlatTrace.Context> tracesContexts = new ArrayDeque<>();

    // add the first transactionTrace context to the queue of transactionTrace contexts
    FlatTrace.Context currentContext = new FlatTrace.Context(firstFlatTraceBuilder);
    tracesContexts.addLast(currentContext);
    flatTraces.add(currentContext.getBuilder());
    // declare the first transactionTrace context as the previous transactionTrace context
    long cumulativeGasCost = 0;

    final Iterator<TraceFrame> iter = transactionTrace.getTraceFrames().iterator();
    Optional<TraceFrame> nextTraceFrame =
        iter.hasNext() ? Optional.of(iter.next()) : Optional.empty();
    while (nextTraceFrame.isPresent()) {
      final TraceFrame traceFrame = nextTraceFrame.get();
      nextTraceFrame = iter.hasNext() ? Optional.of(iter.next()) : Optional.empty();
      cumulativeGasCost +=
          traceFrame.getGasCost().orElse(0L) + traceFrame.getPrecompiledGasCost().orElse(0L);

      final String opcodeString = traceFrame.getOpcode();
      if ("CALL".equals(opcodeString)
          || "CALLCODE".equals(opcodeString)
          || "DELEGATECALL".equals(opcodeString)
          || "STATICCALL".equals(opcodeString)) {

        currentContext =
            handleCall(
                transactionTrace,
                traceFrame,
                nextTraceFrame,
                flatTraces,
                cumulativeGasCost,
                tracesContexts,
                opcodeString.toLowerCase(Locale.US));

      } else if ("CALLDATALOAD".equals(opcodeString)) {
        currentContext = handleCallDataLoad(currentContext, traceFrame);
      } else if ("RETURN".equals(opcodeString) || "STOP".equals(opcodeString)) {
        currentContext =
            handleReturn(
                protocolSchedule,
                transactionTrace,
                block,
                traceFrame,
                tracesContexts,
                currentContext);
      } else if ("SELFDESTRUCT".equals(opcodeString)) {
        if (traceFrame.getExceptionalHaltReason().isPresent()) {
          currentContext =
              handleCall(
                  transactionTrace,
                  traceFrame,
                  nextTraceFrame,
                  flatTraces,
                  cumulativeGasCost,
                  tracesContexts,
                  opcodeString.toLowerCase(Locale.US));
        } else {
          currentContext =
              handleSelfDestruct(traceFrame, tracesContexts, currentContext, flatTraces);
        }
      } else if (("CREATE".equals(opcodeString) || "CREATE2".equals(opcodeString))
          && (traceFrame.getExceptionalHaltReason().isEmpty() || traceFrame.getDepth() == 0)) {
        currentContext =
            handleCreateOperation(
                traceFrame,
                nextTraceFrame,
                flatTraces,
                cumulativeGasCost,
                tracesContexts,
                smartContractAddress);
      } else if ("REVERT".equals(opcodeString)) {
        currentContext = handleRevert(tracesContexts, currentContext);
      }

      if (traceFrame.getExceptionalHaltReason().isPresent()) {
        currentContext = handleHalt(flatTraces, tracesContexts, currentContext, traceFrame);
      }

      if (currentContext == null) {
        break;
      }
    }

    return flatTraces.stream().peek(consumer).map(FlatTrace.Builder::build);
  }

  /**
   * Generates a stream of {@link Trace} from the passed {@link TransactionTrace} and {@link Block}
   * data.
   *
   * @param protocolSchedule the current {@link ProtocolSchedule} to use
   * @param transactionTrace the {@link TransactionTrace} to use
   * @param block the {@link Block} to use
   * @param traceCounter the current traceCounter
   * @return a stream of generated traces {@link Trace}
   */
  public static Stream<Trace> generateFromTransactionTrace(
      final ProtocolSchedule protocolSchedule,
      final TransactionTrace transactionTrace,
      final Block block,
      final AtomicInteger traceCounter) {
    return generateFromTransactionTrace(
        protocolSchedule, transactionTrace, block, traceCounter, true);
  }

  public static Stream<Trace> generateFromTransactionTrace(
      final ProtocolSchedule protocolSchedule,
      final TransactionTrace transactionTrace,
      final Block block,
      final AtomicInteger traceCounter,
      final boolean includeCreationMethod) {
    return generateFromTransactionTrace(
        protocolSchedule,
        transactionTrace,
        block,
        traceCounter,
        includeCreationMethod
            ? builder -> addContractCreationMethodToTrace(transactionTrace, builder)
            : builder -> {});
  }

  /**
   * Generates a stream of {@link Trace} from the passed {@link TransactionTrace} and {@link Block}
   * data with additional Transaction Information added to FlatTrace
   *
   * @param protocolSchedule the current {@link ProtocolSchedule} to use
   * @param transactionTrace the {@link TransactionTrace} to use
   * @param block the {@link Block} to use
   * @return a stream of generated traces {@link Trace}
   */
  public static Stream<Trace> generateFromTransactionTraceAndBlock(
      final ProtocolSchedule protocolSchedule,
      final TransactionTrace transactionTrace,
      final Block block) {
    return generateFromTransactionTrace(
        protocolSchedule,
        transactionTrace,
        block,
        new AtomicInteger(),
        builder ->
            addAdditionalTransactionInformationToFlatTrace(builder, transactionTrace, block));
  }

  private static FlatTrace.Context handleCall(
      final TransactionTrace transactionTrace,
      final TraceFrame traceFrame,
      final Optional<TraceFrame> nextTraceFrame,
      final List<FlatTrace.Builder> flatTraces,
      final long cumulativeGasCost,
      final Deque<FlatTrace.Context> tracesContexts,
      final String opcodeString) {
    final Bytes[] stack = traceFrame.getStack().orElseThrow();
    final FlatTrace.Context lastContext = tracesContexts.peekLast();

    final String callingAddress = calculateCallingAddress(lastContext);

    if (traceFrame.getDepth() >= nextTraceFrame.map(TraceFrame::getDepth).orElse(0)) {
      // don't log calls to calls that don't execute, such as insufficient value and precompiles
      return tracesContexts.peekLast();
    }

    final FlatTrace.Builder subTraceBuilder =
        FlatTrace.builder()
            .traceAddress(calculateTraceAddress(tracesContexts))
            .resultBuilder(Result.builder());
    final Action.Builder subTraceActionBuilder =
        Action.builder()
            .from(callingAddress)
            .input(
                nextTraceFrame.map(TraceFrame::getInputData).map(Bytes::toHexString).orElse(null))
            .gas(
                "0x" + Long.toHexString(nextTraceFrame.map(TraceFrame::getGasRemaining).orElse(0L)))
            .callType(opcodeString.toLowerCase(Locale.US))
            .value(Quantity.create(traceFrame.getValue()));

    if (stack.length > 1) {
      subTraceActionBuilder.to(toAddress(stack[stack.length - 2]).toString());
    }

    nextTraceFrame.ifPresent(
        nextFrame -> {
          if (hasRevertInSubCall(transactionTrace, nextFrame)) {
            subTraceBuilder.error(Optional.of("Reverted"));
          }
        });

    final FlatTrace.Context currentContext =
        new FlatTrace.Context(subTraceBuilder.actionBuilder(subTraceActionBuilder));
    currentContext.decGasUsed(cumulativeGasCost);

    tracesContexts.addLast(currentContext);
    flatTraces.add(currentContext.getBuilder());
    return currentContext;
  }

  private static FlatTrace.Context handleReturn(
      final ProtocolSchedule protocolSchedule,
      final TransactionTrace transactionTrace,
      final Block block,
      final TraceFrame traceFrame,
      final Deque<FlatTrace.Context> tracesContexts,
      final FlatTrace.Context currentContext) {

    final FlatTrace.Builder traceFrameBuilder = currentContext.getBuilder();
    final Result.Builder resultBuilder = traceFrameBuilder.getResultBuilder();
    final Action.Builder actionBuilder = traceFrameBuilder.getActionBuilder();
    actionBuilder.value(Quantity.create(traceFrame.getValue()));

    currentContext.setGasUsed(
        computeGasUsed(tracesContexts, currentContext, transactionTrace, traceFrame));

    if ("STOP".equals(traceFrame.getOpcode()) && resultBuilder.isGasUsedEmpty()) {
      final long callStipend =
          protocolSchedule
              .getByBlockHeader(block.getHeader())
              .getGasCalculator()
              .getAdditionalCallStipend();
      tracesContexts.stream()
          .filter(
              context ->
                  !tracesContexts.getFirst().equals(context)
                      && !tracesContexts.getLast().equals(context))
          .forEach(context -> context.decGasUsed(callStipend));
    }

    final Bytes outputData = traceFrame.getOutputData();
    if (resultBuilder.getCode() == null) {
      resultBuilder.output(outputData.toHexString());
    }

    // set value for contract creation TXes, CREATE, and CREATE2
    if (actionBuilder.getCallType() == null && traceFrame.getMaybeCode().isPresent()) {
      actionBuilder.init(traceFrame.getMaybeCode().get().getBytes().toHexString());
      resultBuilder.code(outputData.toHexString());
      if (currentContext.isCreateOp()) {
        // this is from a CREATE/CREATE2, so add code deposit cost.
        currentContext.incGasUsed(outputData.size() * 200L);
      }
    }

    tracesContexts.removeLast();
    final FlatTrace.Context nextContext = tracesContexts.peekLast();
    if (nextContext != null) {
      nextContext.getBuilder().incSubTraces();
    }
    return nextContext;
  }

  private static FlatTrace.Context handleSelfDestruct(
      final TraceFrame traceFrame,
      final Deque<FlatTrace.Context> tracesContexts,
      final FlatTrace.Context currentContext,
      final List<FlatTrace.Builder> flatTraces) {

    final Action.Builder actionBuilder = currentContext.getBuilder().getActionBuilder();
    final long gasUsed =
        Long.decode(actionBuilder.getGas())
            - traceFrame.getGasRemaining()
            + (traceFrame.getGasCost().orElse(0L));

    currentContext.setGasUsed(gasUsed);

    final Bytes[] stack = traceFrame.getStack().orElseThrow();
    final Address refundAddress = toAddress(stack[stack.length - 1]);
    final FlatTrace.Builder subTraceBuilder =
        FlatTrace.builder()
            .type("suicide")
            .traceAddress(calculateSelfDescructAddress(tracesContexts));

    final AtomicReference<Wei> weiBalance = Atomics.newReference(Wei.ZERO);
    traceFrame
        .getMaybeRefunds()
        .ifPresent(refunds -> weiBalance.set(refunds.getOrDefault(refundAddress, Wei.ZERO)));

    final Action.Builder callingAction = tracesContexts.peekLast().getBuilder().getActionBuilder();
    final String actionAddress =
        getActionAddress(callingAction, traceFrame.getRecipient().getBytes().toHexString());
    final Action.Builder subTraceActionBuilder =
        Action.builder()
            .address(actionAddress)
            .refundAddress(refundAddress.toString())
            .balance(TracingUtils.weiAsHex(weiBalance.get()));

    flatTraces.add(
        new FlatTrace.Context(subTraceBuilder.actionBuilder(subTraceActionBuilder)).getBuilder());
    final FlatTrace.Context lastContext = tracesContexts.removeLast();
    lastContext.getBuilder().incSubTraces();
    final FlatTrace.Context nextContext = tracesContexts.peekLast();
    if (nextContext != null) {
      nextContext.getBuilder().incSubTraces();
    }
    return nextContext;
  }

  private static String getActionAddress(
      final Action.Builder callingAction, final String recipient) {
    if (callingAction.getCallType() != null) {
      return callingAction.getCallType().equals("call")
          ? callingAction.getTo()
          : callingAction.getFrom();
    }
    return firstNonNull("", recipient, callingAction.getFrom(), callingAction.getTo());
  }

  private static String firstNonNull(final String defaultValue, final String... values) {
    for (final String value : values) {
      if (value != null) {
        return value;
      }
    }
    return defaultValue;
  }

  private static FlatTrace.Context handleCreateOperation(
      final TraceFrame traceFrame,
      final Optional<TraceFrame> nextTraceFrame,
      final List<FlatTrace.Builder> flatTraces,
      final long cumulativeGasCost,
      final Deque<FlatTrace.Context> tracesContexts,
      final Optional<String> smartContractAddress) {
    final FlatTrace.Context lastContext = tracesContexts.peekLast();

    final String callingAddress = calculateCallingAddress(lastContext);

    final FlatTrace.Builder subTraceBuilder =
        FlatTrace.builder()
            .type("create")
            .traceAddress(calculateTraceAddress(tracesContexts))
            .resultBuilder(Result.builder());

    final Action.Builder subTraceActionBuilder =
        Action.builder()
            .from(smartContractAddress.orElse(callingAddress))
            .gas("0x" + Long.toHexString(computeGas(traceFrame, nextTraceFrame)))
            .value(Quantity.create(nextTraceFrame.map(TraceFrame::getValue).orElse(Wei.ZERO)));

    traceFrame
        .getMaybeCode()
        .map(Code::getBytes)
        .map(Bytes::toHexString)
        .ifPresent(subTraceActionBuilder::init);

    final FlatTrace.Context currentContext =
        new FlatTrace.Context(subTraceBuilder.actionBuilder(subTraceActionBuilder));

    currentContext
        .getBuilder()
        .getResultBuilder()
        .address(
            nextTraceFrame
                .map(TraceFrame::getRecipient)
                .orElse(Address.ZERO)
                .getBytes()
                .toHexString());
    currentContext.setCreateOp(true);
    currentContext.decGasUsed(cumulativeGasCost);
    tracesContexts.addLast(currentContext);
    flatTraces.add(currentContext.getBuilder());
    return currentContext;
  }

  private static FlatTrace.Context handleHalt(
      final List<FlatTrace.Builder> flatTraces,
      final Deque<FlatTrace.Context> tracesContexts,
      final FlatTrace.Context currentContext,
      final TraceFrame traceFrame) {
    final FlatTrace.Builder traceFrameBuilder;
    if (currentContext == null) {
      traceFrameBuilder = flatTraces.get(flatTraces.size() - 1);
    } else {
      traceFrameBuilder = currentContext.getBuilder();
    }
    traceFrameBuilder.error(
        traceFrame
            .getExceptionalHaltReason()
            .map(
                exceptionalHaltReason -> {
                  if (exceptionalHaltReason
                      .name()
                      .equals(ExceptionalHaltReason.INVALID_OPERATION.name())) {
                    return ExceptionalHaltReason.INVALID_OPERATION.getDescription();
                  } else {
                    return exceptionalHaltReason.getDescription();
                  }
                }));
    if (currentContext != null) {
      final Action.Builder actionBuilder = traceFrameBuilder.getActionBuilder();
      actionBuilder.value(Quantity.create(traceFrame.getValue()));
      tracesContexts.removeLast();
      final FlatTrace.Context nextContext = tracesContexts.peekLast();
      if (nextContext != null) {
        nextContext.getBuilder().incSubTraces();
      }
      return nextContext;
    }
    return currentContext;
  }

  private static FlatTrace.Context handleRevert(
      final Deque<FlatTrace.Context> tracesContexts, final FlatTrace.Context currentContext) {
    currentContext.getBuilder().error(Optional.of("Reverted"));
    tracesContexts.removeLast();
    final FlatTrace.Context nextContext = tracesContexts.peekLast();
    if (nextContext != null) {
      nextContext.getBuilder().incSubTraces();
    }
    return nextContext;
  }

  private static FlatTrace.Context handleCallDataLoad(
      final FlatTrace.Context currentContext, final TraceFrame traceFrame) {
    if (!traceFrame.getValue().isZero()) {
      currentContext
          .getBuilder()
          .getActionBuilder()
          .value(traceFrame.getValue().toShortHexString());
    } else {
      currentContext.getBuilder().getActionBuilder().value("0x0");
    }
    return currentContext;
  }

  private static boolean hasRevertInSubCall(
      final TransactionTrace transactionTrace, final TraceFrame callFrame) {
    for (int i = 0; i < transactionTrace.getTraceFrames().size(); i++) {
      if (i + 1 < transactionTrace.getTraceFrames().size()) {
        final TraceFrame next = transactionTrace.getTraceFrames().get(i + 1);
        if (next.getDepth() == callFrame.getDepth()) {
          if (next.getOpcodeNumber() == RevertOperation.OPCODE) {
            return true;
          } else if (next.getOpcodeNumber() == ReturnOperation.OPCODE) {
            return false;
          }
        }
      }
    }
    return false;
  }

  private static String calculateCallingAddress(final FlatTrace.Context lastContext) {
    final FlatTrace.Builder lastContextBuilder = lastContext.getBuilder();
    final Action.Builder lastActionBuilder = lastContextBuilder.getActionBuilder();
    if (lastActionBuilder.getCallType() == null) {
      if ("create".equals(lastContextBuilder.getType())) {
        return lastContextBuilder.getResultBuilder().getAddress();
      } else {
        return ZERO_ADDRESS_STRING;
      }
    }
    return switch (lastActionBuilder.getCallType()) {
      case "call", "staticcall" -> lastActionBuilder.getTo();
      case "delegatecall", "callcode" -> lastActionBuilder.getFrom();
      case "create", "create2" -> lastContextBuilder.getResultBuilder().getAddress();
      default -> ZERO_ADDRESS_STRING;
    };
  }

  private static long computeGasUsed(
      final Deque<FlatTrace.Context> tracesContexts,
      final FlatTrace.Context currentContext,
      final TransactionTrace transactionTrace,
      final TraceFrame traceFrame) {

    final long gasRemainingBeforeProcessed;
    final long gasRemainingAfterProcessed;
    long gasRefund = 0;
    if (tracesContexts.size() == 1) {
      gasRemainingBeforeProcessed = transactionTrace.getTraceFrames().get(0).getGasRemaining();
      gasRemainingAfterProcessed = transactionTrace.getResult().getGasRemaining();
      if (gasRemainingAfterProcessed > traceFrame.getGasRemaining()) {
        gasRefund = gasRemainingAfterProcessed - traceFrame.getGasRemaining();
      } else {
        gasRefund = traceFrame.getGasRefund();
      }
    } else {
      final Action.Builder actionBuilder = currentContext.getBuilder().getActionBuilder();
      gasRemainingBeforeProcessed = Long.decode(actionBuilder.getGas());
      gasRemainingAfterProcessed = traceFrame.getGasRemaining();
    }
    return gasRemainingBeforeProcessed - gasRemainingAfterProcessed + gasRefund;
  }

  private static long computeGas(
      final TraceFrame traceFrame, final Optional<TraceFrame> nextTraceFrame) {
    if (traceFrame.getGasCost().isPresent()) {
      final long gasNeeded = traceFrame.getGasCost().getAsLong();
      final long currentGas = traceFrame.getGasRemaining();
      if (currentGas >= gasNeeded) {
        final long gasRemaining = currentGas - gasNeeded;
        return gasRemaining - Math.floorDiv(gasRemaining, EIP_150_DIVISOR);
      }
    }
    return nextTraceFrame.map(TraceFrame::getGasRemaining).orElse(0L);
  }

  private static List<Integer> calculateTraceAddress(final Deque<FlatTrace.Context> contexts) {
    return contexts.stream()
        .map(context -> context.getBuilder().getSubtraces())
        .collect(Collectors.toList());
  }

  private static List<Integer> calculateSelfDescructAddress(
      final Deque<FlatTrace.Context> contexts) {
    return Streams.concat(
            contexts.stream()
                .map(context -> context.getBuilder().getSubtraces())) // , Stream.of(0))
        .collect(Collectors.toList());
  }

  protected static void addAdditionalTransactionInformationToFlatTrace(
      final FlatTrace.Builder builder, final TransactionTrace transactionTrace, final Block block) {
    // add block information (hash and number)
    builder
        .blockHash(block.getHash().getBytes().toHexString())
        .blockNumber(block.getHeader().getNumber());
    // add transaction information (position and hash)
    builder
        .transactionPosition(
            block.getBody().getTransactions().indexOf(transactionTrace.getTransaction()))
        .transactionHash(transactionTrace.getTransaction().getHash().getBytes().toHexString());

    addContractCreationMethodToTrace(transactionTrace, builder);
  }

  private static void addContractCreationMethodToTrace(
      final TransactionTrace transactionTrace, final FlatTrace.Builder builder) {
    // add creationMethod for create action
    Optional.ofNullable(builder.getType())
        .filter(type -> type.equals("create"))
        .ifPresent(
            __ ->
                builder
                    .getActionBuilder()
                    .creationMethod(
                        transactionTrace.getTraceFrames().stream()
                            .filter(frame -> "CREATE2".equals(frame.getOpcode()))
                            .findFirst()
                            .map(TraceFrame::getOpcode)
                            .orElse("CREATE")
                            .toLowerCase(Locale.US)));
  }
}
