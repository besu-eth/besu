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
package org.hyperledger.besu.evm.v2;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.EvmSpecVersion;
import org.hyperledger.besu.evm.fluent.EVMExecutor;
import org.hyperledger.besu.evm.fluent.EvmSpec;
import org.hyperledger.besu.evm.fluent.SimpleWorld;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Runs the untraced v2 loop in a JVM of its own until C2 compiles it, and checks C2's report of
 * what it inlined into the loop. C2 inlines into one budget of 8000 bytes of bytecode for the loop
 * and everything inlined into it, in parse order, and once that is used up even the loop's own
 * steps become calls. How much each operation takes depends on what the JVM has run, so the budget
 * has to hold with every operation of the loop's switch inlined as far as C2 goes.
 */
class EvmV2LoopCompilationTest {

  private static final String LOOP = "org.hyperledger.besu.evm.EVM::runToHaltV2Untraced";

  /** What the loop calls on every step besides the operation. */
  private static final Set<String> STEP =
      Set.of(
          "EVM::completeOperation",
          "MessageFrame::getState",
          "MessageFrame::getPC",
          "MessageFrame::setPC",
          "MessageFrame::decrementRemainingGas",
          "Operation$OperationResult::getHaltReason",
          "Operation$OperationResult::getGasCost",
          "Operation$OperationResult::getPcIncrement");

  /**
   * The most the loop and everything inlined into it may take of C2's 8000, leaving room for what a
   * newer JDK or library inlines more. Past the 8000 C2 makes calls of the last operations of the
   * switch, but not of the loop's own steps, which it inlines first.
   */
  private static final int BUDGET = 7500;

  /** One call site in C2's report: the bci, the callee, its size and what C2 decided. */
  private static final Pattern CALL =
      Pattern.compile("@ (\\d+)\\s+(\\S+)::(\\S+) \\((\\d+) bytes\\)\\s*(.*)$");

  @Test
  void everyCompilationOfTheLoopInlinesItsStepsWithinTheBudget(@TempDir final Path dir)
      throws Exception {
    final List<List<String>> compilations = compilations(run(dir));
    assertThat(compilations).as("C2's reports on %s", LOOP).isNotEmpty();
    for (final List<String> compilation : compilations) {
      final String report = String.join("\n", compilation);
      int inlined = EvmV2LoopMethodSizeTest.methodSizes().get("runToHaltV2Untraced");
      final List<String> stepCalls = new ArrayList<>();
      final List<String> clipped = new ArrayList<>();
      int top = Integer.MAX_VALUE;
      for (final String line : compilation) {
        final Matcher call = CALL.matcher(line);
        call.find();
        top = Math.min(top, call.start());
        final String decision = call.group(5);
        if (isInlined(decision)) {
          inlined += Integer.parseInt(call.group(4));
        } else if (decision.contains("DesiredMethodLimit")
            || decision.contains("NodeCountInliningCutoff")) {
          clipped.add(line.strip());
        }
        final String callee =
            call.group(2).substring(call.group(2).lastIndexOf('.') + 1) + "::" + call.group(3);
        if (STEP.contains(callee) && !isInlined(decision)) {
          stepCalls.add(line.strip());
        }
        if (call.start() == top && callee.equals("EVM::executeOperationV2")) {
          assertThat(isInlined(decision))
              .as("executeOperationV2 is inlined into the loop:%n%s", report)
              .isFalse();
        }
      }
      assertThat(clipped).as("calls C2 refused for the loop's budget:%n%s", report).isEmpty();
      assertThat(stepCalls).as("steps of the loop C2 did not inline:%n%s", report).isEmpty();
      assertThat(inlined)
          .as("bytes of bytecode in the loop and inlined into it:%n%s", report)
          .isLessThanOrEqualTo(BUDGET);
    }
  }

  private static List<String> run(final Path dir) throws Exception {
    final Path directives = dir.resolve("directives.json");
    Files.writeString(directives, "[{match: \"" + LOOP + "\", c2: {PrintInlining: true}}]", UTF_8);
    final Path output = dir.resolve("output.txt");
    final Process process =
        new ProcessBuilder(
                ProcessHandle.current()
                    .info()
                    .command()
                    .orElse(Path.of(System.getProperty("java.home"), "bin", "java").toString()),
                "-cp",
                System.getProperty("java.class.path"),
                "-XX:+UnlockDiagnosticVMOptions",
                "-XX:CompilerDirectivesFile=" + directives,
                // waits for each compilation, so that the loop's is done before the JVM exits
                "-Xbatch",
                // compiles with C2 only once the profile of what the loop calls is complete, as it
                // is on a node, so that C2 inlines as much into the loop as it would there
                "-XX:Tier4InvocationThreshold=500000",
                "-XX:Tier4MinInvocationThreshold=600000",
                "-XX:Tier4CompileThreshold=1500000",
                "-XX:Tier4BackEdgeThreshold=40000000",
                // inlines what C2 has already compiled on its own too, as a node may not have yet
                "-XX:InlineSmallCode=1000000",
                // rates every call that ran as hot, as a node's mature profile does: a short run
                // rates calls inside the operations as rare and stops at the operation
                "-XX:InlineFrequencyRatio=0",
                "-XX:MinInlineFrequencyRatio=0",
                Workload.class.getName())
            .redirectErrorStream(true)
            .redirectOutput(output.toFile())
            .start();
    assertThat(process.waitFor(5, TimeUnit.MINUTES)).isTrue();
    final List<String> lines = Files.readAllLines(output, UTF_8);
    assertThat(process.exitValue()).as(String.join("\n", lines)).isZero();
    return lines;
  }

  /**
   * Splits the report into compilations: each lists the loop's own calls in bytecode order, with
   * the calls of an inlined callee indented below it, so a top-level bci that goes back starts the
   * next compilation.
   */
  private static List<List<String>> compilations(final List<String> lines) {
    final List<List<String>> compilations = new ArrayList<>();
    int top = Integer.MAX_VALUE;
    int lastBci = Integer.MAX_VALUE;
    for (final String line : lines) {
      final Matcher call = CALL.matcher(line);
      if (!call.find()) {
        continue;
      }
      final int bci = Integer.parseInt(call.group(1));
      top = Math.min(top, call.start());
      if (call.start() == top) {
        if (bci <= lastBci) {
          compilations.add(new ArrayList<>());
        }
        lastBci = bci;
      }
      compilations.getLast().add(line);
    }
    return compilations;
  }

  private static boolean isInlined(final String decision) {
    return !decision.contains("failed")
        && (decision.contains("inline") || decision.startsWith("accessor"));
  }

  /**
   * Runs one program that reaches every operation of the loop's switch, some of the general path,
   * and the jump variants, until C2 has compiled the loop.
   */
  public static final class Workload {

    private Workload() {}

    /**
     * Runs the program.
     *
     * @param args unused
     */
    public static void main(final String[] args) {
      final Address sender = Address.fromHexString("0x2000");
      final Address receiver = Address.fromHexString("0x1000");
      final SimpleWorld world = new SimpleWorld();
      world.createAccount(sender, 0, Wei.of(1_000_000_000L));
      world.createAccount(receiver, 0, Wei.ZERO);
      final EvmConfiguration v2 =
          new EvmConfiguration(
              EvmConfiguration.DEFAULT.jumpDestCacheWeightKB(),
              EvmConfiguration.DEFAULT.worldUpdaterMode(),
              EvmConfiguration.DEFAULT.enableOptimizedOpcodes(),
              true);
      final EVMExecutor executor =
          new EVMExecutor(EvmSpec.evmSpec(EvmSpecVersion.OSAKA, BigInteger.ONE, v2))
              .worldUpdater(world)
              .code(new Code(program()))
              .callData(Bytes.wrap(callData()))
              .sender(sender)
              .receiver(receiver)
              .gas(10_000_000L)
              .tracer(OperationTracer.NO_TRACING);
      for (int i = 0; i < 20_000; i++) {
        executor.execute();
      }
    }

    /** Call data of the size of a call with three arguments. */
    private static byte[] callData() {
      final byte[] callData = new byte[100];
      for (int i = 0; i < callData.length; i++) {
        callData[i] = (byte) (i + 1);
      }
      return callData;
    }

    /**
     * A loop over one use of every operation of the loop's switch, some of the general path, and
     * the jump variants.
     */
    static Bytes program() {
      final Program a = new Program();
      a.hex("6101005b"); // PUSH2 0x100, the counter, then the head of the loop
      final int loop = 3;
      a.hex("600760050150 600760050350 600360090250 600360090450 600360090650");
      a.hex("600160021650 600160021750 600160021850");
      a.hex("600160021450600160021050600160021150600160021250600160021350");
      a.hex("6001600a1b506001600a1c506001600a1d50");
      a.hex("6005600760090850 6005600760090950");
      a.hex("60011550 601919 50 60ff60000b50 5f50 5a50 3650 60003550");
      a.hex("602a600052 60005150 630102030450");
      // MSTORE and MLOAD one word further each time around, so that memory expands every time
      a.hex("602a 81 610100 03 6005 1b 52 80 610100 03 6005 1b 51 50");
      // CALLDATALOAD inside, across the end of, past the end of and far past the call data
      a.hex("60203550 60503550 60643550 63ffffffff3550");
      a.hex("7f" + "11".repeat(32) + "50");
      a.hex("73" + "22".repeat(20) + "50"); // PUSH20, an address
      // operands of every width and zero, as on a node, so that C2 sees every path of the
      // operations as taken
      final String wide = "7f" + "ff".repeat(32);
      final String high = "7f01" + "00".repeat(31);
      for (final String operation :
          new String[] {
            "01", "02", "03", "04", "0b", "10", "11", "12", "14", "16", "17", "1b", "1c"
          }) {
        a.hex(wide + high + operation + "50");
        a.hex("5f" + wide + operation + "50");
        a.hex(wide + "5f" + operation + "50");
        a.hex("6007" + high + operation + "50");
        a.hex(high + "6007" + operation + "50");
      }
      for (final String operation : new String[] {"08", "09"}) {
        a.hex(wide + wide + high + operation + "50");
        a.hex("6007" + wide + wide + operation + "50");
        a.hex(high + "5f" + wide + operation + "50");
      }
      a.hex(wide + "1550 5f1550 " + high + "1550 " + wide + "1950 " + high + "1950");
      a.hex("808080 90 91 81 50505050"); // DUP1 x3, SWAP1, SWAP2, DUP2, POP x4
      a.hex("602060002050"); // KECCAK256, through the general path
      a.hex("60005450"); // SLOAD, through the general path
      // PUSH2 JUMP to the next instruction, which is a JUMPDEST
      a.push2(a.size() + 4).hex("565b");
      // PUSH2 JUMPI, not taken
      a.hex("6000").push2(loop).hex("57");
      // JUMP and JUMPI taken, with the destination through the stack
      a.push2(a.size() + 6).hex("8050565b");
      a.hex("6001").push2(a.size() + 6).hex("8050575b");
      // JUMPI not taken, with the destination through the stack
      a.hex("6000").push2(loop).hex("805057");
      // counter - 1, and around again while it is not zero
      a.hex("60019003 80").push2(loop).hex("57 50 00");
      return Bytes.fromHexString(a.code.toString());
    }

    private static final class Program {
      private final StringBuilder code = new StringBuilder();

      Program hex(final String hex) {
        code.append(hex.replace(" ", ""));
        return this;
      }

      Program push2(final int value) {
        return hex(String.format("61%04x", value));
      }

      int size() {
        return code.length() / 2;
      }
    }
  }
}
