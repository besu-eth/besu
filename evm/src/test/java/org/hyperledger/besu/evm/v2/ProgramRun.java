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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.frame.BlockValues;
import org.hyperledger.besu.evm.frame.Eip7928AccessList;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.AmsterdamGasCalculator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.internal.Words;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.precompile.PrecompileContractRegistry;
import org.hyperledger.besu.evm.processor.AbstractMessageProcessor;
import org.hyperledger.besu.evm.processor.ContractCreationProcessor;
import org.hyperledger.besu.evm.processor.MessageCallProcessor;
import org.hyperledger.besu.evm.toy.ToyWorld;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Runs a program on an EVM, as the code of a contract that the test accounts can call, and records
 * everything the v1 interpreter and EVM v2 have to agree on afterwards.
 */
final class ProgramRun {

  static final Address ORIGINATOR = Address.fromHexString("0x0419");
  static final Address SENDER = Address.fromHexString("0x5e4d");
  static final Address CONTRACT = Address.fromHexString("0xc0de");
  static final Address CALLEE = Address.fromHexString("0xca11ee");
  static final Address SECOND_CALLEE = Address.fromHexString("0xca11ee2");
  static final Address EMPTY_ACCOUNT = Address.fromHexString("0xe0");

  static final Wei CALL_VALUE = Wei.of(5);

  static final Bytes INPUT_DATA =
      Bytes.fromHexString(
          "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728");

  static final EvmConfiguration V1 = EvmConfiguration.DEFAULT;
  static final EvmConfiguration V2 =
      new EvmConfiguration(
          EvmConfiguration.DEFAULT.jumpDestCacheWeightKB(),
          EvmConfiguration.DEFAULT.worldUpdaterMode(),
          EvmConfiguration.DEFAULT.enableOptimizedOpcodes(),
          true);

  record Fork(String name, Function<EvmConfiguration, EVM> factory) {
    @Override
    public String toString() {
      return name;
    }
  }

  static final List<Fork> FORKS =
      List.of(
          new Fork("frontier", MainnetEVMs::frontier),
          new Fork("homestead", MainnetEVMs::homestead),
          new Fork("tangerine whistle", MainnetEVMs::tangerineWhistle),
          new Fork("spurious dragon", MainnetEVMs::spuriousDragon),
          new Fork("byzantium", MainnetEVMs::byzantium),
          new Fork("constantinople", MainnetEVMs::constantinople),
          new Fork("petersburg", MainnetEVMs::petersburg),
          new Fork("istanbul", MainnetEVMs::istanbul),
          new Fork("berlin", MainnetEVMs::berlin),
          new Fork("london", MainnetEVMs::london),
          new Fork("paris", MainnetEVMs::paris),
          new Fork("shanghai", MainnetEVMs::shanghai),
          new Fork("cancun", MainnetEVMs::cancun),
          new Fork("prague", MainnetEVMs::prague),
          new Fork("osaka", c -> MainnetEVMs.osaka(BigInteger.ONE, c)),
          new Fork(
              "amsterdam",
              c -> MainnetEVMs.amsterdam(new AmsterdamGasCalculator(), BigInteger.ONE, c)),
          new Fork(
              "bogota", c -> MainnetEVMs.bogota(new AmsterdamGasCalculator(), BigInteger.ONE, c)),
          new Fork(
              "future eips",
              c -> MainnetEVMs.futureEips(new AmsterdamGasCalculator(), BigInteger.ONE, c)));

  private static final BlockValues BLOCK_VALUES =
      new BlockValues() {
        @Override
        public Bytes getDifficultyBytes() {
          return Bytes.of(0x02, 0x00);
        }

        @Override
        public Bytes32 getMixHashOrPrevRandao() {
          return Bytes32.fromHexStringLenient("0x7a4d");
        }

        @Override
        public Optional<Wei> getBaseFee() {
          return Optional.of(Wei.of(7));
        }

        @Override
        public long getNumber() {
          return 1337;
        }

        @Override
        public long getTimestamp() {
          return 1_700_000_000L;
        }

        @Override
        public long getGasLimit() {
          return 60_000_000L;
        }

        @Override
        public long getSlotNumber() {
          return 42;
        }
      };

  /** Everything two interpreters must agree on after running the same program. */
  record Outcome(
      MessageFrame.State state,
      String haltReason,
      long remainingGas,
      long gasRefund,
      long stateGasUsed,
      List<String> stack,
      String memory,
      String output,
      String returnData,
      String revertReason,
      List<Log> logs,
      Set<Address> selfDestructs,
      Set<Address> creates,
      Map<Address, Wei> refunds,
      Set<String> accounts,
      Set<String> transientStorage,
      Set<String> accesses,
      List<String> steps) {}

  /** The outermost frame and the world it ran against, after a run, with its outcome. */
  record Execution(MessageFrame frame, WorldUpdater worldUpdater, Outcome outcome) {}

  private ProgramRun() {}

  /**
   * Runs the code as the contract's, with the other accounts holding the given code.
   *
   * @param evm the EVM to run it on
   * @param code the contract's code
   * @param calleeCode the code of CALLEE
   * @param secondCalleeCode the code of SECOND_CALLEE
   * @param gas the gas the contract is called with
   * @param isStatic whether the contract is called in a static frame
   * @param slots the storage and transient storage slots to compare
   * @return the outcome
   */
  static Outcome run(
      final EVM evm,
      final Bytes code,
      final Bytes calleeCode,
      final Bytes secondCalleeCode,
      final long gas,
      final boolean isStatic,
      final List<Bytes32> slots) {
    return execute(evm, code, calleeCode, secondCalleeCode, gas, isStatic, slots, false).outcome();
  }

  /**
   * As {@link #run}, with a tracer that records what it sees before and after every operation: the
   * operation, pc, depth, gas, refund, memory size, stack, gas cost and halt reason.
   *
   * @param evm the EVM to run it on
   * @param code the contract's code
   * @param calleeCode the code of CALLEE
   * @param secondCalleeCode the code of SECOND_CALLEE
   * @param gas the gas the contract is called with
   * @param isStatic whether the contract is called in a static frame
   * @param slots the storage and transient storage slots to compare
   * @return the outcome, with the recorded steps
   */
  static Outcome runTraced(
      final EVM evm,
      final Bytes code,
      final Bytes calleeCode,
      final Bytes secondCalleeCode,
      final long gas,
      final boolean isStatic,
      final List<Bytes32> slots) {
    return execute(evm, code, calleeCode, secondCalleeCode, gas, isStatic, slots, true).outcome();
  }

  /**
   * As {@link #run}, but also hands back the outermost frame and the world.
   *
   * @param evm the EVM to run it on
   * @param code the contract's code
   * @param calleeCode the code of CALLEE
   * @param secondCalleeCode the code of SECOND_CALLEE
   * @param gas the gas the contract is called with
   * @param isStatic whether the contract is called in a static frame
   * @param slots the storage and transient storage slots to compare
   * @return the execution
   */
  static Execution execute(
      final EVM evm,
      final Bytes code,
      final Bytes calleeCode,
      final Bytes secondCalleeCode,
      final long gas,
      final boolean isStatic,
      final List<Bytes32> slots) {
    return execute(evm, code, calleeCode, secondCalleeCode, gas, isStatic, slots, false);
  }

  private static Execution execute(
      final EVM evm,
      final Bytes code,
      final Bytes calleeCode,
      final Bytes secondCalleeCode,
      final long gas,
      final boolean isStatic,
      final List<Bytes32> slots,
      final boolean traced) {
    final StepRecorder recorder = new StepRecorder();
    final OperationTracer tracer = traced ? recorder : OperationTracer.NO_TRACING;
    final ToyWorld world = new ToyWorld();
    final WorldUpdater setup = world.updater();
    setup.getOrCreate(SENDER).setBalance(Wei.of(1_000_000));
    setup.getOrCreate(CONTRACT).setBalance(Wei.of(1_000_000));
    setup.getOrCreate(CALLEE).setCode(calleeCode);
    setup.getOrCreate(SECOND_CALLEE).setCode(secondCalleeCode);
    setup.getOrCreate(SECOND_CALLEE).setBalance(Wei.of(3));
    setup.getOrCreate(EMPTY_ACCOUNT);
    setup.commit();
    final WorldUpdater updater = world.updater();
    final AccessRecorder accesses = new AccessRecorder();
    // as the transaction calling the contract has done, so that an operation touching the contract
    // before it fails on the stack makes no difference, just as in a block access list
    accesses.addTouchedAccount(CONTRACT);
    final MessageFrame frame =
        MessageFrame.builder()
            .type(MessageFrame.Type.MESSAGE_CALL)
            .worldUpdater(updater)
            .initialGas(gas)
            .address(CONTRACT)
            .originator(ORIGINATOR)
            .gasPrice(Wei.ONE)
            .blobGasPrice(Wei.ONE)
            .inputData(INPUT_DATA)
            .sender(SENDER)
            .value(CALL_VALUE)
            .apparentValue(CALL_VALUE)
            .contract(CONTRACT)
            .code(new Code(code))
            .blockValues(BLOCK_VALUES)
            .completer(c -> {})
            .miningBeneficiary(Address.ZERO)
            .blockHashLookup((__, number) -> Hash.hash(Words.longBytes(number)))
            .isStatic(isStatic)
            .eip7928AccessList(accesses)
            .build();
    final AbstractMessageProcessor call =
        new MessageCallProcessor(evm, new PrecompileContractRegistry());
    final AbstractMessageProcessor create = new ContractCreationProcessor(evm, true, List.of(), 1);
    final Deque<MessageFrame> frames = frame.getMessageFrameStack();
    while (!frames.isEmpty()) {
      final MessageFrame next = frames.peekFirst();
      (next.getType() == MessageFrame.Type.CONTRACT_CREATION ? create : call).process(next, tracer);
    }
    final boolean halted = frame.getExceptionalHaltReason().isPresent();
    final Outcome outcome =
        new Outcome(
            frame.getState(),
            frame.getExceptionalHaltReason().map(ExceptionalHaltReason::name).orElse(""),
            frame.getRemainingGas(),
            frame.getGasRefund(),
            frame.getStateGasUsed(),
            // a frame that halted exceptionally is discarded, its stack and memory with it
            halted ? List.of() : stack(frame),
            halted ? "" : frame.shadowReadMemory(0, frame.memoryByteSize()).toHexString(),
            frame.getOutputData().toHexString(),
            frame.getReturnData().toHexString(),
            frame.getRevertReason().map(Bytes::toHexString).orElse(""),
            frame.getLogs(),
            new TreeSet<>(frame.getSelfDestructs()),
            new TreeSet<>(frame.getCreates()),
            new TreeMap<>(frame.getRefunds()),
            accounts(updater, slots),
            transientStorage(frame, slots),
            accesses.accesses,
            recorder.steps);
    return new Execution(frame, updater, outcome);
  }

  /** The balance, nonce, code and storage of every account the program can reach. */
  private static Set<String> accounts(final WorldUpdater updater, final List<Bytes32> slots) {
    final Set<String> accounts = new TreeSet<>();
    for (final Account account : updater.getTouchedAccounts()) {
      accounts.add(describe(account, slots));
    }
    for (final Address address : List.of(SENDER, CONTRACT, CALLEE, SECOND_CALLEE, EMPTY_ACCOUNT)) {
      Optional.ofNullable(updater.get(address))
          .ifPresent(account -> accounts.add(describe(account, slots)));
    }
    return accounts;
  }

  private static String describe(final Account account, final List<Bytes32> slots) {
    final StringBuilder description =
        new StringBuilder()
            .append(account.getAddress())
            .append(" balance ")
            .append(account.getBalance().toShortHexString())
            .append(" nonce ")
            .append(account.getNonce())
            .append(" code ")
            .append(account.getCode().toHexString())
            .append(" storage");
    for (final Bytes32 slot : slots) {
      description
          .append(' ')
          .append(account.getStorageValue(UInt256.fromBytes(slot)).toShortHexString());
    }
    return description.toString();
  }

  private static Set<String> transientStorage(final MessageFrame frame, final List<Bytes32> slots) {
    final Set<String> values = new TreeSet<>();
    for (final Address address : List.of(CONTRACT, CALLEE, SECOND_CALLEE)) {
      for (final Bytes32 slot : slots) {
        final Bytes32 value = frame.getTransientStorageValue(address, slot);
        if (!value.isZero()) {
          values.add(address + " " + slot.toShortHexString() + " " + value.toShortHexString());
        }
      }
    }
    return values;
  }

  /** Records what EIP-7928 block access lists are built from. */
  private static final class AccessRecorder implements Eip7928AccessList {
    private final Set<String> accesses = new TreeSet<>();

    @Override
    public void addTouchedAccount(final Address address) {
      accesses.add(address.toHexString());
    }

    @Override
    public void addSlotAccessForAccount(final Address address, final UInt256 slotKey) {
      accesses.add(address.toHexString() + " " + slotKey.toHexString());
    }

    @Override
    public void clear() {
      accesses.clear();
    }
  }

  /** The frame's stack, top first, from whichever interpreter ran it. */
  private static List<String> stack(final MessageFrame frame) {
    final List<String> items = new ArrayList<>();
    final long[] s = frame.stackDataV2();
    if (s != null) {
      for (int i = frame.stackTopV2() - 1; i >= 0; i--) {
        items.add(
            String.format(
                "%016x%016x%016x%016x",
                s[i << 2], s[(i << 2) + 1], s[(i << 2) + 2], s[(i << 2) + 3]));
      }
    } else {
      for (int i = 0; i < frame.stackSize(); i++) {
        items.add(Bytes32.leftPad(frame.getStackItem(i)).toUnprefixedHexString());
      }
    }
    return items;
  }

  /** Records what a tracer sees of each operation, through the frame's accessors. */
  private static final class StepRecorder implements OperationTracer {
    private final List<String> steps = new ArrayList<>();
    private String before = "";

    @Override
    public void tracePreExecution(final MessageFrame frame) {
      before =
          String.format(
              "depth %d pc %d %s gas %d refund %d memory %d stack %s",
              frame.getDepth(),
              frame.getPC(),
              frame.getCurrentOperation().getName(),
              frame.getRemainingGas(),
              frame.getGasRefund(),
              frame.memoryByteSize(),
              tracedStack(frame));
    }

    @Override
    public void tracePostExecution(
        final MessageFrame frame, final Operation.OperationResult result) {
      steps.add(
          String.format(
              "%s -> cost %d halt %s gas %d stack %s",
              before,
              result.getGasCost(),
              result.getHaltReason(),
              frame.getRemainingGas(),
              tracedStack(frame)));
    }

    private static List<String> tracedStack(final MessageFrame frame) {
      final List<String> items = new ArrayList<>();
      for (int i = 0; i < frame.stackSize(); i++) {
        items.add(Bytes.wrap(Bytes32.leftPad(frame.getStackItem(i)).toArray()).toShortHexString());
      }
      return items;
    }
  }
}
