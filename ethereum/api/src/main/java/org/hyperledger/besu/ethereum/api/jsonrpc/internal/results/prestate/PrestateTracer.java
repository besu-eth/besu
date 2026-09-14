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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.prestate;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.debug.TraceOptions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.evm.EvmSpecVersion;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.internal.Words;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.evm.worldstate.WorldView;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Native {@code prestateTracer} implementation built directly on {@link OperationTracer} context
 * hooks instead of post-processing a full opcode-level trace.
 *
 * <p>Mirrors Geth's {@code eth/tracers/native/prestate.go}: snapshots the pre-transaction state of
 * every account touched by the transaction (sender, recipient, coinbase, 7702 authorities and
 * delegation targets, plus accounts and storage slots referenced by account/storage-accessing
 * opcodes), and in {@code diffMode} reports the post-transaction changes.
 *
 * <p>Geth's {@code OnOpcode} fires only after stack and gas validation succeeded, while Besu fires
 * {@link #tracePreExecution} before validation; state is therefore snapshotted eagerly into a
 * pending record and only installed when {@link #tracePostExecution} observes that the opcode did
 * not exceptionally halt.
 *
 * <p>One instance traces exactly one transaction and is not thread-safe.
 *
 * @see <a
 *     href="https://geth.ethereum.org/docs/developers/evm-tracing/built-in-tracers#prestate-tracer">
 *     Geth prestateTracer Documentation</a>
 */
public class PrestateTracer implements OperationTracer {

  private static final String CALL = "CALL";
  private static final String CALLCODE = "CALLCODE";
  private static final String DELEGATECALL = "DELEGATECALL";
  private static final String STATICCALL = "STATICCALL";
  private static final String CREATE = "CREATE";
  private static final String CREATE2 = "CREATE2";
  private static final String SELFDESTRUCT = "SELFDESTRUCT";
  private static final String SLOAD = "SLOAD";
  private static final String SSTORE = "SSTORE";
  private static final String EXTCODECOPY = "EXTCODECOPY";
  private static final String EXTCODEHASH = "EXTCODEHASH";
  private static final String EXTCODESIZE = "EXTCODESIZE";
  private static final String BALANCE = "BALANCE";

  private final boolean diffMode;
  private final boolean disableCode;
  private final boolean disableStorage;
  private final boolean includeEmpty;
  private final boolean eip6780;
  private final boolean eip7702;
  private final GasCalculator gasCalculator;
  private final int maxInitcodeSize;

  private final Map<Address, AccountState> pre = new HashMap<>();
  private final Map<Address, AccountState> post = new HashMap<>();
  private final Set<Address> created = new HashSet<>();
  private final Set<Address> deleted = new HashSet<>();
  private boolean coinbaseLookedUp;
  private Pending pending;

  /**
   * Instantiates a new prestate tracer.
   *
   * @param traceOptions the trace options containing the tracer configuration
   * @param protocolSpec the protocol spec of the block containing the traced transaction
   */
  public PrestateTracer(final TraceOptions traceOptions, final ProtocolSpec protocolSpec) {
    this.diffMode =
        Boolean.TRUE.equals(traceOptions.tracerConfig().getOrDefault("diffMode", false));
    this.disableCode =
        Boolean.TRUE.equals(traceOptions.tracerConfig().getOrDefault("disableCode", false));
    this.disableStorage =
        Boolean.TRUE.equals(traceOptions.tracerConfig().getOrDefault("disableStorage", false));
    this.includeEmpty =
        Boolean.TRUE.equals(traceOptions.tracerConfig().getOrDefault("includeEmpty", false));
    final EvmSpecVersion evmVersion = protocolSpec.getEvm().getEvmVersion();
    this.eip6780 = evmVersion.compareTo(EvmSpecVersion.CANCUN) >= 0;
    this.eip7702 = evmVersion.compareTo(EvmSpecVersion.PRAGUE) >= 0;
    this.gasCalculator = protocolSpec.getGasCalculator();
    this.maxInitcodeSize = protocolSpec.getEvm().getMaxInitcodeSize();
  }

  @Override
  public void tracePrepareTransaction(final WorldView worldView, final Transaction transaction) {
    pre.clear();
    post.clear();
    created.clear();
    deleted.clear();
    coinbaseLookedUp = false;
    pending = null;

    final Address from = transaction.getSender();
    final Address to;
    if (transaction.isContractCreation()) {
      to = transaction.contractAddress().orElseThrow();
      created.add(to);
    } else {
      to = transaction.getTo().orElseThrow();
      // Lookup the delegation target
      delegationTarget(worldView, to).ifPresent(target -> lookupAccount(worldView, target));
    }
    lookupAccount(worldView, from);
    lookupAccount(worldView, to);

    // Add accounts with authorizations to the prestate before they get applied.
    for (final var authorization : transaction.getCodeDelegationList().orElse(List.of())) {
      authorization.authorizer().ifPresent(authority -> lookupAccount(worldView, authority));
    }
  }

  @Override
  public void traceContextEnter(final MessageFrame frame) {
    lookupCoinbase(frame);
  }

  @Override
  public void traceContextReEnter(final MessageFrame frame) {
    // The top frame is dispatched straight to re-enter when transaction preparation already
    // halted it (EIP-8037 state-gas out-of-gas), so this is the first hook Besu fires for it.
    lookupCoinbase(frame);
  }

  private void lookupCoinbase(final MessageFrame frame) {
    if (!coinbaseLookedUp) {
      coinbaseLookedUp = true;
      lookupAccount(frame.getWorldUpdater(), frame.getMiningBeneficiary());
    }
  }

  @Override
  public void tracePreExecution(final MessageFrame frame) {
    pending = null;
    final Operation op = frame.getCurrentOperation();
    if (op == null || frame.stackSize() < op.getStackItemsConsumed()) {
      return;
    }
    final WorldUpdater world = frame.getWorldUpdater();
    final Address self = frame.getRecipientAddress();
    final List<Map.Entry<Address, AccountState>> accounts = new ArrayList<>(1);
    switch (op.getName()) {
      case SLOAD, SSTORE -> {
        if (disableStorage) {
          return;
        }
        final UInt256 key = UInt256.fromBytes(frame.getStackItem(0));
        snapshotInto(accounts, world, self);
        final AccountState existing = pre.get(self);
        if (existing != null && existing.storage != null && existing.storage.containsKey(key)) {
          return;
        }
        final Account account = world.get(self);
        final UInt256 value = account == null ? UInt256.ZERO : account.getStorageValue(key);
        pending = new Pending(accounts, self, key, value, null, null);
      }
      case EXTCODECOPY, EXTCODEHASH, EXTCODESIZE, BALANCE -> {
        final Address target = Words.toAddress(frame.getStackItem(0));
        snapshotInto(accounts, world, target);
        pending = new Pending(accounts, null, null, null, null, null);
      }
      case SELFDESTRUCT -> {
        final Address target = Words.toAddress(frame.getStackItem(0));
        snapshotInto(accounts, world, target);
        final Address deletedAddress = (!eip6780 || created.contains(self)) ? self : null;
        pending = new Pending(accounts, null, null, null, null, deletedAddress);
      }
      case CALL, CALLCODE, DELEGATECALL, STATICCALL -> {
        final Address target = Words.toAddress(frame.getStackItem(1));
        snapshotInto(accounts, world, target);
        // Lookup the delegation target
        delegationTarget(world, target).ifPresent(t -> snapshotInto(accounts, world, t));
        pending = new Pending(accounts, null, null, null, null, null);
      }
      case CREATE -> {
        final Account caller = world.get(self);
        final Address addr = Address.contractAddress(self, caller == null ? 0L : caller.getNonce());
        snapshotInto(accounts, world, addr);
        pending = new Pending(accounts, null, null, null, addr, null);
      }
      case CREATE2 -> {
        final int offset = Words.clampedToInt(frame.getStackItem(1));
        final int size = Words.clampedToInt(frame.getStackItem(2));
        // Mirror AbstractCreateOperation's early aborts: Geth's OnOpcode fires only after gas
        // and initcode-size validation, and reading initcode before it would allocate memory
        // the EVM will refuse to expand.
        final long cost =
            Words.clampedAdd(
                Words.clampedAdd(
                    gasCalculator.txCreateCost(),
                    gasCalculator.memoryExpansionGasCost(frame, offset, size)),
                Words.clampedAdd(
                    gasCalculator.createKeccakCost(size), gasCalculator.initcodeCost(size)));
        if (frame.getRemainingGas() < cost
            || size > maxInitcodeSize
            || (long) offset + size > Integer.MAX_VALUE) {
          return;
        }
        final Bytes initCode = size == 0 ? Bytes.EMPTY : frame.shadowReadMemory(offset, size);
        final Bytes32 salt = Bytes32.leftPad(frame.getStackItem(3));
        final Bytes32 create2Hash =
            Bytes32.wrap(
                Hash.hash(
                        Bytes.concatenate(
                            Bytes.of((byte) 0xff),
                            self.getBytes(),
                            salt,
                            Hash.hash(initCode).getBytes()))
                    .getBytes());
        final Address addr = Address.extract(create2Hash);
        snapshotInto(accounts, world, addr);
        pending = new Pending(accounts, null, null, null, addr, null);
      }
      default -> {}
    }
  }

  @Override
  public void tracePostExecution(
      final MessageFrame frame, final Operation.OperationResult operationResult) {
    final Pending currentPending = pending;
    pending = null;
    // Besu fires tracePreExecution before stack/gas validation whereas Geth's OnOpcode fires
    // after it; every halt Besu can raise on these opcodes corresponds to a Geth error raised
    // before OnOpcode, so discard the snapshot on exceptional halt.
    if (currentPending != null && frame.getState() != MessageFrame.State.EXCEPTIONAL_HALT) {
      currentPending.accounts().forEach(entry -> install(entry.getKey(), entry.getValue()));
      if (currentPending.storageAddress() != null) {
        final AccountState state = pre.get(currentPending.storageAddress());
        if (state != null && state.storage != null) {
          state.storage.putIfAbsent(currentPending.storageKey(), currentPending.storageValue());
        }
      }
      if (currentPending.createdAddress() != null) {
        created.add(currentPending.createdAddress());
      }
      if (currentPending.deletedAddress() != null) {
        deleted.add(currentPending.deletedAddress());
      }
    }
  }

  @Override
  public void traceEndTransaction(
      final WorldView worldView,
      final Transaction tx,
      final boolean status,
      final org.apache.tuweni.bytes.Bytes output,
      final List<org.hyperledger.besu.datatypes.Log> logs,
      final long gasUsed,
      final Set<Address> selfDestructs,
      final long timeNs) {
    if (diffMode) {
      processDiffState(worldView);
    }
    // Remove accounts that were empty prior to execution, unless the user
    // requested to include empty accounts.
    if (!includeEmpty) {
      pre.values().removeIf(state -> state.empty);
    }
  }

  /**
   * Builds the prestate tracer result for the traced transaction.
   *
   * @return the completed result (diff shape when {@code diffMode} is enabled)
   */
  public PrestateTracerResult buildResult() {
    return diffMode
        ? new PrestateTracerResult.Diff(toJson(post), toJson(pre))
        : new PrestateTracerResult.Prestate(toJson(pre));
  }

  private Map<String, PrestateTracerResult.Account> toJson(
      final Map<Address, AccountState> accounts) {
    final Map<String, PrestateTracerResult.Account> result = new TreeMap<>();
    accounts.forEach((addr, state) -> result.put(addr.toHexString(), state.toJson()));
    return result;
  }

  private void processDiffState(final WorldView world) {
    for (final Address addr : List.copyOf(pre.keySet())) {
      // The deleted account's state is pruned from post but kept in pre.
      if (deleted.contains(addr)) {
        continue;
      }
      final AccountState prevState = pre.get(addr);
      final Account now = world.get(addr);
      final Wei newBalance = now == null ? Wei.ZERO : now.getBalance();
      final long newNonce = now == null ? 0L : now.getNonce();
      // Geth reports the literal zero codeHash for an address with no stateObject, distinct
      // from an existing codeless account's EmptyCodeHash. Besu eagerly materializes an empty
      // stub for e.g. a zero-value SELFDESTRUCT beneficiary before clearing runs; treat a fully
      // empty post-tx account the same as nonexistent so the comparison against the
      // EmptyCodeHash default below matches Geth's output.
      final boolean nonexistent =
          now == null
              || (now.getNonce() == 0 && now.getCode().isEmpty() && now.getBalance().isZero());
      final Hash newCodeHash = nonexistent ? Hash.ZERO : now.getCodeHash();
      final Bytes newCode = now == null ? Bytes.EMPTY : now.getCode();

      boolean modified = false;
      final AccountState postState = new AccountState();
      postState.storage = disableStorage ? null : new HashMap<>();

      if (!newBalance.equals(prevState.balance)) {
        modified = true;
        postState.balance = newBalance;
      }
      if (newNonce != prevState.nonce) {
        modified = true;
        postState.nonce = newNonce;
      }
      final Hash prevCodeHash = prevState.codeHash == null ? Hash.EMPTY : prevState.codeHash;
      if (!newCodeHash.equals(prevCodeHash)) {
        modified = true;
        postState.codeHash = newCodeHash;
      }
      if (!disableCode) {
        final Bytes prevCode = prevState.code == null ? Bytes.EMPTY : prevState.code;
        if (!newCode.equals(prevCode)) {
          modified = true;
          postState.code = newCode;
        }
      }
      if (!disableStorage && prevState.storage != null) {
        for (final var entry : List.copyOf(prevState.storage.entrySet())) {
          final UInt256 key = entry.getKey();
          final UInt256 val = entry.getValue();
          if (val.isZero()) {
            prevState.storage.remove(key);
          }
          final UInt256 newVal = now == null ? UInt256.ZERO : now.getStorageValue(key);
          if (val.equals(newVal)) {
            prevState.storage.remove(key);
          } else {
            modified = true;
            if (!newVal.isZero()) {
              postState.storage.put(key, newVal);
            }
          }
        }
      }
      if (modified) {
        post.put(addr, postState);
      } else {
        pre.remove(addr);
      }
    }
  }

  /** Snapshots the account if it is not already recorded. */
  private void lookupAccount(final WorldView world, final Address addr) {
    if (!pre.containsKey(addr)) {
      install(addr, snapshotAccount(world, addr));
    }
  }

  /** Snapshots the account into {@code out} if not already recorded in {@code pre}. */
  private void snapshotInto(
      final List<Map.Entry<Address, AccountState>> out,
      final WorldUpdater world,
      final Address addr) {
    if (!pre.containsKey(addr)) {
      out.add(Map.entry(addr, snapshotAccount(world, addr)));
    }
  }

  private void install(final Address addr, final AccountState state) {
    pre.putIfAbsent(addr, state);
  }

  private AccountState snapshotAccount(final WorldView world, final Address addr) {
    final Account account = world.get(addr);
    final AccountState state = new AccountState();
    state.balance = account == null ? Wei.ZERO : account.getBalance();
    state.nonce = account == null ? 0L : account.getNonce();
    final Bytes rawCode = account == null ? Bytes.EMPTY : account.getCode();
    state.code = rawCode.isEmpty() ? null : rawCode;
    final Hash rawCodeHash = account == null ? Hash.EMPTY : account.getCodeHash();
    state.codeHash =
        (Hash.EMPTY.equals(rawCodeHash) || Hash.ZERO.equals(rawCodeHash)) ? null : rawCodeHash;
    // Storage is always empty at snapshot time, so the emptiness check reduces to
    // nonce/code/balance; it must run before disableCode clears the code.
    state.empty = state.nonce == 0 && state.code == null && state.balance.isZero();
    if (disableCode) {
      state.code = null;
    }
    state.storage = disableStorage ? null : new HashMap<>();
    return state;
  }

  private Optional<Address> delegationTarget(final WorldView world, final Address addr) {
    if (eip7702) {
      final Account account = world.get(addr);
      if (account != null && CodeDelegationHelper.hasCodeDelegation(account.getCode())) {
        return Optional.of(CodeDelegationHelper.getTargetAddress(account.getCode()));
      }
    }
    return Optional.empty();
  }

  /**
   * Snapshot read during {@link #tracePreExecution} and installed only when the opcode did not
   * exceptionally halt.
   *
   * @param accounts accounts to install (never null, may be empty)
   * @param storageAddress contract whose storage slot was read (null when not a storage access)
   * @param storageKey the storage slot key
   * @param storageValue the storage slot value read before execution
   * @param createdAddress address created by a CREATE/CREATE2 opcode (null when not creating)
   * @param deletedAddress address marked deleted by SELFDESTRUCT (null when not deleting)
   */
  private record Pending(
      List<Map.Entry<Address, AccountState>> accounts,
      Address storageAddress,
      UInt256 storageKey,
      UInt256 storageValue,
      Address createdAddress,
      Address deletedAddress) {}

  private static final class AccountState {
    Wei balance;
    long nonce;
    Bytes code;
    Hash codeHash;
    Map<UInt256, UInt256> storage;
    boolean empty;

    PrestateTracerResult.Account toJson() {
      return new PrestateTracerResult.Account(
          balance == null ? null : balance.toShortHexString(),
          code == null ? null : code.toHexString(),
          codeHash == null ? null : codeHash.toHexString(),
          nonce == 0 ? null : nonce,
          storage == null || storage.isEmpty()
              ? null
              : storage.entrySet().stream()
                  .collect(
                      TreeMap::new,
                      (m, e) -> m.put(e.getKey().toHexString(), e.getValue().toHexString()),
                      TreeMap::putAll));
    }
  }
}
