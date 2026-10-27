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

import static org.hyperledger.besu.evm.v2.ProgramRun.CALLEE;
import static org.hyperledger.besu.evm.v2.ProgramRun.CONTRACT;
import static org.hyperledger.besu.evm.v2.ProgramRun.SECOND_CALLEE;
import static org.hyperledger.besu.evm.v2.ProgramRun.SENDER;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.LogTopic;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.frame.FrameTransactionContext;
import org.hyperledger.besu.evm.frame.FrameTransactionContext.FrameInfo;
import org.hyperledger.besu.evm.frame.FrameTransactionContext.SignatureInfo;
import org.hyperledger.besu.evm.frame.TransactionDiff;
import org.hyperledger.besu.evm.frame.TransactionDiff.AccountFields;
import org.hyperledger.besu.evm.toy.ToyWorld;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Frame transaction contexts whose current frame is an EIP-7906 POST_TX frame: a VERIFY frame
 * targeting the contract, which may approve both scopes, a SENDER frame sending 3 wei to the
 * callee, and the POST_TX frame targeting the contract, which is the one executing.
 *
 * <p>The installed transaction prestate deliberately differs from the world {@link ProgramRun} runs
 * against, so that "before" and "after" reads can be told apart: the sender held 2,000,000, the
 * contract's slots 1 and 2 held 0 and 5, the callee's slot 0 held 9, {@link #DELEGATED} carries a
 * delegation designator to the callee, and {@link #NEVER_EXISTED} does not exist.
 */
final class PostTxContexts {

  /** The index of the POST_TX frame, the current one. */
  static final int POST_TX_FRAME = 2;

  /** The maximum cost the payer was charged. */
  static final Wei MAX_COST = Wei.of(0x1234_5678L);

  /** An account delegated to the callee in the prestate. */
  static final Address DELEGATED = Address.fromHexString("0xde1e");

  /** An account that never existed. */
  static final Address NEVER_EXISTED = Address.fromHexString("0x0e0e");

  /** Topic values of the sample events. */
  static final Bytes32 T1 = Bytes32.fromHexString("0x" + "11".repeat(32));

  /** Topic values of the sample events. */
  static final Bytes32 T2 = Bytes32.fromHexString("0x" + "22".repeat(32));

  /** A topic value starting with zero bytes, as a short push leaves it on a v1 stack. */
  static final Bytes32 T3 = Bytes32.leftPad(Bytes.of(0x7f, 0x06));

  /** The data of the first sample event, 40 bytes, the byte at offset i being i. */
  static final Bytes EVENT_DATA =
      Bytes.wrap(IntStream.range(0, 40).mapToObj(i -> Bytes.of(i)).toArray(Bytes[]::new));

  /** The code hash the sample diff reports for its deployed contract. */
  static final Hash DEPLOYED_CODE_HASH = Hash.hash(Bytes.of(0x60, 0x00));

  private PostTxContexts() {}

  /**
   * A POST_TX context with the sample diff installed.
   *
   * @param payerSet whether a frame has approved payment, with the sender as the payer
   * @return the context
   */
  static FrameTransactionContext withDiff(final boolean payerSet) {
    final FrameTransactionContext context = withoutDiff(payerSet);
    context.installTransactionDiff(prestate(), PostTxContexts::sampleDiff);
    return context;
  }

  /**
   * A POST_TX context of a transaction that changed nothing and emitted no event.
   *
   * @return the context
   */
  static FrameTransactionContext withEmptyDiff() {
    final FrameTransactionContext context = withoutDiff(true);
    context.installTransactionDiff(prestate(), () -> TransactionDiff.EMPTY);
    return context;
  }

  /**
   * The transaction prestate the contexts install.
   *
   * @return the prestate world
   */
  static ToyWorld prestate() {
    final ToyWorld world = new ToyWorld();
    final WorldUpdater setup = world.updater();
    setup.createAccount(SENDER, 0, Wei.of(2_000_000));
    setup
        .createAccount(CONTRACT, 1, Wei.of(1_000_000))
        .setStorageValue(UInt256.valueOf(2), UInt256.valueOf(5));
    setup.createAccount(CALLEE, 1, Wei.ZERO).setStorageValue(UInt256.ZERO, UInt256.valueOf(9));
    setup
        .createAccount(DELEGATED, 1, Wei.ONE)
        .setCode(Bytes.concatenate(Bytes.fromHexString("0xef0100"), CALLEE.getBytes()));
    setup.commit();
    return world;
  }

  /**
   * The sample diff: two balance changes, three slot changes, one deployment, and three events: a
   * LOG3 with 40 bytes of data from the contract, a LOG0 without data from the callee, and a LOG4
   * repeating a topic, with 1 byte of data, from the contract.
   *
   * @return the diff
   */
  static TransactionDiff sampleDiff() {
    return TransactionDiff.builder()
        .account(
            SENDER,
            new AccountFields(0, Wei.of(2_000_000), Hash.EMPTY),
            new AccountFields(1, Wei.of(1_000_000), Hash.EMPTY))
        .account(
            CALLEE,
            new AccountFields(1, Wei.ZERO, Hash.EMPTY),
            new AccountFields(1, Wei.of(3), Hash.EMPTY))
        .account(
            SECOND_CALLEE, AccountFields.EMPTY, new AccountFields(1, Wei.ZERO, DEPLOYED_CODE_HASH))
        .slot(CONTRACT, UInt256.ONE, UInt256.ZERO, UInt256.valueOf(7))
        .slot(CONTRACT, UInt256.valueOf(2), UInt256.valueOf(5), UInt256.ZERO)
        .slot(CALLEE, UInt256.ZERO, UInt256.valueOf(9), UInt256.ONE)
        .logs(
            List.of(
                log(CONTRACT, EVENT_DATA, T1, T3, T2),
                log(CALLEE, Bytes.EMPTY),
                log(CONTRACT, Bytes.of(0x42), T1, T2, T2, T3)))
        .build();
  }

  private static Log log(final Address logger, final Bytes data, final Bytes32... topics) {
    return new Log(logger, data, Arrays.stream(topics).map(LogTopic::create).toList());
  }

  /**
   * A POST_TX context with nothing installed for the assertion operations, as before the
   * processor's first POST_TX frame.
   *
   * @param payerSet whether a frame has approved payment, with the sender as the payer
   * @return the context
   */
  static FrameTransactionContext withoutDiff(final boolean payerSet) {
    final FrameTransactionContext context =
        new FrameTransactionContext(
            List.of(
                new FrameInfo(
                    FrameTransactionContext.MODE_VERIFY,
                    FrameTransactionContext.APPROVE_SCOPE_MASK,
                    CONTRACT,
                    50_000L,
                    10_000L,
                    Wei.ZERO,
                    Bytes.of(0xab, 0xcd)),
                new FrameInfo(
                    FrameTransactionContext.MODE_SENDER,
                    0,
                    CALLEE,
                    70_000L,
                    0L,
                    Wei.of(3),
                    Bytes.of(0x11, 0x22, 0x33)),
                new FrameInfo(
                    FrameTransactionContext.MODE_POST_TX,
                    0,
                    CONTRACT,
                    90_000L,
                    0L,
                    Wei.ZERO,
                    Bytes.of(0x44))),
            List.of(
                new SignatureInfo(FrameTransactionContext.SCHEME_SECP256K1, SENDER, null, null)),
            SENDER,
            Bytes32.leftPad(Bytes.of(0x5e)),
            9L,
            List.of(UInt256.ZERO),
            9L,
            Wei.of(2),
            Wei.of(11),
            Wei.ZERO,
            MAX_COST);
    context.recordFrameCompletion(0, FrameTransactionContext.STATUS_SUCCESS, 3_100L, 300L);
    context.recordFrameCompletion(1, FrameTransactionContext.STATUS_SUCCESS, 23_000L, 0L);
    if (payerSet) {
      context.setPayer(SENDER);
      context.approveSender();
    }
    context.setCurrentFrameIndex(POST_TX_FRAME);
    return context;
  }
}
