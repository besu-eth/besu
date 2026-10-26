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
import static org.hyperledger.besu.evm.v2.ProgramRun.SENDER;

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.frame.FrameTransactionContext;
import org.hyperledger.besu.evm.frame.FrameTransactionContext.FrameInfo;
import org.hyperledger.besu.evm.frame.FrameTransactionContext.SignatureInfo;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Frame transaction contexts whose current frame is an EIP-7906 POST_TX frame: a VERIFY frame
 * targeting the contract, which may approve both scopes, a SENDER frame sending 3 wei to the
 * callee, and the POST_TX frame targeting the contract, which is the one executing.
 */
final class PostTxContexts {

  /** The index of the POST_TX frame, the current one. */
  static final int POST_TX_FRAME = 2;

  /** The maximum cost the payer was charged. */
  static final Wei MAX_COST = Wei.of(0x1234_5678L);

  private PostTxContexts() {}

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
