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
package org.hyperledger.besu.evm.frame;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.LogTopic;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.frame.TransactionDiff.AccountFields;
import org.hyperledger.besu.evm.frame.TransactionDiff.BalanceChange;
import org.hyperledger.besu.evm.frame.TransactionDiff.CodeChange;
import org.hyperledger.besu.evm.frame.TransactionDiff.NonceChange;
import org.hyperledger.besu.evm.frame.TransactionDiff.SlotChange;

import java.util.Arrays;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

/** The tables and orders {@link TransactionDiff.Builder} derives. */
class TransactionDiffTest {

  private static final Address LOW =
      Address.fromHexString("0x7fffffffffffffffffffffffffffffffffffffff");
  private static final Address HIGH =
      Address.fromHexString("0x8000000000000000000000000000000000000000");

  private static final Bytes32 T1 = Bytes32.fromHexString("0x" + "11".repeat(32));
  // starts with a zero byte, which the event keeps
  private static final Bytes32 SHORT = Bytes32.leftPad(Bytes.of(0x7f, 0x06));

  private static AccountFields fields(final long nonce, final long balance, final Hash codeHash) {
    return new AccountFields(nonce, Wei.of(balance), codeHash);
  }

  private static Log log(final Address logger, final int dataSize, final Bytes32... topics) {
    return new Log(
        logger,
        Bytes.wrap(new byte[dataSize]),
        Arrays.stream(topics).map(LogTopic::create).toList());
  }

  @Test
  void restoredValuesLeaveNoEntry() {
    final TransactionDiff diff =
        TransactionDiff.builder()
            .account(LOW, fields(1, 10, Hash.EMPTY), fields(1, 10, Hash.EMPTY))
            .slot(LOW, UInt256.ONE, UInt256.valueOf(5), UInt256.valueOf(5))
            .slot(LOW, UInt256.valueOf(2), UInt256.ZERO, UInt256.ZERO)
            .build();

    assertThat(diff.balances()).isEmpty();
    assertThat(diff.slots()).isEmpty();
    assertThat(diff.codes()).isEmpty();
    assertThat(diff.nonces()).isEmpty();
  }

  @Test
  void entriesAscendAsUnsignedNumbersWhateverTheInsertionOrder() {
    final UInt256 highKey = UInt256.fromHexString("0x80" + "00".repeat(31));
    final TransactionDiff diff =
        TransactionDiff.builder()
            .account(HIGH, fields(0, 1, Hash.EMPTY), fields(0, 2, Hash.EMPTY))
            .account(LOW, fields(0, 1, Hash.EMPTY), fields(0, 3, Hash.EMPTY))
            .slot(HIGH, highKey, UInt256.ZERO, UInt256.ONE)
            .slot(HIGH, UInt256.ONE, UInt256.ZERO, UInt256.ONE)
            .slot(LOW, highKey, UInt256.ZERO, UInt256.ONE)
            .build();

    assertThat(diff.balances())
        .containsExactly(
            new BalanceChange(LOW, Wei.of(1), Wei.of(3)),
            new BalanceChange(HIGH, Wei.of(1), Wei.of(2)));
    assertThat(diff.slots())
        .extracting(SlotChange::address, SlotChange::key)
        .containsExactly(
            org.assertj.core.groups.Tuple.tuple(LOW, highKey),
            org.assertj.core.groups.Tuple.tuple(HIGH, UInt256.ONE),
            org.assertj.core.groups.Tuple.tuple(HIGH, highKey));
  }

  /**
   * EIP-7906 compares raw code hashes, so installing, replacing and clearing an EIP-7702 delegation
   * designator are code changes like a deployment, while creating an account without code is not.
   */
  @Test
  void codeTableListsEveryRawCodeHashChange() {
    final Hash code = Hash.hash(Bytes.of(0x00));
    final Hash designator = Hash.hash(Bytes.fromHexString("0xef0100" + "11".repeat(20)));
    final Address created = Address.fromHexString("0x01");
    final Address existedWithoutCode = Address.fromHexString("0x02");
    final Address delegated = Address.fromHexString("0x03");
    final Address emptyCode = Address.fromHexString("0x04");
    final Address replaced = Address.fromHexString("0x05");
    final Address cleared = Address.fromHexString("0x06");
    final Address unchanged = Address.fromHexString("0x07");
    final TransactionDiff diff =
        TransactionDiff.builder()
            .account(created, AccountFields.EMPTY, fields(1, 0, code))
            .account(existedWithoutCode, fields(0, 9, Hash.EMPTY), fields(1, 9, code))
            .account(delegated, fields(1, 0, Hash.EMPTY), fields(2, 0, designator))
            .account(emptyCode, AccountFields.EMPTY, fields(1, 0, Hash.EMPTY))
            .account(replaced, fields(1, 0, designator), fields(1, 0, code))
            .account(cleared, fields(1, 0, designator), AccountFields.EMPTY)
            .account(unchanged, fields(1, 0, code), fields(1, 0, code))
            .build();

    assertThat(diff.codes())
        .containsExactly(
            new CodeChange(created, Hash.EMPTY, code),
            new CodeChange(existedWithoutCode, Hash.EMPTY, code),
            new CodeChange(delegated, Hash.EMPTY, designator),
            new CodeChange(replaced, designator, code),
            new CodeChange(cleared, designator, Hash.EMPTY));
  }

  @Test
  void nonceTableListsChangedNoncesAsUnsignedValues() {
    final Address created = Address.fromHexString("0x01");
    final Address bumped = Address.fromHexString("0x02");
    final Address deleted = Address.fromHexString("0x03");
    final Address unchanged = Address.fromHexString("0x04");
    final TransactionDiff diff =
        TransactionDiff.builder()
            .account(unchanged, fields(-1L, 0, Hash.EMPTY), fields(-1L, 1, Hash.EMPTY))
            .account(deleted, fields(4, 0, Hash.EMPTY), AccountFields.EMPTY)
            .account(
                bumped,
                fields(Long.MAX_VALUE, 0, Hash.EMPTY),
                fields(Long.MIN_VALUE, 0, Hash.EMPTY))
            .account(created, AccountFields.EMPTY, fields(1, 0, Hash.EMPTY))
            .build();

    assertThat(diff.nonces())
        .containsExactly(
            new NonceChange(created, 0, 1),
            new NonceChange(bumped, Long.MAX_VALUE, Long.MIN_VALUE),
            new NonceChange(deleted, 4, 0));
  }

  @Test
  void topicsAreHeldAsThirtyTwoByteValues() {
    final TransactionDiff diff =
        TransactionDiff.builder().logs(List.of(log(LOW, 40, T1, SHORT))).build();

    assertThat(diff.events().get(0).topics()).containsExactly(T1, SHORT);
    assertThat(diff.events().get(0).topics().get(1)).isInstanceOf(Bytes32.class);
    assertThat(diff.events().get(0).data().size()).isEqualTo(40);
  }

  @Test
  void emptyDiffHasEmptyTables() {
    assertThat(TransactionDiff.EMPTY.balances()).isEmpty();
    assertThat(TransactionDiff.EMPTY.slots()).isEmpty();
    assertThat(TransactionDiff.EMPTY.codes()).isEmpty();
    assertThat(TransactionDiff.EMPTY.nonces()).isEmpty();
    assertThat(TransactionDiff.EMPTY.events()).isEmpty();
  }
}
