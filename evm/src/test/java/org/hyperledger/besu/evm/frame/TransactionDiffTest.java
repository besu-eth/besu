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
import static org.assertj.core.groups.Tuple.tuple;
import static org.hyperledger.besu.evm.frame.TransactionDiff.FLAG_BALANCE;
import static org.hyperledger.besu.evm.frame.TransactionDiff.FLAG_CODE;
import static org.hyperledger.besu.evm.frame.TransactionDiff.FLAG_NONCE;
import static org.hyperledger.besu.evm.frame.TransactionDiff.FLAG_STORAGE;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.LogTopic;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.frame.TransactionDiff.AccountFields;
import org.hyperledger.besu.evm.frame.TransactionDiff.BalanceChange;
import org.hyperledger.besu.evm.frame.TransactionDiff.DeployedContract;
import org.hyperledger.besu.evm.frame.TransactionDiff.SlotChange;

import java.util.Arrays;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

/** The tables, orders, views and flags {@link TransactionDiff.Builder} derives. */
class TransactionDiffTest {

  private static final Address LOW =
      Address.fromHexString("0x7fffffffffffffffffffffffffffffffffffffff");
  private static final Address HIGH =
      Address.fromHexString("0x8000000000000000000000000000000000000000");
  private static final Address OTHER =
      Address.fromHexString("0x00000000000000000000000000000000000000aa");

  private static final Bytes32 T1 = Bytes32.fromHexString("0x" + "11".repeat(32));
  private static final Bytes32 T2 = Bytes32.fromHexString("0x" + "22".repeat(32));
  private static final Bytes32 T3 = Bytes32.fromHexString("0x" + "33".repeat(32));
  private static final Bytes32 T4 = Bytes32.fromHexString("0x" + "44".repeat(32));
  // starts with a zero byte: a short operand of a v1 stack must still match it
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
    assertThat(diff.deployed()).isEmpty();
    assertThat(diff.accountChangeFlags(LOW)).isZero();
    assertThat(diff.slotIndicesOf(LOW)).isEmpty();
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
        .containsExactly(tuple(LOW, highKey), tuple(HIGH, UInt256.ONE), tuple(HIGH, highKey));
    assertThat(diff.slotIndicesOf(LOW)).containsExactly(0);
    assertThat(diff.slotIndicesOf(HIGH)).containsExactly(1, 2);
  }

  @Test
  void deployedTableListsNewNonDelegationCodeOnly() {
    final Hash code = Hash.hash(Bytes.of(0x00));
    final Hash designator = Hash.hash(Bytes.fromHexString("0xef0100" + "11".repeat(20)));
    final Address created = Address.fromHexString("0x01");
    final Address existedWithoutCode = Address.fromHexString("0x02");
    final Address delegated = Address.fromHexString("0x03");
    final Address emptyCode = Address.fromHexString("0x04");
    final Address replaced = Address.fromHexString("0x05");
    final TransactionDiff diff =
        TransactionDiff.builder()
            .account(created, AccountFields.EMPTY, fields(1, 0, code))
            .account(existedWithoutCode, fields(0, 9, Hash.EMPTY), fields(1, 9, code))
            .account(delegated, fields(1, 0, Hash.EMPTY), fields(2, 0, designator), true)
            .account(emptyCode, AccountFields.EMPTY, fields(1, 0, Hash.EMPTY))
            .account(replaced, fields(1, 0, designator), fields(1, 0, code))
            .build();

    assertThat(diff.deployed())
        .containsExactly(
            new DeployedContract(created, code), new DeployedContract(existedWithoutCode, code));
    // the CODE flag compares the raw hashes, so a delegation counts there
    assertThat(diff.accountChangeFlags(delegated)).isEqualTo(FLAG_NONCE | FLAG_CODE);
    assertThat(diff.accountChangeFlags(replaced)).isEqualTo(FLAG_CODE);
    assertThat(diff.accountChangeFlags(emptyCode)).isEqualTo(FLAG_NONCE);
  }

  @Test
  void accountChangeFlagsFollowTheMixedBody() {
    final Address payer = Address.fromHexString("0x01");
    final Address writer = Address.fromHexString("0x02");
    final Address recipient = Address.fromHexString("0x03");
    final Address deployed = Address.fromHexString("0x04");
    final Address factory = Address.fromHexString("0x05");
    final Address untouched = Address.fromHexString("0x06");
    final Address restorer = Address.fromHexString("0x07");
    final Address neverExisted = Address.fromHexString("0x08");
    final Hash factoryCode = Hash.hash(Bytes.of(0xf0));
    final Hash writerCode = Hash.hash(Bytes.of(0x55));
    final TransactionDiff diff =
        TransactionDiff.builder()
            .account(payer, fields(0, 1_000, Hash.EMPTY), fields(1, 900, Hash.EMPTY))
            .account(writer, fields(1, 0, writerCode), fields(1, 0, writerCode))
            .account(recipient, fields(0, 1_000, Hash.EMPTY), fields(0, 1_007, Hash.EMPTY))
            .account(deployed, AccountFields.EMPTY, fields(1, 0, Hash.hash(Bytes.of(0x00))))
            .account(factory, fields(1, 0, factoryCode), fields(3, 0, factoryCode))
            .account(restorer, fields(1, 0, writerCode), fields(1, 0, writerCode))
            .slot(writer, UInt256.valueOf(0xa), UInt256.ZERO, UInt256.ONE)
            .slot(writer, UInt256.valueOf(0xb), UInt256.valueOf(5), UInt256.valueOf(7))
            .slot(writer, UInt256.valueOf(0xc), UInt256.ZERO, UInt256.ZERO)
            .slot(restorer, UInt256.valueOf(0xa), UInt256.ZERO, UInt256.ZERO)
            .build();

    assertThat(diff.accountChangeFlags(payer)).isEqualTo(FLAG_NONCE | FLAG_BALANCE);
    assertThat(diff.accountChangeFlags(writer)).isEqualTo(FLAG_STORAGE);
    assertThat(diff.accountChangeFlags(recipient)).isEqualTo(FLAG_BALANCE);
    assertThat(diff.accountChangeFlags(deployed)).isEqualTo(FLAG_NONCE | FLAG_CODE);
    assertThat(diff.accountChangeFlags(factory)).isEqualTo(FLAG_NONCE);
    assertThat(diff.accountChangeFlags(untouched)).isZero();
    assertThat(diff.accountChangeFlags(restorer)).isZero();
    assertThat(diff.accountChangeFlags(neverExisted)).isZero();
    assertThat(diff.slotIndicesOf(writer)).containsExactly(0, 1);
    assertThat(diff.slotIndicesOf(restorer)).isEmpty();
    assertThat(diff.slotIndicesOf(recipient)).isEmpty();
    // a slot-only account (no account entry) still gets the STORAGE flag
    final TransactionDiff slotOnly =
        TransactionDiff.builder().slot(untouched, UInt256.ONE, UInt256.ZERO, UInt256.ONE).build();
    assertThat(slotOnly.accountChangeFlags(untouched)).isEqualTo(FLAG_STORAGE);
  }

  @Test
  void slotViewMapsLocalIndicesOntoTheGlobalTable() {
    final TransactionDiff diff =
        TransactionDiff.builder()
            .slot(HIGH, UInt256.valueOf(0xb), UInt256.ZERO, UInt256.ONE)
            .slot(HIGH, UInt256.valueOf(0xa), UInt256.ZERO, UInt256.ONE)
            .slot(LOW, UInt256.valueOf(0xb), UInt256.ZERO, UInt256.ONE)
            .slot(LOW, UInt256.valueOf(0xa), UInt256.ZERO, UInt256.ONE)
            .build();

    assertThat(diff.slotIndicesOf(LOW)).containsExactly(0, 1);
    assertThat(diff.slotIndicesOf(HIGH)).containsExactly(2, 3);
    assertThat(diff.slots().get(2).key()).isEqualTo(UInt256.valueOf(0xa));
  }

  @Test
  void eventAndTopicViewsFollowTheFixture() {
    final Address a = Address.fromHexString("0x0a");
    final Address b = Address.fromHexString("0x0b");
    final Address c = Address.fromHexString("0x0c");
    final TransactionDiff diff =
        TransactionDiff.builder()
            .logs(
                List.of(
                    log(a, 0, T1, T3, T2),
                    log(b, 0, T1),
                    log(a, 0, T1, T3, T2),
                    log(c, 0, T1, T2, T3, T4)))
            .build();

    assertThat(diff.events()).hasSize(4);
    assertThat(diff.eventIndicesOf(a)).containsExactly(0, 2);
    assertThat(diff.eventIndicesOf(b)).containsExactly(1);
    assertThat(diff.eventIndicesOf(c)).containsExactly(3);
    assertThat(diff.eventIndicesOf(OTHER)).isEmpty();
    assertThat(diff.eventIndicesWithTopic(T2)).containsExactly(0, 2, 3);
    assertThat(diff.eventIndicesWithTopic(T3)).containsExactly(0, 2, 3);
    assertThat(diff.eventIndicesWithTopic(T4)).containsExactly(3);
    assertThat(diff.eventIndicesWithTopic(T1)).isEmpty();
    assertThat(diff.eventIndicesWithTopic(Bytes32.fromHexString("0x" + "99".repeat(32)))).isEmpty();
  }

  @Test
  void eventCarryingATopicTwiceIsListedOnce() {
    final TransactionDiff diff =
        TransactionDiff.builder().logs(List.of(log(LOW, 1, T1, T2, T2, T3))).build();

    assertThat(diff.eventIndicesWithTopic(T2)).containsExactly(0);
  }

  @Test
  void topicsAreComparedAsThirtyTwoByteValues() {
    final TransactionDiff diff =
        TransactionDiff.builder().logs(List.of(log(LOW, 40, T1, SHORT))).build();

    // the lookup key may come from a v1 stack word, which is not 32 bytes wide
    assertThat(diff.eventIndicesWithTopic(Bytes32.leftPad(Bytes.of(0x7f, 0x06))))
        .containsExactly(0);
    assertThat(diff.events().get(0).topics()).containsExactly(T1, SHORT);
    assertThat(diff.events().get(0).topics().get(1)).isInstanceOf(Bytes32.class);
    assertThat(diff.events().get(0).data().size()).isEqualTo(40);
  }

  @Test
  void emptyDiffHasEmptyTablesAndViews() {
    assertThat(TransactionDiff.EMPTY.balances()).isEmpty();
    assertThat(TransactionDiff.EMPTY.slots()).isEmpty();
    assertThat(TransactionDiff.EMPTY.deployed()).isEmpty();
    assertThat(TransactionDiff.EMPTY.events()).isEmpty();
    assertThat(TransactionDiff.EMPTY.eventIndicesWithTopic(T1)).isEmpty();
    assertThat(TransactionDiff.EMPTY.accountChangeFlags(LOW)).isZero();
  }
}
