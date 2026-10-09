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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.evm.v2.ProgramRun.CONTRACT;

import org.hyperledger.besu.crypto.Hash;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.v2.ProgramRun.Execution;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

/** Runs the create operations of EVM v2 through child frames, as a transaction does. */
class CreateOperationsV2Test {

  private static final EVM EVM_V2 = MainnetEVMs.cancun(ProgramRun.V2);

  // PUSH1 0xaa PUSH1 0 MSTORE8 PUSH1 1 PUSH1 0 RETURN: deploys the code 0xaa
  private static final Bytes INITCODE = Bytes.fromHexString("0x60aa60005360016000f3");
  // PUSH1 0x2a PUSH1 0 MSTORE PUSH1 0x20 PUSH1 0 REVERT
  private static final Bytes REVERTING_INITCODE = Bytes.fromHexString("0x602a60005260206000fd");

  private static final Bytes32 SALT = Bytes32.leftPad(Bytes.of(0x42));

  @Test
  void createDeploysTheCodeAtTheNonceAddress() {
    final Execution execution = execute(create(INITCODE, false), false);

    final Address created = Address.contractAddress(CONTRACT, 0);
    assertThat(execution.outcome().haltReason()).isEmpty();
    assertThat(execution.outcome().stack()).containsExactly(stackItem(created));
    assertThat(execution.worldUpdater().get(created).getCode()).isEqualTo(Bytes.of(0xaa));
    assertThat(execution.worldUpdater().get(CONTRACT).getNonce()).isEqualTo(1);
    assertThat(execution.frame().getReturnData()).isEqualTo(Bytes.EMPTY);
  }

  @Test
  void create2DeploysTheCodeAtTheSaltedAddress() {
    final Execution execution = execute(create(INITCODE, true), false);

    final Address created =
        Address.extract(
            Hash.keccak256(
                Bytes.concatenate(
                    Bytes.of(0xff), CONTRACT.getBytes(), SALT, Hash.keccak256(INITCODE))));
    assertThat(execution.outcome().haltReason()).isEmpty();
    assertThat(execution.outcome().stack()).containsExactly(stackItem(created));
    assertThat(execution.worldUpdater().get(created).getCode()).isEqualTo(Bytes.of(0xaa));
  }

  @Test
  void createPushesZeroAndKeepsTheRevertData() {
    final Execution execution = execute(create(REVERTING_INITCODE, false), false);

    assertThat(execution.outcome().haltReason()).isEmpty();
    assertThat(execution.outcome().stack()).containsExactly(stackItem(Bytes32.ZERO));
    assertThat(execution.frame().getReturnData()).isEqualTo(Bytes32.leftPad(Bytes.of(0x2a)));
    assertThat(execution.worldUpdater().get(Address.contractAddress(CONTRACT, 0))).isNull();
  }

  @Test
  void createHaltsInAStaticFrame() {
    final Execution execution = execute(create(INITCODE, false), true);

    assertThat(execution.outcome().haltReason())
        .isEqualTo(ExceptionalHaltReason.ILLEGAL_STATE_CHANGE.name());
  }

  @Test
  void createHaltsOnAnInitcodeOverTheLimit() {
    // CREATE with value 0, offset 0 and a size of 0xc001, one byte over the limit
    final Execution execution = execute(Bytes.fromHexString("0x61c00160006000f000"), false);

    assertThat(execution.outcome().haltReason())
        .isEqualTo(ExceptionalHaltReason.CODE_TOO_LARGE.name());
  }

  @Test
  void createHaltsOnStackUnderflow() {
    final Execution execution = execute(Bytes.fromHexString("0x60006000f000"), false);

    assertThat(execution.outcome().haltReason())
        .isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS.name());
  }

  private static Execution execute(final Bytes code, final boolean isStatic) {
    return ProgramRun.execute(
        EVM_V2, code, Bytes.EMPTY, Bytes.EMPTY, 1_000_000L, isStatic, List.of(Bytes32.ZERO));
  }

  /** The code of a CREATE, or a CREATE2 with SALT, of the initcode, which is stored first. */
  private static Bytes create(final Bytes initcode, final boolean create2) {
    final int offset = 32 - initcode.size();
    return Bytes.concatenate(
        // PUSH initcode, PUSH1 0, MSTORE: the initcode ends at byte 32
        Bytes.of(0x5f + initcode.size()),
        initcode,
        Bytes.fromHexString("0x600052"),
        create2 ? Bytes.concatenate(Bytes.of(0x7f), SALT) : Bytes.EMPTY,
        // size, offset, value
        Bytes.of(0x60, initcode.size(), 0x60, offset, 0x60, 0x00),
        Bytes.of(create2 ? 0xf5 : 0xf0, 0x00));
  }

  /** A stack item as the outcome lists it. */
  private static String stackItem(final Address address) {
    return stackItem(Bytes32.leftPad(address.getBytes()));
  }

  private static String stackItem(final Bytes32 value) {
    return value.toUnprefixedHexString();
  }
}
