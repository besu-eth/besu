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
package org.hyperledger.besu.ethereum.vm.operations.v2;

import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.frame.BlockValues;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.Random;

import org.apache.tuweni.bytes.Bytes32;

public class BenchmarkHelperV2 {
  /**
   * Creates a minimal {@link MessageFrame} suitable for opcode benchmarks.
   *
   * <p>The frame is configured with mocked dependencies and deterministic zero/default values.
   *
   * @return a message-call frame ready to use in benchmark setup
   */
  public static MessageFrame createMessageCallFrame() {
    return MessageFrame.builder()
        .enableEvmV2(true)
        .worldUpdater(mock(WorldUpdater.class))
        .originator(Address.ZERO)
        .gasPrice(Wei.ONE)
        .blobGasPrice(Wei.ONE)
        .blockValues(mock(BlockValues.class))
        .miningBeneficiary(Address.ZERO)
        .blockHashLookup((__, ___) -> Hash.ZERO)
        .type(MessageFrame.Type.MESSAGE_CALL)
        .initialGas(Long.MAX_VALUE)
        .address(Address.ZERO)
        .contract(Address.ZERO)
        .inputData(Bytes32.ZERO)
        .sender(Address.ZERO)
        .value(Wei.ZERO)
        .apparentValue(Wei.ZERO)
        .code(Code.EMPTY_CODE)
        .completer(__ -> {})
        .build();
  }

  /**
   * Fills an array with random UInt256 values.
   *
   * @param pool the destination array
   */
  public static void fillUInt256Pool(final org.hyperledger.besu.evm.UInt256[] pool) {
    final Random random = new Random();
    for (int i = 0; i < pool.length; i++) {
      final byte[] a = new byte[1 + random.nextInt(32)];
      random.nextBytes(a);
      pool[i] = bytesToUInt256(a);
    }
  }

  /**
   * Converts a byte array to UInt256 (left-pads to 32 bytes).
   *
   * @param bytes the byte array
   * @return the UInt256 value
   */
  static org.hyperledger.besu.evm.UInt256 bytesToUInt256(final byte[] bytes) {
    final byte[] padded = new byte[32];
    System.arraycopy(bytes, 0, padded, 32 - bytes.length, bytes.length);
    return org.hyperledger.besu.evm.UInt256.fromBytesBE(padded);
  }

  /**
   * Pushes a UInt256 value onto the frame's stack by writing limbs directly.
   *
   * @param frame the message frame
   * @param value the UInt256 value to push
   */
  static void pushUInt256(final MessageFrame frame, final org.hyperledger.besu.evm.UInt256 value) {
    final long[] s = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final int dst = top << 2;
    s[dst] = value.u3();
    s[dst + 1] = value.u2();
    s[dst + 2] = value.u1();
    s[dst + 3] = value.u0();
    frame.setTopV2(top + 1);
  }
}
