/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.core.encoding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.CodeChange;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/** EIP-7928 encodes every uint256 field of a block access list as a minimal RLP scalar. */
class BlockAccessListDecoderTest {

  private static final Address ADDRESS =
      Address.fromHexString("0x00000000000000000000000000000000000000aa");
  private static final UInt256 SLOT = UInt256.ONE;
  private static final UInt256 VALUE = UInt256.valueOf(2);
  private static final UInt256 READ = UInt256.valueOf(3);
  private static final UInt256 BALANCE = UInt256.valueOf(4);
  private static final Bytes CODE = Bytes.fromHexString("0x6001600101");
  private static final Hash CODE_HASH = Hash.hash(CODE);

  /** The same scalar as {@code writeUInt256Scalar} emits, plus a redundant leading zero byte. */
  private static void writeNonMinimal(final RLPOutput out, final UInt256 value) {
    out.writeBytes(Bytes.concatenate(Bytes.of(0), value.trimLeadingZeros()));
  }

  private static Bytes encodeBlockAccessList(final String nonMinimalField) {
    return RLP.encode(
        out -> {
          out.startList();
          out.startList();
          out.writeBytes(ADDRESS.getBytes());

          out.startList(); // storage changes
          out.startList();
          if ("storage_slot".equals(nonMinimalField)) {
            writeNonMinimal(out, SLOT);
          } else {
            out.writeUInt256Scalar(SLOT);
          }
          out.startList();
          out.startList();
          out.writeUnsignedInt(1);
          if ("storage_value".equals(nonMinimalField)) {
            writeNonMinimal(out, VALUE);
          } else {
            out.writeUInt256Scalar(VALUE);
          }
          out.endList();
          out.endList();
          out.endList();
          out.endList();

          out.startList(); // storage reads
          if ("storage_read".equals(nonMinimalField)) {
            writeNonMinimal(out, READ);
          } else {
            out.writeUInt256Scalar(READ);
          }
          out.endList();

          out.startList(); // balance changes
          out.startList();
          out.writeUnsignedInt(1);
          if ("balance".equals(nonMinimalField)) {
            writeNonMinimal(out, BALANCE);
          } else {
            out.writeUInt256Scalar(BALANCE);
          }
          out.endList();
          out.endList();

          out.startList(); // nonce changes
          out.endList();
          out.startList(); // code changes
          out.endList();

          out.endList();
          out.endList();
        });
  }

  private static BlockAccessList decode(final Bytes encoded) {
    return BlockAccessListDecoder.decode(new BytesValueRLPInput(encoded, false));
  }

  @Test
  void decodesMinimallyEncodedScalars() {
    final BlockAccessList bal = decode(encodeBlockAccessList(null));

    assertThat(bal.accountChanges()).hasSize(1);
    final BlockAccessList.AccountChanges account = bal.accountChanges().getFirst();
    assertThat(account.address()).isEqualTo(ADDRESS);
    assertThat(account.storageChanges().getFirst().slot().getSlotKey()).contains(SLOT);
    assertThat(account.storageChanges().getFirst().changes().getFirst().newValue())
        .isEqualTo(VALUE);
    assertThat(account.storageReads().getFirst().slot().getSlotKey()).contains(READ);
    assertThat(account.balanceChanges().getFirst().postBalance().toUInt256()).isEqualTo(BALANCE);
  }

  @Test
  void keepsItsOwnEncodingWhenReadFromALargerInput() {
    final Bytes blockAccessList = encodeBlockAccessList(null);
    // e.g. a consensus message carrying a block access list between other fields
    final RLPInput message =
        RLP.input(
            RLP.encode(
                out -> {
                  out.startList();
                  out.writeLongScalar(1);
                  out.writeRaw(blockAccessList);
                  out.writeLongScalar(2);
                  out.endList();
                }));
    message.enterList();
    message.skipNext();

    final BlockAccessList decoded = BlockAccessListDecoder.decode(message);

    assertThat(decoded.rawRlp()).contains(blockAccessList);
    assertThat(message.readLongScalar()).isEqualTo(2);
  }

  @ParameterizedTest
  @ValueSource(strings = {"storage_slot", "storage_value", "storage_read", "balance"})
  void rejectsNonMinimallyEncodedScalars(final String field) {
    assertThatThrownBy(() -> decode(encodeBlockAccessList(field)))
        .isInstanceOf(RLPException.class)
        .hasMessageContaining("leading zero");
  }

  /** EIP-8298: a code change is {@code [index, code]} or {@code [index, b"", code_hash]}. */
  @Test
  void roundTripsBothCodeChangeForms() {
    final BlockAccessList original =
        new BlockAccessList(
            List.of(
                new BlockAccessList.AccountChanges(
                    ADDRESS,
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of(new CodeChange(1, CODE), CodeChange.adoptedCode(2, CODE_HASH)))));
    final Bytes encoded = original.encode();

    final BlockAccessList decoded = decode(encoded);

    assertThat(decoded).isEqualTo(original);
    assertThat(decoded.encode()).isEqualTo(encoded);
    final List<List<Bytes>> codeChanges =
        codeChangesOf(encoded).readList(entry -> entry.readList(RLPInput::readBytes));
    assertThat(codeChanges)
        .isEqualTo(
            List.of(
                List.of(Bytes.of(1), CODE),
                List.of(Bytes.of(2), Bytes.EMPTY, CODE_HASH.getBytes())));
  }

  static Stream<Arguments> malformedCodeChanges() {
    return Stream.of(
        Arguments.of(
            "hash form with code",
            (Consumer<RLPOutput>)
                out -> {
                  out.writeUnsignedInt(1);
                  out.writeBytes(CODE);
                  out.writeBytes(CODE_HASH.getBytes());
                }),
        Arguments.of(
            "four elements",
            (Consumer<RLPOutput>)
                out -> {
                  out.writeUnsignedInt(1);
                  out.writeBytes(Bytes.EMPTY);
                  out.writeBytes(CODE_HASH.getBytes());
                  out.writeBytes(Bytes.EMPTY);
                }),
        Arguments.of("index only", (Consumer<RLPOutput>) out -> out.writeUnsignedInt(1)),
        Arguments.of(
            "short hash",
            (Consumer<RLPOutput>)
                out -> {
                  out.writeUnsignedInt(1);
                  out.writeBytes(Bytes.EMPTY);
                  out.writeBytes(CODE_HASH.getBytes().slice(1));
                }),
        Arguments.of(
            "long hash",
            (Consumer<RLPOutput>)
                out -> {
                  out.writeUnsignedInt(1);
                  out.writeBytes(Bytes.EMPTY);
                  out.writeBytes(Bytes.concatenate(CODE_HASH.getBytes(), Bytes.of(0)));
                }),
        Arguments.of(
            "hash as a list",
            (Consumer<RLPOutput>)
                out -> {
                  out.writeUnsignedInt(1);
                  out.writeBytes(Bytes.EMPTY);
                  out.writeList(List.of(CODE_HASH.getBytes()), (hash, o) -> o.writeBytes(hash));
                }),
        Arguments.of(
            "code as a list",
            (Consumer<RLPOutput>)
                out -> {
                  out.writeUnsignedInt(1);
                  out.writeList(List.of(), (code, o) -> {});
                }));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("malformedCodeChanges")
  void rejectsMalformedCodeChanges(final String name, final Consumer<RLPOutput> entry) {
    final Bytes encoded =
        RLP.encode(
            out -> {
              out.startList();
              out.startList();
              out.writeBytes(ADDRESS.getBytes());
              out.startList(); // storage changes
              out.endList();
              out.startList(); // storage reads
              out.endList();
              out.startList(); // balance changes
              out.endList();
              out.startList(); // nonce changes
              out.endList();
              out.startList(); // code changes
              out.startList();
              entry.accept(out);
              out.endList();
              out.endList();
              out.endList();
              out.endList();
            });

    assertThatThrownBy(() -> decode(encoded)).isInstanceOf(RLPException.class);
  }

  private static RLPInput codeChangesOf(final Bytes encodedBlockAccessList) {
    final RLPInput in = RLP.input(encodedBlockAccessList);
    in.enterList();
    in.enterList();
    for (int field = 0; field < 5; field++) {
      in.skipNext();
    }
    return in;
  }
}
