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
package org.hyperledger.besu.ethereum.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.GWei;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt64;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class BlockSizeTest {

  static final Map<Block, Bytes> blocksAndRlp = new HashMap<>();

  @BeforeAll
  public static void before() {
    BlockchainSetupUtil.forMainnet()
        .getBlocks()
        .forEach(
            b -> {
              final BytesValueRLPOutput out = new BytesValueRLPOutput();
              b.writeTo(out);
              blocksAndRlp.put(b, out.encoded());
            });
  }

  @Test
  public void blocksReadFromRlpShouldReturnCorrectSize() {
    blocksAndRlp.forEach(
        (b, r) -> {
          final BytesValueRLPInput rlp = new BytesValueRLPInput(r, false);
          final Block block = Block.readFrom(rlp, new MainnetBlockHeaderFunctions());
          assertThat(block.getSize()).isEqualTo(r.size());
        });
  }

  @Test
  public void blocksInstantiatedWithoutLengthShouldReturnCorrectSize() {
    blocksAndRlp.forEach(
        (b, r) -> {
          final Block block = new Block(b.getHeader(), b.getBody());
          assertThat(block.getSize()).isEqualTo(r.size());
        });
  }

  @Test
  public void blockOfEveryTransactionTypeAndWithdrawalsHasTheSizeOfItsEncoding() {
    final KeyPair keys = SignatureAlgorithmFactory.getInstance().generateKeyPair();
    final List<Transaction> transactions = new ArrayList<>();
    for (final TransactionType type :
        List.of(
            TransactionType.FRONTIER,
            TransactionType.ACCESS_LIST,
            TransactionType.EIP1559,
            TransactionType.BLOB,
            TransactionType.DELEGATE_CODE)) {
      // short and long encodings, whose byte strings have one and two length bytes
      for (final Bytes payload : List.of(Bytes.EMPTY, Bytes.random(300))) {
        final TransactionTestFixture fixture = new TransactionTestFixture().type(type);
        if (type == TransactionType.BLOB) {
          fixture.versionedHashes(Optional.of(List.of(VersionedHash.DEFAULT_VERSIONED_HASH)));
        }
        transactions.add(fixture.payload(payload).createTransaction(keys));
      }
    }
    final List<Withdrawal> withdrawals =
        List.of(
            new Withdrawal(
                UInt64.valueOf(9001),
                UInt64.valueOf(313),
                Address.fromHexString("0x000000000000000000000000000000000000c0de"),
                GWei.of(11)));
    final BlockBody body = new BlockBody(transactions, List.of(), Optional.of(withdrawals));
    final Block built = new Block(new BlockHeaderTestFixture().buildHeader(), body);
    final Bytes rlp = built.toRlp();

    assertThat(built.getSize()).isEqualTo(rlp.size());
    // decoded transactions keep the size of their encoding
    final Block decoded = Block.readFrom(RLP.input(rlp), new MainnetBlockHeaderFunctions());
    assertThat(new Block(decoded.getHeader(), decoded.getBody()).getSize()).isEqualTo(rlp.size());
  }
}
