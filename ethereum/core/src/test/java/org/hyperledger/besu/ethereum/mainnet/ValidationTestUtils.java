/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.mainnet;

import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import com.google.common.io.Resources;
import org.apache.tuweni.bytes.Bytes;

public final class ValidationTestUtils {

  public static BlockHeader readHeader(final long num) throws IOException {
    final RLPInput input =
        new BytesValueRLPInput(
            Bytes.wrap(
                Resources.toByteArray(
                    ValidationTestUtils.class.getResource(String.format("block_%d.blocks", num)))),
            false);
    input.enterList();
    return BlockHeader.readFrom(input, new MainnetBlockHeaderFunctions());
  }

  public static BlockBody readBody(final long num) throws IOException {
    final RLPInput input =
        new BytesValueRLPInput(
            Bytes.wrap(
                Resources.toByteArray(
                    ValidationTestUtils.class.getResource(String.format("block_%d.blocks", num)))),
            false);
    input.enterList();
    input.skipNext();
    final List<Transaction> transactions = input.readList(Transaction::readFrom);
    final List<BlockHeader> ommers =
        input.readList(rlp -> BlockHeader.readFrom(rlp, new MainnetBlockHeaderFunctions()));
    final Optional<List<Withdrawal>> withdrawals =
        input.isEndOfCurrentList()
            ? Optional.empty()
            : Optional.of(input.readList(Withdrawal::readFrom));
    return new BlockBody(transactions, ommers, withdrawals);
  }

  public static Block readBlock(final long num) throws IOException {
    final RLPInput input =
        new BytesValueRLPInput(
            Bytes.wrap(
                Resources.toByteArray(
                    ValidationTestUtils.class.getResource(String.format("block_%d.blocks", num)))),
            false);
    return Block.readFrom(input, new MainnetBlockHeaderFunctions());
  }
}
