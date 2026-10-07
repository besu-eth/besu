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
package org.hyperledger.besu.consensus.clique;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.LogsBloomFilter;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.Difficulty;

import java.util.Arrays;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

public class CliqueBlockHashingTest {

  private final BlockHeader expectedHeader = createKnownHeaderFromCapturedData();

  // clique.getSignersAtHash("0x8b27a29300811af926039b90288d3d384dcb55931049c17c4f762e45c116776e")
  private static final List<Address> VALIDATORS_IN_HEADER =
      Arrays.asList(
          Address.fromHexString("0x42eb768f2244c8811c63729a21a3569731535f06"),
          Address.fromHexString("0x7ffc57839b00206d1ad20c69a1981b489f772031"),
          Address.fromHexString("0xb279182d99e65703f0076e4812653aab85fca0f0"));
  private static final Hash KNOWN_BLOCK_HASH =
      Hash.fromHexString("0x8b27a29300811af926039b90288d3d384dcb55931049c17c4f762e45c116776e");

  @Test
  public void recoverProposerAddressFromSeal() {
    final CliqueExtraData cliqueExtraData = CliqueExtraData.decode(expectedHeader);
    final Address proposerAddress =
        CliqueBlockHashing.recoverProposerAddress(expectedHeader, cliqueExtraData);

    assertThat(VALIDATORS_IN_HEADER.contains(proposerAddress)).isTrue();
  }

  @Test
  public void recoverProposerAddressForGenesisBlockReturnsAddressZero() {
    final BlockHeader genesisBlockHeader = createGenesisBlock();
    final CliqueExtraData cliqueExtraData = CliqueExtraData.decode(genesisBlockHeader);
    final Address proposerAddress =
        CliqueBlockHashing.recoverProposerAddress(genesisBlockHeader, cliqueExtraData);

    assertThat(proposerAddress).isEqualTo(Address.ZERO);
  }

  @Test
  public void readValidatorListFromExtraData() {
    final CliqueExtraData cliqueExtraData = CliqueExtraData.decode(expectedHeader);
    assertThat(cliqueExtraData.getValidators()).isEqualTo(VALIDATORS_IN_HEADER);
  }

  @Test
  public void calculateBlockHash() {
    assertThat(expectedHeader.getHash()).isEqualTo(KNOWN_BLOCK_HASH);
  }

  private BlockHeader createKnownHeaderFromCapturedData() {
    // The following text was a dump from the geth console of the 30_000 block on Rinkeby.
    // eth.getBlock(30000)
    final BlockHeaderBuilder builder = new BlockHeaderBuilder();
    builder.difficulty(Difficulty.of(2));
    builder.extraData(
        Bytes.fromHexString(
            "0xd783010600846765746887676f312e372e33856c696e7578000000000000000042eb768f2244c8811c63729a21a3569731535f067ffc57839b00206d1ad20c69a1981b489f772031b279182d99e65703f0076e4812653aab85fca0f0c5bc40d0535af16266714ccb26fc49448c10bdf2969411514707d7442956b3397b09a980f4bea9347f70eea52183326247a0239b6d01fa0b07afc44e8a05463301"));
    builder.gasLimit(4712388);
    builder.gasUsed(0);
    // Do not do Hash.
    builder.logsBloom(
        LogsBloomFilter.fromHexString(
            "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"));
    builder.coinbase(Address.fromHexString("0x0000000000000000000000000000000000000000"));
    builder.mixHash(
        Hash.fromHexString("0x0000000000000000000000000000000000000000000000000000000000000000"));
    builder.nonce(0);
    builder.number(30000);
    builder.parentHash(
        Hash.fromHexString("0xff570bb9893cb9bac64e346419fb9ad51e203c1cf6da5cfcc0c0dff3351b454b"));
    builder.receiptsRoot(
        Hash.fromHexString("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"));
    builder.ommersHash(
        Hash.fromHexString("0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347"));
    builder.stateRoot(
        Hash.fromHexString("0x829b58faacea7b0aa625617ba90c93e08150bed160364a7a96505e8205043d34"));
    builder.timestamp(1492460444);
    builder.transactionsRoot(
        Hash.fromHexString("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"));

    builder.blockHeaderFunctions(new CliqueBlockHeaderFunctions());

    return builder.buildBlockHeader();
  }

  private BlockHeader createGenesisBlock() {
    // The following was taken from the Rinkeby genesis file
    final BlockHeaderBuilder builder = new BlockHeaderBuilder();
    builder.difficulty(Difficulty.ONE);
    builder.extraData(
        Bytes.fromHexString(
            "0x52657370656374206d7920617574686f7269746168207e452e436172746d616e42eb768f2244c8811c63729a21a3569731535f067ffc57839b00206d1ad20c69a1981b489f772031b279182d99e65703f0076e4812653aab85fca0f00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"));
    builder.gasLimit(4700000);
    builder.gasUsed(0);
    // Do not do Hash.
    builder.logsBloom(
        LogsBloomFilter.fromHexString(
            "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"));
    builder.coinbase(Address.fromHexString("0x0000000000000000000000000000000000000000"));
    builder.mixHash(Hash.ZERO);
    builder.nonce(0);
    builder.number(0);
    builder.parentHash(Hash.ZERO);
    builder.receiptsRoot(Hash.ZERO);
    builder.ommersHash(Hash.ZERO);
    builder.stateRoot(Hash.ZERO);
    builder.timestamp(1492009146);
    builder.transactionsRoot(Hash.ZERO);

    builder.blockHeaderFunctions(new CliqueBlockHeaderFunctions());

    return builder.buildBlockHeader();
  }
}
