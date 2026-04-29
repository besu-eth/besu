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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.hyperledger.besu.crypto.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.AbstractJsonRpcHttpServiceTest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameterOrBlockHash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.util.Optional;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * This test only exercises bonsai worldstate since forest is essentially a no-op for moving the
 * worldstate.
 */
public class DebugSetHeadTest extends AbstractJsonRpcHttpServiceTest {

  DebugSetHead debugSetHead;
  Blockchain blockchain;
  WorldStateArchive archive;
  ProtocolContext protocolContext;
  ProtocolSchedule protocolSchedule;

  @Override
  @BeforeEach
  public void setup() throws Exception {
    setupBonsaiBlockchain();
    blockchain = blockchainSetupUtil.getBlockchain();
    protocolContext = blockchainSetupUtil.getProtocolContext();
    protocolSchedule = blockchainSetupUtil.getProtocolSchedule();
    ;
    archive = blockchainSetupUtil.getWorldArchive();
    debugSetHead =
        new DebugSetHead(
            new BlockchainQueries(
                protocolSchedule, blockchain, archive, MiningConfiguration.MINING_DISABLED),
            protocolContext);
    startService();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"0x01", "0x4e9a67b663f9abe03e7e9fd5452c9497998337077122f44ee78a466f6a7358de"})
  public void assertOnlyChainHeadMovesWorldParameterAbsent(final String blockParam) {
    var chainTip = blockchain.getChainHead().getBlockHeader();
    var blockOne = getBlockHeaderForHashOrNumber(blockParam).orElse(null);

    assertThat(blockOne).isNotNull();
    assertThat(blockOne).isNotEqualTo(chainTip);

    // move the head to param val, number or hash
    debugSetHead.response(debugSetHead(blockParam, Optional.empty()));

    // get the new chainTip:
    var newChainTip = blockchain.getChainHead().getBlockHeader();

    // assert the chain moved, and the worldstate did not
    assertThat(newChainTip).isEqualTo(blockOne);
    assertThat(archive.getWorldState().rootHash()).isEqualTo(chainTip.getStateRoot());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "0x01",
        "0x02",
        "0x3d813a0ffc9cd04436e17e3e9c309f1e80df0407078e50355ce0d570b5424812",
        "0x4e9a67b663f9abe03e7e9fd5452c9497998337077122f44ee78a466f6a7358de"
      })
  public void assertOnlyChainHeadMoves(final String blockParam) {
    var chainTip = blockchain.getChainHead().getBlockHeader();
    var blockOne = getBlockHeaderForHashOrNumber(blockParam).orElse(null);

    assertThat(blockOne).isNotNull();
    assertThat(blockOne).isNotEqualTo(chainTip);

    // move the head to param val, number or hash
    debugSetHead.response(debugSetHead(blockParam, Optional.of(FALSE)));

    // get the new chainTip:
    var newChainTip = blockchain.getChainHead().getBlockHeader();

    // assert the chain moved, and the worldstate did not
    assertThat(newChainTip).isEqualTo(blockOne);
    assertThat(archive.getWorldState().rootHash()).isEqualTo(chainTip.getStateRoot());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "0x01",
        "0x02",
        "0x3d813a0ffc9cd04436e17e3e9c309f1e80df0407078e50355ce0d570b5424812",
        "0x4e9a67b663f9abe03e7e9fd5452c9497998337077122f44ee78a466f6a7358de"
      })
  public void assertBothChainHeadAndWorldStatByNumber(final String blockParam) {
    var chainTip = blockchain.getChainHead().getBlockHeader();
    var blockOne = getBlockHeaderForHashOrNumber(blockParam).orElse(null);

    assertThat(blockOne).isNotNull();
    assertThat(blockOne).isNotEqualTo(chainTip);

    // move the head and worldstate to param val number or hash
    debugSetHead.response(debugSetHead(blockParam, Optional.of(TRUE)));

    // get the new chainTip:
    var newChainTip = blockchain.getChainHead().getBlockHeader();

    // assert both the chain and worldstate moved to block one
    assertThat(newChainTip).isEqualTo(blockOne);
    assertThat(archive.getWorldState().rootHash()).isEqualTo(blockOne.getStateRoot());
  }

  @Test
  public void assertNullWhenBlockNotFound() {
    var chainTip = blockchain.getChainHead().getBlockHeader();

    // move the head to number just after chain head
    var resp =
        debugSetHead.response(debugSetHead("" + chainTip.getNumber() + 1, Optional.of(TRUE)));
    // success with null result if block not found
    assertThat(resp.getType()).isEqualTo(RpcResponseType.SUCCESS);
    assertThat(((JsonRpcSuccessResponse) resp).getResult()).isNull();

    // move the head to some arbitrary hash
    var resp2 =
        debugSetHead.response(
            debugSetHead(
                Hash.keccak256(Bytes.fromHexString("0xdeadbeef")).toHexString(),
                Optional.of(TRUE)));

    // success with null result if block not found
    assertThat(resp2.getType()).isEqualTo(RpcResponseType.SUCCESS);
    assertThat(((JsonRpcSuccessResponse) resp2).getResult()).isNull();

    // get the new chainTip:
    var newChainTip = blockchain.getChainHead().getBlockHeader();

    // assert neither the chain nor the worldstate moved
    assertThat(newChainTip).isEqualTo(chainTip);
    assertThat(archive.getWorldState().rootHash()).isEqualTo(chainTip.getStateRoot());
  }

  private JsonRpcRequestContext debugSetHead(
      final String numberOrHash, final Optional<Boolean> moveWorldState) {
    if (moveWorldState.isPresent()) {
      return new JsonRpcRequestContext(
          new JsonRpcRequest(
              "2.0", "debug_setHead", new Object[] {numberOrHash, moveWorldState.get()}));
    } else {
      return new JsonRpcRequestContext(
          new JsonRpcRequest("2.0", "debug_setHead", new Object[] {numberOrHash}));
    }
  }

  private Optional<BlockHeader> getBlockHeaderForHashOrNumber(final String input) {
    try {
      var param = new BlockParameterOrBlockHash(input);
      if (param.getHash().isPresent()) {
        return blockchain.getBlockHeader(param.getHash().get());
      } else if (param.getNumber().isPresent()) {
        return blockchain.getBlockHeader(param.getNumber().getAsLong());
      }
    } catch (JsonProcessingException ignored) {
      // meh
    }
    return Optional.empty();
  }
}
