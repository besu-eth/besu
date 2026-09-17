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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BadBlockResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlockResultFactory;
import org.hyperledger.besu.ethereum.chain.BadBlockCause;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
public class DebugGetBadBlockTest {

  private final TransactionTestFixture transactionTestFixture = new TransactionTestFixture();

  private final ProtocolContext protocolContext = mock(ProtocolContext.class);
  private final BlockResultFactory blockResult = new BlockResultFactory();
  private final BadBlockManager badBlockManager = new BadBlockManager();

  private final DebugGetBadBlocks debugGetBadBlocks =
      new DebugGetBadBlocks(protocolContext, blockResult);

  @BeforeEach
  public void setup() {
    when(protocolContext.getBadBlockManager()).thenReturn(badBlockManager);
  }

  @Test
  public void nameShouldBeDebugGetBadBlocks() {
    assertThat(debugGetBadBlocks.getName()).isEqualTo("debug_getBadBlocks");
  }

  @Test
  public void shouldReturnCorrectResponse() {

    final KeyPair keyPair = SignatureAlgorithmFactory.getInstance().generateKeyPair();
    final List<Transaction> transactions = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      transactions.add(transactionTestFixture.createTransaction(keyPair));
    }

    final Block parentBlock =
        new BlockDataGenerator()
            .block(
                BlockDataGenerator.BlockOptions.create()
                    .setBlockHeaderFunctions(new MainnetBlockHeaderFunctions()));
    final Block badBlockWithTransaction =
        new BlockDataGenerator()
            .block(
                BlockDataGenerator.BlockOptions.create()
                    .addTransaction(transactions)
                    .setBlockNumber(1)
                    .setBlockHeaderFunctions(new MainnetBlockHeaderFunctions())
                    .setParentHash(parentBlock.getHash()));
    final Block badBlockWoTransaction =
        new BlockDataGenerator()
            .block(
                BlockDataGenerator.BlockOptions.create()
                    .setBlockNumber(2)
                    .hasTransactions(false)
                    .setBlockHeaderFunctions(new MainnetBlockHeaderFunctions())
                    .setParentHash(parentBlock.getHash()));

    badBlockManager.addBadBlock(
        badBlockWithTransaction, BadBlockCause.fromValidationFailure("failed"));
    badBlockManager.addBadBlock(
        badBlockWoTransaction, BadBlockCause.fromValidationFailure("failed"));

    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", "debug_getBadBlocks", new Object[] {}));

    final JsonRpcSuccessResponse response =
        (JsonRpcSuccessResponse) debugGetBadBlocks.response(request);
    final Collection<BadBlockResult> result = (Collection<BadBlockResult>) response.getResult();
    assertThat(result).hasSize(2);

    for (BadBlockResult badBlockResult : result) {
      if (badBlockResult.getBlockResult().getNumber().equals("0x1")) {
        assertThat(badBlockResult.getBlockResult().getTransactions().size()).isEqualTo(3);
      } else if (badBlockResult.getBlockResult().getNumber().equals("0x2")) {
        assertThat(badBlockResult.getBlockResult().getTransactions()).isEmpty();
      } else {
        fail("Invalid response");
      }
      assertThat(badBlockResult.getRlp()).isNotEmpty();
      assertThat(badBlockResult.getHash()).isNotEmpty();
      assertThat(badBlockResult.getBlockResult().getNonce()).isNotEmpty();
      assertThat(badBlockResult.getGeneratedBlockAccessList()).isEmpty();
    }
  }

  @Test
  public void shouldIncludeBlockAccessListInResponse() {
    final BlockDataGenerator blockDataGenerator = new BlockDataGenerator();
    final Block baseBlock =
        blockDataGenerator.block(
            BlockDataGenerator.BlockOptions.create()
                .setBlockNumber(1)
                .setBlockHeaderFunctions(new MainnetBlockHeaderFunctions()));

    final BlockAccessList blockAccessList =
        new BlockAccessList(
            List.of(
                new BlockAccessList.AccountChanges(
                    Address.fromHexString("0x0000000000000000000000000000000000000001"),
                    List.of(),
                    List.of(new BlockAccessList.SlotRead(new StorageSlotKey(UInt256.ONE))),
                    List.of(),
                    List.of(),
                    List.of())));

    final BlockBody bodyWithBal =
        new BlockBody(
            baseBlock.getBody().getTransactions(),
            baseBlock.getBody().getOmmers(),
            baseBlock.getBody().getWithdrawals());

    final Block blockWithBal =
        new Block(
            BlockHeaderBuilder.fromHeader(baseBlock.getHeader())
                .blockHeaderFunctions(new MainnetBlockHeaderFunctions())
                .balHash(BodyValidation.balHash(blockAccessList))
                .buildBlockHeader(),
            bodyWithBal);

    badBlockManager.addBadBlock(
        blockWithBal,
        BadBlockCause.fromValidationFailure("failed"),
        Optional.of(blockAccessList),
        Optional.empty());

    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", "debug_getBadBlocks", new Object[] {}));

    final JsonRpcSuccessResponse response =
        (JsonRpcSuccessResponse) debugGetBadBlocks.response(request);

    final Collection<BadBlockResult> result = (Collection<BadBlockResult>) response.getResult();
    assertThat(result).hasSize(1);

    final BadBlockResult badBlockResult = result.iterator().next();
    assertThat(badBlockResult.getBlockResult().getBalHash())
        .isEqualTo(BodyValidation.balHash(blockAccessList).toString());
    assertThat(badBlockResult.getGeneratedBlockAccessList()).isEmpty();
  }

  @Test
  public void shouldIncludeGeneratedBlockAccessListInResponse() {
    final BlockDataGenerator blockDataGenerator = new BlockDataGenerator();
    final Block baseBlock =
        blockDataGenerator.block(
            BlockDataGenerator.BlockOptions.create()
                .setBlockNumber(1)
                .setBlockHeaderFunctions(new MainnetBlockHeaderFunctions()));

    final BlockAccessList providedBlockAccessList =
        new BlockAccessList(
            List.of(
                new BlockAccessList.AccountChanges(
                    Address.fromHexString("0x0000000000000000000000000000000000000002"),
                    List.of(),
                    List.of(new BlockAccessList.SlotRead(new StorageSlotKey(UInt256.valueOf(2)))),
                    List.of(),
                    List.of(),
                    List.of())));

    final BlockBody bodyWithBal =
        new BlockBody(
            baseBlock.getBody().getTransactions(),
            baseBlock.getBody().getOmmers(),
            baseBlock.getBody().getWithdrawals());

    final Block blockWithBal =
        new Block(
            BlockHeaderBuilder.fromHeader(baseBlock.getHeader())
                .blockHeaderFunctions(new MainnetBlockHeaderFunctions())
                .balHash(BodyValidation.balHash(providedBlockAccessList))
                .buildBlockHeader(),
            bodyWithBal);

    final BlockAccessList generatedBlockAccessList =
        new BlockAccessList(
            List.of(
                new BlockAccessList.AccountChanges(
                    Address.fromHexString("0x0000000000000000000000000000000000000003"),
                    List.of(),
                    List.of(new BlockAccessList.SlotRead(new StorageSlotKey(UInt256.valueOf(3)))),
                    List.of(),
                    List.of(),
                    List.of())));

    badBlockManager.addBadBlock(
        blockWithBal,
        BadBlockCause.fromValidationFailure("failed"),
        Optional.of(providedBlockAccessList),
        Optional.of(generatedBlockAccessList));

    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", "debug_getBadBlocks", new Object[] {}));

    final JsonRpcSuccessResponse response =
        (JsonRpcSuccessResponse) debugGetBadBlocks.response(request);

    final Collection<BadBlockResult> result = (Collection<BadBlockResult>) response.getResult();
    assertThat(result).hasSize(1);

    final BadBlockResult badBlockResult = result.iterator().next();
    assertThat(badBlockResult.getGeneratedBlockAccessList()).isPresent();
    assertThat(
            badBlockResult.getGeneratedBlockAccessList().orElseThrow().stream()
                .map(accountChangesResult -> Address.fromHexString(accountChangesResult.address)))
        .containsExactly(Address.fromHexString("0x0000000000000000000000000000000000000003"));
  }

  @Test
  public void shouldReturnCorrectResponseWhenNoInvalidBlockFound() {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", "debug_getBadBlocks", new Object[] {}));

    final JsonRpcSuccessResponse response =
        (JsonRpcSuccessResponse) debugGetBadBlocks.response(request);
    final Collection<BadBlockResult> result = (Collection<BadBlockResult>) response.getResult();
    assertThat(result).hasSize(0);
  }
}
