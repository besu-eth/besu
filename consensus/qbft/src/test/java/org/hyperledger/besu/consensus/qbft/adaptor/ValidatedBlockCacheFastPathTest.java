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
package org.hyperledger.besu.consensus.qbft.adaptor;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.common.bft.BftBlockHeaderFunctions;
import org.hyperledger.besu.consensus.common.bft.BftExtraData;
import org.hyperledger.besu.consensus.qbft.QbftExtraDataCodec;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlock;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.BlockValidator;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.blockcreation.BlockCreationTiming;
import org.hyperledger.besu.ethereum.blockcreation.BlockCreator;
import org.hyperledger.besu.ethereum.blockcreation.txselection.TransactionSelectionResults;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.BlockImporter;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.BlockHeaderValidator;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/** Proposer flow with the real adaptors sharing one cache: the block is executed only once. */
@ExtendWith(MockitoExtension.class)
class ValidatedBlockCacheFastPathTest {
  @Mock private BlockCreator besuBlockCreator;
  @Mock private TransactionSelectionResults transactionSelectionResults;
  @Mock private BlockValidator blockValidator;
  @Mock private BlockImporter blockImporter;
  @Mock private BlockHeaderValidator blockHeaderValidator;
  @Mock private ProtocolContext protocolContext;
  @Mock private MutableBlockchain blockchain;
  @Mock private PathBasedWorldStateProvider worldStateArchive;
  @Mock private BonsaiWorldStateKeyValueStorage worldStateStorage;

  private final QbftExtraDataCodec codec = new QbftExtraDataCodec();
  private final BlockDataGenerator generator = new BlockDataGenerator();
  private final ValidatedBlockCache validatedBlockCache = new ValidatedBlockCache();

  @Test
  void proposerExecutesItsBlockOnlyWhenCreatingIt() {
    final BlockHeader parentHeader = new BlockHeaderTestFixture().number(1).buildHeader();
    // round 0 proposal, hashed like BftBlockCreator does
    final BlockHeader header =
        new BlockHeaderTestFixture()
            .number(2)
            .parentHash(parentHeader.getHash())
            .extraData(
                codec.encode(
                    new BftExtraData(
                        Bytes.wrap(new byte[32]),
                        emptyList(),
                        Optional.empty(),
                        0,
                        List.of(Address.ZERO))))
            .blockHeaderFunctions(BftBlockHeaderFunctions.forCommittedSeal(codec))
            .buildHeader();
    final Block createdBlock =
        new Block(
            header,
            new BlockBody(List.of(generator.transaction(), generator.transaction()), emptyList()));
    final List<TransactionReceipt> receipts = generator.receipts(createdBlock);
    when(transactionSelectionResults.getReceipts()).thenReturn(receipts);
    when(besuBlockCreator.createBlock(10L, parentHeader))
        .thenReturn(
            new BlockCreator.BlockCreationResult(
                createdBlock,
                transactionSelectionResults,
                new BlockCreationTiming(),
                Optional.empty()));
    final QbftBlockCreatorAdaptor creator =
        new QbftBlockCreatorAdaptor(besuBlockCreator, codec, validatedBlockCache);

    final QbftBlock proposal =
        creator.createBlock(10L, new QbftBlockHeaderAdaptor(parentHeader)).block();

    // validating its own proposal does not execute the block again
    assertThat(
            new QbftBlockValidatorAdaptor(blockValidator, protocolContext, validatedBlockCache)
                .validateBlock(proposal, Optional.empty())
                .success())
        .isTrue();
    verifyNoInteractions(blockValidator);

    // the sealed block keeps the proposal hash in round 0, and its body object
    final QbftBlock sealed =
        creator.createSealedBlock(
            proposal, 0, List.of(new SECPSignature(BigInteger.ONE, BigInteger.ONE, (byte) 1)));
    final Block sealedBlock = AdaptorUtil.toBesuBlock(sealed);
    assertThat(sealedBlock.getHash()).isEqualTo(createdBlock.getHash());
    assertThat(sealedBlock.getBody()).isSameAs(createdBlock.getBody());

    // importing the committed block uses the receipts from block creation
    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    when(protocolContext.getWorldStateArchive()).thenReturn(worldStateArchive);
    when(blockchain.getChainHeadHash()).thenReturn(parentHeader.getHash());
    when(blockchain.getBlockHeader(parentHeader.getHash())).thenReturn(Optional.of(parentHeader));
    when(worldStateArchive.getWorldStateKeyValueStorage()).thenReturn(worldStateStorage);
    when(worldStateStorage.getTrieLog(sealedBlock.getHash())).thenReturn(Optional.of(new byte[0]));
    when(blockHeaderValidator.validateHeader(
            sealedBlock.getHeader(), parentHeader, protocolContext, HeaderValidationMode.FULL))
        .thenReturn(true);

    assertThat(
            new QbftBlockImporterAdaptor(
                    blockImporter, blockHeaderValidator, protocolContext, validatedBlockCache)
                .importBlock(sealed, Optional.empty()))
        .isTrue();
    verify(blockchain).appendBlock(sealedBlock, receipts, Optional.empty());
    verifyNoInteractions(blockImporter);
  }
}
