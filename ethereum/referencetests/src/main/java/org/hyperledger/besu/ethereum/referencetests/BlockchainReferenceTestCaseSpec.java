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
package org.hyperledger.besu.ethereum.referencetests;

import static org.hyperledger.besu.evm.internal.Words.decodeUnsignedLong;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.LogsBloomFilter;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.core.ConsensusContextFixture;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.ParsedExtraData;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.NoopBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.services.BesuService;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BlockchainReferenceTestCaseSpec {

  private final String network;

  private final CandidateBlock[] candidateBlocks;

  private final ReferenceTestBlockHeader genesisBlockHeader;

  private final Map<String, ReferenceTestWorldState.AccountMock> accounts;
  private final Hash lastBlockHash;

  private final String sealEngine;

  private WorldStateArchive buildWorldStateArchive(
      final long cacheSize, final Blockchain blockchain) {

    final InMemoryKeyValueStorageProvider inMemoryKeyValueStorageProvider =
        new InMemoryKeyValueStorageProvider();
    final WorldStateArchive worldStateArchive =
        new BonsaiWorldStateProvider(
            (BonsaiWorldStateKeyValueStorage)
                inMemoryKeyValueStorageProvider.createWorldStateStorage(
                    DataStorageConfiguration.DEFAULT_BONSAI_CONFIG),
            blockchain,
            ImmutablePathBasedExtraStorageConfiguration.builder()
                .maxLayersToLoad(cacheSize)
                .build(),
            new NoopBonsaiCachedMerkleTrieLoader(),
            new ServiceManager() {
              @Override
              public <T extends BesuService> void addService(
                  final Class<T> serviceType, final T service) {}

              @Override
              public <T extends BesuService> Optional<T> getService(final Class<T> serviceType) {
                return Optional.empty();
              }
            },
            EvmConfiguration.DEFAULT,
            () -> (__, ___) -> {},
            new CodeCache());

    final MutableWorldState worldState = worldStateArchive.getWorldState();
    final WorldUpdater updater = worldState.updater();

    for (final Map.Entry<String, ReferenceTestWorldState.AccountMock> entry : accounts.entrySet()) {
      ReferenceTestWorldState.insertAccount(
          updater, Address.fromHexString(entry.getKey()), entry.getValue());
    }

    updater.commit();
    worldState.persist(null);

    worldStateArchive.resetArchiveStateTo(genesisBlockHeader);
    return worldStateArchive;
  }

  public MutableBlockchain buildBlockchain() {
    final Block genesisBlock = new Block(genesisBlockHeader, BlockBody.empty());
    return InMemoryKeyValueStorageProvider.createInMemoryBlockchain(genesisBlock);
  }

  @JsonCreator
  public BlockchainReferenceTestCaseSpec(
      @JsonProperty("network") final String network,
      @JsonProperty("blocks") final CandidateBlock[] candidateBlocks,
      @JsonProperty("genesisBlockHeader") final ReferenceTestBlockHeader genesisBlockHeader,
      @SuppressWarnings("unused") @JsonProperty("genesisRLP") final String genesisRLP,
      @JsonProperty("pre") final Map<String, ReferenceTestWorldState.AccountMock> accounts,
      @JsonProperty("lastblockhash") final String lastBlockHash,
      @JsonProperty("sealEngine") final String sealEngine) {
    this.network = network;
    this.candidateBlocks = candidateBlocks;
    this.genesisBlockHeader = genesisBlockHeader;
    this.accounts = accounts;
    this.lastBlockHash = Hash.fromHexString(lastBlockHash);
    this.sealEngine = sealEngine;
  }

  public String getNetwork() {
    return network;
  }

  public CandidateBlock[] getCandidateBlocks() {
    return candidateBlocks;
  }

  public BlockHeader getGenesisBlockHeader() {
    return genesisBlockHeader;
  }

  public ProtocolContext buildProtocolContext(final MutableBlockchain blockchain) {
    return new ProtocolContext.Builder()
        .withBlockchain(blockchain)
        .withWorldStateArchive(
            buildWorldStateArchive(
                Stream.of(candidateBlocks).filter(CandidateBlock::isExecutable).count(),
                blockchain))
        .withConsensusContext(new ConsensusContextFixture())
        .build();
  }

  public Hash getLastBlockHash() {
    return lastBlockHash;
  }

  public String getSealEngine() {
    return sealEngine;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ReferenceTestBlockHeader extends BlockHeader {

    @JsonCreator
    public ReferenceTestBlockHeader(
        @JsonProperty("parentHash") final String parentHash,
        @JsonProperty("uncleHash") final String uncleHash,
        @JsonProperty("coinbase") final String coinbase,
        @JsonProperty("stateRoot") final String stateRoot,
        @JsonProperty("transactionsTrie") final String transactionsTrie,
        @JsonProperty("receiptTrie") final String receiptTrie,
        @JsonProperty("bloom") final String bloom,
        @JsonProperty("difficulty") final String difficulty,
        @JsonProperty("number") final String number,
        @JsonProperty("gasLimit") final String gasLimit,
        @JsonProperty("gasUsed") final String gasUsed,
        @JsonProperty("timestamp") final String timestamp,
        @JsonProperty("extraData") final String extraData,
        @JsonProperty("baseFeePerGas") final String baseFee,
        @JsonProperty("mixHash") final String mixHash,
        @JsonProperty("nonce") final String nonce,
        @JsonProperty("withdrawalsRoot") final String withdrawalsRoot,
        @JsonProperty("requestsHash") final String requestsHash,
        @JsonProperty("blobGasUsed") final String blobGasUsed,
        @JsonProperty("excessBlobGas") final String excessBlobGas,
        @JsonProperty("parentBeaconBlockRoot") final String parentBeaconBlockRoot,
        @JsonProperty("hash") final String hash,
        @JsonProperty("blockAccessListHash") final String blockAccessListHash,
        @JsonProperty("slotNumber") final String slotNumber) {
      super(
          Hash.fromHexString(parentHash), // parentHash
          uncleHash == null ? Hash.EMPTY_LIST_HASH : Hash.fromHexString(uncleHash), // ommersHash
          Address.fromHexString(coinbase), // coinbase
          Hash.fromHexString(stateRoot), // stateRoot
          transactionsTrie == null
              ? Hash.EMPTY_TRIE_HASH
              : Hash.fromHexString(transactionsTrie), // transactionsRoot
          receiptTrie == null
              ? Hash.EMPTY_TRIE_HASH
              : Hash.fromHexString(receiptTrie), // receiptTrie
          LogsBloomFilter.fromHexString(bloom), // bloom
          Difficulty.fromHexString(difficulty), // difficulty
          Long.decode(number), // number
          Long.decode(gasLimit), // gasLimit
          Long.decode(gasUsed), // gasUsed
          Long.decode(timestamp), // timestamp
          Bytes.fromHexString(extraData), // extraData
          baseFee != null ? Wei.fromHexString(baseFee) : null, // baseFee
          Bytes32.wrap(Hash.fromHexString(mixHash).getBytes()), // mixHash
          Bytes.fromHexStringLenient(nonce).toLong(),
          withdrawalsRoot != null ? Hash.fromHexString(withdrawalsRoot) : null,
          blobGasUsed != null ? Long.decode(blobGasUsed) : 0,
          excessBlobGas != null ? BlobGas.fromHexString(excessBlobGas) : null,
          parentBeaconBlockRoot != null ? Bytes32.fromHexString(parentBeaconBlockRoot) : null,
          requestsHash != null ? Hash.fromHexString(requestsHash) : null,
          blockAccessListHash != null ? Hash.fromHexString(blockAccessListHash) : null,
          slotNumber != null ? decodeUnsignedLong(slotNumber) : null,
          new BlockHeaderFunctions() {
            @Override
            public Hash hash(final BlockHeader header) {
              return hash == null ? null : Hash.fromHexString(hash);
            }

            @Override
            public ParsedExtraData parseExtraData(final BlockHeader header) {
              return null;
            }
          });
    }
  }

  @JsonIgnoreProperties({
    "blocknumber",
    "chainname",
    "chainnetwork",
    "expectExceptionByzantium",
    "expectExceptionConstantinople",
    "expectExceptionConstantinopleFix",
    "expectExceptionIstanbul",
    "expectExceptionEIP150",
    "expectExceptionEIP158",
    "expectExceptionFrontier",
    "expectExceptionHomestead",
    "hasBigInt",
    "rlp_decoded",
    "receipts"
  })
  public static class CandidateBlock {

    private final Bytes rlp;

    private final Boolean valid;
    private final List<TransactionSequence> transactionSequence;
    private final BlockAccessList blockAccessList;
    private final String expectException;
    private final String expectExceptionALL;

    @JsonCreator
    public CandidateBlock(
        @JsonProperty("rlp") final String rlp,
        @JsonProperty("blockHeader") final Object blockHeader,
        @JsonProperty("transactions") final Object transactions,
        @JsonProperty("uncleHeaders") final Object uncleHeaders,
        @JsonProperty("withdrawals") final Object withdrawals,
        @JsonProperty("depositRequests") final Object depositRequests,
        @JsonProperty("withdrawalRequests") final Object withdrawalRequests,
        @JsonProperty("consolidationRequests") final Object consolidationRequests,
        @JsonProperty("transactionSequence") final List<TransactionSequence> transactionSequence,
        @JsonDeserialize(using = BlockAccessListDeserializer.class)
            @JsonProperty("blockAccessList")
            @JsonAlias("rlp_decoded")
            final BlockAccessList blockAccessList,
        @JsonProperty("expectException") final String expectException,
        @JsonProperty("expectExceptionALL") final String expectExceptionALL) {
      boolean blockValid = true;
      Bytes rlpAttempt = null;
      try {
        rlpAttempt = rlp != null ? Bytes.fromHexString(rlp) : null;
      } catch (final IllegalArgumentException e) {
        blockValid = false;
      }
      this.rlp = rlpAttempt;

      if (blockHeader == null
          && transactions == null
          && uncleHeaders == null
          && withdrawals == null) {
        blockValid = false;
      }

      this.valid = blockValid;
      this.transactionSequence = transactionSequence;
      this.blockAccessList = blockAccessList;
      this.expectException = expectException;
      this.expectExceptionALL = expectExceptionALL;
    }

    public boolean isValid() {
      return valid;
    }

    /**
     * Returns the expected exception key (e.g. {@code "BlockException.GAS_USED_OVERFLOW"}) for this
     * invalid block, if specified in the fixture. Returns empty when no exception is expected
     * (valid blocks) or when the fixture does not specify one.
     */
    public Optional<String> getExpectedException() {
      if (expectException != null) return Optional.of(expectException);
      if (expectExceptionALL != null) return Optional.of(expectExceptionALL);
      return Optional.empty();
    }

    public boolean areAllTransactionsValid() {
      return transactionSequence == null
          || transactionSequence.stream().filter(t -> !t.valid()).count() == 0;
    }

    public boolean isExecutable() {
      return rlp != null;
    }

    public Block getBlock() {
      final RLPInput input = new BytesValueRLPInput(rlp, false);
      input.enterList();
      final MainnetBlockHeaderFunctions blockHeaderFunctions = new MainnetBlockHeaderFunctions();
      final BlockHeader header = BlockHeader.readFrom(input, blockHeaderFunctions);
      final List<Transaction> transactions = input.readList(Transaction::readFrom);
      final List<BlockHeader> ommers =
          input.readList(inputData -> BlockHeader.readFrom(inputData, blockHeaderFunctions));
      final Optional<List<Withdrawal>> withdrawals =
          input.isEndOfCurrentList()
              ? Optional.empty()
              : Optional.of(input.readList(Withdrawal::readFrom));
      final BlockBody body = new BlockBody(transactions, ommers, withdrawals);
      return new Block(header, body);
    }

    public Optional<BlockAccessList> getBlockAccessList() {
      return Optional.ofNullable(blockAccessList);
    }
  }
}
