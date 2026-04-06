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
package org.hyperledger.besu.consensus.qbft.validator;

import org.hyperledger.besu.config.QbftConfigOptions;
import org.hyperledger.besu.consensus.common.ForksSchedule;
import org.hyperledger.besu.consensus.common.validator.ValidatorProvider;
import org.hyperledger.besu.consensus.common.validator.VoteProvider;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/** The Transaction validator provider. */
public class TransactionValidatorProvider implements ValidatorProvider {

  private final Blockchain blockchain;
  private final ValidatorContractController validatorContractController;
  private final ForksSchedule<QbftConfigOptions> forksSchedule;
  private final Cache<Long, Collection<Address>> afterBlockValidatorCache =
      CacheBuilder.newBuilder().maximumSize(100).build();
  private final Cache<Long, Collection<Address>> forBlockValidatorCache =
      CacheBuilder.newBuilder().maximumSize(100).build();

  /**
   * Instantiates a new Transaction validator provider.
   *
   * @param blockchain the blockchain
   * @param validatorContractController the validator contract controller
   * @param forksSchedule the forks schedule
   */
  public TransactionValidatorProvider(
      final Blockchain blockchain,
      final ValidatorContractController validatorContractController,
      final ForksSchedule<QbftConfigOptions> forksSchedule) {
    this.blockchain = blockchain;
    this.validatorContractController = validatorContractController;
    this.forksSchedule = forksSchedule;
  }

  @Override
  public Collection<Address> getValidatorsAtHead() {
    return getValidatorsAfterBlock(blockchain.getChainHeadHeader());
  }

  @Override
  public Collection<Address> getValidatorsAfterBlock(final BlockHeader parentHeader) {
    // For the validator contract we determine the validators from the previous block but the
    // address from block about to be created
    final long nextBlock = parentHeader.getNumber() + 1;
    final long blockTimestamp = parentHeader.getTimestamp();
    final Address contractAddress = resolveContractAddress(nextBlock, blockTimestamp);
    return getValidatorsFromContract(afterBlockValidatorCache, parentHeader, contractAddress);
  }

  @Override
  public Collection<Address> getValidatorsForBlock(final BlockHeader header) {
    final Address contractAddress =
        resolveContractAddress(header.getNumber(), header.getTimestamp());
    return getValidatorsFromContract(forBlockValidatorCache, header, contractAddress);
  }

  private Collection<Address> getValidatorsFromContract(
      final Cache<Long, Collection<Address>> validatorCache,
      final BlockHeader header,
      final Address contractAddress) {
    final long blockNumber = header.getNumber();
    try {
      return validatorCache.get(
          blockNumber,
          () ->
              validatorContractController.getValidators(blockNumber, contractAddress).stream()
                  .sorted()
                  .collect(Collectors.toList()));
    } catch (final ExecutionException e) {
      throw new RuntimeException("Unable to determine a validators for the requested block.");
    }
  }

  @Override
  public Optional<VoteProvider> getVoteProviderAtHead() {
    return Optional.empty();
  }

  private Address resolveContractAddress(final long blockNumber, final long blockTimestamp) {
    return forksSchedule
        .getFork(blockNumber, blockTimestamp)
        .getValue()
        .getValidatorContractAddress()
        .map(Address::fromHexString)
        .orElseThrow(
            () ->
                new RuntimeException(
                    "Error resolving smart contract address unable to make validator contract call"));
  }
}
