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
import static org.hyperledger.besu.ethereum.mainnet.OsakaTargetingGasLimitCalculator.EIP_7825_TRANSACTION_GAS_LIMIT_CAP;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobType;
import org.hyperledger.besu.datatypes.CodeDelegation;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.GasLimitCalculator;
import org.hyperledger.besu.ethereum.core.ChainId;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.AmsterdamTargetingGasLimitCalculator;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionValidator;
import org.hyperledger.besu.ethereum.mainnet.OsakaTargetingGasLimitCalculator;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.mainnet.feemarket.BaseFeeMarket;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.evm.gascalculator.AmsterdamGasCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.gascalculator.OsakaGasCalculator;
import org.hyperledger.besu.evm.gascalculator.PragueGasCalculator;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Checks the limit against the transaction validator of each fork rather than against its own
 * arithmetic: one authorization more than the limit allows must always be invalid. A peer is
 * disconnected for a list over the limit, so a limit tighter than the validator would disconnect
 * honest peers.
 */
class CodeDelegationListLimitValidatorTest {
  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();
  private static final KeyPair KEYS = SIGNATURE_ALGORITHM.generateKeyPair();
  private static final BaseFeeMarket FEE_MARKET = FeeMarket.london(0L);
  private static final long PRAGUE_BLOCK_GAS_LIMIT = 30_000_000L;
  private static final long BLOCK_GAS_LIMIT = 60_000_000L;

  static Stream<Arguments> forksAndGasLimits() {
    final GasCalculator prague = new PragueGasCalculator();
    final GasCalculator osaka = new OsakaGasCalculator();
    final GasCalculator amsterdam = new AmsterdamGasCalculator();
    final GasLimitCalculator osakaCaps =
        new OsakaTargetingGasLimitCalculator(
            0L, FEE_MARKET, osaka, 6, 3, OptionalInt.of(6), OptionalInt.empty());
    final GasLimitCalculator amsterdamCaps =
        new AmsterdamTargetingGasLimitCalculator(
            0L, FEE_MARKET, amsterdam, 6, 3, OptionalInt.of(6), OptionalInt.empty());
    // The pool, not the validator, refuses a gas limit above the block's, so none is used here.
    return Stream.of(
        Arguments.of(
            "Prague", prague, GasLimitCalculator.constant(), PRAGUE_BLOCK_GAS_LIMIT, 100_000L),
        Arguments.of(
            "Prague", prague, GasLimitCalculator.constant(), PRAGUE_BLOCK_GAS_LIMIT, 30_000_000L),
        Arguments.of("Osaka", osaka, osakaCaps, BLOCK_GAS_LIMIT, 100_000L),
        Arguments.of(
            "Osaka", osaka, osakaCaps, BLOCK_GAS_LIMIT, EIP_7825_TRANSACTION_GAS_LIMIT_CAP),
        Arguments.of("Amsterdam", amsterdam, amsterdamCaps, BLOCK_GAS_LIMIT, 100_000L),
        Arguments.of(
            "Amsterdam",
            amsterdam,
            amsterdamCaps,
            BLOCK_GAS_LIMIT,
            EIP_7825_TRANSACTION_GAS_LIMIT_CAP),
        // Amsterdam lets tx.gas exceed 2^24 and caps intrinsic gas there instead.
        Arguments.of("Amsterdam", amsterdam, amsterdamCaps, BLOCK_GAS_LIMIT, BLOCK_GAS_LIMIT));
  }

  @ParameterizedTest(name = "{0}, gas limit {4}")
  @MethodSource("forksAndGasLimits")
  void validatorRefusesOneAuthorizationMoreThanTheLimit(
      final String fork,
      final GasCalculator gasCalculator,
      final GasLimitCalculator gasLimitCalculator,
      final long blockGasLimit,
      final long gasLimit) {
    final int limit =
        CodeDelegationListLimit.forFork(gasCalculator, gasLimitCalculator, blockGasLimit)
            .maxCodeDelegations(gasLimit);
    assertThat(limit).isPositive();

    final ValidationResult<TransactionInvalidReason> result =
        validator(gasCalculator, gasLimitCalculator)
            .validate(
                setCodeTransaction(gasLimit, limit + 1),
                Optional.of(Wei.ONE),
                Optional.empty(),
                TransactionValidationParams.transactionPool());

    assertThat(result.isValid()).isFalse();
    assertThat(result.getInvalidReason())
        .isEqualTo(TransactionInvalidReason.INTRINSIC_GAS_EXCEEDS_GAS_LIMIT);
  }

  private static MainnetTransactionValidator validator(
      final GasCalculator gasCalculator, final GasLimitCalculator gasLimitCalculator) {
    return new MainnetTransactionValidator(
        gasCalculator,
        gasLimitCalculator,
        FEE_MARKET,
        false,
        ChainId.fromOptional(Optional.of(BigInteger.ONE)),
        Set.of(TransactionType.DELEGATE_CODE),
        Set.of(BlobType.KZG_PROOF),
        Integer.MAX_VALUE);
  }

  private static Transaction setCodeTransaction(final long gasLimit, final int authorizations) {
    final CodeDelegation delegation =
        new org.hyperledger.besu.ethereum.core.CodeDelegation(
            BigInteger.ONE,
            Address.fromHexString("0x0000000000000000000000000000000000001000"),
            0,
            SIGNATURE_ALGORITHM.createCodeDelegationSignature(
                BigInteger.ONE, BigInteger.ONE, (byte) 0));
    final List<CodeDelegation> delegations = Collections.nCopies(authorizations, delegation);
    return Transaction.builder()
        .type(TransactionType.DELEGATE_CODE)
        .chainId(BigInteger.ONE)
        .nonce(0)
        .maxPriorityFeePerGas(Wei.of(1))
        .maxFeePerGas(Wei.of(1))
        .gasLimit(gasLimit)
        .to(Address.ZERO)
        .value(Wei.ZERO)
        .payload(Bytes.EMPTY)
        .accessList(List.of())
        .codeDelegations(delegations)
        .signAndBuild(KEYS);
  }
}
