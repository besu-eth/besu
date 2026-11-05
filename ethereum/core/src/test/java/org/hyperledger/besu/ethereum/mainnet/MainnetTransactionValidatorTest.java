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
package org.hyperledger.besu.ethereum.mainnet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams.processingBlockParams;
import static org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams.transactionPoolParams;
import static org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams.transactionSimulatorParams;
import static org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason.BLOB_GAS_PRICE_BELOW_CURRENT_BLOB_BASE_FEE;
import static org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason.GAS_PRICE_BELOW_CURRENT_BASE_FEE;
import static org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason.INVALID_TRANSACTION_FORMAT;
import static org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason.MAX_PRIORITY_FEE_PER_GAS_EXCEEDS_MAX_FEE_PER_GAS;
import static org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason.UPFRONT_GAS_COST_EXCEEDS_BALANCE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECP256K1;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.AccessListEntry;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobType;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.GasLimitCalculator;
import org.hyperledger.besu.ethereum.core.BlobTestFixture;
import org.hyperledger.besu.ethereum.core.ChainId;
import org.hyperledger.besu.ethereum.core.Frame;
import org.hyperledger.besu.ethereum.core.FrameSignature;
import org.hyperledger.besu.ethereum.core.KeyedNonceGasCapVectors;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.core.kzg.Blob;
import org.hyperledger.besu.ethereum.core.kzg.BlobsWithCommitments;
import org.hyperledger.besu.ethereum.core.kzg.KZGCommitment;
import org.hyperledger.besu.ethereum.core.kzg.KZGProof;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.ethereum.util.TrustedSetupClassLoaderExtension;
import org.hyperledger.besu.evm.EvmSpecVersion;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.Bytes48;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.FieldSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith({MockitoExtension.class})
@MockitoSettings(strictness = Strictness.LENIENT)
public class MainnetTransactionValidatorTest extends TrustedSetupClassLoaderExtension {

  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();
  protected static final KeyPair senderKeys = SIGNATURE_ALGORITHM.generateKeyPair();

  private static final TransactionValidationParams transactionProcessingParams =
      processingBlockParams;
  private static final TransactionValidationParams transactionSimulationParams =
      transactionSimulatorParams;

  @Mock protected GasCalculator gasCalculator;

  private final Transaction basicTransaction =
      new TransactionTestFixture()
          .nonce(30)
          .chainId(Optional.of(BigInteger.ONE))
          .createTransaction(senderKeys);

  protected TransactionValidator createTransactionValidator(
      final GasCalculator gasCalculator,
      final GasLimitCalculator gasLimitCalculator,
      final FeeMarket feeMarket,
      final boolean checkSignatureMalleability,
      final Optional<BigInteger> chainId,
      final Set<TransactionType> acceptedTransactionTypes,
      final int maxInitcodeSize) {
    return new MainnetTransactionValidator(
        gasCalculator,
        gasLimitCalculator,
        feeMarket,
        checkSignatureMalleability,
        ChainId.fromOptional(chainId),
        acceptedTransactionTypes,
        Set.of(BlobType.KZG_PROOF),
        maxInitcodeSize);
  }

  protected TransactionValidator createTransactionValidator(
      final GasCalculator gasCalculator,
      final GasLimitCalculator gasLimitCalculator,
      final boolean checkSignatureMalleability,
      final Optional<BigInteger> chainId) {
    return createTransactionValidator(
        gasCalculator,
        gasLimitCalculator,
        FeeMarket.legacy(),
        checkSignatureMalleability,
        chainId,
        Set.of(TransactionType.FRONTIER),
        Integer.MAX_VALUE);
  }

  @Test
  public void shouldRejectTransactionIfIntrinsicGasExceedsGasLimit() {
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator, GasLimitCalculator.constant(), false, Optional.empty());
    final Transaction transaction =
        new TransactionTestFixture()
            .gasLimit(10)
            .chainId(Optional.empty())
            .createTransaction(senderKeys);
    when(gasCalculator.transactionIntrinsicGasCost(any(), anyLong())).thenReturn(50L);

    assertThat(
            validator.validate(
                transaction, Optional.empty(), Optional.empty(), transactionProcessingParams))
        .isEqualTo(
            ValidationResult.invalid(TransactionInvalidReason.INTRINSIC_GAS_EXCEEDS_GAS_LIMIT));
  }

  @Test
  public void shouldRejectTransactionIfFloorExceedsGasLimit_EIP_7623() {
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator, GasLimitCalculator.constant(), false, Optional.empty());
    final Transaction transaction =
        new TransactionTestFixture()
            .gasLimit(10)
            .chainId(Optional.empty())
            .createTransaction(senderKeys);
    when(gasCalculator.transactionIntrinsicGasCost(any(), anyLong())).thenReturn(5L);
    when(gasCalculator.transactionFloorCost(any(Transaction.class))).thenReturn(51L);

    assertThat(
            validator.validate(
                transaction, Optional.empty(), Optional.empty(), transactionProcessingParams))
        .isEqualTo(
            ValidationResult.invalid(TransactionInvalidReason.INTRINSIC_GAS_EXCEEDS_GAS_LIMIT));
  }

  @Test
  public void shouldRejectAccessListTransactionIfFloorExceedsGasLimit_EIP_7981() {
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator,
            GasLimitCalculator.constant(),
            FeeMarket.london(0L),
            false,
            Optional.of(BigInteger.ONE),
            Set.of(TransactionType.ACCESS_LIST),
            Integer.MAX_VALUE);
    // Under EIP-7981, access list bytes contribute to the transaction floor cost; this test
    // simulates that contribution pushing the floor above the gas limit.
    final List<AccessListEntry> accessList =
        List.of(
            new AccessListEntry(
                Address.fromHexString("0x00000000000000000000000000000000000000aa"),
                List.of(Bytes32.ZERO, Bytes32.ZERO)));
    final Transaction transaction =
        new TransactionTestFixture()
            .type(TransactionType.ACCESS_LIST)
            .gasLimit(10)
            .accessList(accessList)
            .chainId(Optional.of(BigInteger.ONE))
            .createTransaction(senderKeys);
    when(gasCalculator.transactionIntrinsicGasCost(any(), anyLong())).thenReturn(5L);
    when(gasCalculator.transactionFloorCost(any(Transaction.class))).thenReturn(51L);

    assertThat(
            validator.validate(
                transaction, Optional.empty(), Optional.empty(), transactionProcessingParams))
        .isEqualTo(
            ValidationResult.invalid(TransactionInvalidReason.INTRINSIC_GAS_EXCEEDS_GAS_LIMIT));
  }

  @Test
  public void shouldRejectTransactionWhenTransactionHasChainIdAndValidatorDoesNot() {
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator, GasLimitCalculator.constant(), false, Optional.empty());
    assertThat(
            validator.validate(
                basicTransaction, Optional.empty(), Optional.empty(), transactionProcessingParams))
        .isEqualTo(
            ValidationResult.invalid(
                TransactionInvalidReason.REPLAY_PROTECTED_SIGNATURES_NOT_SUPPORTED));
  }

  @Test
  public void shouldRejectTransactionWhenTransactionHasIncorrectChainId() {
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator,
            GasLimitCalculator.constant(),
            false,
            Optional.of(BigInteger.valueOf(2)));
    assertThat(
            validator.validate(
                basicTransaction, Optional.empty(), Optional.empty(), transactionProcessingParams))
        .isEqualTo(ValidationResult.invalid(TransactionInvalidReason.WRONG_CHAIN_ID));
  }

  @Test
  public void shouldRejectTransactionWithUnrecoverableSignature() {
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator, GasLimitCalculator.constant(), false, Optional.of(BigInteger.ONE));
    // r = 5 is a valid scalar but not the x-coordinate of any curve point
    final Transaction transaction =
        Transaction.builder()
            .copiedFrom(basicTransaction)
            .sender(null)
            .signature(
                SIGNATURE_ALGORITHM.createSignature(
                    BigInteger.valueOf(5), BigInteger.ONE, (byte) 0))
            .build();

    assertThat(
            validator.validate(
                transaction, Optional.empty(), Optional.empty(), transactionProcessingParams))
        .isEqualTo(ValidationResult.invalid(TransactionInvalidReason.INVALID_SIGNATURE));
  }

  @Test
  public void shouldRejectTransactionWhenSenderAccountDoesNotExist() {
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator, GasLimitCalculator.constant(), false, Optional.of(BigInteger.ONE));
    assertThat(validator.validateForSender(basicTransaction, null, processingBlockParams))
        .isEqualTo(
            ValidationResult.invalid(TransactionInvalidReason.UPFRONT_GAS_COST_EXCEEDS_BALANCE));
  }

  @Test
  public void shouldRejectTransactionWhenTransactionNonceBelowAccountNonce() {
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator, GasLimitCalculator.constant(), false, Optional.of(BigInteger.ONE));

    final Account account = accountWithNonce(basicTransaction.getNonce() + 1);
    assertThat(validator.validateForSender(basicTransaction, account, processingBlockParams))
        .isEqualTo(ValidationResult.invalid(TransactionInvalidReason.NONCE_TOO_LOW));
  }

  @Test
  public void
      shouldRejectTransactionWhenTransactionNonceAboveAccountNonceAndFutureNonceIsNotAllowed() {
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator, GasLimitCalculator.constant(), false, Optional.of(BigInteger.ONE));

    final Account account = accountWithNonce(basicTransaction.getNonce() - 1);
    assertThat(validator.validateForSender(basicTransaction, account, processingBlockParams))
        .isEqualTo(ValidationResult.invalid(TransactionInvalidReason.NONCE_TOO_HIGH));
  }

  @Test
  public void
      shouldAcceptTransactionWhenTransactionNonceAboveAccountNonceAndFutureNonceIsAllowed() {
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator, GasLimitCalculator.constant(), false, Optional.of(BigInteger.ONE));

    final Account account = accountWithNonce(basicTransaction.getNonce() - 1);
    assertThat(validator.validateForSender(basicTransaction, account, transactionPoolParams))
        .isEqualTo(ValidationResult.valid());
  }

  @Test
  public void shouldAcceptTransactionNonceBelowAccountNonceInNonStrictBlockSimulation() {
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator, GasLimitCalculator.constant(), false, Optional.of(BigInteger.ONE));

    final Account account = accountWithNonce(basicTransaction.getNonce() + 1);
    assertThat(
            validator.validateForSender(
                basicTransaction, account, TransactionValidationParams.blockSimulatorNonStrict()))
        .isEqualTo(ValidationResult.valid());
    assertThat(
            validator.validateForSender(
                basicTransaction, account, TransactionValidationParams.blockSimulatorStrict()))
        .isEqualTo(ValidationResult.invalid(TransactionInvalidReason.NONCE_TOO_LOW));
  }

  @Test
  public void shouldRejectTransactionWhenNonceExceedsMaximumAllowedNonce() {
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator, GasLimitCalculator.constant(), false, Optional.of(BigInteger.ONE));

    final Transaction transaction =
        new TransactionTestFixture().nonce(11).createTransaction(senderKeys);
    final Account account = accountWithNonce(5);

    assertThat(validator.validateForSender(transaction, account, processingBlockParams))
        .isEqualTo(ValidationResult.invalid(TransactionInvalidReason.NONCE_TOO_HIGH));
  }

  @Test
  public void shouldRejectTransactionWithMaxNonceWhenFutureNonceNotAllowed() {
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator, GasLimitCalculator.constant(), false, Optional.of(BigInteger.ONE));

    final Transaction transaction =
        new TransactionTestFixture()
            .nonce(Account.MAX_NONCE)
            .chainId(Optional.of(BigInteger.ONE))
            .createTransaction(senderKeys);

    assertThat(
            validator.validate(
                transaction, Optional.empty(), Optional.empty(), processingBlockParams))
        .isEqualTo(ValidationResult.invalid(TransactionInvalidReason.NONCE_OVERFLOW));
  }

  @Test
  public void shouldAcceptTransactionWithMaxNonceWhenFutureNonceIsAllowed() {
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator, GasLimitCalculator.constant(), false, Optional.of(BigInteger.ONE));

    final Transaction transaction =
        new TransactionTestFixture()
            .nonce(Account.MAX_NONCE)
            .chainId(Optional.of(BigInteger.ONE))
            .createTransaction(senderKeys);

    assertThat(
            validator.validate(
                transaction,
                Optional.empty(),
                Optional.empty(),
                TransactionValidationParams.transactionSimulatorAllowFutureNonce()))
        .isEqualTo(ValidationResult.valid());
  }

  @Test
  public void transactionWithNullSenderCanBeValidIfGasPriceAndValueIsZero() {
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator, GasLimitCalculator.constant(), false, Optional.of(BigInteger.ONE));

    final TransactionTestFixture builder = new TransactionTestFixture();
    final KeyPair senderKeyPair = SIGNATURE_ALGORITHM.generateKeyPair();
    final Address arbitrarySender = Address.fromHexString("1");
    builder.gasPrice(Wei.ZERO).nonce(0).sender(arbitrarySender).value(Wei.ZERO);

    assertThat(
            validator.validateForSender(
                builder.createTransaction(senderKeyPair), null, processingBlockParams))
        .isEqualTo(ValidationResult.valid());
  }

  @Test
  public void shouldRejectTransactionIfAccountIsNotEOA() {
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator, GasLimitCalculator.constant(), false, Optional.empty());

    Account invalidEOA =
        when(account(basicTransaction.getUpfrontCost(0L), basicTransaction.getNonce())
                .getCodeHash())
            .thenReturn(Hash.fromHexStringLenient("0xdeadbeef"))
            .getMock();

    assertThat(validator.validateForSender(basicTransaction, invalidEOA, processingBlockParams))
        .isEqualTo(ValidationResult.invalid(TransactionInvalidReason.TX_SENDER_NOT_AUTHORIZED));
  }

  static List<Arguments> transactionWithMaxFeeTimesGasLimitGreaterThanBalanceArguments =
      List.of(
          Arguments.of(
              transactionSimulationParams,
              ValidationResult.invalid(UPFRONT_GAS_COST_EXCEEDS_BALANCE)),
          Arguments.of(transactionPoolParams, ValidationResult.valid()));

  @ParameterizedTest
  @FieldSource("transactionWithMaxFeeTimesGasLimitGreaterThanBalanceArguments")
  public void transactionWithMaxFeeTimesGasLimitGreaterThanBalance(
      final TransactionValidationParams txValidationParams,
      final ValidationResult<TransactionInvalidReason> validationResult) {
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator, GasLimitCalculator.constant(), false, Optional.empty());

    assertThat(
            validator.validateForSender(
                Transaction.builder()
                    .type(TransactionType.EIP1559)
                    .nonce(0)
                    .maxPriorityFeePerGas(Wei.of(5))
                    .maxFeePerGas(Wei.of(7))
                    .gasLimit(15)
                    .to(Address.ZERO)
                    .value(Wei.of(0))
                    .payload(Bytes.EMPTY)
                    .chainId(BigInteger.ONE)
                    .signAndBuild(new SECP256K1().generateKeyPair()),
                account(Wei.of(100), 0),
                txValidationParams))
        .isEqualTo(validationResult);
  }

  @ParameterizedTest
  // a base fee of 5 is also above the fee cap: the tip above the cap is reported first
  @ValueSource(longs = {1, 5})
  public void shouldRejectTransactionWithMaxPriorityFeeGreaterThanMaxFee(final long baseFee) {
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator,
            GasLimitCalculator.constant(),
            FeeMarket.london(0L),
            false,
            Optional.of(BigInteger.ONE),
            Set.of(
                new TransactionType[] {
                  TransactionType.FRONTIER, TransactionType.ACCESS_LIST, TransactionType.EIP1559
                }),
            Integer.MAX_VALUE);

    final Transaction transaction =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .nonce(0)
            .maxPriorityFeePerGas(Wei.of(7))
            .maxFeePerGas(Wei.of(4))
            .gasLimit(15)
            .to(Address.ZERO)
            .value(Wei.of(0))
            .payload(Bytes.EMPTY)
            .chainId(BigInteger.ONE)
            .signAndBuild(new SECP256K1().generateKeyPair());

    final ValidationResult<TransactionInvalidReason> validationResult =
        validator.validate(
            transaction,
            Optional.of(Wei.of(baseFee)),
            Optional.empty(),
            transactionProcessingParams);
    assertThat(validationResult)
        .isEqualTo(ValidationResult.invalid(MAX_PRIORITY_FEE_PER_GAS_EXCEEDS_MAX_FEE_PER_GAS));
    assertThat(validationResult.getErrorMessage())
        .isEqualTo("max priority fee per gas cannot be greater than max fee per gas");
  }

  @Test
  public void shouldRejectCodeDelegationTransactionWithEmptyDelegationList() {
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator,
            GasLimitCalculator.constant(),
            FeeMarket.london(0L),
            false,
            Optional.of(BigInteger.ONE),
            Set.of(TransactionType.DELEGATE_CODE),
            Integer.MAX_VALUE);
    final Transaction transaction =
        Transaction.builder()
            .type(TransactionType.DELEGATE_CODE)
            .nonce(0)
            .maxPriorityFeePerGas(Wei.of(1))
            .maxFeePerGas(Wei.of(2))
            .gasLimit(21_000)
            .to(Address.ZERO)
            .value(Wei.ZERO)
            .payload(Bytes.EMPTY)
            .chainId(BigInteger.ONE)
            .codeDelegations(List.of())
            .signAndBuild(senderKeys);

    final ValidationResult<TransactionInvalidReason> validationResult =
        validator.validate(
            transaction, Optional.of(Wei.ONE), Optional.empty(), transactionPoolParams);

    assertThat(validationResult.isValid()).isFalse();
    assertThat(validationResult.getInvalidReason())
        .isEqualTo(TransactionInvalidReason.EMPTY_CODE_DELEGATION);
    assertThat(validationResult.getErrorMessage())
        .isEqualTo(
            "transaction code delegation transactions must have a non-empty code delegation list");
  }

  @Test
  public void shouldRejectCodeDelegationTransactionWhenAuthorizationChainIdIsOutOfRange() {
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator,
            GasLimitCalculator.constant(),
            FeeMarket.london(0L),
            false,
            Optional.of(BigInteger.ONE),
            Set.of(TransactionType.DELEGATE_CODE),
            Integer.MAX_VALUE);
    final BigInteger twoPow256 = BigInteger.TWO.pow(256);
    final Transaction transaction =
        Transaction.builder()
            .type(TransactionType.DELEGATE_CODE)
            .nonce(0)
            .maxPriorityFeePerGas(Wei.of(1))
            .maxFeePerGas(Wei.of(2))
            .gasLimit(21_000)
            .to(Address.ZERO)
            .value(Wei.ZERO)
            .payload(Bytes.EMPTY)
            .chainId(BigInteger.ONE)
            .codeDelegations(
                List.of(
                    new org.hyperledger.besu.ethereum.core.CodeDelegation(
                        twoPow256,
                        Address.ZERO,
                        0L,
                        SIGNATURE_ALGORITHM.createCodeDelegationSignature(
                            BigInteger.ONE, BigInteger.ONE, (byte) 0))))
            .signAndBuild(senderKeys);

    final ValidationResult<TransactionInvalidReason> validationResult =
        validator.validate(
            transaction, Optional.of(Wei.ONE), Optional.empty(), transactionPoolParams);

    assertThat(validationResult.isValid()).isFalse();
    assertThat(validationResult.getInvalidReason()).isEqualTo(INVALID_TRANSACTION_FORMAT);
    assertThat(validationResult.getErrorMessage())
        .isEqualTo("Invalid 'chainId' value, should be < 2^256 but got " + twoPow256);
  }

  @Test
  public void shouldRejectTransactionWithMaxBlobPriorityFeeSmallerThanBlobBaseFee() {
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator,
            GasLimitCalculator.constant(),
            FeeMarket.cancunDefault(0L, Optional.empty()),
            false,
            Optional.of(BigInteger.ONE),
            Set.of(
                new TransactionType[] {
                  TransactionType.FRONTIER,
                  TransactionType.ACCESS_LIST,
                  TransactionType.EIP1559,
                  TransactionType.BLOB
                }),
            Integer.MAX_VALUE);

    BlobTestFixture blobTestFixture = new BlobTestFixture();
    BlobsWithCommitments bwc = blobTestFixture.createBlobsWithCommitments(1);

    final Transaction transaction =
        new TransactionTestFixture()
            .to(Optional.of(Address.fromHexString("0xDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF")))
            .type(TransactionType.BLOB)
            .chainId(Optional.of(BigInteger.ONE))
            .maxFeePerGas(Optional.of(Wei.of(15)))
            .maxFeePerBlobGas(Optional.of(Wei.of(7)))
            .maxPriorityFeePerGas(Optional.of(Wei.of(1)))
            .blobsWithCommitments(Optional.of(bwc))
            .versionedHashes(Optional.of(bwc.getVersionedHashes()))
            .createTransaction(senderKeys);

    final ValidationResult<TransactionInvalidReason> validationResult =
        validator.validate(
            transaction,
            Optional.of(Wei.ONE),
            Optional.of(Wei.of(10)),
            transactionProcessingParams);
    assertThat(validationResult)
        .isEqualTo(ValidationResult.invalid(BLOB_GAS_PRICE_BELOW_CURRENT_BLOB_BASE_FEE));
    assertThat(validationResult.getErrorMessage())
        .matches(
            "tx max fee per blob gas less than block blob gas fee: address 0x[0-9a-f]+ blobGasFeeCap: 7 wei, blobBaseFee: 10 wei");
  }

  @Test
  public void shouldAcceptOnlyTransactionsInAcceptedTransactionTypes() {
    final TransactionValidator frontierValidator =
        createTransactionValidator(
            gasCalculator,
            GasLimitCalculator.constant(),
            FeeMarket.legacy(),
            false,
            Optional.of(BigInteger.ONE),
            Set.of(TransactionType.FRONTIER),
            Integer.MAX_VALUE);

    final TransactionValidator eip1559Validator =
        new MainnetTransactionValidator(
            gasCalculator,
            GasLimitCalculator.constant(),
            FeeMarket.london(0L),
            false,
            ChainId.of(1),
            Set.of(TransactionType.FRONTIER, TransactionType.EIP1559),
            Set.of(BlobType.KZG_PROOF),
            Integer.MAX_VALUE);

    final Transaction transaction =
        new TransactionTestFixture()
            .type(TransactionType.EIP1559)
            .maxPriorityFeePerGas(Optional.of(Wei.of(3)))
            .maxFeePerGas(Optional.of(Wei.of(6)))
            .gasLimit(21000)
            .chainId(Optional.of(BigInteger.ONE))
            .createTransaction(senderKeys);

    assertThat(
            frontierValidator.validate(
                transaction, Optional.empty(), Optional.empty(), transactionProcessingParams))
        .isEqualTo(ValidationResult.invalid(INVALID_TRANSACTION_FORMAT));

    when(gasCalculator.transactionIntrinsicGasCost(any(), anyLong())).thenReturn(0L);

    assertThat(
            eip1559Validator.validate(
                transaction, Optional.of(Wei.ONE), Optional.empty(), transactionProcessingParams))
        .isEqualTo(ValidationResult.valid());
  }

  @Test
  public void shouldRejectTransactionIfEIP1559TransactionGasPriceLessBaseFee() {
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator,
            GasLimitCalculator.constant(),
            FeeMarket.london(0L),
            false,
            Optional.of(BigInteger.ONE),
            Set.of(TransactionType.FRONTIER, TransactionType.EIP1559),
            Integer.MAX_VALUE);
    final Transaction transaction =
        new TransactionTestFixture()
            .type(TransactionType.EIP1559)
            .maxPriorityFeePerGas(Optional.of(Wei.of(1)))
            .maxFeePerGas(Optional.of(Wei.of(1)))
            .chainId(Optional.of(BigInteger.ONE))
            .createTransaction(senderKeys);
    final Optional<Wei> basefee = Optional.of(Wei.of(150000L));
    assertThat(
            validator.validate(transaction, basefee, Optional.empty(), transactionProcessingParams))
        .isEqualTo(ValidationResult.invalid(GAS_PRICE_BELOW_CURRENT_BASE_FEE));
  }

  @Test
  public void shouldAcceptZeroGasPriceTransactionIfBaseFeeIsZero() {
    final Optional<Wei> zeroBaseFee = Optional.of(Wei.ZERO);
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator,
            GasLimitCalculator.constant(),
            FeeMarket.london(0L, zeroBaseFee),
            false,
            Optional.of(BigInteger.ONE),
            Set.of(TransactionType.FRONTIER, TransactionType.EIP1559),
            Integer.MAX_VALUE);
    final Transaction transaction =
        new TransactionTestFixture()
            .type(TransactionType.EIP1559)
            .maxPriorityFeePerGas(Optional.of(Wei.ZERO))
            .maxFeePerGas(Optional.of(Wei.ZERO))
            .chainId(Optional.of(BigInteger.ONE))
            .createTransaction(senderKeys);

    assertThat(
            validator.validate(
                transaction, zeroBaseFee, Optional.empty(), transactionProcessingParams))
        .isEqualTo(ValidationResult.valid());
  }

  @Test
  public void shouldAcceptValidEIP1559() {
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator,
            GasLimitCalculator.constant(),
            FeeMarket.london(0L),
            false,
            Optional.of(BigInteger.ONE),
            Set.of(TransactionType.FRONTIER, TransactionType.EIP1559),
            Integer.MAX_VALUE);
    final Transaction transaction =
        new TransactionTestFixture()
            .maxPriorityFeePerGas(Optional.of(Wei.of(1)))
            .maxFeePerGas(Optional.of(Wei.of(150000L)))
            .type(TransactionType.EIP1559)
            .chainId(Optional.of(BigInteger.ONE))
            .createTransaction(senderKeys);
    final Optional<Wei> basefee = Optional.of(Wei.of(150000L));
    when(gasCalculator.transactionIntrinsicGasCost(any(), anyLong())).thenReturn(50L);

    assertThat(
            validator.validate(transaction, basefee, Optional.empty(), transactionProcessingParams))
        .isEqualTo(ValidationResult.valid());
  }

  @Test
  public void shouldValidate1559TransactionWithPriceLowerThanBaseFeeForTransactionPool() {
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator,
            GasLimitCalculator.constant(),
            FeeMarket.london(0L),
            false,
            Optional.of(BigInteger.ONE),
            Set.of(TransactionType.FRONTIER, TransactionType.EIP1559),
            Integer.MAX_VALUE);
    final Transaction transaction =
        new TransactionTestFixture()
            .maxPriorityFeePerGas(Optional.of(Wei.of(1)))
            .maxFeePerGas(Optional.of(Wei.of(1)))
            .type(TransactionType.EIP1559)
            .chainId(Optional.of(BigInteger.ONE))
            .createTransaction(senderKeys);
    when(gasCalculator.transactionIntrinsicGasCost(any(), anyLong())).thenReturn(50L);

    assertThat(
            validator.validate(
                transaction,
                Optional.of(Wei.ONE),
                Optional.empty(),
                TransactionValidationParams.transactionPool()))
        .isEqualTo(ValidationResult.valid());
  }

  @Test
  public void shouldRejectTooLargeInitcode() {
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator,
            GasLimitCalculator.constant(),
            FeeMarket.london(0L),
            false,
            Optional.of(BigInteger.ONE),
            Set.of(TransactionType.FRONTIER, TransactionType.EIP1559),
            0xc000);

    var bigPayload =
        new TransactionTestFixture()
            .payload(Bytes.fromHexString("0x" + "00".repeat(0xc001)))
            .chainId(Optional.of(BigInteger.ONE))
            .createTransaction(senderKeys);
    var validationResult =
        validator.validate(
            bigPayload, Optional.empty(), Optional.empty(), transactionProcessingParams);

    assertThat(validationResult.isValid()).isFalse();
    assertThat(validationResult.getInvalidReason())
        .isEqualTo(TransactionInvalidReason.INITCODE_TOO_LARGE);
    assertThat(validationResult.getErrorMessage())
        .isEqualTo("Initcode size of 49153 exceeds maximum size of 49152");
  }

  @Test
  public void shouldAcceptInitcodeAtAmsterdamLimit() {
    final int maxInitcodeSize = EvmSpecVersion.AMSTERDAM.getMaxInitcodeSize();
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator,
            GasLimitCalculator.constant(),
            FeeMarket.london(0L),
            false,
            Optional.of(BigInteger.ONE),
            Set.of(TransactionType.FRONTIER, TransactionType.EIP1559),
            maxInitcodeSize);

    var transaction =
        new TransactionTestFixture()
            .payload(Bytes.fromHexString("0x" + "00".repeat(maxInitcodeSize)))
            .chainId(Optional.of(BigInteger.ONE))
            .createTransaction(senderKeys);
    var validationResult =
        validator.validate(
            transaction, Optional.empty(), Optional.empty(), transactionProcessingParams);

    assertThat(validationResult.isValid()).isTrue();
  }

  @Test
  public void shouldRejectInitcodeAboveAmsterdamLimit() {
    final int maxInitcodeSize = EvmSpecVersion.AMSTERDAM.getMaxInitcodeSize();
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator,
            GasLimitCalculator.constant(),
            FeeMarket.london(0L),
            false,
            Optional.of(BigInteger.ONE),
            Set.of(TransactionType.FRONTIER, TransactionType.EIP1559),
            maxInitcodeSize);

    var transaction =
        new TransactionTestFixture()
            .payload(Bytes.fromHexString("0x" + "00".repeat(maxInitcodeSize + 1)))
            .chainId(Optional.of(BigInteger.ONE))
            .createTransaction(senderKeys);
    var validationResult =
        validator.validate(
            transaction, Optional.empty(), Optional.empty(), transactionProcessingParams);

    assertThat(validationResult.isValid()).isFalse();
    assertThat(validationResult.getInvalidReason())
        .isEqualTo(TransactionInvalidReason.INITCODE_TOO_LARGE);
  }

  @Test
  public void shouldRejectContractCreateWithBlob() {
    /*
    https://github.com/ethereum/EIPs/blob/master/EIPS/eip-4844.md#blob-transaction
    "The field to deviates slightly from the semantics with the exception that it
    MUST NOT be nil and therefore must always represent a 20-byte address.
    This means that blob transactions cannot have the form of a create transaction."
     */
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator,
            GasLimitCalculator.constant(),
            FeeMarket.cancunDefault(0L, Optional.empty()),
            false,
            Optional.of(BigInteger.ONE),
            Set.of(TransactionType.FRONTIER, TransactionType.EIP1559, TransactionType.BLOB),
            0xc000);

    var blobTx =
        new TransactionTestFixture()
            .to(Optional.empty())
            .type(TransactionType.BLOB)
            .chainId(Optional.of(BigInteger.ONE))
            .maxFeePerGas(Optional.of(Wei.of(15)))
            .maxFeePerBlobGas(Optional.of(Wei.of(128)))
            .maxPriorityFeePerGas(Optional.of(Wei.of(1)))
            .blobsWithCommitments(
                Optional.of(
                    BlobsWithCommitments.createFromBlobsType0(
                        List.of(new KZGCommitment(Bytes48.ZERO)),
                        List.of(new Blob(Bytes.EMPTY)),
                        List.of(new KZGProof(Bytes48.ZERO)),
                        List.of(VersionedHash.DEFAULT_VERSIONED_HASH))))
            .versionedHashes(Optional.of(List.of(VersionedHash.DEFAULT_VERSIONED_HASH)))
            .createTransaction(senderKeys);
    var validationResult =
        validator.validate(
            blobTx, Optional.empty(), Optional.of(Wei.of(15)), transactionProcessingParams);
    if (!validationResult.isValid()) {
      System.out.println(
          validationResult.getInvalidReason() + " " + validationResult.getErrorMessage());
    }

    assertThat(validationResult.isValid()).isFalse();
    assertThat(validationResult.getInvalidReason())
        .isEqualTo(TransactionInvalidReason.INVALID_TRANSACTION_FORMAT);
  }

  @Test
  public void shouldAcceptTransactionWithAtLeastOneBlob() {
    when(gasCalculator.blobGasCost(anyLong())).thenReturn(2L);
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator,
            GasLimitCalculator.constant(),
            FeeMarket.cancunDefault(0L, Optional.empty()),
            false,
            Optional.of(BigInteger.ONE),
            Set.of(TransactionType.FRONTIER, TransactionType.EIP1559, TransactionType.BLOB),
            0xc000);

    BlobTestFixture blobTestFixture = new BlobTestFixture();
    BlobsWithCommitments bwc = blobTestFixture.createBlobsWithCommitments(1);
    var blobTx =
        new TransactionTestFixture()
            .to(Optional.of(Address.fromHexString("0xDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF")))
            .type(TransactionType.BLOB)
            .chainId(Optional.of(BigInteger.ONE))
            .maxFeePerGas(Optional.of(Wei.of(15)))
            .maxFeePerBlobGas(Optional.of(Wei.of(128)))
            .maxPriorityFeePerGas(Optional.of(Wei.of(1)))
            .blobsWithCommitments(Optional.of(bwc))
            .versionedHashes(Optional.of(bwc.getVersionedHashes()))
            .createTransaction(senderKeys);
    var validationResult =
        validator.validate(
            blobTx, Optional.empty(), Optional.of(Wei.of(15)), transactionProcessingParams);
    if (!validationResult.isValid()) {
      System.out.println(
          validationResult.getInvalidReason() + " " + validationResult.getErrorMessage());
    }

    assertThat(validationResult.isValid()).isTrue();
  }

  @ParameterizedTest
  @MethodSource("shouldSupportTransactionGasLimitCap_EIP_7825")
  public void shouldSupportTransactionGasLimitCap_EIP_7825(
      final ValidationParamsVariant validationParamsVariant,
      final long txGasLimit,
      final boolean valid) {
    final var feeMarket = FeeMarket.london(0L);
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator,
            new OsakaTargetingGasLimitCalculator(
                0L, feeMarket, gasCalculator, 6, 3, OptionalInt.of(6), OptionalInt.empty()),
            feeMarket,
            false,
            Optional.of(BigInteger.ONE),
            Set.of(TransactionType.FRONTIER, TransactionType.EIP1559),
            Integer.MAX_VALUE);
    final Transaction transaction =
        new TransactionTestFixture()
            .maxPriorityFeePerGas(Optional.of(Wei.of(1)))
            .maxFeePerGas(Optional.of(Wei.of(150000L)))
            .type(TransactionType.EIP1559)
            .chainId(Optional.of(BigInteger.ONE))
            .gasLimit(txGasLimit)
            .createTransaction(senderKeys);
    final Optional<Wei> basefee = Optional.of(Wei.of(150000L));
    when(gasCalculator.transactionIntrinsicGasCost(any(), anyLong())).thenReturn(50L);

    final var validationParams =
        switch (validationParamsVariant) {
          case PROCESSING -> processingBlockParams;
          case SIMULATING -> transactionSimulationParams;
        };

    final var validationResult =
        validator.validate(transaction, basefee, Optional.empty(), validationParams);

    if (valid) {
      assertThat(validationResult.isValid()).isTrue();
    } else {
      assertThat(validationResult.isValid()).isFalse();
      assertThat(validationResult.getInvalidReason())
          .isEqualTo(TransactionInvalidReason.EXCEEDS_TRANSACTION_GAS_LIMIT);
      assertThat(validationResult.getErrorMessage())
          .isEqualTo("Transaction gas limit must be at most 16777216");
    }
  }

  @ParameterizedTest
  @MethodSource("shouldSupportTransactionTotalGasLimitCap_EIP_8037")
  public void shouldSupportTransactionTotalGasLimitCap_EIP_8037(
      final ValidationParamsVariant validationParamsVariant,
      final long txGasLimit,
      final boolean valid) {
    final var feeMarket = FeeMarket.london(0L);
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator,
            new AmsterdamTargetingGasLimitCalculator(
                0L, feeMarket, gasCalculator, 6, 3, OptionalInt.of(6), OptionalInt.empty()),
            feeMarket,
            false,
            Optional.of(BigInteger.ONE),
            Set.of(TransactionType.FRONTIER, TransactionType.EIP1559),
            Integer.MAX_VALUE);
    final Transaction transaction =
        new TransactionTestFixture()
            .maxPriorityFeePerGas(Optional.of(Wei.of(1)))
            .maxFeePerGas(Optional.of(Wei.of(150000L)))
            .type(TransactionType.EIP1559)
            .chainId(Optional.of(BigInteger.ONE))
            .gasLimit(txGasLimit)
            .createTransaction(senderKeys);
    final Optional<Wei> basefee = Optional.of(Wei.of(150000L));
    when(gasCalculator.transactionIntrinsicGasCost(any(), anyLong())).thenReturn(50L);

    final var validationParams =
        switch (validationParamsVariant) {
          case PROCESSING -> processingBlockParams;
          case SIMULATING -> transactionSimulationParams;
        };

    final var validationResult =
        validator.validate(transaction, basefee, Optional.empty(), validationParams);

    if (valid) {
      assertThat(validationResult.isValid()).isTrue();
    } else {
      assertThat(validationResult.isValid()).isFalse();
      assertThat(validationResult.getInvalidReason())
          .isEqualTo(TransactionInvalidReason.EXCEEDS_TRANSACTION_GAS_LIMIT);
      assertThat(validationResult.getErrorMessage())
          .isEqualTo("Transaction gas limit must be at most 4294967295");
    }
  }

  @Test
  public void shouldCapIntrinsicGasInStrictButNotInNonStrictBlockSimulation_EIP_8037() {
    final var feeMarket = FeeMarket.london(0L);
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator,
            new AmsterdamTargetingGasLimitCalculator(
                0L, feeMarket, gasCalculator, 6, 3, OptionalInt.of(6), OptionalInt.empty()),
            feeMarket,
            false,
            Optional.of(BigInteger.ONE),
            Set.of(TransactionType.FRONTIER, TransactionType.EIP1559),
            Integer.MAX_VALUE);
    final Transaction transaction =
        new TransactionTestFixture()
            .maxPriorityFeePerGas(Optional.of(Wei.of(1)))
            .maxFeePerGas(Optional.of(Wei.of(150000L)))
            .type(TransactionType.EIP1559)
            .chainId(Optional.of(BigInteger.ONE))
            .gasLimit(30_000_000L)
            .createTransaction(senderKeys);
    final Optional<Wei> basefee = Optional.of(Wei.of(150000L));
    when(gasCalculator.transactionIntrinsicGasCost(any(), anyLong())).thenReturn(16_777_217L);

    assertThat(
            validator
                .validate(
                    transaction,
                    basefee,
                    Optional.empty(),
                    TransactionValidationParams.blockSimulatorStrict())
                .getInvalidReason())
        .isEqualTo(TransactionInvalidReason.INTRINSIC_GAS_EXCEEDS_GAS_LIMIT);
    assertThat(
            validator.validate(
                transaction,
                basefee,
                Optional.empty(),
                TransactionValidationParams.blockSimulatorNonStrict()))
        .isEqualTo(ValidationResult.valid());
  }

  private enum ValidationParamsVariant {
    PROCESSING,
    SIMULATING
  }

  private static Stream<Arguments> shouldSupportTransactionGasLimitCap_EIP_7825() {
    return Stream.of(
        Arguments.of(ValidationParamsVariant.PROCESSING, 16_777_216L, true),
        Arguments.of(ValidationParamsVariant.PROCESSING, 16_777_217L, false),
        // Long.MIN_VALUE == 2^63 in unsigned representation; signed '>' comparison would treat
        // it as negative and incorrectly pass the cap check — must be rejected unsigned
        Arguments.of(ValidationParamsVariant.PROCESSING, Long.MIN_VALUE, false),
        Arguments.of(ValidationParamsVariant.SIMULATING, 16_777_216L, true),
        Arguments.of(ValidationParamsVariant.SIMULATING, 16_777_217L, true));
  }

  private static Stream<Arguments> shouldSupportTransactionTotalGasLimitCap_EIP_8037() {
    return Stream.of(
        // above the EIP-7825 cap, which bounds only execution gas in Amsterdam
        Arguments.of(ValidationParamsVariant.PROCESSING, 16_777_217L, true),
        Arguments.of(ValidationParamsVariant.PROCESSING, 4_294_967_295L, true),
        Arguments.of(ValidationParamsVariant.PROCESSING, 4_294_967_296L, false),
        Arguments.of(ValidationParamsVariant.PROCESSING, Long.MIN_VALUE, false),
        Arguments.of(ValidationParamsVariant.SIMULATING, 4_294_967_296L, true));
  }

  /**
   * EIP-8141 bounds the running total of frame gas at 2^64, which is more than a signed long can
   * express, so the validator accumulates it as unsigned. A total of 2^63 is valid even though it
   * overflows a signed long; 2^64 is not. No fixture covers this range -- such a transaction is
   * always rejected later by the EIP-7825 gas cap -- so the boundary is only checked here.
   */
  @ParameterizedTest
  @MethodSource("frameGasLimits")
  void shouldBoundTotalFrameGasAtTwoToThe64(final List<Frame> frames, final boolean withinBound) {
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator,
            GasLimitCalculator.constant(),
            FeeMarket.london(0L),
            false,
            Optional.of(BigInteger.ONE),
            Set.of(TransactionType.FRAME),
            Integer.MAX_VALUE);

    final Transaction transaction =
        Transaction.builder()
            .type(TransactionType.FRAME)
            .chainId(BigInteger.ONE)
            .nonce(0)
            .sender(Address.fromHexString("0x1111111111111111111111111111111111111111"))
            .frames(frames)
            .frameSignatures(List.of())
            .maxPriorityFeePerGas(Wei.ZERO)
            .maxFee(Wei.ZERO)
            .build();

    final ValidationResult<TransactionInvalidReason> result =
        validator.validate(transaction, Optional.empty(), Optional.empty(), processingBlockParams);
    // getErrorMessage throws when the result is valid, which is itself a pass for the bound.
    final String error = result.isValid() ? "" : result.getErrorMessage();

    if (withinBound) {
      // Within the bound the accumulation must not be what rejects it; later rules, the EIP-7825
      // cap among them, are another test's business.
      assertThat(error).doesNotContain("total frame gas exceeds");
    } else {
      assertThat(error).contains("total frame gas exceeds 2^64-1");
    }
  }

  private static Stream<Arguments> frameGasLimits() {
    final long twoToThe62 = 1L << 62;
    return Stream.of(
        // 2^63 exactly: overflows a signed long, but the EIP allows it.
        Arguments.of(List.of(frame(twoToThe62, twoToThe62)), true),
        // 2^64-2, the largest total a single frame can declare.
        Arguments.of(List.of(frame(Long.MAX_VALUE, Long.MAX_VALUE)), true),
        // 2^64 exactly: one above the bound, and only reachable across frames.
        Arguments.of(List.of(frame(Long.MAX_VALUE, Long.MAX_VALUE), frame(2L, 0L)), false));
  }

  private static Frame frame(final long executionGasLimit, final long stateGasLimit) {
    return new Frame(
        Frame.MODE_VERIFY,
        Frame.APPROVE_SCOPE_MASK,
        Optional.empty(),
        executionGasLimit,
        stateGasLimit,
        Wei.ZERO,
        Bytes.EMPTY);
  }

  /**
   * EIP-7906 static validity of ASSERT frames. The messages are asserted verbatim, because the
   * reference-test exception mapping matches them to {@code TYPE_6_INVALID_FRAME_FORMAT}.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("assertFrameSets")
  void shouldValidateAssertFrames(
      final String name, final List<Frame> frames, final String expectedError) {
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator,
            GasLimitCalculator.constant(),
            FeeMarket.london(0L),
            false,
            Optional.of(BigInteger.ONE),
            Set.of(TransactionType.FRAME),
            Integer.MAX_VALUE);
    final Transaction transaction =
        Transaction.builder()
            .type(TransactionType.FRAME)
            .chainId(BigInteger.ONE)
            .nonce(0)
            .sender(Address.fromHexString("0x1111111111111111111111111111111111111111"))
            .frames(frames)
            .frameSignatures(List.of())
            .maxPriorityFeePerGas(Wei.ZERO)
            .maxFee(Wei.ZERO)
            .build();

    final ValidationResult<TransactionInvalidReason> result =
        validator.validate(transaction, Optional.empty(), Optional.empty(), processingBlockParams);

    if (expectedError == null) {
      // the frames are statically valid; any later rule is another test's business
      assertThat(result.isValid() ? null : result.getInvalidReason())
          .isNotEqualTo(TransactionInvalidReason.INVALID_FRAMES);
    } else {
      assertThat(result.getInvalidReason()).isEqualTo(TransactionInvalidReason.INVALID_FRAMES);
      assertThat(result.getErrorMessage()).isEqualTo(expectedError);
    }
  }

  private static Stream<Arguments> assertFrameSets() {
    final Frame verify = modeFrame(Frame.MODE_VERIFY, Frame.APPROVE_SCOPE_MASK, Wei.ZERO);
    final Frame sender = modeFrame(Frame.MODE_SENDER, 0, Wei.ZERO);
    final Frame batchedSender = modeFrame(Frame.MODE_SENDER, Frame.ATOMIC_BATCH_FLAG, Wei.ZERO);
    final Frame assertFrame = modeFrame(Frame.MODE_ASSERT, 0, Wei.ZERO);
    return Stream.of(
        Arguments.of("ASSERT accepted", List.of(verify, sender, assertFrame), null),
        Arguments.of(
            "ASSERT suffix accepted", List.of(verify, sender, assertFrame, assertFrame), null),
        Arguments.of(
            "approval scope on ASSERT accepted",
            List.of(verify, modeFrame(Frame.MODE_ASSERT, Frame.APPROVE_SCOPE_MASK, Wei.ZERO)),
            null),
        Arguments.of(
            "mode 4 undefined", List.of(verify, modeFrame(4, 0, Wei.ZERO)), "unknown frame mode 4"),
        Arguments.of(
            "SENDER after ASSERT",
            List.of(verify, assertFrame, sender),
            "frame 2 follows an ASSERT frame"),
        Arguments.of(
            "DEFAULT after ASSERT",
            List.of(verify, assertFrame, modeFrame(Frame.MODE_DEFAULT, 0, Wei.ZERO)),
            "frame 2 follows an ASSERT frame"),
        Arguments.of(
            "VERIFY after ASSERT",
            List.of(verify, sender, assertFrame, verify),
            "frame 3 follows an ASSERT frame"),
        Arguments.of(
            "batched ASSERT",
            List.of(
                verify,
                modeFrame(Frame.MODE_ASSERT, Frame.ATOMIC_BATCH_FLAG, Wei.ZERO),
                assertFrame),
            "ASSERT frame 1 carries the atomic batch flag"),
        Arguments.of(
            "batch ending on ASSERT",
            List.of(verify, batchedSender, assertFrame),
            "atomic batch cannot end on ASSERT frame 2"),
        Arguments.of(
            "ASSERT with value",
            List.of(verify, sender, modeFrame(Frame.MODE_ASSERT, 0, Wei.ONE)),
            "frame 2 carries value outside SENDER mode"));
  }

  private static Frame modeFrame(final int mode, final int flags, final Wei value) {
    return new Frame(mode, flags, Optional.empty(), 50_000L, 0L, value, Bytes.EMPTY);
  }

  /**
   * EIP-8250 static validity of the nonce key set: at most 16 strictly increasing keys. The empty
   * set selects the account nonce, and the zero key is an ordinary key that may join others.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("nonceKeySets")
  void shouldValidateNonceKeySets(
      final String name, final List<UInt256> nonceKeys, final String expectedError) {
    // Built afresh rather than copied, so the derived gas limit prices these keys.
    final Transaction transaction =
        Transaction.builder()
            .type(TransactionType.FRAME)
            .chainId(BigInteger.ONE)
            .nonceKeys(nonceKeys)
            .nonce(0)
            .sender(Address.fromHexString("0x1111111111111111111111111111111111111111"))
            .frames(List.of(frame(50_000L, 0L)))
            .frameSignatures(List.of())
            .maxPriorityFeePerGas(Wei.ZERO)
            .maxFee(Wei.ZERO)
            .build();

    final ValidationResult<TransactionInvalidReason> result =
        frameValidator()
            .validate(transaction, Optional.empty(), Optional.empty(), processingBlockParams);

    if (expectedError == null) {
      assertThat(result).isEqualTo(ValidationResult.valid());
    } else {
      assertThat(result.getInvalidReason()).isEqualTo(TransactionInvalidReason.INVALID_FRAMES);
      assertThat(result.getErrorMessage()).isEqualTo(expectedError);
    }
  }

  private static Stream<Arguments> nonceKeySets() {
    return Stream.of(
        Arguments.of("empty set", List.of(), null),
        Arguments.of("zero key alone", List.of(UInt256.ZERO), null),
        Arguments.of("zero key first of two", List.of(UInt256.ZERO, UInt256.ONE), null),
        Arguments.of(
            "sixteen keys", IntStream.range(0, 16).mapToObj(UInt256::valueOf).toList(), null),
        Arguments.of(
            "seventeen keys",
            IntStream.range(0, 17).mapToObj(UInt256::valueOf).toList(),
            "invalid nonce key count"),
        Arguments.of(
            "repeated zero key",
            List.of(UInt256.ZERO, UInt256.ZERO),
            "nonce keys not strictly increasing"),
        Arguments.of(
            "decreasing keys",
            List.of(UInt256.valueOf(2), UInt256.ONE),
            "nonce keys not strictly increasing"));
  }

  /**
   * EIP-8250: a frame transaction at MAX_NONCE_SEQ overflows only with the empty key set; with any
   * keys it is single-use.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("maxNonceSeqKeySets")
  void shouldRejectMaxNonceSeqOnlyForTheEmptyKeySet(
      final String name, final List<UInt256> nonceKeys, final boolean overflows) {
    final Transaction transaction =
        Transaction.builder()
            .type(TransactionType.FRAME)
            .chainId(BigInteger.ONE)
            .nonceKeys(nonceKeys)
            .nonce(Account.MAX_NONCE)
            .sender(Address.fromHexString("0x1111111111111111111111111111111111111111"))
            .frames(List.of(frame(50_000L, 0L)))
            .frameSignatures(List.of())
            .maxPriorityFeePerGas(Wei.ZERO)
            .maxFee(Wei.ZERO)
            .build();

    final ValidationResult<TransactionInvalidReason> result =
        frameValidator()
            .validate(transaction, Optional.empty(), Optional.empty(), processingBlockParams);

    if (overflows) {
      assertThat(result)
          .isEqualTo(ValidationResult.invalid(TransactionInvalidReason.NONCE_OVERFLOW));
    } else {
      assertThat(result).isEqualTo(ValidationResult.valid());
    }
  }

  private static Stream<Arguments> maxNonceSeqKeySets() {
    return Stream.of(
        Arguments.of("empty set", List.of(), true),
        Arguments.of("zero key", List.of(UInt256.ZERO), false),
        Arguments.of("two keys", List.of(UInt256.ONE, UInt256.valueOf(2)), false));
  }

  /**
   * EIP-8250: the keyed nonce access cost of a frame transaction counts towards the EIP-7825 cap.
   * The vectors are the spec's 16-key cap tests, whose 193,600 of keyed nonce access gas is what
   * takes the above-cap ones over.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("keyedNonceCapVectors")
  void shouldCountKeyedNonceAccessTowardsTheFrameTransactionGasCap(
      final String name, final Transaction transaction, final boolean withinCap) {
    final var feeMarket = FeeMarket.london(0L);
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator,
            new AmsterdamTargetingGasLimitCalculator(
                0L, feeMarket, gasCalculator, 6, 3, OptionalInt.of(6), OptionalInt.empty()),
            feeMarket,
            false,
            Optional.of(BigInteger.ONE),
            Set.of(TransactionType.FRAME),
            Integer.MAX_VALUE);

    final ValidationResult<TransactionInvalidReason> result =
        validator.validate(
            transaction, Optional.of(Wei.of(7)), Optional.empty(), processingBlockParams);

    if (withinCap) {
      assertThat(result).isEqualTo(ValidationResult.valid());
    } else {
      assertThat(result.getInvalidReason())
          .isEqualTo(TransactionInvalidReason.EXCEEDS_TRANSACTION_GAS_LIMIT);
      assertThat(result.getErrorMessage())
          .isEqualTo("Transaction gas limit must be at most " + KeyedNonceGasCapVectors.CAP);
    }
  }

  private static Stream<Arguments> keyedNonceCapVectors() {
    return Stream.of(
        Arguments.of("at_cap-standard", KeyedNonceGasCapVectors.atCapStandard(), true),
        Arguments.of("above_cap-standard", KeyedNonceGasCapVectors.aboveCapStandard(), false),
        Arguments.of("at_cap-floor", KeyedNonceGasCapVectors.atCapFloor(), true),
        Arguments.of("above_cap-floor", KeyedNonceGasCapVectors.aboveCapFloor(), false));
  }

  /**
   * EIP-8141: max_fee must cover max_gas at the base fee plus the blob gas at the blob base fee. A
   * shortfall reuses the base fee message, because the exception mappings match its prefix for
   * {@code INSUFFICIENT_MAX_FEE_PER_GAS}.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("frameFeeBudgets")
  void shouldRequireTheFrameFeeBudgetToCoverTheBaseFees(
      final String name, final int blobCount, final long belowRequired) {
    when(gasCalculator.blobGasCost(anyLong()))
        .thenAnswer(invocation -> invocation.getArgument(0, Long.class) * 131_072L);
    final Wei baseFee = Wei.of(7);
    final Wei blobBaseFee = Wei.of(3);
    final Transaction unpriced = frameTransaction(Wei.ZERO, Wei.ZERO, blobCount);
    final Wei requiredMaxFee =
        baseFee.multiply(unpriced.getGasLimit()).add(blobBaseFee.multiply(blobCount * 131_072L));
    final Transaction transaction =
        frameTransaction(requiredMaxFee.subtract(belowRequired), Wei.ONE, blobCount);

    final ValidationResult<TransactionInvalidReason> result =
        frameValidator()
            .validate(
                transaction, Optional.of(baseFee), Optional.of(blobBaseFee), processingBlockParams);

    if (belowRequired == 0) {
      assertThat(result).isEqualTo(ValidationResult.valid());
    } else {
      assertThat(result.getInvalidReason()).isEqualTo(GAS_PRICE_BELOW_CURRENT_BASE_FEE);
      assertThat(result.getErrorMessage())
          .isEqualTo(
              "gasPrice is less than the current BaseFee: max fee %s below the required %s",
              requiredMaxFee.subtract(belowRequired).toDecimalString(),
              requiredMaxFee.toDecimalString());
    }
  }

  private static Stream<Arguments> frameFeeBudgets() {
    return Stream.of(
        Arguments.of("exact, no blobs", 0, 0L),
        Arguments.of("one below, no blobs", 0, 1L),
        Arguments.of("exact, one blob", 1, 0L),
        Arguments.of("one below, one blob", 1, 1L));
  }

  /**
   * EIP-8141 drops the per-gas rules for frame transactions: a tip above the budget is capped
   * rather than rejected, a zero budget passes when there is no base fee, and the simulator's
   * relaxed parameters skip the budget check.
   */
  @Test
  void shouldNotApplyPerGasFeeRulesToFrameTransactions() {
    final Transaction maxTip = frameTransaction(Wei.fromEth(1), Wei.MAX_WEI, 0);
    final Transaction zeroBudget = frameTransaction(Wei.ZERO, Wei.ZERO, 0);

    assertThat(
            frameValidator()
                .validate(maxTip, Optional.of(Wei.of(7)), Optional.empty(), processingBlockParams))
        .isEqualTo(ValidationResult.valid());
    assertThat(
            frameValidator()
                .validate(zeroBudget, Optional.empty(), Optional.empty(), processingBlockParams))
        .isEqualTo(ValidationResult.valid());
    assertThat(
            frameValidator()
                .validate(
                    zeroBudget,
                    Optional.of(Wei.of(7)),
                    Optional.empty(),
                    TransactionValidationParams
                        .transactionSimulatorAllowUnderpricedAndFutureNonce()))
        .isEqualTo(ValidationResult.valid());
  }

  /**
   * EIP-8141: a frame transaction invalid in two ways reports the failure the reference
   * implementation checks first. It types the frame modes, flags and signature schemes, so it
   * rejects an undefined one while decoding, before the chain id. It then checks the legacy nonce
   * overflow, the nonce key set, the blobs, the frame count, each signature entry, the frames and,
   * last, the fee budget.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("frameTransactionsWithTwoFaults")
  void shouldReportTheFirstFrameFaultInTheReferenceOrder(
      final String name,
      final Transaction transaction,
      final TransactionInvalidReason expectedReason,
      final String expectedError) {
    when(gasCalculator.blobGasCost(anyLong()))
        .thenAnswer(invocation -> invocation.getArgument(0, Long.class) * 131_072L);

    final ValidationResult<TransactionInvalidReason> result =
        frameValidator()
            .validate(
                transaction, Optional.of(Wei.of(7)), Optional.of(Wei.ONE), processingBlockParams);

    assertThat(result.getInvalidReason()).isEqualTo(expectedReason);
    assertThat(result.getErrorMessage()).isEqualTo(expectedError);
  }

  private static Stream<Arguments> frameTransactionsWithTwoFaults() {
    final Frame verify = frame(50_000L, 0L);
    final Frame verifyWithValue =
        new Frame(
            Frame.MODE_VERIFY,
            Frame.APPROVE_SCOPE_MASK,
            Optional.empty(),
            50_000L,
            0L,
            Wei.ONE,
            Bytes.EMPTY);
    final FrameSignature outOfRange =
        new FrameSignature(
            FrameSignature.SCHEME_SECP256K1,
            Bytes.EMPTY,
            Bytes.EMPTY,
            Bytes.concatenate(Bytes.of(0), UInt256.ZERO, UInt256.ONE));
    final FrameSignature arbitraryWithSigner =
        new FrameSignature(
            FrameSignature.SCHEME_ARBITRARY,
            Address.fromHexString("0x2222222222222222222222222222222222222222").getBytes(),
            Bytes.EMPTY,
            Bytes.EMPTY);
    final FrameSignature undefinedScheme =
        new FrameSignature(3, Bytes.EMPTY, Bytes.EMPTY, Bytes.EMPTY);
    final List<Frame> tooManyFrames = Collections.nCopies(65, verify);
    final List<UInt256> seventeenKeys = IntStream.range(0, 17).mapToObj(UInt256::valueOf).toList();
    final String invalidSignature = "secp256k1 frame signature values out of range";
    return Stream.of(
        Arguments.of(
            "undefined frame mode before the nonce overflow",
            faultyFrameTransaction(
                Account.MAX_NONCE, List.of(), List.of(modeFrame(4, 0, Wei.ZERO)), List.of(), 0),
            TransactionInvalidReason.INVALID_FRAMES,
            "unknown frame mode 4"),
        Arguments.of(
            "undefined frame flags before the chain id",
            Transaction.builder()
                .type(TransactionType.FRAME)
                .chainId(BigInteger.TWO)
                .nonce(0)
                .sender(Address.fromHexString("0x1111111111111111111111111111111111111111"))
                .frames(List.of(modeFrame(Frame.MODE_VERIFY, 8, Wei.ZERO)))
                .frameSignatures(List.of())
                .maxPriorityFeePerGas(Wei.ZERO)
                .maxFee(Wei.ZERO)
                .versionedHashes(List.of())
                .build(),
            TransactionInvalidReason.INVALID_FRAMES,
            "reserved frame flag bits set: 8"),
        Arguments.of(
            "undefined signature scheme before an earlier entry's signer",
            faultyFrameTransaction(
                0, List.of(), List.of(verify), List.of(arbitraryWithSigner, undefinedScheme), 0),
            TransactionInvalidReason.INVALID_FRAMES,
            "unknown signature scheme 3"),
        Arguments.of(
            "nonce key set before the blobs",
            faultyFrameTransaction(0, seventeenKeys, List.of(verify), List.of(), 7),
            TransactionInvalidReason.INVALID_FRAMES,
            "invalid nonce key count"),
        Arguments.of(
            "blobs before the frame count",
            faultyFrameTransaction(0, List.of(), tooManyFrames, List.of(), 7),
            TransactionInvalidReason.TOTAL_BLOB_GAS_TOO_HIGH,
            "Blob transaction has too many blobs: 7"),
        Arguments.of(
            "frame count before the signature entries",
            faultyFrameTransaction(0, List.of(), tooManyFrames, List.of(outOfRange), 0),
            TransactionInvalidReason.INVALID_FRAMES,
            "frame count must be between 1 and 64"),
        Arguments.of(
            "signature entries before the frames",
            faultyFrameTransaction(0, List.of(), List.of(verifyWithValue), List.of(outOfRange), 0),
            TransactionInvalidReason.INVALID_SIGNATURE,
            invalidSignature),
        Arguments.of(
            "signature entries before the fee budget",
            faultyFrameTransaction(0, List.of(), List.of(verify), List.of(outOfRange), 0),
            TransactionInvalidReason.INVALID_SIGNATURE,
            invalidSignature),
        Arguments.of(
            "frames before the fee budget",
            faultyFrameTransaction(0, List.of(), List.of(verifyWithValue), List.of(), 0),
            TransactionInvalidReason.INVALID_FRAMES,
            "frame 0 carries value outside SENDER mode"));
  }

  /** A frame transaction with a zero fee budget, which any base fee leaves short. */
  private static Transaction faultyFrameTransaction(
      final long nonce,
      final List<UInt256> nonceKeys,
      final List<Frame> frames,
      final List<FrameSignature> signatures,
      final int blobCount) {
    return Transaction.builder()
        .type(TransactionType.FRAME)
        .chainId(BigInteger.ONE)
        .nonceKeys(nonceKeys)
        .nonce(nonce)
        .sender(Address.fromHexString("0x1111111111111111111111111111111111111111"))
        .frames(frames)
        .frameSignatures(signatures)
        .maxPriorityFeePerGas(Wei.ZERO)
        .maxFee(Wei.ZERO)
        .versionedHashes(versionedHashes(blobCount))
        .build();
  }

  private static List<VersionedHash> versionedHashes(final int blobCount) {
    return Stream.generate(
            () ->
                new VersionedHash(
                    Bytes32.wrap(Bytes.concatenate(Bytes.of(1), Bytes.repeat((byte) 5, 31)))))
        .limit(blobCount)
        .toList();
  }

  private TransactionValidator frameValidator() {
    return createTransactionValidator(
        gasCalculator,
        GasLimitCalculator.constant(),
        FeeMarket.london(0L),
        false,
        Optional.of(BigInteger.ONE),
        Set.of(TransactionType.FRAME),
        Integer.MAX_VALUE);
  }

  private static Transaction frameTransaction(
      final Wei maxFee, final Wei maxPriorityFeePerGas, final int blobCount) {
    return Transaction.builder()
        .type(TransactionType.FRAME)
        .chainId(BigInteger.ONE)
        .nonce(0)
        .sender(Address.fromHexString("0x1111111111111111111111111111111111111111"))
        .frames(List.of(frame(50_000L, 0L)))
        .frameSignatures(List.of())
        .maxPriorityFeePerGas(maxPriorityFeePerGas)
        .maxFee(maxFee)
        .versionedHashes(
            Stream.generate(
                    () ->
                        new VersionedHash(
                            Bytes32.wrap(
                                Bytes.concatenate(Bytes.of(1), Bytes.repeat((byte) 5, 31)))))
                .limit(blobCount)
                .toList())
        .build();
  }

  private Account accountWithNonce(final long nonce) {
    return account(basicTransaction.getUpfrontCost(0L), nonce);
  }

  private Account account(final Wei balance, final long nonce) {
    final Account account = mock(Account.class);
    when(account.getBalance()).thenReturn(balance);
    when(account.getNonce()).thenReturn(nonce);
    return account;
  }
}
