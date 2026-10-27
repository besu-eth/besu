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
import org.hyperledger.besu.ethereum.core.Frame;
import org.hyperledger.besu.ethereum.core.FrameSignature;
import org.hyperledger.besu.ethereum.core.FrameTransactionGas;
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
import org.hyperledger.besu.evm.frame.KeyedNonces;
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
        chainId,
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
            Optional.of(BigInteger.ONE),
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
            .maxFeePerGas(Wei.ZERO)
            .maxFeePerBlobGas(Wei.ZERO)
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

  // ---------------------------------------------------------------------------------------------
  // EIP-8141 frame transactions
  // ---------------------------------------------------------------------------------------------

  private static final Address FRAME_SENDER =
      Address.fromHexString("0x1111111111111111111111111111111111111111");
  private static final Address OTHER_TARGET =
      Address.fromHexString("0x2222222222222222222222222222222222222222");
  private static final long FRAME_GAS = 50_000L;

  @Test
  void unsignedFrameTransactionSkipsTheOuterSignatureCheck() {
    final Transaction transaction = frameTransaction(verifyFrame(), senderFrame());
    assertThat(transaction.getSignature()).isNull();

    // Malleability checking is on, so a frame transaction reaching the outer signature check would
    // fail on its missing signature.
    assertThat(
            frameValidator(GasLimitCalculator.constant())
                .validate(transaction, Optional.empty(), Optional.empty(), processingBlockParams))
        .isEqualTo(ValidationResult.valid());
  }

  @Test
  void frameTransactionStillGoesThroughTheFeeChecks() {
    final Transaction transaction = frameTransaction(verifyFrame(), senderFrame());

    assertThat(
            frameValidator(GasLimitCalculator.constant())
                .validate(
                    transaction, Optional.of(Wei.of(7)), Optional.empty(), processingBlockParams))
        .isEqualTo(ValidationResult.invalid(GAS_PRICE_BELOW_CURRENT_BASE_FEE));
  }

  @Test
  void frameTransactionIsAFormatErrorWhereTheForkDoesNotAcceptIt() {
    // Every fork scheduled today: FRAME is not an accepted type. The missing outer signature is
    // never read.
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator,
            GasLimitCalculator.constant(),
            FeeMarket.london(0L),
            true,
            Optional.of(BigInteger.ONE),
            Set.of(
                TransactionType.FRONTIER,
                TransactionType.ACCESS_LIST,
                TransactionType.EIP1559,
                TransactionType.BLOB,
                TransactionType.DELEGATE_CODE),
            Integer.MAX_VALUE);

    final ValidationResult<TransactionInvalidReason> result =
        validator.validate(
            frameTransaction(verifyFrame(), senderFrame()),
            Optional.empty(),
            Optional.empty(),
            processingBlockParams);

    assertThat(result.getInvalidReason()).isEqualTo(INVALID_TRANSACTION_FORMAT);
    assertThat(result.getErrorMessage()).startsWith("Transaction type FRAME is invalid");
  }

  @Test
  void frameTransactionAtTheIntrinsicCapIsValidAndOneAboveExceedsTheGasLimit() {
    final long cap = OsakaTargetingGasLimitCalculator.EIP_7825_TRANSACTION_GAS_LIMIT_CAP;
    // EIP-8141 caps max(intrinsic + total execution gas, calldata floor). The intrinsic gas does
    // not depend on the frames' gas limits, so it is the same for both transactions.
    final long intrinsic =
        FrameTransactionGas.intrinsicGas(
            List.of(senderFrame()), List.of(), FRAME_SENDER, KeyedNonces.LEGACY_NONCE_KEYS, 0L);

    final Transaction atCap = frameTransaction(senderFrame(cap - intrinsic, 0L));
    final Transaction aboveCap = frameTransaction(senderFrame(cap - intrinsic + 1, 0L));

    assertThat(
            amsterdamFrameValidator()
                .validate(atCap, Optional.empty(), Optional.empty(), processingBlockParams))
        .isEqualTo(ValidationResult.valid());
    final ValidationResult<TransactionInvalidReason> result =
        amsterdamFrameValidator()
            .validate(aboveCap, Optional.empty(), Optional.empty(), processingBlockParams);
    assertThat(result.getInvalidReason())
        .isEqualTo(TransactionInvalidReason.EXCEEDS_TRANSACTION_GAS_LIMIT);
    assertThat(result.getErrorMessage()).isEqualTo("Transaction gas limit must be at most " + cap);
  }

  /**
   * EIP-7906 static validity of POST_TX frames. The messages are asserted verbatim, because the
   * reference-test exception mapping matches them to {@code TYPE_6_INVALID_FRAME_FORMAT}.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("postTxFrameSets")
  void shouldValidatePostTxFrames(
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
            .maxFeePerGas(Wei.ZERO)
            .maxFeePerBlobGas(Wei.ZERO)
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

  private static Stream<Arguments> postTxFrameSets() {
    final Frame verify = modeFrame(Frame.MODE_VERIFY, Frame.APPROVE_SCOPE_MASK, Wei.ZERO);
    final Frame sender = modeFrame(Frame.MODE_SENDER, 0, Wei.ZERO);
    final Frame batchedSender = modeFrame(Frame.MODE_SENDER, Frame.ATOMIC_BATCH_FLAG, Wei.ZERO);
    final Frame postTx = modeFrame(Frame.MODE_POST_TX, 0, Wei.ZERO);
    return Stream.of(
        Arguments.of("POST_TX accepted", List.of(verify, sender, postTx), null),
        Arguments.of("POST_TX suffix accepted", List.of(verify, sender, postTx, postTx), null),
        Arguments.of(
            "approval scope on POST_TX accepted",
            List.of(verify, modeFrame(Frame.MODE_POST_TX, Frame.APPROVE_SCOPE_MASK, Wei.ZERO)),
            null),
        Arguments.of(
            "mode 4 undefined", List.of(verify, modeFrame(4, 0, Wei.ZERO)), "unknown frame mode 4"),
        Arguments.of(
            "SENDER after POST_TX",
            List.of(verify, postTx, sender),
            "frame 2 follows a POST_TX frame"),
        Arguments.of(
            "DEFAULT after POST_TX",
            List.of(verify, postTx, modeFrame(Frame.MODE_DEFAULT, 0, Wei.ZERO)),
            "frame 2 follows a POST_TX frame"),
        Arguments.of(
            "VERIFY after POST_TX",
            List.of(verify, sender, postTx, verify),
            "frame 3 follows a POST_TX frame"),
        Arguments.of(
            "batched POST_TX",
            List.of(
                verify, modeFrame(Frame.MODE_POST_TX, Frame.ATOMIC_BATCH_FLAG, Wei.ZERO), postTx),
            "POST_TX frame 1 carries the atomic batch flag"),
        Arguments.of(
            "batch ending on POST_TX",
            List.of(verify, batchedSender, postTx),
            "atomic batch cannot end on POST_TX frame 2"),
        Arguments.of(
            "POST_TX with value",
            List.of(verify, sender, modeFrame(Frame.MODE_POST_TX, 0, Wei.ONE)),
            "frame 2 carries value outside SENDER mode"));
  }

  private static Frame modeFrame(final int mode, final int flags, final Wei value) {
    return new Frame(mode, flags, Optional.empty(), 50_000L, 0L, value, Bytes.EMPTY);
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

  @Test
  void frameStateGasIsExemptFromTheIntrinsicCap() {
    final long cap = OsakaTargetingGasLimitCalculator.EIP_7825_TRANSACTION_GAS_LIMIT_CAP;
    final Transaction transaction = frameTransaction(senderFrame(FRAME_GAS, 10 * cap));

    assertThat(transaction.getGasLimit()).isGreaterThan(cap);
    assertThat(
            amsterdamFrameValidator()
                .validate(transaction, Optional.empty(), Optional.empty(), processingBlockParams))
        .isEqualTo(ValidationResult.valid());
  }

  /**
   * Regression guard: only frame transactions report the intrinsic cap as {@code
   * EXCEEDS_TRANSACTION_GAS_LIMIT}. Every other type must keep {@code
   * INTRINSIC_GAS_EXCEEDS_GAS_LIMIT}.
   */
  @Test
  void nonFrameTransactionOverTheIntrinsicCapKeepsTheIntrinsicGasFailure() {
    final Transaction transaction =
        new TransactionTestFixture()
            .maxPriorityFeePerGas(Optional.of(Wei.of(1)))
            .maxFeePerGas(Optional.of(Wei.of(150000L)))
            .type(TransactionType.EIP1559)
            .chainId(Optional.of(BigInteger.ONE))
            .gasLimit(30_000_000L)
            .createTransaction(senderKeys);
    when(gasCalculator.transactionIntrinsicGasCost(any(), anyLong())).thenReturn(16_777_217L);

    final ValidationResult<TransactionInvalidReason> result =
        amsterdamFrameValidator()
            .validate(
                transaction, Optional.of(Wei.of(150000L)), Optional.empty(), processingBlockParams);

    assertThat(result.getInvalidReason())
        .isEqualTo(TransactionInvalidReason.INTRINSIC_GAS_EXCEEDS_GAS_LIMIT);
    assertThat(result.getErrorMessage())
        .isEqualTo("intrinsic gas cost 16777217 exceeds gas limit 16777216");
  }

  @Test
  void staticFrameRulesRunBeforeTheGasLimitCap() {
    // An undefined scheme has no verification cost, so the derived gas limit saturates far above
    // the EIP-7825 cap. The frame error is what makes the transaction invalid, and it is what is
    // reported.
    final Transaction transaction =
        frameTransactionBuilder(List.of(verifyFrame(), senderFrame()))
            .frameSignatures(List.of(new FrameSignature(3, Bytes.EMPTY, Bytes.EMPTY, Bytes.EMPTY)))
            .build();
    assertThat(transaction.getGasLimit()).isEqualTo(Long.MAX_VALUE);

    final ValidationResult<TransactionInvalidReason> result =
        amsterdamFrameValidator()
            .validate(transaction, Optional.empty(), Optional.empty(), processingBlockParams);

    assertThat(result.getInvalidReason()).isEqualTo(TransactionInvalidReason.INVALID_FRAMES);
    assertThat(result.getErrorMessage()).isEqualTo("unknown signature scheme 3");
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("staticallyInvalidFrameTransactions")
  void shouldRejectStaticallyInvalidFrameTransactions(
      final String description, final Transaction transaction, final String expectedError) {
    final ValidationResult<TransactionInvalidReason> result =
        frameValidator(GasLimitCalculator.constant())
            .validate(transaction, Optional.empty(), Optional.empty(), processingBlockParams);

    assertThat(result.isValid()).isFalse();
    assertThat(result.getInvalidReason()).isEqualTo(TransactionInvalidReason.INVALID_FRAMES);
    assertThat(result.getErrorMessage()).isEqualTo(expectedError);
  }

  private static Stream<Arguments> staticallyInvalidFrameTransactions() {
    final Frame batchedSender = frameOf(Frame.MODE_SENDER, Frame.ATOMIC_BATCH_FLAG);
    return Stream.of(
        Arguments.of(
            "65 frames",
            frameTransaction(Collections.nCopies(65, senderFrame())),
            "frame count must be between 1 and 64"),
        Arguments.of(
            "high undefined mode", frameTransaction(frameOf(255, 0)), "unknown frame mode 255"),
        Arguments.of(
            "reserved flag bit",
            frameTransaction(frameOf(Frame.MODE_SENDER, 0x8)),
            "reserved frame flag bits set: 8"),
        Arguments.of(
            "execution gas limit at 2^63",
            frameTransaction(senderFrame(Long.MIN_VALUE, 0L)),
            "frame gas limits exceed 2^63-1"),
        Arguments.of(
            "state gas limit at 2^64-1",
            frameTransaction(senderFrame(FRAME_GAS, -1L)),
            "frame gas limits exceed 2^63-1"),
        Arguments.of(
            "value in a DEFAULT frame",
            frameTransaction(
                verifyFrame(),
                new Frame(
                    Frame.MODE_DEFAULT,
                    0,
                    Optional.of(OTHER_TARGET),
                    FRAME_GAS,
                    0L,
                    Wei.ONE,
                    Bytes.EMPTY)),
            "frame 1 carries value outside SENDER mode"),
        Arguments.of(
            "value in a VERIFY frame",
            frameTransaction(
                new Frame(
                    Frame.MODE_VERIFY,
                    Frame.APPROVE_SCOPE_MASK,
                    Optional.empty(),
                    FRAME_GAS,
                    0L,
                    Wei.ONE,
                    Bytes.EMPTY)),
            "frame 0 carries value outside SENDER mode"),
        Arguments.of(
            "execution approval allowed for a target other than the sender",
            frameTransaction(
                new Frame(
                    Frame.MODE_VERIFY,
                    Frame.FLAG_APPROVE_EXECUTION,
                    Optional.of(OTHER_TARGET),
                    FRAME_GAS,
                    0L,
                    Wei.ZERO,
                    Bytes.EMPTY)),
            "frame 0 allows execution approval with a target other than the sender"),
        Arguments.of(
            "batched VERIFY frame",
            frameTransaction(frameOf(Frame.MODE_VERIFY, Frame.ATOMIC_BATCH_FLAG), senderFrame()),
            "frame 0 is a batched VERIFY frame"),
        Arguments.of(
            "VERIFY frame closing a batch",
            frameTransaction(verifyFrame(), batchedSender, verifyFrame()),
            "frame 2 is a batched VERIFY frame"),
        Arguments.of(
            "atomic batch flag on the last frame",
            frameTransaction(verifyFrame(), batchedSender),
            "last frame cannot carry the atomic batch flag"),
        Arguments.of(
            "approval scope on the first frame of a batch",
            frameTransaction(
                verifyFrame(),
                frameOf(Frame.MODE_SENDER, Frame.ATOMIC_BATCH_FLAG | Frame.FLAG_APPROVE_PAYMENT),
                senderFrame()),
            "batched frame 1 must not carry an approval scope"),
        Arguments.of(
            "approval scope on the last frame of a batch",
            frameTransaction(
                verifyFrame(),
                batchedSender,
                frameOf(Frame.MODE_SENDER, Frame.FLAG_APPROVE_PAYMENT)),
            "batched frame 2 must not carry an approval scope"),
        Arguments.of(
            "blob fee cap without blobs",
            frameTransactionBuilder(List.of(verifyFrame(), senderFrame()))
                .maxFeePerBlobGas(Wei.ONE)
                .build(),
            "max fee per blob gas must be zero without blob versioned hashes"),
        Arguments.of(
            "undefined signature scheme",
            frameTransactionWithSignature(
                new FrameSignature(3, Bytes.EMPTY, Bytes.EMPTY, Bytes.EMPTY)),
            "unknown signature scheme 3"),
        Arguments.of(
            "ARBITRARY entry with a signer",
            frameTransactionWithSignature(
                new FrameSignature(
                    FrameSignature.SCHEME_ARBITRARY,
                    OTHER_TARGET.getBytes(),
                    Bytes.EMPTY,
                    Bytes.EMPTY)),
            "ARBITRARY signature entries must not declare a signer"),
        Arguments.of(
            "explicit zero digest",
            frameTransactionWithSignature(
                new FrameSignature(
                    FrameSignature.SCHEME_SECP256K1,
                    Bytes.EMPTY,
                    Bytes32.ZERO,
                    Bytes.wrap(new byte[65]))),
            "explicit zero signature digest is invalid"),
        Arguments.of(
            "no nonce keys", frameTransactionWithNonceKeys(List.of()), "invalid nonce key count"),
        Arguments.of(
            "17 nonce keys",
            frameTransactionWithNonceKeys(
                IntStream.rangeClosed(1, 17).mapToObj(UInt256::valueOf).toList()),
            "invalid nonce key count"),
        Arguments.of(
            "decreasing nonce keys",
            frameTransactionWithNonceKeys(List.of(UInt256.valueOf(2), UInt256.ONE)),
            "nonce keys not strictly increasing"),
        Arguments.of(
            "repeated nonce key",
            frameTransactionWithNonceKeys(List.of(UInt256.ONE, UInt256.ONE)),
            "nonce keys not strictly increasing"),
        Arguments.of(
            "zero nonce key with other keys",
            frameTransactionWithNonceKeys(List.of(UInt256.ZERO, UInt256.ONE)),
            "zero nonce key alongside other keys"),
        Arguments.of(
            "two expiry verifier frames",
            frameTransaction(expiryFrame(), expiryFrame(), verifyFrame(), senderFrame()),
            "at most one expiry verifier frame is allowed"),
        Arguments.of(
            "expiry verifier frame with an approval scope",
            frameTransaction(
                expiryFrame(Frame.FLAG_APPROVE_PAYMENT, 0L, Bytes.ofUnsignedLong(1L)),
                senderFrame()),
            "malformed expiry verifier frame 0"),
        Arguments.of(
            "expiry verifier frame with state gas",
            frameTransaction(expiryFrame(0, 1L, Bytes.ofUnsignedLong(1L)), senderFrame()),
            "malformed expiry verifier frame 0"),
        Arguments.of(
            "expiry verifier frame with 7 bytes of data",
            frameTransaction(expiryFrame(0, 0L, Bytes.wrap(new byte[7])), senderFrame()),
            "malformed expiry verifier frame 0"),
        Arguments.of(
            "expiry verifier frame with 9 bytes of data",
            frameTransaction(expiryFrame(0, 0L, Bytes.wrap(new byte[9])), senderFrame()),
            "malformed expiry verifier frame 0"),
        // A VERIFY frame never carries value, so that rule reports a valued expiry frame first.
        Arguments.of(
            "expiry verifier frame with value",
            frameTransaction(
                new Frame(
                    Frame.MODE_VERIFY,
                    0,
                    Optional.of(MainnetTransactionValidator.EXPIRY_VERIFIER),
                    FRAME_GAS,
                    0L,
                    Wei.ONE,
                    Bytes.ofUnsignedLong(1L)),
                senderFrame()),
            "frame 0 carries value outside SENDER mode"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("staticallyValidFrameTransactions")
  void shouldAcceptStaticallyValidFrameTransactions(
      final String description, final Transaction transaction) {
    assertThat(
            frameValidator(GasLimitCalculator.constant())
                .validate(transaction, Optional.empty(), Optional.empty(), processingBlockParams))
        .isEqualTo(ValidationResult.valid());
  }

  private static Stream<Arguments> staticallyValidFrameTransactions() {
    return Stream.of(
        Arguments.of("64 frames", frameTransaction(Collections.nCopies(64, senderFrame()))),
        Arguments.of(
            "value in a SENDER frame",
            frameTransaction(
                verifyFrame(),
                new Frame(
                    Frame.MODE_SENDER,
                    0,
                    Optional.of(OTHER_TARGET),
                    FRAME_GAS,
                    0L,
                    Wei.ONE,
                    Bytes.EMPTY))),
        Arguments.of(
            "execution approval allowed for the sender as an explicit target",
            frameTransaction(
                new Frame(
                    Frame.MODE_VERIFY,
                    Frame.APPROVE_SCOPE_MASK,
                    Optional.of(FRAME_SENDER),
                    FRAME_GAS,
                    0L,
                    Wei.ZERO,
                    Bytes.EMPTY))),
        Arguments.of(
            "payment approval allowed for a paymaster",
            frameTransaction(
                frameOf(Frame.MODE_VERIFY, Frame.FLAG_APPROVE_EXECUTION),
                new Frame(
                    Frame.MODE_VERIFY,
                    Frame.FLAG_APPROVE_PAYMENT,
                    Optional.of(OTHER_TARGET),
                    FRAME_GAS,
                    0L,
                    Wei.ZERO,
                    Bytes.EMPTY))),
        Arguments.of(
            "a batch of DEFAULT and SENDER frames",
            frameTransaction(
                verifyFrame(),
                frameOf(Frame.MODE_DEFAULT, Frame.ATOMIC_BATCH_FLAG),
                frameOf(Frame.MODE_SENDER, Frame.ATOMIC_BATCH_FLAG),
                senderFrame())),
        Arguments.of(
            "one entry of each scheme",
            frameTransactionBuilder(List.of(verifyFrame(), senderFrame()))
                .frameSignatures(
                    List.of(
                        new FrameSignature(
                            FrameSignature.SCHEME_ARBITRARY,
                            Bytes.EMPTY,
                            Bytes.EMPTY,
                            Bytes.fromHexString("0x01")),
                        new FrameSignature(
                            FrameSignature.SCHEME_SECP256K1,
                            OTHER_TARGET.getBytes(),
                            Bytes32.fromHexString("0x01"),
                            Bytes.wrap(new byte[65])),
                        new FrameSignature(
                            FrameSignature.SCHEME_P256,
                            Bytes.EMPTY,
                            Bytes.EMPTY,
                            Bytes.wrap(new byte[128]))))
                .build()),
        Arguments.of(
            "keyed nonces",
            frameTransactionWithNonceKeys(
                List.of(UInt256.ONE, UInt256.valueOf(5), UInt256.MAX_VALUE))),
        Arguments.of(
            "16 nonce keys",
            frameTransactionWithNonceKeys(
                IntStream.rangeClosed(1, 16).mapToObj(UInt256::valueOf).toList())),
        Arguments.of(
            "a well-formed expiry verifier frame",
            frameTransaction(expiryFrame(), verifyFrame(), senderFrame())),
        // Only a VERIFY frame targeting the verifier is an expiry frame.
        Arguments.of(
            "DEFAULT frames calling the expiry verifier",
            frameTransaction(
                verifyFrame(),
                new Frame(
                    Frame.MODE_DEFAULT,
                    0,
                    Optional.of(MainnetTransactionValidator.EXPIRY_VERIFIER),
                    FRAME_GAS,
                    1L,
                    Wei.ZERO,
                    Bytes.EMPTY),
                new Frame(
                    Frame.MODE_DEFAULT,
                    0,
                    Optional.of(MainnetTransactionValidator.EXPIRY_VERIFIER),
                    FRAME_GAS,
                    1L,
                    Wei.ZERO,
                    Bytes.EMPTY))));
  }

  @Test
  void frameSenderNeedsNoBalance() {
    final TransactionValidator validator = frameValidator(GasLimitCalculator.constant());
    final Transaction transaction =
        frameTransactionBuilder(List.of(verifyFrame(), senderFrame()))
            .maxFeePerGas(Wei.of(1_000))
            .maxPriorityFeePerGas(Wei.of(1))
            .build();

    // The payer chosen during execution pays, so neither an empty nor a missing sender account is
    // rejected for its balance.
    assertThat(
            validator.validateForSender(transaction, account(Wei.ZERO, 0L), processingBlockParams))
        .isEqualTo(ValidationResult.valid());
    assertThat(validator.validateForSender(transaction, null, processingBlockParams))
        .isEqualTo(ValidationResult.valid());
  }

  @Test
  void frameSenderMayHaveCodeButOtherSendersMayNot() {
    final TransactionValidator validator = frameValidator(GasLimitCalculator.constant());
    final Account contractSender =
        when(account(basicTransaction.getUpfrontCost(0L), basicTransaction.getNonce())
                .getCodeHash())
            .thenReturn(Hash.fromHexStringLenient("0xdeadbeef"))
            .getMock();

    // EIP-8141 lifts EIP-3607 for frame transactions only.
    assertThat(
            validator.validateForSender(
                frameTransactionBuilder(List.of(verifyFrame(), senderFrame()))
                    .nonce(basicTransaction.getNonce())
                    .build(),
                contractSender,
                processingBlockParams))
        .isEqualTo(ValidationResult.valid());
    assertThat(validator.validateForSender(basicTransaction, contractSender, processingBlockParams))
        .isEqualTo(ValidationResult.invalid(TransactionInvalidReason.TX_SENDER_NOT_AUTHORIZED));
  }

  @Test
  void keyedNoncesSkipTheAccountNonceChecks() {
    final TransactionValidator validator = frameValidator(GasLimitCalculator.constant());
    final Transaction transaction =
        frameTransactionBuilder(List.of(verifyFrame(), senderFrame()))
            .nonceKeys(List.of(UInt256.ONE, UInt256.valueOf(7)))
            .nonce(5)
            .build();

    // The keyed sequences live in the nonce manager, and the frame transaction processor checks
    // them; the account nonce is not this transaction's nonce.
    assertThat(
            validator.validateForSender(transaction, account(Wei.ZERO, 0L), processingBlockParams))
        .isEqualTo(ValidationResult.valid());
    assertThat(
            validator.validateForSender(transaction, account(Wei.ZERO, 10L), processingBlockParams))
        .isEqualTo(ValidationResult.valid());
  }

  @Test
  void legacyNonceKeyKeepsTheAccountNonceChecks() {
    final TransactionValidator validator = frameValidator(GasLimitCalculator.constant());
    final Transaction explicitLegacyKey =
        frameTransactionBuilder(List.of(verifyFrame(), senderFrame()))
            .nonceKeys(KeyedNonces.LEGACY_NONCE_KEYS)
            .nonce(5)
            .build();
    final Transaction defaultKey =
        frameTransactionBuilder(List.of(verifyFrame(), senderFrame())).nonce(5).build();

    for (final Transaction transaction : List.of(explicitLegacyKey, defaultKey)) {
      assertThat(
              validator.validateForSender(
                  transaction, account(Wei.ZERO, 0L), processingBlockParams))
          .isEqualTo(ValidationResult.invalid(TransactionInvalidReason.NONCE_TOO_HIGH));
      assertThat(
              validator.validateForSender(
                  transaction, account(Wei.ZERO, 10L), processingBlockParams))
          .isEqualTo(ValidationResult.invalid(TransactionInvalidReason.NONCE_TOO_LOW));
      assertThat(
              validator.validateForSender(
                  transaction, account(Wei.ZERO, 5L), processingBlockParams))
          .isEqualTo(ValidationResult.valid());
    }
  }

  @Test
  void blobCarryingFrameTransactionNeedsNoRecipient() {
    when(gasCalculator.blobGasCost(anyLong())).thenReturn(2L);
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator,
            GasLimitCalculator.constant(),
            FeeMarket.cancunDefault(0L, Optional.empty()),
            false,
            Optional.of(BigInteger.ONE),
            Set.of(TransactionType.BLOB, TransactionType.FRAME),
            0xc000);
    final Transaction transaction =
        frameTransactionBuilder(List.of(verifyFrame(), senderFrame()))
            .versionedHashes(List.of(VersionedHash.DEFAULT_VERSIONED_HASH))
            .maxFeePerBlobGas(Wei.of(128))
            .build();
    assertThat(transaction.getTo()).isEmpty();

    // The blob rules apply because it carries blobs; the recipient rule is BLOB's alone.
    assertThat(
            validator.validate(
                transaction, Optional.empty(), Optional.of(Wei.of(15)), processingBlockParams))
        .isEqualTo(ValidationResult.valid());
  }

  private TransactionValidator frameValidator(final GasLimitCalculator gasLimitCalculator) {
    return createTransactionValidator(
        gasCalculator,
        gasLimitCalculator,
        FeeMarket.london(0L),
        true,
        Optional.of(BigInteger.ONE),
        Set.of(TransactionType.EIP1559, TransactionType.FRAME),
        Integer.MAX_VALUE);
  }

  private TransactionValidator amsterdamFrameValidator() {
    final var feeMarket = FeeMarket.london(0L);
    return createTransactionValidator(
        gasCalculator,
        new AmsterdamTargetingGasLimitCalculator(
            0L, feeMarket, gasCalculator, 6, 3, OptionalInt.of(6), OptionalInt.empty()),
        feeMarket,
        false,
        Optional.of(BigInteger.ONE),
        Set.of(TransactionType.EIP1559, TransactionType.FRAME),
        Integer.MAX_VALUE);
  }

  private static Transaction.Builder frameTransactionBuilder(final List<Frame> frames) {
    return Transaction.builder()
        .type(TransactionType.FRAME)
        .chainId(BigInteger.ONE)
        .nonce(0)
        .sender(FRAME_SENDER)
        .frames(frames)
        .frameSignatures(List.of())
        .maxPriorityFeePerGas(Wei.ZERO)
        .maxFeePerGas(Wei.ZERO)
        .maxFeePerBlobGas(Wei.ZERO);
  }

  private static Transaction frameTransaction(final Frame... frames) {
    return frameTransaction(List.of(frames));
  }

  private static Transaction frameTransaction(final List<Frame> frames) {
    return frameTransactionBuilder(frames).build();
  }

  private static Transaction frameTransactionWithSignature(final FrameSignature signature) {
    return frameTransactionBuilder(List.of(verifyFrame(), senderFrame()))
        .frameSignatures(List.of(signature))
        .build();
  }

  private static Transaction frameTransactionWithNonceKeys(final List<UInt256> nonceKeys) {
    return frameTransactionBuilder(List.of(verifyFrame(), senderFrame()))
        .nonceKeys(nonceKeys)
        .build();
  }

  private static Frame frameOf(final int mode, final int flags) {
    return new Frame(mode, flags, Optional.empty(), FRAME_GAS, 0L, Wei.ZERO, Bytes.EMPTY);
  }

  private static Frame verifyFrame() {
    return frameOf(Frame.MODE_VERIFY, Frame.APPROVE_SCOPE_MASK);
  }

  private static Frame senderFrame() {
    return frameOf(Frame.MODE_SENDER, 0);
  }

  private static Frame senderFrame(final long executionGasLimit, final long stateGasLimit) {
    return new Frame(
        Frame.MODE_SENDER,
        0,
        Optional.empty(),
        executionGasLimit,
        stateGasLimit,
        Wei.ZERO,
        Bytes.EMPTY);
  }

  /** A well-formed expiry verifier frame: VERIFY, no flags, no state gas, an 8-byte deadline. */
  private static Frame expiryFrame() {
    return expiryFrame(0, 0L, Bytes.ofUnsignedLong(1_000L));
  }

  private static Frame expiryFrame(final int flags, final long stateGasLimit, final Bytes data) {
    return new Frame(
        Frame.MODE_VERIFY,
        flags,
        Optional.of(MainnetTransactionValidator.EXPIRY_VERIFIER),
        FRAME_GAS,
        stateGasLimit,
        Wei.ZERO,
        data);
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
