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
import static org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason.UPFRONT_COST_EXCEEDS_BALANCE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECP256K1;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobType;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.GasLimitCalculator;
import org.hyperledger.besu.ethereum.core.BlobTestFixture;
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
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes48;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.FieldSource;
import org.junit.jupiter.params.provider.MethodSource;
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
    when(gasCalculator.transactionFloorCost(any(), anyLong())).thenReturn(51L);

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
  public void shouldRejectTransactionWhenSenderAccountDoesNotExist() {
    final TransactionValidator validator =
        createTransactionValidator(
            gasCalculator, GasLimitCalculator.constant(), false, Optional.of(BigInteger.ONE));
    assertThat(validator.validateForSender(basicTransaction, null, processingBlockParams))
        .isEqualTo(ValidationResult.invalid(TransactionInvalidReason.UPFRONT_COST_EXCEEDS_BALANCE));
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
              transactionSimulationParams, ValidationResult.invalid(UPFRONT_COST_EXCEEDS_BALANCE)),
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

  @Test
  public void shouldRejectTransactionWithMaxPriorityFeeGreaterThanMaxFee() {
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
            transaction, Optional.of(Wei.ONE), Optional.empty(), transactionProcessingParams);
    assertThat(validationResult)
        .isEqualTo(ValidationResult.invalid(MAX_PRIORITY_FEE_PER_GAS_EXCEEDS_MAX_FEE_PER_GAS));
    assertThat(validationResult.getErrorMessage())
        .isEqualTo("max priority fee per gas cannot be greater than max fee per gas");
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
                    new BlobsWithCommitments(
                        BlobType.KZG_PROOF,
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

  private enum ValidationParamsVariant {
    PROCESSING,
    SIMULATING
  }

  private static Stream<Arguments> shouldSupportTransactionGasLimitCap_EIP_7825() {
    return Stream.of(
        Arguments.of(ValidationParamsVariant.PROCESSING, 16_777_216L, true),
        Arguments.of(ValidationParamsVariant.PROCESSING, 16_777_217L, false),
        Arguments.of(ValidationParamsVariant.SIMULATING, 16_777_216L, true),
        Arguments.of(ValidationParamsVariant.SIMULATING, 16_777_217L, true));
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
