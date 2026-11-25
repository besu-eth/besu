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
package org.hyperledger.besu.ethereum.eth.core.transactions;

import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.CodeDelegation;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;

public final class SetCodeTransactionFixture {

  private SetCodeTransactionFixture() {}

  /**
   * A signed EIP-7702 transaction carrying the given number of identical authorizations.
   *
   * @param gasLimit the transaction gas limit
   * @param authorizations the length of the authorization list
   * @return the transaction
   */
  public static Transaction createSetCodeTransaction(
      final long gasLimit, final int authorizations) {
    final SignatureAlgorithm signatureAlgorithm = SignatureAlgorithmFactory.getInstance();
    final CodeDelegation delegation =
        new org.hyperledger.besu.ethereum.core.CodeDelegation(
            BigInteger.ONE,
            Address.fromHexString("0x0000000000000000000000000000000000001000"),
            0,
            signatureAlgorithm.createCodeDelegationSignature(
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
        .signAndBuild(signatureAlgorithm.generateKeyPair());
  }
}
