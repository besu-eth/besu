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
package org.hyperledger.besu.ethereum.eth.transactions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.core.TransactionTestFixture.createSignedCodeDelegation;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.CodeDelegation;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.core.Util;

import java.math.BigInteger;
import java.util.List;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;

public class PendingDelegationsTest {

  private static final Address TARGET =
      Address.fromHexString("0x00000000000000000000000000000000000000aa");
  private static final BigInteger TX_CHAIN_ID = BigInteger.valueOf(1337);

  private final KeyPair authority = SignatureAlgorithmFactory.getInstance().generateKeyPair();
  private final Address authorityAddress = Util.publicKeyToAddress(authority.getPublicKey());

  @Test
  public void authorizationForAnyChainOrTheChainOfTheTransactionIsKept() {
    final var authorities =
        PendingDelegations.authoritiesOf(
            transaction(
                signed(BigInteger.ZERO, 3L, authority), signed(TX_CHAIN_ID, 4L, authority)));
    assertThat(authorities).containsOnlyKeys(authorityAddress);
    assertThat(authorities.get(authorityAddress)).containsExactly(3L, 4L);
  }

  @Test
  public void authorizationForAnotherChainIsSkipped() {
    final var authorities =
        PendingDelegations.authoritiesOf(transaction(signed(BigInteger.valueOf(1), 3L, authority)));
    assertThat(authorities).isEmpty();
  }

  @Test
  public void authorizationsWithNoncesThatCanNeverBeAppliedAreSkipped() {
    final var authorities =
        PendingDelegations.authoritiesOf(
            transaction(
                // 2^64-1 and 2^63, unsigned, are negative as a long
                signed(BigInteger.ZERO, -1L, authority),
                signed(BigInteger.ZERO, Long.MIN_VALUE, authority),
                signed(BigInteger.ZERO, Long.MAX_VALUE, authority),
                signed(BigInteger.ZERO, Long.MAX_VALUE - 1, authority)));
    assertThat(authorities.get(authorityAddress)).containsExactly(Long.MAX_VALUE - 1);
  }

  @Test
  public void severalAuthorizationsOfAnAuthorityAreKeptSorted() {
    final var authorities =
        PendingDelegations.authoritiesOf(
            transaction(
                signed(BigInteger.ZERO, 9L, authority),
                signed(BigInteger.ZERO, 2L, authority),
                signed(BigInteger.ZERO, 5L, authority)));
    assertThat(authorities.get(authorityAddress)).containsExactly(2L, 5L, 9L);
  }

  @Test
  public void selfDelegationNeedsTheNonceAfterTheOneOfTheTransaction() {
    assertThat(PendingDelegations.selfDelegationOf(0, nonces(0L))).isEmpty();
    assertThat(PendingDelegations.selfDelegationOf(3, nonces(1L, 9L))).isEmpty();
    assertThat(PendingDelegations.selfDelegationOf(0, nonces(1L)))
        .contains(new PendingDelegations.SelfDelegation(1, 1));
  }

  @Test
  public void selfDelegationConsumesTheConsecutiveNoncesAfterTheOneOfTheTransaction() {
    final var delegation = PendingDelegations.selfDelegationOf(4, nonces(5L, 6L, 7L, 9L, 3L));
    assertThat(delegation).contains(new PendingDelegations.SelfDelegation(5, 7));
    assertThat(delegation.get().consumes(4)).isFalse();
    assertThat(delegation.get().consumes(5)).isTrue();
    assertThat(delegation.get().consumes(7)).isTrue();
    assertThat(delegation.get().consumes(8)).isFalse();
    assertThat(delegation.get().highestAllowedNonce()).isEqualTo(8);
  }

  @Test
  public void selfDelegationDoesNotOverflow() {
    assertThat(PendingDelegations.selfDelegationOf(-1, nonces(0L))).isEmpty();
    assertThat(PendingDelegations.selfDelegationOf(Long.MAX_VALUE, nonces(1L))).isEmpty();
    assertThat(PendingDelegations.selfDelegationOf(Long.MAX_VALUE - 2, nonces(Long.MAX_VALUE - 1)))
        .contains(new PendingDelegations.SelfDelegation(Long.MAX_VALUE - 1, Long.MAX_VALUE - 1));
  }

  @Test
  public void pendingSelfDelegationsIgnoreTransactionsThatCanNotBeExecutedAnymore() {
    final PendingDelegations pendingDelegations = new PendingDelegations();
    final Transaction stale = transaction(0, signed(BigInteger.ZERO, 1L, authority));
    final Transaction live = transaction(5, signed(BigInteger.ZERO, 6L, authority));
    pendingDelegations.add(stale, PendingDelegations.authoritiesOf(stale));
    pendingDelegations.add(live, PendingDelegations.authoritiesOf(live));

    assertThat(pendingDelegations.pendingSelfDelegations(authorityAddress, 3))
        .containsExactly(new PendingDelegations.SelfDelegation(6, 6));
    assertThat(pendingDelegations.pendingSelfDelegations(authorityAddress, 0)).hasSize(2);
  }

  private static NavigableSet<Long> nonces(final Long... nonces) {
    return new TreeSet<>(List.of(nonces));
  }

  private static CodeDelegation signed(
      final BigInteger chainId, final long nonce, final KeyPair authority) {
    return createSignedCodeDelegation(chainId, TARGET, nonce, authority);
  }

  private Transaction transaction(final CodeDelegation... delegations) {
    return transaction(0, delegations);
  }

  // the sender is the authority, so the self delegation helpers can be exercised too
  private Transaction transaction(final long nonce, final CodeDelegation... delegations) {
    return new TransactionTestFixture()
        .type(TransactionType.DELEGATE_CODE)
        .nonce(nonce)
        .gasLimit(100_000L)
        .to(Optional.of(TARGET))
        .codeDelegations(List.of(delegations))
        .createTransaction(authority);
  }
}
