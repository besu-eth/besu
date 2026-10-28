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
package org.hyperledger.besu.datatypes;

import static com.google.common.base.Preconditions.checkState;

import org.hyperledger.besu.datatypes.parameters.UnsignedLongParameter;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/** Account Override parameter class */
@JsonDeserialize(builder = StateOverride.Builder.class)
public class StateOverride {
  private final Optional<Wei> balance;
  private final Optional<Long> nonce;
  private final Optional<String> code;
  private final Optional<Map<String, String>> state;
  private final Optional<Map<String, String>> stateDiff;
  private final Optional<Address> movePrecompileToAddress;

  private StateOverride(
      final Optional<Wei> balance,
      final Optional<Long> nonce,
      final Optional<String> code,
      final Optional<Map<String, String>> state,
      final Optional<Map<String, String>> stateDiff,
      final Optional<Address> movePrecompileToAddress) {
    this.balance = balance;
    this.nonce = nonce;
    this.code = code;
    this.state = state;
    this.stateDiff = stateDiff;
    this.movePrecompileToAddress = movePrecompileToAddress;
  }

  /**
   * Gets the balance override
   *
   * @return the balance if present
   */
  public Optional<Wei> getBalance() {
    return balance;
  }

  /**
   * Gets the nonce override
   *
   * @return the nonce if present
   */
  public Optional<Long> getNonce() {
    return nonce;
  }

  /**
   * Gets the code override
   *
   * @return the code if present
   */
  public Optional<String> getCode() {
    return code;
  }

  /**
   * Gets the state override map
   *
   * @return the state override map if present
   */
  public Optional<Map<String, String>> getState() {
    return state;
  }

  /**
   * Gets the state diff override map
   *
   * @return the state diff override map if present
   */
  public Optional<Map<String, String>> getStateDiff() {
    return stateDiff;
  }

  /**
   * Gets the new address for the pre-compiled contract
   *
   * @return the new address for the pre-compiled contract if present
   */
  public Optional<Address> getMovePrecompileToAddress() {
    return movePrecompileToAddress;
  }

  /**
   * Creates a new builder for State overrides
   *
   * @return the builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder class for Account overrides */
  public static class Builder {
    private Optional<Wei> balance = Optional.empty();
    private Optional<Long> nonce = Optional.empty();
    private Optional<String> code = Optional.empty();
    private Optional<Map<String, String>> state = Optional.empty();
    private Optional<Map<String, String>> stateDiff = Optional.empty();
    private Optional<Address> movePrecompileToAddress = Optional.empty();

    /** Default constructor. */
    public Builder() {}

    /**
     * Sets the balance override
     *
     * @param balance the balance override
     * @return the builder
     */
    public Builder withBalance(final Wei balance) {
      this.balance = Optional.ofNullable(balance);
      return this;
    }

    /**
     * Sets the nonce override
     *
     * @param nonce the nonce override in hex
     * @return the builder
     */
    public Builder withNonce(final UnsignedLongParameter nonce) {
      this.nonce = Optional.of(nonce.getValue());
      return this;
    }

    /**
     * Sets the code override
     *
     * @param code the code override
     * @return the builder
     */
    public Builder withCode(final String code) {
      this.code = Optional.ofNullable(code);
      return this;
    }

    /**
     * Sets the state override
     *
     * @param state the map of state overrides
     * @return the builder
     */
    public Builder withState(final Map<String, String> state) {
      this.state = Optional.ofNullable(state);
      return this;
    }

    /**
     * Sets the state diff override
     *
     * @param stateDiff the map of state overrides
     * @return the builder
     */
    public Builder withStateDiff(final Map<String, String> stateDiff) {
      this.stateDiff = Optional.ofNullable(stateDiff);
      return this;
    }

    /**
     * Sets the new address for the pre-compiled contract
     *
     * @param newPrecompileAddress the new address for the pre-compile contract
     * @return the builder
     */
    public Builder withMovePrecompileToAddress(final Address newPrecompileAddress) {
      this.movePrecompileToAddress = Optional.ofNullable(newPrecompileAddress);
      return this;
    }

    /**
     * build the account override from the builder
     *
     * @return account override
     */
    public StateOverride build() {
      checkState(state.isEmpty() || stateDiff.isEmpty(), "Cannot set both state and stateDiff");
      return new StateOverride(balance, nonce, code, state, stateDiff, movePrecompileToAddress);
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final StateOverride stateOverride = (StateOverride) o;
    return balance.equals(stateOverride.balance)
        && nonce.equals(stateOverride.nonce)
        && code.equals(stateOverride.code)
        && state.equals(stateOverride.state)
        && stateDiff.equals(stateOverride.stateDiff);
  }

  @Override
  public int hashCode() {
    return Objects.hash(balance, nonce, code, state, stateDiff);
  }

  @Override
  public String toString() {
    return "StateOverride{"
        + "balance="
        + balance
        + ", nonce="
        + nonce
        + ", code="
        + code
        + ", state="
        + state
        + ", stateDiff="
        + stateDiff
        + ", movePrecompileToAddress="
        + movePrecompileToAddress
        + '}';
  }
}
