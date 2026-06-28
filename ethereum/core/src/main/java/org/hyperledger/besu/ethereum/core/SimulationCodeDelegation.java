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
package org.hyperledger.besu.ethereum.core;

import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.CodeDelegation;
import org.hyperledger.besu.ethereum.core.json.ChainIdJson;

import java.math.BigInteger;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.tuweni.bytes.Bytes;

/**
 * Code delegation used only for simulations where the authority is provided directly rather than
 * recovered from a signature.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SimulationCodeDelegation implements CodeDelegation {
  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();
  // A placeholder SECPSignature is needed to satisfy the CodeDelegation interface. This approach is
  // consistent
  // with the simulator’s existing use of a fixed fake signature for transactions and does not leak
  // into any
  // transaction-handling code.
  private static final SECPSignature DUMMY_SIGNATURE =
      SIGNATURE_ALGORITHM.createSignature(BigInteger.ONE, BigInteger.ONE, (byte) 0);

  private final BigInteger chainId;
  private final Address address;
  private final long nonce;
  private final Address authority;

  @JsonCreator
  public SimulationCodeDelegation(
      @JsonProperty("chainId") @JsonDeserialize(using = ChainIdJson.Deserializer.class)
          final BigInteger chainId,
      @JsonProperty("address") final Address address,
      @JsonProperty("nonce") final String nonce,
      @JsonProperty("authority") final Address authority) {
    this.chainId = chainId;
    this.address = address;
    this.nonce = Bytes.fromHexStringLenient(nonce).toLong();
    this.authority = authority;
  }

  @Override
  @JsonProperty("chainId")
  public BigInteger chainId() {
    return chainId;
  }

  @Override
  @JsonProperty("address")
  public Address address() {
    return address;
  }

  @Override
  @JsonIgnore
  public SECPSignature signature() {
    return DUMMY_SIGNATURE;
  }

  @Override
  @JsonProperty("authority")
  public Optional<Address> authorizer() {
    return Optional.of(authority);
  }

  @Override
  public long nonce() {
    return nonce;
  }

  @Override
  @JsonIgnore
  public byte v() {
    return DUMMY_SIGNATURE.getRecId();
  }

  @Override
  @JsonIgnore
  public BigInteger r() {
    return DUMMY_SIGNATURE.getR();
  }

  @Override
  @JsonIgnore
  public BigInteger s() {
    return DUMMY_SIGNATURE.getS();
  }
}
