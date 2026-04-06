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
package org.hyperledger.besu.ethereum.core;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.encoding.CodeDelegationTransactionEncoder;
import org.hyperledger.besu.ethereum.core.json.ChainIdDeserializer;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import java.math.BigInteger;
import java.util.Optional;
import java.util.function.Supplier;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Suppliers;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

// ignore `signer` field used in execution-spec-tests
@JsonIgnoreProperties(ignoreUnknown = true)
public class CodeDelegation implements org.hyperledger.besu.datatypes.CodeDelegation {
  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();

  public static final Bytes MAGIC = Bytes.fromHexString("05");

  private final BigInteger chainId;
  private final Address address;
  private final long nonce;
  private final SECPSignature signature;
  private final Supplier<Optional<Address>> authorizerSupplier =
      Suppliers.memoize(this::computeAuthority);

  /**
   * An access list entry as defined in EIP-7702
   *
   * @param chainId can be either the current chain id or zero
   * @param address the address from which the code will be set into the EOA account
   * @param nonce the nonce after which this auth expires
   * @param signature the signature of the EOA account which will be used to set the code
   */
  public CodeDelegation(
      final BigInteger chainId,
      final Address address,
      final long nonce,
      final SECPSignature signature) {
    this.chainId = chainId;
    this.address = address;
    this.nonce = nonce;
    this.signature = signature;
  }

  /**
   * Create code delegation.
   *
   * @param chainId can be either the current chain id or zero
   * @param address the address from which the code will be set into the EOA account
   * @param nonce the nonce
   * @param v the recovery id
   * @param r the r value of the signature
   * @param s the s value of the signature
   * @return CodeDelegation
   */
  @JsonCreator
  public static org.hyperledger.besu.datatypes.CodeDelegation createCodeDelegation(
      @JsonProperty("chainId") @JsonDeserialize(using = ChainIdDeserializer.class)
          final BigInteger chainId,
      @JsonProperty("address") final Address address,
      @JsonProperty("nonce") final String nonce,
      @JsonProperty("v") final String v,
      @JsonProperty("r") final String r,
      @JsonProperty("s") final String s) {
    return new CodeDelegation(
        chainId,
        address,
        Bytes.fromHexStringLenient(nonce).toLong(),
        SIGNATURE_ALGORITHM.createCodeDelegationSignature(
            Bytes.fromHexStringLenient(r).toUnsignedBigInteger(),
            Bytes.fromHexStringLenient(s).toUnsignedBigInteger(),
            Bytes.fromHexStringLenient(v).get(0)));
  }

  @JsonProperty("chainId")
  @Override
  public BigInteger chainId() {
    return chainId;
  }

  @JsonProperty("address")
  @Override
  public Address address() {
    return address;
  }

  @JsonProperty("signature")
  @Override
  public SECPSignature signature() {
    return signature;
  }

  @Override
  public Optional<Address> authorizer() {
    // recId needs to be between either 0 or 1, otherwise the signature is invalid
    // which means we can't recover the authorizer.
    if (signature.getRecId() > 1 || signature.getRecId() < 0) {
      return Optional.empty();
    }

    return authorizerSupplier.get();
  }

  @Override
  public long nonce() {
    return nonce;
  }

  @JsonProperty("v")
  @Override
  public byte v() {
    return signature.getRecId();
  }

  @JsonProperty("r")
  @Override
  public BigInteger r() {
    return signature.getR();
  }

  @JsonProperty("s")
  @Override
  public BigInteger s() {
    return signature.getS();
  }

  private Optional<Address> computeAuthority() {
    BytesValueRLPOutput rlpOutput = new BytesValueRLPOutput();
    CodeDelegationTransactionEncoder.encodeSingleCodeDelegationWithoutSignature(this, rlpOutput);

    final Hash hash = Hash.hash(Bytes.concatenate(MAGIC, rlpOutput.encoded()));

    Optional<Address> authorityAddress;
    try {
      authorityAddress =
          SIGNATURE_ALGORITHM
              .recoverPublicKeyFromSignature(Bytes32.wrap(hash.getBytes()), signature)
              .map(Address::extract);
    } catch (final IllegalArgumentException e) {
      authorityAddress = Optional.empty();
    }

    return authorityAddress;
  }

  /**
   * Create a code delegation authorization with a builder.
   *
   * @return CodeDelegation.Builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for CodeDelegation authorizations. */
  public static class Builder {
    private BigInteger chainId = BigInteger.ZERO;
    private Address address;
    private Long nonce;
    private SECPSignature signature;

    /** Create a new builder. */
    protected Builder() {}

    /**
     * Set the optional chain id.
     *
     * @param chainId the chain id
     * @return this builder
     */
    public Builder chainId(final BigInteger chainId) {
      this.chainId = chainId;
      return this;
    }

    /**
     * Set the address of the authorized smart contract.
     *
     * @param address the address
     * @return this builder
     */
    public Builder address(final Address address) {
      this.address = address;
      return this;
    }

    /**
     * Set the nonce.
     *
     * @param nonce the nonce.
     * @return this builder
     */
    public Builder nonce(final long nonce) {
      this.nonce = nonce;
      return this;
    }

    /**
     * Set the signature of the authorizer account.
     *
     * @param signature the signature
     * @return this builder
     */
    public Builder signature(final SECPSignature signature) {
      this.signature = signature;
      return this;
    }

    /**
     * Sign the authorization with the given key pair and return the authorization.
     *
     * @param keyPair the key pair
     * @return CodeDelegation
     */
    public org.hyperledger.besu.datatypes.CodeDelegation signAndBuild(final KeyPair keyPair) {
      final BytesValueRLPOutput output = new BytesValueRLPOutput();
      output.startList();
      output.writeBigIntegerScalar(chainId);
      output.writeBytes(address.getBytes());
      output.writeLongScalar(nonce);
      output.endList();

      signature(
          SIGNATURE_ALGORITHM.sign(
              Bytes32.wrap(Hash.hash(Bytes.concatenate(MAGIC, output.encoded())).getBytes()),
              keyPair));
      return build();
    }

    /**
     * Build the authorization.
     *
     * @return CodeDelegation
     */
    public org.hyperledger.besu.datatypes.CodeDelegation build() {
      if (address == null) {
        throw new IllegalStateException("Address must be set");
      }

      if (nonce == null) {
        throw new IllegalStateException("Nonce must be set");
      }

      if (signature == null) {
        throw new IllegalStateException("Signature must be set");
      }

      return new CodeDelegation(chainId, address, nonce, signature);
    }
  }

  @Override
  public String toString() {
    return "CodeDelegation{"
        + "chainId="
        + chainId
        + ", address="
        + address
        + ", nonce="
        + nonce
        + ", signature="
        + signature
        + ", authorizerSupplier="
        + authorizerSupplier
        + '}';
  }
}
