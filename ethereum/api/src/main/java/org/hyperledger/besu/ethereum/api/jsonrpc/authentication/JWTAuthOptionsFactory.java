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
package org.hyperledger.besu.ethereum.api.jsonrpc.authentication;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;

import io.vertx.core.buffer.Buffer;
import io.vertx.ext.auth.JWTOptions;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.impl.Codec;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import org.apache.tuweni.bytes.Bytes32;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;

public class JWTAuthOptionsFactory {

  private static final JwtAlgorithm DEFAULT = JwtAlgorithm.RS256;

  public JWTAuthOptions createForExternalPublicKey(final File externalPublicKeyFile) {
    return createForExternalPublicKeyWithAlgorithm(externalPublicKeyFile, DEFAULT);
  }

  public JWTAuthOptions createForExternalPublicKeyWithAlgorithm(
      final File externalPublicKeyFile, final JwtAlgorithm algorithm) {
    if (algorithm.toString().startsWith("H")) {
      throw new IllegalArgumentException(
          "Cannot use a public key file with HMAC algorithm "
              + algorithm
              + "; use an asymmetric algorithm (RS256, ES256) or supply a secret key file");
    }
    final byte[] externalJwtPublicKey = readPublicKey(externalPublicKeyFile);
    return new JWTAuthOptions()
        .addPubSecKey(
            new PubSecKeyOptions()
                .setAlgorithm(algorithm.toString())
                .setBuffer(keyPairToPublicPemString(externalJwtPublicKey)));
  }

  public JWTAuthOptions createWithGeneratedKeyPair(final JwtAlgorithm jwtAlgorithm) {
    if (jwtAlgorithm.toString().startsWith("H")) {
      throw new IllegalArgumentException(
          "Cannot use keypairs with HMAC tokens, please call createWithGeneratedKey");
    }
    final KeyPair keypair = generateRsaKeyPair();
    return new JWTAuthOptions()
        .addPubSecKey(
            new PubSecKeyOptions()
                .setAlgorithm(jwtAlgorithm.toString())
                .setBuffer(keyPairToPublicPemString(keypair.getPublic().getEncoded())))
        .addPubSecKey(
            new PubSecKeyOptions()
                .setAlgorithm(jwtAlgorithm.toString())
                .setBuffer(keyPairToPrivatePemString(keypair)));
  }

  public JWTAuthOptions engineApiJWTOptions(final JwtAlgorithm jwtAlgorithm) {
    byte[] ephemeralKey = Bytes32.random().toArray();
    return new JWTAuthOptions()
        .setJWTOptions(new JWTOptions().setIgnoreExpiration(true).setLeeway(5))
        .addPubSecKey(
            new PubSecKeyOptions()
                .setAlgorithm(jwtAlgorithm.toString())
                .setBuffer(Buffer.buffer(ephemeralKey)));
  }

  public JWTAuthOptions createWithGeneratedKeyPair() {
    return createWithGeneratedKeyPair(JwtAlgorithm.RS256);
  }

  private byte[] readPublicKey(final File publicKeyFile) {
    try (final BufferedReader reader = Files.newBufferedReader(publicKeyFile.toPath(), UTF_8);
        final PemReader pemReader = new PemReader(reader)) {
      final PemObject pemObject = pemReader.readPemObject();
      if (pemObject == null) {
        throw new IllegalStateException("Authentication RPC public key file format is invalid");
      }
      return pemObject.getContent();
    } catch (IOException e) {
      throw new IllegalStateException("Authentication RPC public key could not be read", e);
    }
  }

  private KeyPair generateRsaKeyPair() {
    final KeyPairGenerator keyGenerator;
    try {
      keyGenerator = KeyPairGenerator.getInstance("RSA");
      keyGenerator.initialize(2048);
    } catch (final NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }

    return keyGenerator.generateKeyPair();
  }

  private String keyPairToPublicPemString(final byte[] publicKey) {
    StringBuilder pemBuffer = new StringBuilder();
    pemBuffer.append("-----BEGIN PUBLIC KEY-----\r\n");
    pemBuffer.append(Codec.base64MimeEncode(publicKey));
    pemBuffer.append("\r\n");
    pemBuffer.append("-----END PUBLIC KEY-----\r\n");
    return pemBuffer.toString();
  }

  private String keyPairToPrivatePemString(final KeyPair kp) {
    StringBuilder pemBuffer = new StringBuilder();
    pemBuffer.append("-----BEGIN PRIVATE KEY-----\r\n");
    pemBuffer.append(Codec.base64MimeEncode(kp.getPrivate().getEncoded()));
    pemBuffer.append("\r\n");
    pemBuffer.append("-----END PRIVATE KEY-----\r\n");
    return pemBuffer.toString();
  }
}
