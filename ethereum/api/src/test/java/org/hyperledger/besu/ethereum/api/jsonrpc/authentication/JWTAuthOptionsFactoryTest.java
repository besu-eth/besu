/*
 * Copyright ConsenSys AG.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import org.bouncycastle.util.encoders.Base64;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;
import org.junit.jupiter.api.Test;

public class JWTAuthOptionsFactoryTest {

  private static final String JWT_PUBLIC_KEY_RS256 =
      "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAw6tMhjogMulRbYby7bCL"
          + "rFhukDnxvm4XR3KSXLKdLLQHHyouMOQaLac9M+/Z1KkIpqfZPjLfW2/yUg2IKx4T"
          + "dvFVzbVq17X6dq49ZS8jJtb8l2+Vius4d3LnpvxCOematRG9Acn+2qLwC+sK7RPY"
          + "OxEqKPU5LNBH1C0FfviazY5jkixBFICzIq/SyyRnGX+iIONnNsu0TlhWVLSlZbg5"
          + "NYf4cAzu/1d5MgspyZwnRo468gqaak3wQzkmk69Z25L1N7TXZvk2b7rT7/ssFnt+"
          + "//fKVpD6qkQ3OopD+7gOziAYUxChw6RUWekV+uRgNADQhaqV6wDdogBz77wTJedV"
          + "YwIDAQAB";

  private static final String JWT_PUBLIC_KEY_ES256 =
      "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEy8+qE0ZFo4woPUCXfszptuDgaDpW"
          + "Sv6D5F/pbolJ2wZTVkYXoGhA3wqy1RM1RYmROp9NEPLm3mZP+kzI4TMiGg==";

  @Test
  public void createsOptionsWithGeneratedKeyPair() {
    final JWTAuthOptionsFactory jwtAuthOptionsFactory = new JWTAuthOptionsFactory();
    final JWTAuthOptions jwtAuthOptions = jwtAuthOptionsFactory.createWithGeneratedKeyPair();

    assertThat(jwtAuthOptions.getPubSecKeys()).isNotNull();
    assertThat(jwtAuthOptions.getPubSecKeys()).hasSize(2);
    assertThat(jwtAuthOptions.getPubSecKeys().get(0).getAlgorithm()).isEqualTo("RS256");
    assertThat(jwtAuthOptions.getPubSecKeys().get(0).getBuffer()).isNotNull();
    assertThat(jwtAuthOptions.getPubSecKeys().get(1).getAlgorithm()).isEqualTo("RS256");
    assertThat(jwtAuthOptions.getPubSecKeys().get(1).getBuffer()).isNotNull();
  }

  @Test
  public void createsOptionsWithGeneratedKeyPairThatIsDifferentEachTime() {
    final JWTAuthOptionsFactory jwtAuthOptionsFactory = new JWTAuthOptionsFactory();
    final JWTAuthOptions jwtAuthOptions1 = jwtAuthOptionsFactory.createWithGeneratedKeyPair();
    final JWTAuthOptions jwtAuthOptions2 = jwtAuthOptionsFactory.createWithGeneratedKeyPair();

    final PubSecKeyOptions pubSecKeyOptions1 = jwtAuthOptions1.getPubSecKeys().get(0);
    final PubSecKeyOptions pubSecKeyOptions2 = jwtAuthOptions2.getPubSecKeys().get(0);
    assertThat(pubSecKeyOptions1.getBuffer()).isNotEqualTo(pubSecKeyOptions2.getBuffer());
  }

  @Test
  public void createsOptionsUsingPublicKeyFile() throws URISyntaxException, IOException {
    final JWTAuthOptionsFactory jwtAuthOptionsFactory = new JWTAuthOptionsFactory();
    final File enclavePublicKeyFile =
        Paths.get(ClassLoader.getSystemResource("authentication/jwt_public_key_rsa").toURI())
            .toAbsolutePath()
            .toFile();

    final JWTAuthOptions jwtAuthOptions =
        jwtAuthOptionsFactory.createForExternalPublicKey(enclavePublicKeyFile);
    assertThat(jwtAuthOptions.getPubSecKeys()).hasSize(1);
    final PubSecKeyOptions pubSecKeyOptions = jwtAuthOptions.getPubSecKeys().get(0);
    assertThat(pubSecKeyOptions.getAlgorithm()).isEqualTo("RS256");
    PemObject publicKey =
        new PemReader(
                new InputStreamReader(
                    new ByteArrayInputStream(pubSecKeyOptions.getBuffer().getBytes()),
                    StandardCharsets.UTF_8))
            .readPemObject();
    assertThat(publicKey.getContent()).containsExactly(Base64.decode(JWT_PUBLIC_KEY_RS256));
  }

  @Test
  public void createOptionsUsingECDASPublicKeyFile() throws URISyntaxException {
    final JWTAuthOptionsFactory jwtAuthOptionsFactory = new JWTAuthOptionsFactory();
    final File enclavePublicKeyFile =
        Paths.get(ClassLoader.getSystemResource("authentication/jwt_public_key_ecdsa").toURI())
            .toAbsolutePath()
            .toFile();

    try {
      final JWTAuthOptions jwtAuthOptions =
          jwtAuthOptionsFactory.createForExternalPublicKeyWithAlgorithm(
              enclavePublicKeyFile, JwtAlgorithm.ES256);
      assertThat(jwtAuthOptions.getPubSecKeys()).hasSize(1);
      final PubSecKeyOptions pubSecKeyOptions = jwtAuthOptions.getPubSecKeys().get(0);
      assertThat(pubSecKeyOptions.getAlgorithm()).isEqualTo("ES256");
      PemObject publicKey =
          new PemReader(
                  new InputStreamReader(
                      new ByteArrayInputStream(pubSecKeyOptions.getBuffer().getBytes()),
                      StandardCharsets.UTF_8))
              .readPemObject();
      assertThat(publicKey.getContent()).containsExactly(Base64.decode(JWT_PUBLIC_KEY_ES256));
    } catch (Exception e) {
      fail("Should not have exceptions thrown" + Arrays.toString(e.getStackTrace()));
    }
  }

  @Test
  public void failsToCreateOptionsWhenPublicKeyFileDoesNotExist() {
    final JWTAuthOptionsFactory jwtAuthOptionsFactory = new JWTAuthOptionsFactory();
    final File enclavePublicKeyFile = new File("doesNotExist");

    assertThatThrownBy(() -> jwtAuthOptionsFactory.createForExternalPublicKey(enclavePublicKeyFile))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Authentication RPC public key could not be read");
  }

  @Test
  public void failsToCreateOptionsWhenPublicKeyFileIsInvalid() throws IOException {
    final JWTAuthOptionsFactory jwtAuthOptionsFactory = new JWTAuthOptionsFactory();
    final Path enclavePublicKeyFile = Files.createTempFile("enclave", "pub");
    Files.writeString(enclavePublicKeyFile, "invalidDataNo---HeadersAndNotBase64");

    assertThatThrownBy(
            () -> jwtAuthOptionsFactory.createForExternalPublicKey(enclavePublicKeyFile.toFile()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Authentication RPC public key file format is invalid");
  }

  @Test
  public void createsEphemeralHmacOptions() {
    final JWTAuthOptionsFactory factory = new JWTAuthOptionsFactory();
    JWTAuthOptions engineOptions = factory.engineApiJWTOptions(JwtAlgorithm.HS256);
    byte[] publicKey = engineOptions.getPubSecKeys().get(0).getBuffer().getBytes();
    assertThat(publicKey.length).isEqualTo(32);
  }
}
