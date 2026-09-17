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
package org.hyperledger.besu.ethereum.transaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.hyperledger.besu.datatypes.Address;

import java.math.BigInteger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

public class CallParameterTest {

  private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new Jdk8Module());

  @Test
  public void acceptsAndCapMaxValueForGas() throws JsonProcessingException {
    final String json =
        """
        {
          "gas": "0xffffffffffffffff"
        }
        """;

    final CallParameter callParameter = objectMapper.readValue(json, CallParameter.class);

    assertThat(callParameter.getGas()).hasValue(Long.MAX_VALUE);
  }

  @Test
  public void dataAsPayLoad() throws JsonProcessingException {
    final String json =
        """
        {
          "data": "0x1234"
        }
        """;

    final CallParameter callParameter = objectMapper.readValue(json, CallParameter.class);

    assertThat(callParameter.getPayload()).contains(Bytes.fromHexString("0x1234"));
  }

  @Test
  public void inputAsPayLoad() throws JsonProcessingException {
    final String json =
        """
            {
              "input": "0x1234"
            }
            """;

    final CallParameter callParameter = objectMapper.readValue(json, CallParameter.class);

    assertThat(callParameter.getPayload()).contains(Bytes.fromHexString("0x1234"));
  }

  @Test
  public void inputAndDataWithSameValueAsPayLoad() throws JsonProcessingException {
    final String json =
        """
            {
              "input": "0x1234",
              "data": "0x1234"
            }
            """;

    final CallParameter callParameter = objectMapper.readValue(json, CallParameter.class);

    assertThat(callParameter.getPayload()).contains(Bytes.fromHexString("0x1234"));
  }

  @Test
  public void inputAndDataWithDifferentValueAsPayLoadCauseException() {
    final String json =
        """
            {
              "input": "0x1234",
              "data": "0x1235"
            }
            """;

    assertThatExceptionOfType(JsonMappingException.class)
        .isThrownBy(() -> objectMapper.readValue(json, CallParameter.class))
        .withMessageContaining("problem: Only one of 'input' or 'data' should be provided");
  }

  @Test
  public void extraParametersAreIgnored() throws JsonProcessingException {
    // 0x96 = 150
    final String json =
        """
            {
              "gas": "0x96",
              "gasLimit": "0xfa",
              "extraField": "extra"
            }
            """;

    final CallParameter callParameter = objectMapper.readValue(json, CallParameter.class);

    assertThat(callParameter.getGas()).hasValue(150);
  }

  @Test
  public void signedAuthorizationListSerializationRoundtrip() throws JsonProcessingException {
    final String json =
        """
        {
          "authorizationList": [
            {
              "chainId": "0x1",
              "address": "0x6b7879a5d747e30a3adb37a9e41c046928fce933",
              "nonce": "0x82",
              "v": "0x1",
              "r": "0x462a70678128d9dd8f5b8010aaecddda1ba9ad767f0807c341f38d4dcb7eb893",
              "s": "0x645f7afd51a86bafe8939c8498fc89769918a38213859843ad7b19ffd4273a48"
            }
          ]
        }
        """;

    final CallParameter callParameter = objectMapper.readValue(json, CallParameter.class);

    assertThat(callParameter.getCodeDelegationAuthorizations())
        .hasSize(1)
        .first()
        .satisfies(
            auth -> {
              assertThat(auth.chainId()).isEqualTo(BigInteger.ONE);
              assertThat(auth.address().getBytes().toHexString())
                  .isEqualTo("0x6b7879a5d747e30a3adb37a9e41c046928fce933");
              assertThat(auth.nonce()).isEqualTo(130L);
              assertThat(auth.v()).isEqualTo((byte) 0x1);
              assertThat(auth.r())
                  .isEqualTo(
                      new BigInteger(
                          "462a70678128d9dd8f5b8010aaecddda1ba9ad767f0807c341f38d4dcb7eb893", 16));
              assertThat(auth.s())
                  .isEqualTo(
                      new BigInteger(
                          "645f7afd51a86bafe8939c8498fc89769918a38213859843ad7b19ffd4273a48", 16));
            });

    final String serialized = objectMapper.writeValueAsString(callParameter);
    assertThat(serialized).contains("\"r\"").contains("\"s\"").contains("\"v\"");
    assertThat(serialized).doesNotContain("authority");
  }

  @Test
  public void signedAuthorizationListWithYParity() throws JsonProcessingException {
    final String json =
        """
        {
          "authorizationList": [
            {
              "chainId": "0x1",
              "address": "0x6b7879a5d747e30a3adb37a9e41c046928fce933",
              "nonce": "0x82",
              "yParity": "0x1",
              "r": "0x462a70678128d9dd8f5b8010aaecddda1ba9ad767f0807c341f38d4dcb7eb893",
              "s": "0x645f7afd51a86bafe8939c8498fc89769918a38213859843ad7b19ffd4273a48"
            }
          ]
        }
        """;

    final CallParameter callParameter = objectMapper.readValue(json, CallParameter.class);

    assertThat(callParameter.getCodeDelegationAuthorizations())
        .hasSize(1)
        .first()
        .satisfies(
            auth -> {
              assertThat(auth.chainId()).isEqualTo(BigInteger.ONE);
              assertThat(auth.address().getBytes().toHexString())
                  .isEqualTo("0x6b7879a5d747e30a3adb37a9e41c046928fce933");
              assertThat(auth.nonce()).isEqualTo(130L);
              assertThat(auth.v()).isEqualTo((byte) 0x1);
              assertThat(auth.r())
                  .isEqualTo(
                      new BigInteger(
                          "462a70678128d9dd8f5b8010aaecddda1ba9ad767f0807c341f38d4dcb7eb893", 16));
              assertThat(auth.s())
                  .isEqualTo(
                      new BigInteger(
                          "645f7afd51a86bafe8939c8498fc89769918a38213859843ad7b19ffd4273a48", 16));
            });
  }

  @Test
  public void unsignedAuthorizationListSerializationRoundtrip() throws JsonProcessingException {
    final String json =
        """
        {
          "authorizationList": [
            {
              "chainId": "0x1",
              "address": "0x6b7879a5d747e30a3adb37a9e41c046928fce933",
              "nonce": "0x82",
              "authority": "0xe0f5206bbd039e7b0592d8918820024e2a7437b9"
            }
          ]
        }
        """;

    final CallParameter callParameter = objectMapper.readValue(json, CallParameter.class);

    assertThat(callParameter.getCodeDelegationAuthorizations())
        .hasSize(1)
        .first()
        .satisfies(
            auth -> {
              assertThat(auth.chainId()).isEqualTo(BigInteger.ONE);
              assertThat(auth.address().getBytes().toHexString())
                  .isEqualTo("0x6b7879a5d747e30a3adb37a9e41c046928fce933");
              assertThat(auth.nonce()).isEqualTo(130L);
              assertThat(auth.authorizer())
                  .contains(Address.fromHexString("0xe0f5206bbd039e7b0592d8918820024e2a7437b9"));
            });

    final String serialized = objectMapper.writeValueAsString(callParameter);
    assertThat(serialized).contains("authority");
    assertThat(serialized).doesNotContain("\"r\"");
  }

  @Test
  public void authorityAndSignatureProvidedCausesException() {
    final String json =
        """
            {
              "authorizationList": [
                {
                  "chainId": "0x1",
                  "address": "0x6b7879a5d747e30a3adb37a9e41c046928fce933",
                  "nonce": "0x82",
                  "authority": "0xe0f5206bbd039e7b0592d8918820024e2a7437b9",
                  "r": "0x1",
                  "s": "0x1",
                  "v": "0x1"
                }
              ]
            }
            """;

    assertThatExceptionOfType(JsonMappingException.class)
        .isThrownBy(() -> objectMapper.readValue(json, CallParameter.class))
        .withMessageContaining("authority and signature");
  }
}
