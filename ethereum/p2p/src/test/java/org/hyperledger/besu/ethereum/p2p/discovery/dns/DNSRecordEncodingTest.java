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
package org.hyperledger.besu.ethereum.p2p.discovery.dns;

import static org.assertj.core.api.Assertions.assertThat;

import java.security.Security;
import java.util.Optional;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * RFC 1035 caps a TXT &lt;character-string&gt; at 255 bytes, so anything longer arrives as several
 * of them in one RDATA. EIP-1459 budgets a record against the 512 byte UDP limit and EIP-778
 * permits a 300 byte ENR (about 404 characters once base64url encoded), so records above 255 bytes
 * are routine: go-ethereum's writer sizes branch entries up to 370 bytes deliberately.
 */
@ExtendWith(VertxExtension.class)
class DNSRecordEncodingTest {
  private static final String DOMAIN = "all.holesky.ethdisco.net";
  private static final String ENR_LINK =
      "enrtree://APFGGTFOBVE2ZNAB3CSMNNX6RRK3ODIRLP2AA5U4YFAA6MSYZUYTQ@" + DOMAIN;
  private final MockDnsServerVerticle mockDnsServerVerticle = new MockDnsServerVerticle();

  @BeforeAll
  static void setup() {
    Security.addProvider(new BouncyCastleProvider());
  }

  @BeforeEach
  void prepare(final Vertx vertx, final VertxTestContext vertxTestContext) {
    vertx.deployVerticle(mockDnsServerVerticle, vertxTestContext.succeedingThenComplete());
  }

  private DNSResolver resolver(final Vertx vertx) {
    return new DNSResolver(
        vertx, ENR_LINK, 0, Optional.of("127.0.0.1:" + mockDnsServerVerticle.port()));
  }

  @Test
  @DisplayName("A record spanning several character-strings is rejoined, not truncated")
  void multipleCharacterStringsAreRejoined(final Vertx vertx) {
    final String content = "enrtree-branch:" + "A".repeat(700);
    final String name = "multistring." + DOMAIN;
    mockDnsServerVerticle.addTxtRecord(name, content);

    final Optional<String> raw = resolver(vertx).resolveRawRecord(name);

    assertThat(raw).isPresent();
    assertThat(raw.get()).hasSize(content.length()).isEqualTo(content);
  }

  @Test
  @DisplayName("A single character-string record is unchanged")
  void singleCharacterStringIsUnchanged(final Vertx vertx) {
    final String content = "enrtree-branch:SHORT";
    final String name = "singlestring." + DOMAIN;
    mockDnsServerVerticle.addTxtRecord(name, content);

    assertThat(resolver(vertx).resolveRawRecord(name)).contains(content);
  }

  @Test
  @DisplayName("An ENR longer than one character-string survives the round trip")
  void longEnrRecordIsRejoined(final Vertx vertx) {
    final String content = "enr:" + "B".repeat(400);
    final String name = "longenr." + DOMAIN;
    mockDnsServerVerticle.addTxtRecord(name, content);

    assertThat(resolver(vertx).resolveRawRecord(name)).contains(content);
  }

  /**
   * A tree apex is an ordinary name that may also publish SPF or an ownership token. Concatenating
   * those into the root corrupts the trailing signature and the root then fails its signature
   * check, so the comparison here is against the whole entry rather than a single field.
   */
  @Test
  @DisplayName("The tree root is found intact alongside unrelated TXT records at the same name")
  void rootIsSelectedAmongOtherTxtRecordsAtTheApex(final Vertx vertx) {
    final String rootRecord =
        "enrtree-root:v1 e=SNIGOIP7I67HGIGFKVFYHSSDYM l=FDXN3SN67NA5DKA4J2GOK7BVQI seq=1 "
            + "sig=QSou7Q43nN2CKaxwAw868cUbKK-gl2FVCkpF86KFz4ghDqIo5hfqChZ5gEkaHfsBsfzEYMBjuYIxBNn0_cZQdAA";
    final String apex = "coexisting." + DOMAIN;
    mockDnsServerVerticle.addTxtRecord(apex, "v=spf1 -all");
    mockDnsServerVerticle.appendTxtRecord(apex, rootRecord);

    final Optional<DNSEntry> entry = resolver(vertx).resolveRecord(apex);

    assertThat(entry).isPresent();
    assertThat(entry.get()).isInstanceOf(DNSEntry.ENRTreeRoot.class);
    assertThat(entry.get().toString()).isEqualTo(DNSResolver.readDNSEntry(rootRecord).toString());
    assertThat(((DNSEntry.ENRTreeRoot) entry.get()).enrRoot())
        .isEqualTo("SNIGOIP7I67HGIGFKVFYHSSDYM");
  }
}
