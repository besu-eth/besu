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
package org.hyperledger.besu.ethereum.p2p.discovery.dns;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.security.Security;
import java.util.Optional;

import com.google.common.io.Resources;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
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
    vertx
        .deployVerticle(mockDnsServerVerticle)
        .onComplete(vertxTestContext.succeedingThenComplete());
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

  @Test
  @DisplayName("Every fixture entry is served at the subdomain matching its content hash")
  void fixtureEntriesAreBoundToTheirHashes() throws Exception {
    final JsonObject fixture =
        new JsonObject(
            Resources.toString(
                Resources.getResource("discovery/dns/dns-records.json"), StandardCharsets.UTF_8));

    int checked = 0;
    for (final String name : fixture.fieldNames()) {
      if (name.equals(DOMAIN)) {
        continue;
      }
      final String label = name.substring(0, name.indexOf('.'));
      final String content = fixture.getString(name);
      assertThat(DNSResolver.contentMatchesHash(label, content))
          .withFailMessage("%s is not the hash of its content %s", label, content)
          .isTrue();
      assertThat(DNSResolver.contentMatchesHash(label, content + "tampered"))
          .withFailMessage("%s accepted tampered content", label)
          .isFalse();
      checked++;
    }
    assertThat(checked).isGreaterThan(100);
  }

  @Test
  @DisplayName("A malformed subdomain label is rejected rather than throwing")
  void malformedLabelIsRejected() {
    assertThat(DNSResolver.contentMatchesHash("not-base32!", "enrtree-branch:")).isFalse();
    assertThat(DNSResolver.contentMatchesHash("", "enrtree-branch:")).isFalse();
  }

  /**
   * go-ethereum's isValidHash accepts an abbreviated label of 12 bytes up to a full digest, so the
   * check must not demand the canonical 16, but a shorter prefix than 12 bytes is cheap to collide
   * and must not be honoured.
   */
  @Test
  @DisplayName("An abbreviated label is honoured only down to 12 decoded bytes")
  void labelLengthBoundsMatchGoEthereum() {
    final String content = "enrtree-branch:";
    final String canonical = "FDXN3SN67NA5DKA4J2GOK7BVQI";

    assertThat(DNSResolver.contentMatchesHash(canonical, content)).isTrue();
    // 20 base32 characters decode to 12 bytes, the shortest go-ethereum permits.
    assertThat(DNSResolver.contentMatchesHash(canonical.substring(0, 20), content)).isTrue();
    // 16 characters decode to 10 bytes, below the floor.
    assertThat(DNSResolver.contentMatchesHash(canonical.substring(0, 16), content)).isFalse();
  }
}
