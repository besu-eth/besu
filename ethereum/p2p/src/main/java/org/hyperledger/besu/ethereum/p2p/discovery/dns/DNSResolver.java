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

import static org.hyperledger.besu.ethereum.p2p.discovery.dns.KVReader.readKV;

import org.hyperledger.besu.crypto.Hash;
import org.hyperledger.besu.ethereum.p2p.discovery.dns.DNSEntry.ENRNode;
import org.hyperledger.besu.ethereum.p2p.discovery.dns.DNSEntry.ENRTreeLink;
import org.hyperledger.besu.ethereum.p2p.discovery.dns.DNSEntry.ENRTreeRoot;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.dns.DnsClient;
import io.vertx.core.dns.DnsClientOptions;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.crypto.SECP256K1;
import org.apache.tuweni.io.Base32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Adapted from https://github.com/tmio/tuweni and licensed under Apache 2.0
/** Resolves a set of ENR nodes from a host name. */
public class DNSResolver {
  private static final Logger LOG = LoggerFactory.getLogger(DNSResolver.class);
  private static final String ENR_TREE_ROOT_PREFIX = "enrtree-root:";
  private static final int MIN_HASH_BYTES = 12;
  private final String enrLink;
  private long seq;
  private final DnsClient dnsClient;

  /**
   * Creates a new DNSResolver.
   *
   * @param vertx Vertx instance which is used to create DNS Client
   * @param enrLink the ENR link to start with, of the form enrtree://PUBKEY@domain
   * @param seq the sequence number of the root record. If the root record seq is higher, proceed
   *     with visit.
   * @param dnsServer the DNS server to use for DNS query. If empty, the default DNS server will be
   *     used.
   */
  public DNSResolver(
      final Vertx vertx, final String enrLink, final long seq, final Optional<String> dnsServer) {
    this.enrLink = enrLink;
    this.seq = seq;
    final DnsClientOptions dnsClientOptions =
        dnsServer.map(DNSResolver::buildDnsClientOptions).orElseGet(DnsClientOptions::new);
    dnsClient = vertx.createDnsClient(dnsClientOptions);
  }

  private static DnsClientOptions buildDnsClientOptions(final String server) {
    final List<String> hostPort = Splitter.on(":").splitToList(server);
    final DnsClientOptions dnsClientOptions = new DnsClientOptions();
    dnsClientOptions.setHost(hostPort.get(0));
    if (hostPort.size() > 1) {
      try {
        int port = Integer.parseInt(hostPort.get(1));
        dnsClientOptions.setPort(port);
      } catch (NumberFormatException e) {
        LOG.trace("Invalid port number {}, ignoring", hostPort.get(1));
      }
    }
    return dnsClientOptions;
  }

  /**
   * Convenience method to read all ENRs, from a top-level record.
   *
   * @return all ENRs collected
   */
  public List<EthereumNodeRecord> collectAll() {
    final List<EthereumNodeRecord> nodes = new ArrayList<>();
    final DNSVisitor visitor = nodes::add;
    visitTree(new ENRTreeLink(enrLink), visitor);
    if (!nodes.isEmpty()) {
      LOG.debug("Resolved {} nodes from DNS for enr link {}", nodes.size(), enrLink);
    } else {
      LOG.debug("No nodes resolved from DNS");
    }
    return Collections.unmodifiableList(nodes);
  }

  /**
   * Sequence number of the root record.
   *
   * @return the current sequence number of the root record
   */
  public long sequence() {
    return seq;
  }

  /**
   * Reads a complete tree of record, starting with the top-level record.
   *
   * @param link the ENR link to start with
   * @param visitor the visitor that will look at each record
   */
  private void visitTree(final ENRTreeLink link, final DNSVisitor visitor) {
    Optional<DNSEntry> optionalEntry = resolveRecord(link.domainName());
    if (optionalEntry.isEmpty()) {
      LOG.trace("No DNS record found for {}", link.domainName());
      return;
    }

    final DNSEntry dnsEntry = optionalEntry.get();
    if (!(dnsEntry instanceof ENRTreeRoot treeRoot)) {
      LOG.debug("Root entry {} is not an ENR tree root", dnsEntry);
      return;
    }

    if (!checkSignature(treeRoot, link.publicKey(), treeRoot.sig())) {
      LOG.debug("ENR tree root {} failed signature check", link.domainName());
      return;
    }
    if (treeRoot.seq() <= seq) {
      LOG.debug("ENR tree root seq {} is not higher than {}, aborting", treeRoot.seq(), seq);
      return;
    }
    seq = treeRoot.seq();

    internalVisit(treeRoot.enrRoot(), link.domainName(), visitor);
    internalVisit(treeRoot.linkRoot(), link.domainName(), visitor);
  }

  private boolean internalVisit(
      final String entryName, final String domainName, final DNSVisitor visitor) {
    final String name = entryName + "." + domainName;
    final Optional<String> rawRecord = resolveRawRecord(name);
    if (rawRecord.isEmpty()) {
      return true;
    }
    // The signature only covers the root. Every entry below it is bound to that signed root solely
    // by its subdomain being the hash of its content, so skipping this check would leave the
    // signature securing nothing that is actually consumed.
    if (!contentMatchesHash(entryName, rawRecord.get())) {
      LOG.warn("Content of {} does not match its hash, discarding subtree", name);
      return true;
    }
    final Optional<DNSEntry> optionalDNSEntry = Optional.ofNullable(readDNSEntry(rawRecord.get()));
    if (optionalDNSEntry.isEmpty()) {
      return true;
    }

    final DNSEntry entry = optionalDNSEntry.get();
    switch (entry) {
      case ENRNode node -> {
        return visitor.visit(node.nodeRecord());
      }
      case DNSEntry.ENRTree tree -> {
        for (String e : tree.entries()) {
          boolean keepGoing = internalVisit(e, domainName, visitor);
          if (!keepGoing) {
            return false;
          }
        }
      }
      case ENRTreeLink link -> visitTree(link, visitor);
      default -> LOG.debug("Unsupported type of node {}", entry);
    }
    return true;
  }

  /**
   * Maps the tree root TXT record of a domain to a DNSEntry.
   *
   * <p>A tree apex is an ordinary domain name that may also serve unrelated TXT records such as SPF
   * or an ownership token. A root entry is around 173 bytes so it always fits a single
   * &lt;character-string&gt;, which means the root can be picked out by its prefix instead of
   * concatenating whatever else is published at that name into it.
   *
   * @param domainName the domain name to query
   * @return the tree root entry published at the domain. Empty if no root record is found.
   */
  Optional<DNSEntry> resolveRecord(final String domainName) {
    return resolveTxtStrings(domainName)
        .flatMap(
            records ->
                records.stream()
                    .map(DNSResolver::trimQuotes)
                    .filter(record -> record.startsWith(ENR_TREE_ROOT_PREFIX))
                    .findFirst())
        .map(DNSResolver::readDNSEntry);
  }

  /**
   * Read a DNS entry from a String.
   *
   * @param serialized the serialized form of a DNS entry
   * @return DNS entry if found
   * @throws IllegalArgumentException if the record cannot be read
   */
  @VisibleForTesting
  static DNSEntry readDNSEntry(final String serialized) {
    final String record = trimQuotes(serialized);
    final String prefix = getPrefix(record);
    try {
      switch (prefix) {
        case "enrtree-root":
          return new ENRTreeRoot(readKV(record));
        case "enrtree-branch":
          return new DNSEntry.ENRTree(record.substring(prefix.length() + 1));
        case "enr":
          return ENRNode.fromAttrs(readKV(record));
        case "enrtree":
          return new ENRTreeLink(record);
      }
      LOG.error("{} should contain enrtree-branch, enr, enrtree-root or enrtree", serialized);
    } catch (Throwable t) {
      LOG.warn("Failed to parse record: {}", record);
    }
    return null;
  }

  private static String trimQuotes(final String str) {
    if (str.startsWith("\"") && str.endsWith("\"")) {
      return str.substring(1, str.length() - 1);
    }
    return str;
  }

  private static String getPrefix(final String input) {
    final String[] parts = input.split(":", 2);
    return parts.length > 0 ? parts[0] : "";
  }

  /**
   * Resolves the TXT record for a domain name and returns it.
   *
   * <p>RFC 1035 caps a single &lt;character-string&gt; at 255 bytes, so a record longer than that
   * is transported as several of them and must be rejoined without a separator. EIP-1459 budgets up
   * to the 512 byte UDP limit per record, and EIP-778 permits a 300 byte ENR (about 404 characters
   * once base64url encoded), so multi-string records are normal rather than exceptional. Reading
   * only the first string silently truncates them.
   *
   * @param domainName the name of the DNS domain to query
   * @return the TXT entry of the DNS record. Empty if no record is found.
   */
  Optional<String> resolveRawRecord(final String domainName) {
    return resolveTxtStrings(domainName).map(records -> String.join("", records));
  }

  private Optional<List<String>> resolveTxtStrings(final String domainName) {
    LOG.trace("Resolving TXT records on domain: {}", domainName);
    try {
      // Future.await parks current virtual thread and waits for the result. Any failure is
      // thrown as a Throwable.
      // NOTE: Vert.x 5's DnsClientImpl (rewritten to delegate to Netty's DnsNameResolver) filters
      // resolved records with a case-sensitive comparison of the answer's owner name against the
      // query name (record.name().equals(name)); Vert.x 4.5.x had no such check. A server whose
      // TXT response doesn't echo the query name byte-for-byte (e.g. differing case from
      // lowercased zone data) will have its records silently dropped here rather than raising an
      // error, which would surface as this method returning Optional.empty() for a subdomain that
      // genuinely has a record. See MockDnsServerVerticle for how this was diagnosed.
      final List<String> records = Future.await(dnsClient.resolveTXT(domainName));
      if (records == null || records.isEmpty()) {
        return Optional.empty();
      }
      return Optional.of(records);
    } catch (final Throwable e) {
      LOG.trace("Error while resolving TXT records on domain: {}", domainName, e);
      return Optional.empty();
    }
  }

  /**
   * Checks that a record's content hashes to the subdomain it was served from, as required by the
   * EIP-1459 client protocol. The label is the unpadded base32 of a keccak256 prefix, so the
   * decoded label is compared against the leading bytes of the digest.
   *
   * @param entryName the base32 subdomain label the record was found at
   * @param rawRecord the record content as served
   * @return true when the content hashes to the label
   */
  @VisibleForTesting
  static boolean contentMatchesHash(final String entryName, final String rawRecord) {
    try {
      final Bytes wantHash = Bytes.wrap(Base32.decodeBytes(entryName));
      // A label may be an abbreviated hash, but too short a prefix is cheap to collide, so the
      // accepted range matches go-ethereum's isValidHash: 12 bytes up to a full keccak256 digest.
      if (wantHash.size() < MIN_HASH_BYTES || wantHash.size() > Bytes32.SIZE) {
        return false;
      }
      final Bytes digest =
          Hash.keccak256(Bytes.wrap(trimQuotes(rawRecord).getBytes(StandardCharsets.UTF_8)));
      return digest.slice(0, wantHash.size()).equals(wantHash);
    } catch (final RuntimeException e) {
      LOG.trace("Could not verify hash of {}", entryName, e);
      return false;
    }
  }

  private boolean checkSignature(
      final ENRTreeRoot root, final SECP256K1.PublicKey pubKey, final SECP256K1.Signature sig) {
    final Bytes32 hash =
        Hash.keccak256(Bytes.wrap(root.signedContent().getBytes(StandardCharsets.UTF_8)));
    return SECP256K1.verifyHashed(hash, sig, pubKey);
  }
}
