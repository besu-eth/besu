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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

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

  /**
   * Most DNS records one refresh resolves. Lists are published by third parties, so this bounds the
   * work a list can ask for; it is far above the size of the public lists.
   */
  static final int MAX_RECORDS_PER_REFRESH = 50_000;

  /**
   * Longest a refresh reads records when the resolver is not told how often it refreshes. A refresh
   * is otherwise bounded by the records it reads, and each lookup may wait for its timeout, so a
   * linked tree whose name server does not answer could make one refresh last for days, during
   * which no other refresh starts and new roots of the configured tree are not read.
   */
  static final Duration DEFAULT_REFRESH_TIME_LIMIT = Duration.ofMinutes(5);

  /** Deepest chain of linked trees followed, counting the configured tree as depth 0. */
  static final int MAX_LINK_DEPTH = 8;

  /** Deepest chain of branches followed within one tree. */
  static final int MAX_BRANCH_DEPTH = 32;

  private final String enrLink;
  private final long initialSeq;
  private final int maxRecordsPerRefresh;
  private final Duration refreshTimeLimit;
  private final LongSupplier nanoClock;

  // The root last accepted for each tree, keyed by the tree's identity (see treeKey), in the order
  // the trees were read. Linked trees are published under different keys, so each must only be
  // compared with its own previous root.
  private volatile Map<String, TreeState> treeStates = Map.of();
  private final AtomicBoolean refreshing = new AtomicBoolean();
  private final DnsClient dnsClient;
  private volatile int largestQueueOfLastRefresh;

  /**
   * The root last accepted for a tree.
   *
   * @param seq its sequence number
   * @param enrsRead false if not all of its ENRs could be read, so that they are read again
   */
  private record TreeState(long seq, boolean enrsRead) {}

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
    this(vertx, enrLink, seq, dnsServer, MAX_RECORDS_PER_REFRESH);
  }

  DNSResolver(
      final Vertx vertx,
      final String enrLink,
      final long seq,
      final Optional<String> dnsServer,
      final int maxRecordsPerRefresh) {
    this(
        vertx,
        enrLink,
        seq,
        dnsServer,
        maxRecordsPerRefresh,
        DEFAULT_REFRESH_TIME_LIMIT,
        System::nanoTime);
  }

  /**
   * Creates a new DNSResolver with explicit limits.
   *
   * @param vertx Vertx instance which is used to create DNS Client
   * @param enrLink the ENR link to start with, of the form enrtree://PUBKEY@domain
   * @param seq the sequence number of the root record
   * @param dnsServer the DNS server to use for DNS query, or empty for the default one
   * @param maxRecordsPerRefresh the most DNS records one refresh reads
   * @param refreshTimeLimit how long one refresh may start reading records for, which should be
   *     shorter than the time between refreshes
   * @param nanoClock the source of {@link System#nanoTime()}
   */
  DNSResolver(
      final Vertx vertx,
      final String enrLink,
      final long seq,
      final Optional<String> dnsServer,
      final int maxRecordsPerRefresh,
      final Duration refreshTimeLimit,
      final LongSupplier nanoClock) {
    this.enrLink = enrLink;
    this.initialSeq = seq;
    this.maxRecordsPerRefresh = maxRecordsPerRefresh;
    this.refreshTimeLimit = refreshTimeLimit;
    this.nanoClock = nanoClock;
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
   * <p>The ENRs of a tree are only returned when its root has a higher sequence number than the
   * last time it was read, or when not all of them could be read then; the links of every tree are
   * followed on each call, so that a linked tree is read when it changes even if the tree linking
   * to it did not.
   *
   * @return all ENRs collected
   */
  public List<EthereumNodeRecord> collectAll() {
    // a refresh started while another is still waiting on DNS would read the same trees and could
    // publish older sequence numbers over the newer ones the other refresh accepts
    if (!refreshing.compareAndSet(false, true)) {
      LOG.debug("A refresh of {} is still running, not starting another", enrLink);
      return List.of();
    }
    try {
      final List<EthereumNodeRecord> nodes = new ArrayList<>();
      final ENRTreeLink rootTree = new ENRTreeLink(enrLink);
      final Refresh refresh = new Refresh(treeKey(rootTree), treeStates, nodes::add);
      refresh.run(rootTree);
      treeStates = refresh.acceptedStates();
      largestQueueOfLastRefresh = refresh.largestQueue;
      if (!nodes.isEmpty()) {
        LOG.debug("Resolved {} nodes from DNS for enr link {}", nodes.size(), enrLink);
      } else {
        LOG.debug("No nodes resolved from DNS");
      }
      return Collections.unmodifiableList(nodes);
    } finally {
      refreshing.set(false);
    }
  }

  /**
   * Sequence number of the root record.
   *
   * @return the current sequence number of the root record
   */
  public long sequence() {
    final TreeState state = treeStates.get(treeKey(new ENRTreeLink(enrLink)));
    return state != null ? state.seq() : initialSeq;
  }

  /**
   * The most entries queued at once in the last refresh.
   *
   * @return the largest size of the queue of records still to read in the last refresh
   */
  @VisibleForTesting
  int largestQueueOfLastRefresh() {
    return largestQueueOfLastRefresh;
  }

  /**
   * The identity of a tree: its public key and its domain. A link names them as text, but DNS names
   * are case-insensitive and may end with a dot, and a key can be encoded in more than one way, so
   * two links that spell the same tree differently must still be the same tree. Otherwise a link
   * could make a tree already read look new, and an older root of it be accepted.
   */
  private static String treeKey(final ENRTreeLink link) {
    String domain = link.domainName() == null ? "" : link.domainName().toLowerCase(Locale.ROOT);
    if (domain.endsWith(".")) {
      domain = domain.substring(0, domain.length() - 1);
    }
    return link.publicKey().bytes().toHexString() + "@" + domain;
  }

  /**
   * The state of one read of the trees. It remembers what was already queued, so that a tree or a
   * subtree reached several times is read once, and it bounds how much is read.
   *
   * <p>Records are read in order of how many links, then how many branches, separate them from the
   * configured tree. A record is therefore first reached by its shortest path, so the depth limits
   * and the memory of what was queued agree whichever paths lead to it, and the configured tree is
   * read before the trees it links to.
   *
   * <p>Each queued entry costs one record of the budget when read, so no more entries are queued
   * than the budget can still read: a branch may name hundreds of children, and queuing all of them
   * would let a list hold memory far beyond what one refresh reads.
   */
  private final class Refresh {
    private final String rootTreeKey;
    private final Map<String, TreeState> previousStates;
    private final Map<String, TreeState> acceptedStates = new LinkedHashMap<>();
    private final Set<String> queuedTrees = new HashSet<>();
    // the trees accepted in this refresh whose ENRs could not all be read
    private final Set<String> treesWithUnreadEnrs = new HashSet<>();
    private final Set<String> queuedNames = new HashSet<>();
    // ordered by distance from the configured tree, the order being unique to each entry
    private final NavigableSet<Pending> pending =
        new TreeSet<>(
            Comparator.comparingInt(Pending::linkDepth)
                .thenComparingInt(Pending::branchDepth)
                .thenComparingLong(Pending::order));
    private final DNSVisitor visitor;
    private int remainingRecords = maxRecordsPerRefresh;
    private final long startNanos = nanoClock.getAsLong();
    private int largestQueue;
    private long nextOrder;
    // false once something reachable could not be read: a lookup failed, a record was invalid, a
    // root was ignored, or the record budget or the time ran out
    private boolean complete = true;

    /**
     * A tree root or a record still to read.
     *
     * @param linkDepth links followed from the configured tree
     * @param branchDepth branches followed from the tree's root, -1 for the root itself
     * @param order insertion order, so that records at the same depth are read as listed
     * @param tree the tree whose root to read, or null for a record
     * @param entryName the label of the record, or null for a tree root
     * @param domainName the domain of the tree the record belongs to
     * @param treeKey the identity of the tree the record belongs to, or null for a tree root
     * @param enrSubtree true if the record is below the tree's ENR root, not its link root
     */
    private record Pending(
        int linkDepth,
        int branchDepth,
        long order,
        ENRTreeLink tree,
        String entryName,
        String domainName,
        String treeKey,
        boolean enrSubtree) {}

    private Refresh(
        final String rootTreeKey,
        final Map<String, TreeState> previousStates,
        final DNSVisitor visitor) {
      this.rootTreeKey = rootTreeKey;
      this.previousStates = previousStates;
      this.visitor = visitor;
    }

    private void run(final ENRTreeLink configuredTree) {
      addTree(configuredTree, 0);
      while (!pending.isEmpty()) {
        final Pending next = pending.pollFirst();
        final boolean keepGoing = next.tree() != null ? readTree(next) : readRecord(next);
        if (!keepGoing) {
          // the ENRs refused or still queued are not read in this refresh
          notRead(next);
          pending.forEach(this::notRead);
          pending.clear();
        }
      }
    }

    private void notRead(final Pending entry) {
      complete = false;
      if (entry.enrSubtree()) {
        treesWithUnreadEnrs.add(entry.treeKey());
      }
    }

    private void enqueue(final Pending entry) {
      pending.add(entry);
      if (pending.size() > remainingRecords) {
        // the entries after the budget cannot be read in this refresh
        notRead(pending.pollLast());
      }
      largestQueue = Math.max(largestQueue, pending.size());
    }

    /**
     * The roots to compare the next roots with.
     *
     * <p>A tree whose ENRs could not all be read keeps the sequence number of its root, so that an
     * older root is still not accepted, but its ENRs are read again from a root with the same one.
     *
     * <p>When everything reachable was read, the trees that were not reached are no longer linked
     * and are forgotten, which bounds the state. Otherwise a tree that was not reached, or whose
     * root could not be accepted, may still be linked: forgetting its number would let an older
     * root of it be taken for a new one the next time it is reached, so the previous numbers are
     * kept, the most recently read trees first and up to as many trees as one refresh can read.
     */
    private Map<String, TreeState> acceptedStates() {
      final Map<String, TreeState> states = new LinkedHashMap<>(acceptedStates);
      for (final String treeKey : treesWithUnreadEnrs) {
        states.computeIfPresent(treeKey, (key, state) -> new TreeState(state.seq(), false));
      }
      if (!complete) {
        for (final Map.Entry<String, TreeState> previous : previousStates.entrySet()) {
          if (states.size() >= maxRecordsPerRefresh) {
            break;
          }
          states.putIfAbsent(previous.getKey(), previous.getValue());
        }
      }
      return Collections.unmodifiableMap(states);
    }

    private void addTree(final ENRTreeLink link, final int linkDepth) {
      if (linkDepth > MAX_LINK_DEPTH) {
        LOG.debug("Not following link to {}, more than {} links deep", link, MAX_LINK_DEPTH);
        return;
      }
      if (!queuedTrees.add(treeKey(link))) {
        // reached again through a cycle of links, or linked from more than one tree
        return;
      }
      enqueue(new Pending(linkDepth, -1, nextOrder++, link, null, link.domainName(), null, false));
    }

    private void addRecord(
        final String entryName,
        final String domainName,
        final int branchDepth,
        final int linkDepth,
        final String treeKey,
        final boolean enrSubtree) {
      if (branchDepth > MAX_BRANCH_DEPTH) {
        LOG.debug(
            "Not reading {}.{}, more than {} branches deep",
            entryName,
            domainName,
            MAX_BRANCH_DEPTH);
        return;
      }
      if (!queuedNames.add(entryName + "." + domainName)) {
        // a subtree referenced by more than one branch: it is read once
        return;
      }
      enqueue(
          new Pending(
              linkDepth,
              branchDepth,
              nextOrder++,
              null,
              entryName,
              domainName,
              treeKey,
              enrSubtree));
    }

    /**
     * Reads the root of a tree.
     *
     * @return false if no more records may be read in this refresh
     */
    private boolean readTree(final Pending next) {
      final ENRTreeLink link = next.tree();
      final String treeKey = treeKey(link);
      if (!takeRecord()) {
        return false;
      }
      final Optional<DNSEntry> optionalEntry = resolveRecord(link.domainName());
      if (optionalEntry.isEmpty()) {
        LOG.trace("No DNS record found for {}", link.domainName());
        complete = false;
        return true;
      }

      final DNSEntry dnsEntry = optionalEntry.get();
      if (!(dnsEntry instanceof ENRTreeRoot treeRoot)) {
        LOG.debug("Root entry {} is not an ENR tree root", dnsEntry);
        complete = false;
        return true;
      }

      if (!checkSignature(treeRoot, link.publicKey(), treeRoot.sig())) {
        LOG.debug("ENR tree root {} failed signature check", link.domainName());
        complete = false;
        return true;
      }

      final TreeState previous =
          treeKey.equals(rootTreeKey)
              ? previousStates.getOrDefault(treeKey, new TreeState(initialSeq, true))
              : previousStates.get(treeKey);
      if (previous == null || treeRoot.seq() > previous.seq()) {
        acceptedStates.put(treeKey, new TreeState(treeRoot.seq(), true));
        addRecord(treeRoot.enrRoot(), link.domainName(), 0, next.linkDepth(), treeKey, true);
      } else if (treeRoot.seq() < previous.seq()) {
        // an older root than the one already accepted, such as a replayed one: its links may
        // include trees a newer root has removed, so nothing below it is read
        LOG.debug(
            "ENR tree root seq {} of {} is older than {}, ignoring it",
            treeRoot.seq(),
            link.domainName(),
            previous.seq());
        complete = false;
        return true;
      } else if (!previous.enrsRead()) {
        // the same root as last time, whose ENRs could not all be read then
        acceptedStates.put(treeKey, new TreeState(previous.seq(), true));
        addRecord(treeRoot.enrRoot(), link.domainName(), 0, next.linkDepth(), treeKey, true);
      } else {
        acceptedStates.put(treeKey, previous);
        LOG.debug(
            "ENR tree root seq {} of {} is unchanged, not reading its ENRs",
            treeRoot.seq(),
            link.domainName());
      }
      // the links of an unchanged tree are still followed, so that a linked tree that changed is
      // read even though the tree linking to it did not
      addRecord(treeRoot.linkRoot(), link.domainName(), 0, next.linkDepth(), treeKey, false);
      return true;
    }

    /**
     * Reads a record below a tree root.
     *
     * @return false if no more records may be read in this refresh
     */
    private boolean readRecord(final Pending next) {
      final String entryName = next.entryName();
      final String domainName = next.domainName();
      final String name = entryName + "." + domainName;
      if (!takeRecord()) {
        return false;
      }
      final Optional<String> rawRecord = resolveRawRecord(name);
      if (rawRecord.isEmpty()) {
        complete = false;
        if (next.enrSubtree()) {
          // the lookup may succeed next time. Invalid content below is the publisher's error and
          // would fail again, so it does not make the tree's ENRs be read again.
          treesWithUnreadEnrs.add(next.treeKey());
        }
        return true;
      }
      // The signature only covers the root. Every entry below it is bound to that signed root
      // solely by its subdomain being the hash of its content, so skipping this check would leave
      // the signature securing nothing that is actually consumed.
      if (!contentMatchesHash(entryName, rawRecord.get())) {
        LOG.warn("Content of {} does not match its hash, discarding subtree", name);
        complete = false;
        return true;
      }
      final Optional<DNSEntry> optionalDNSEntry =
          Optional.ofNullable(readDNSEntry(rawRecord.get()));
      if (optionalDNSEntry.isEmpty()) {
        complete = false;
        return true;
      }

      final DNSEntry entry = optionalDNSEntry.get();
      switch (entry) {
        case ENRNode node -> {
          if (!next.enrSubtree()) {
            // as go-ethereum does: an unchanged tree's link subtree is read on every refresh, so
            // ENRs published there would be passed on every time
            LOG.debug("{} is an ENR below the link root of its tree, ignoring it", name);
            return true;
          }
          return visitor.visit(node.nodeRecord());
        }
        case DNSEntry.ENRTree tree -> {
          for (String e : tree.entries()) {
            addRecord(
                e,
                domainName,
                next.branchDepth() + 1,
                next.linkDepth(),
                next.treeKey(),
                next.enrSubtree());
          }
        }
        case ENRTreeLink link -> {
          if (next.enrSubtree()) {
            // as go-ethereum does: an unchanged tree's ENR subtree is not read, so a link there
            // would only be followed when the tree changes
            LOG.debug("{} is a link below the ENR root of its tree, not following it", name);
          } else {
            addTree(link, next.linkDepth() + 1);
          }
        }
        default -> LOG.debug("Unsupported type of node {}", entry);
      }
      return true;
    }

    private boolean takeRecord() {
      if (remainingRecords == 0) {
        return false;
      }
      if (nanoClock.getAsLong() - startNanos >= refreshTimeLimit.toNanos()) {
        LOG.debug(
            "Reading DNS records for {} took {}, not reading more until the next refresh",
            enrLink,
            refreshTimeLimit);
        return false;
      }
      if (--remainingRecords == 0) {
        LOG.debug(
            "Resolved {} DNS records for {}, not reading more until the next refresh",
            maxRecordsPerRefresh,
            enrLink);
      }
      return true;
    }
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
