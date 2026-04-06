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
package org.hyperledger.besu.testutil;

import static com.google.common.base.Preconditions.checkState;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.jspecify.annotations.Nullable;

/**
 * Utility class for generating JUnit test parameters from json files. Each set of test parameters
 * will contain a String name followed by an object representing a deserialized json test case.
 *
 * @param <S> the type parameter
 * @param <T> the type parameter
 */
public class JsonTestParameters<S, T> {

  private static final String TEST_PATTERN_STR = System.getProperty("test.ethereum.include");

  /**
   * The Collector.
   *
   * @param <S> the type parameter
   */
  public static class Collector<S> {

    @Nullable private final Predicate<String> includes;
    private final Predicate<String> ignore;

    private Collector(@Nullable final Predicate<String> includes, final Predicate<String> ignore) {
      this.includes = includes;
      this.ignore = ignore;
    }

    // Reference tests are plentiful so we'll add quite a bit of element, so starting with a
    // relatively large capacity (without getting crazy) to avoid resizing. It's going to waste
    // memory when we run a single test, but it's not the case we're trying to optimize.
    private final List<Object[]> testParameters = new ArrayList<>(256);

    /**
     * Add standard reference test.
     *
     * @param name the name
     * @param fullPath the full path of the test
     * @param value the value
     * @param runTest the run test
     */
    public void add(
        final String name, final String fullPath, final S value, final boolean runTest) {
      testParameters.add(
          new Object[] {name, value, runTest && includes(name) && includes(fullPath)});
    }

    private boolean includes(final String name) {
      // If there is no specific includes, everything is included unless it is ignored, otherwise,
      // only what is in includes is included whether or not it is ignored.
      if (includes == null) {
        return !ignore.test(name);
      } else {
        return includes.test(name);
      }
    }

    private Collection<Object[]> getParameters() {
      return testParameters;
    }
  }

  /**
   * The interface Generator.
   *
   * @param <S> the type parameter
   * @param <T> the type parameter
   */
  @FunctionalInterface
  public interface Generator<S, T> {
    /**
     * Generate.
     *
     * @param name the name
     * @param fullPath the full path of the test
     * @param mappedType the mapped type
     * @param collector the collector
     */
    void generate(String name, String fullPath, S mappedType, Collector<T> collector);
  }

  private static final ObjectMapper objectMapper =
      new ObjectMapper(
              new JsonFactoryBuilder()
                  .streamReadConstraints(
                      StreamReadConstraints.builder().maxStringLength(Integer.MAX_VALUE).build())
                  .build())
          .registerModule(new Jdk8Module());

  // The type to which the json file is directly mapped
  private final Class<S> jsonFileMappedType;

  // The final type of the test case spec, which may or may not be same than jsonFileMappedType
  // Note that we don't really use this field as of now, but as this is the actual type of the final
  // spec used by tests, it feels "right" to have it passed explicitly at construction and having it
  // around could prove useful later.
  @SuppressWarnings({"FieldCanBeLocal", "unused"})
  private final Class<T> testCaseSpec;

  private final Set<String> fileExcludes = new HashSet<>();
  private Generator<S, T> generator;

  private final List<Predicate<String>> testIncludes = new ArrayList<>();
  private final List<Predicate<String>> testIgnores = new ArrayList<>();

  private JsonTestParameters(final Class<S> jsonFileMappedType, final Class<T> testCaseSpec) {
    this.jsonFileMappedType = jsonFileMappedType;
    this.testCaseSpec = testCaseSpec;

    if (TEST_PATTERN_STR != null) {
      includeTests(TEST_PATTERN_STR);
    }
  }

  /**
   * Create json test parameters.
   *
   * @param <T> the type parameter
   * @param testCaseSpec the test case spec
   * @return the json test parameters
   */
  public static <T> JsonTestParameters<T, T> create(final Class<T> testCaseSpec) {
    return new JsonTestParameters<>(testCaseSpec, testCaseSpec)
        .generator(
            (name, fullPath, testCase, collector) -> collector.add(name, fullPath, testCase, true));
  }

  /**
   * Create json test parameters.
   *
   * @param <S> the type parameter
   * @param <T> the type parameter
   * @param jsonFileMappedType the json file mapped type
   * @param testCaseSpec the test case spec
   * @return the json test parameters
   */
  public static <S, T> JsonTestParameters<S, T> create(
      final Class<S> jsonFileMappedType, final Class<T> testCaseSpec) {
    return new JsonTestParameters<>(jsonFileMappedType, testCaseSpec);
  }

  /**
   * Exclude files json test parameters.
   *
   * @param filenames the filenames
   * @return the json test parameters
   */
  @SuppressWarnings("unused")
  public JsonTestParameters<S, T> excludeFiles(final String... filenames) {
    fileExcludes.addAll(Arrays.asList(filenames));
    return this;
  }

  private void addPatterns(final String[] patterns, final List<Predicate<String>> listForAddition) {
    for (final String pattern : patterns) {
      final Pattern compiled = Pattern.compile(pattern);
      listForAddition.add(t -> compiled.matcher(t).find());
    }
  }

  @SuppressWarnings({"unused"})
  private void includeTests(final String... patterns) {
    addPatterns(patterns, testIncludes);
  }

  /**
   * Ignore json test parameters.
   *
   * @param patterns the patterns
   * @return the json test parameters
   */
  public JsonTestParameters<S, T> ignore(final String... patterns) {
    addPatterns(patterns, testIgnores);
    return this;
  }

  /** Ignore all. */
  public void ignoreAll() {
    testIgnores.add(t -> true);
  }

  /**
   * Generator json test parameters.
   *
   * @param generator the generator
   * @return the json test parameters
   */
  public JsonTestParameters<S, T> generator(final Generator<S, T> generator) {
    this.generator = generator;
    return this;
  }

  /**
   * Generate collection.
   *
   * @param paths the paths
   * @return the collection
   */
  public Collection<Object[]> generate(final String... paths) {
    return generate(getFilteredFiles(paths));
  }

  /**
   * Generate collection.
   *
   * @param filteredFiles the filtered files
   * @return the collection
   */
  public Collection<Object[]> generate(final Collection<File> filteredFiles) {
    checkState(generator != null, "Missing generator function");

    final Collector<T> collector =
        new Collector<>(
            testIncludes.isEmpty() ? null : t -> matchAny(t, testIncludes),
            t -> matchAny(t, testIgnores));

    for (final File file : filteredFiles) {
      final JsonTestCaseReader<S> testCase = parseFile(file);
      for (final Map.Entry<String, S> entry : testCase.testCaseSpecs.entrySet()) {
        final String testName = entry.getKey();
        final S mappedType = entry.getValue();
        generator.generate(testName, file.getPath(), mappedType, collector);
      }
    }
    return collector.getParameters();
  }

  private static <T> boolean matchAny(final T toTest, final List<Predicate<T>> tests) {
    for (final Predicate<T> predicate : tests) {
      if (predicate.test(toTest)) {
        return true;
      }
    }
    return false;
  }

  private Collection<File> getFilteredFiles(final String[] paths) {
    final ClassLoader classLoader = JsonTestParameters.class.getClassLoader();
    final List<File> files = new ArrayList<>();
    for (final String path : paths) {
      final URL url = classLoader.getResource(path);
      checkState(url != null, "Cannot find test directory %s", path);
      final Path dir;
      try {
        dir = Paths.get(url.toURI());
      } catch (final URISyntaxException e) {
        throw new RuntimeException("Problem converting URL to URI " + url, e);
      }
      try (final Stream<Path> s = Files.walk(dir)) {
        s.map(Path::toFile)
            .filter(f -> f.getPath().endsWith(".json"))
            .filter(f -> !fileExcludes.contains(f.getName()))
            .forEach(files::add);
      } catch (final IOException e) {
        throw new RuntimeException("Problem reading directory " + dir, e);
      }
    }
    return files;
  }

  private JsonTestCaseReader<S> parseFile(final File file) {
    final JavaType javaType =
        objectMapper
            .getTypeFactory()
            .constructParametricType(JsonTestCaseReader.class, jsonFileMappedType);

    try {
      return objectMapper.readValue(file, javaType);
    } catch (final IOException e) {
      throw new RuntimeException(
          "Error parsing test case file " + file + " to class " + jsonFileMappedType, e);
    }
  }

  /**
   * Parameterized wrapper for deserialization.
   *
   * @param <T> the type parameter
   */
  public static class JsonTestCaseReader<T> {

    /** The Test case specs. */
    public final Map<String, T> testCaseSpecs;

    /**
     * Public constructor.
     *
     * @param testCaseSpecs The test cases to run.
     */
    @JsonCreator
    JsonTestCaseReader(@JsonProperty final Map<String, T> testCaseSpecs) {
      this.testCaseSpecs = testCaseSpecs;
    }
  }
}
