/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.common.function;

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.lang.Boolean.TRUE;
import static java.util.stream.Stream.of;
import static java.util.stream.StreamSupport.stream;

/**
 * The utilities class for Java {@link Predicate}
 *
 * @since 2.7.5
 */
public interface Predicates {

    /**
     * {@link Predicate} always return <code>true</code>
     *
     * @param <T> the type to test
     * @return <code>true</code>
     */
    static <T> Predicate<T> alwaysTrue() {
        return e -> true;
    }

    /**
     * {@link Predicate} always return <code>false</code>
     *
     * @param <T> the type to test
     * @return <code>false</code>
     */
    static <T> Predicate<T> alwaysFalse() {
        return e -> false;
    }

    static <T> List<T> filterAll(Iterable<T> values, Predicate<T>... predicates) {
        return stream(values.spliterator(), false)
                .filter(and(predicates))
                .collect(Collectors.toList());
    }

    static <T> List<T> filterAny(Iterable<T> values, Predicate<T>... predicates) {
        return stream(values.spliterator(), false)
                .filter(or(predicates))
                .collect(Collectors.toList());
    }

    /**
     * Convert a {@link Function} to {@link Predicate}
     *
     * @param function a {@link Function}
     * @param <T>      the type to test
     * @return non-null
     */
    static <T> Predicate<T> predicate(Function<T, Boolean> function) {
        return t -> TRUE.equals(function.apply(t));
    }

    /**
     * a composed predicate that represents a short-circuiting logical AND of {@link Predicate predicates}
     *
     * @param predicates {@link Predicate predicates}
     * @param <T>        the type to test
     * @return non-null
     */
    static <T> Predicate<T> and(Predicate<T>... predicates) {
        return of(predicates).reduce((a, b) -> a.and(b)).orElseGet(Predicates::alwaysTrue);
    }

    /**
     * a composed predicate that represents a short-circuiting logical OR of {@link Predicate predicates}
     *
     * @param predicates {@link Predicate predicates}
     * @param <T>        the detected type
     * @return non-null
     */
    static <T> Predicate<T> or(Predicate<T>... predicates) {
        return of(predicates).reduce((a, b) -> a.or(b)).orElse(e -> true);
    }

}
