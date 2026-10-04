/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.data.binary;

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.IntFunction;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Threads that convert one shared {@link LazyBinaryFormat} at once each see the conversion whole,
 * and all of them end up with the same one.
 *
 * <p>The same-instance check fails on any platform whenever two conversions overlap and the second
 * replaces the first. The byte check catches a thread seeing a form before its contents, which
 * needs stores to be reordered, by aarch64 hardware or by the JIT. On x86 in practice only the
 * same-instance check can fire, so the release ordering itself isn't observable there.
 */
class LazyBinaryFormatConcurrencyTest {

    private static final int THREADS = 4;
    private static final int MAX_ROUNDS = 10_000;
    private static final Duration TIME_BUDGET = Duration.ofSeconds(2);
    private static final Duration ROUND_TIMEOUT = Duration.ofSeconds(10);

    /** Yield this often while spinning, so an oversubscribed machine keeps every thread moving. */
    private static final int SPINS_PER_YIELD = 1024;

    /** A short value, and a long one with multi-byte characters. */
    private static final String[] VALUES = {"ms", "µs-latency-histogram-bucket-boundary"};

    private static final StringSerializer SERIALIZER = StringSerializer.INSTANCE;

    @Test
    void racingStringMaterializationsInstallOneWholeSection() throws Exception {
        race(
                round -> BinaryStringData.fromString(VALUES[round % VALUES.length]),
                string -> {
                    // The getters the binary writer uses, each materializing on first touch.
                    byte[] actual =
                            BinarySegmentUtils.copyToBytes(
                                    string.getSegments(),
                                    string.getOffset(),
                                    string.getSizeInBytes());
                    assertThat(actual)
                            .isEqualTo(string.toString().getBytes(StandardCharsets.UTF_8));
                    return string.getBinarySection();
                });
    }

    @Test
    void racingRawValueMaterializationsInstallOneWholeSection() throws Exception {
        byte[] expected = InstantiationUtil.serializeToByteArray(SERIALIZER, VALUES[1]);
        race(
                round -> BinaryRawValueData.fromObject(VALUES[1]),
                raw -> {
                    assertThat(raw.toBytes(SERIALIZER)).isEqualTo(expected);
                    return raw.getBinarySection();
                });
    }

    @Test
    void racingRawValueDeserializationsInstallOneObject() throws Exception {
        byte[] bytes = InstantiationUtil.serializeToByteArray(SERIALIZER, VALUES[1]);
        race(
                round -> BinaryRawValueData.<String>fromBytes(bytes),
                raw -> {
                    String object = raw.toObject(SERIALIZER);
                    assertThat(object).isEqualTo(VALUES[1]);
                    return object;
                });
    }

    /**
     * Each round hands a fresh, unconverted instance to {@link #THREADS} workers at once. Every
     * worker reads it, and they must all report the same installed instance. Runs {@link
     * #MAX_ROUNDS} rounds or for {@link #TIME_BUDGET}, whichever ends first.
     */
    private static <T> void race(IntFunction<T> fresh, Function<T, Object> read)
            throws InterruptedException {
        AtomicReference<T> current = new AtomicReference<>();
        AtomicInteger round = new AtomicInteger();
        AtomicInteger finished = new AtomicInteger();
        Object[] installed = new Object[THREADS];
        Queue<String> failures = new ConcurrentLinkedQueue<>();

        List<Thread> workers = new ArrayList<>();
        for (int t = 0; t < THREADS; t++) {
            int index = t;
            Thread worker =
                    new Thread(
                            () -> {
                                int seen = 0;
                                while (true) {
                                    int r;
                                    for (int spins = 1; (r = round.get()) == seen; spins++) {
                                        spin(spins);
                                    }
                                    if (r < 0) {
                                        return;
                                    }
                                    seen = r;
                                    try {
                                        installed[index] = read.apply(current.get());
                                    } catch (Throwable e) {
                                        failures.add("round " + r + ": " + e);
                                    } finally {
                                        finished.incrementAndGet();
                                    }
                                }
                            });
            worker.setDaemon(true);
            worker.start();
            workers.add(worker);
        }

        long end = System.nanoTime() + TIME_BUDGET.toNanos();
        try {
            for (int r = 1; r <= MAX_ROUNDS && System.nanoTime() < end && failures.isEmpty(); r++) {
                current.set(fresh.apply(r));
                finished.set(0);
                round.set(r);
                long deadline = System.nanoTime() + ROUND_TIMEOUT.toNanos();
                for (int spins = 1; finished.get() < THREADS; spins++) {
                    if (System.nanoTime() > deadline) {
                        failures.add("round " + r + ": a worker did not finish");
                        break;
                    }
                    spin(spins);
                }
                for (int t = 1; t < THREADS && failures.isEmpty(); t++) {
                    if (installed[t] != installed[0]) {
                        failures.add("round " + r + ": workers saw different instances");
                    }
                }
            }
        } finally {
            round.set(-1);
            for (Thread worker : workers) {
                worker.join(ROUND_TIMEOUT.toMillis());
            }
        }
        assertThat(failures).isEmpty();
    }

    private static void spin(int spins) {
        if (spins % SPINS_PER_YIELD == 0) {
            Thread.yield();
        } else {
            Thread.onSpinWait();
        }
    }
}
