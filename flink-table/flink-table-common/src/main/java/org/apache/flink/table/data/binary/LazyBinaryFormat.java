/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.data.binary;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.util.WrappingRuntimeException;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * An abstract implementation fo {@link BinaryFormat} which is lazily serialized into binary or
 * lazily deserialized into Java object.
 *
 * <p>The reason why we introduce this data structure is in order to save (de)serialization in
 * nested function calls. Consider the following function call chain:
 *
 * <pre>UDF0(input) -> UDF1(result0) -> UDF2(result1) -> UDF3(result2)</pre>
 *
 * <p>Such nested calls, if the return values of UDFs are Java object format, it will result in
 * multiple conversions between Java object and binary format:
 *
 * <pre>
 * converterToBinary(UDF0(converterToJavaObject(input))) ->
 *   converterToBinary(UDF1(converterToJavaObject(result0))) ->
 *     converterToBinary(UDF2(converterToJavaObject(result1))) ->
 *       ...
 * </pre>
 *
 * <p>So we introduced {@link LazyBinaryFormat} to avoid the redundant cost, it has three forms:
 *
 * <ul>
 *   <li>Binary form
 *   <li>Java object form
 *   <li>Binary and Java object both exist
 * </ul>
 *
 * <p>It can lazy the conversions as much as possible. It will be converted into required form only
 * when it is needed.
 *
 * <p>An instance may be shared between threads, for example as a {@code static final} constant.
 * Threads that materialize it at the same time each build a binary section, and the first one
 * installed wins: the others discard theirs, so every thread reads the same section. {@link
 * BinaryRawValueData} installs its deserialized object the same way. The section is installed with
 * a release compare-and-exchange once it is fully built, and readers keep plain loads. Why a reader
 * that sees the installed section also sees its contents, and keeps seeing it, on HotSpot:
 *
 * <ul>
 *   <li>aarch64 (e.g. Graviton): the JIT compiles the install to a store-release ({@code casl} with
 *       LSE, or an {@code ldxr}/{@code stlxr} loop without it), which makes every earlier store
 *       that built the section visible to other cores before the reference. A reader reaches the
 *       contents only through the reference it loaded, and aarch64 orders a load whose address
 *       comes from an earlier load after that load, so the reader needs no barrier. Reads of one
 *       location are coherent, so a thread that has seen the section never reads null again.
 *   <li>x86-64: the install is a {@code lock cmpxchg}, stores become visible in program order, and
 *       loads are not reordered with other loads. The release mode is what stops the JIT from
 *       moving the section's stores after the install.
 * </ul>
 *
 * <p>The interpreter and C1 use a full volatile compare-and-exchange, which is stronger. This is
 * how HotSpot implements final fields on both architectures, not a Java memory model guarantee.
 * Mutators ({@link #setJavaObject}, {@code getBinarySection().pointTo(...)}) are not covered and
 * need the instance confined to one thread.
 */
@Internal
public abstract class LazyBinaryFormat<T> implements BinaryFormat {

    private static final VarHandle JAVA_OBJECT;
    private static final VarHandle BINARY_SECTION;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            JAVA_OBJECT = lookup.findVarHandle(LazyBinaryFormat.class, "javaObject", Object.class);
            BINARY_SECTION =
                    lookup.findVarHandle(
                            LazyBinaryFormat.class, "binarySection", BinarySection.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    T javaObject;
    BinarySection binarySection;

    public LazyBinaryFormat() {
        this(null, null);
    }

    public LazyBinaryFormat(MemorySegment[] segments, int offset, int sizeInBytes, T javaObject) {
        this(javaObject, new BinarySection(segments, offset, sizeInBytes));
    }

    public LazyBinaryFormat(MemorySegment[] segments, int offset, int sizeInBytes) {
        this(null, new BinarySection(segments, offset, sizeInBytes));
    }

    public LazyBinaryFormat(T javaObject) {
        this(javaObject, null);
    }

    public LazyBinaryFormat(T javaObject, BinarySection binarySection) {
        this.javaObject = javaObject;
        this.binarySection = binarySection;
    }

    public T getJavaObject() {
        return javaObject;
    }

    public BinarySection getBinarySection() {
        return binarySection;
    }

    /** Must be public as it is used during code generation. */
    public void setJavaObject(T javaObject) {
        this.javaObject = javaObject;
    }

    @Override
    public MemorySegment[] getSegments() {
        return materializedSection().segments;
    }

    @Override
    public int getOffset() {
        return materializedSection().offset;
    }

    @Override
    public int getSizeInBytes() {
        return materializedSection().sizeInBytes;
    }

    /** Reads {@link #binarySection} once. */
    private BinarySection materializedSection() {
        BinarySection section = binarySection;
        if (section == null) {
            throw new IllegalStateException("Lazy Binary Format was not materialized");
        }
        return section;
    }

    /** Ensure we have materialized binary format. */
    public final void ensureMaterialized(TypeSerializer<T> serializer) {
        if (binarySection == null) {
            try {
                installBinarySection(materialize(serializer));
            } catch (IOException e) {
                throw new WrappingRuntimeException(e);
            }
        }
    }

    /**
     * Installs {@code section} unless another thread installed one first. Release, not plain or
     * acquire, is what orders the section's contents before the reference, because readers use
     * plain loads. Keeping the result keeps the {@link VarHandle} call on its guard's direct path;
     * a discarded result would go through {@code asType} on every call.
     */
    private void installBinarySection(BinarySection section) {
        BinarySection unused =
                (BinarySection) BINARY_SECTION.compareAndExchangeRelease(this, null, section);
    }

    /**
     * Installs a lazily built Java object unless another thread installed one first, and returns
     * whichever is installed. Subclasses use it where they cache a converted object that other
     * threads may read. Release matters here for the same reason as for the binary section.
     */
    @SuppressWarnings("unchecked")
    protected final T installJavaObject(T javaObject) {
        T installed = (T) JAVA_OBJECT.compareAndExchangeRelease(this, null, javaObject);
        return installed == null ? javaObject : installed;
    }

    /**
     * Materialize java object to binary format. Inherited classes need to hold the information they
     * need. (For example, {@link RawValueData} needs javaObjectSerializer).
     */
    protected abstract BinarySection materialize(TypeSerializer<T> serializer) throws IOException;
}
