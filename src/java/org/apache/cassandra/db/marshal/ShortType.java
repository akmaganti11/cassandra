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
package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.Objects;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.ShortSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

public class ShortType extends NumberType<Short>
{
    public static final ShortType instance = new ShortType();

    private static final ByteBuffer MASKED_VALUE = instance.decompose((short) 0);

    ShortType()
    {
        super(ComparisonType.CUSTOM);
    } // singleton

    public <VL, VR> int compareCustom(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR)
    {
        int diff = accessorL.getByte(left, 0) - accessorR.getByte(right, 0);
        if (diff != 0)
            return diff;
        return ValueAccessor.compare(left, accessorL, right, accessorR);
    }

    @Override
    public <V> ByteSource asComparableBytes(ValueAccessor<V> accessor, V data, ByteComparable.Version version)
    {
        // This type does not allow non-present values, but we do just to avoid future complexity.
        return ByteSource.optionalSignedFixedLengthNumber(accessor, data);
    }

    @Override
    public <V> V fromComparableBytes(ValueAccessor<V> accessor, ByteSource.Peekable comparableBytes, ByteComparable.Version version)
    {
        return ByteSourceInverse.getOptionalSignedFixedLength(accessor, comparableBytes, 2);
    }

    public ByteBuffer fromString(String source) throws MarshalException
    {
        // Return an empty ByteBuffer for an empty string.
        if (source.isEmpty())
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        short s;

        try
        {
            s = Short.parseShort(source);
        }
        catch (Exception e)
        {
            throw new MarshalException(String.format("Unable to make short from '%s'", source), e);
        }

        return decompose(s);
    }

    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        if (parsed instanceof String || parsed instanceof Number)
            return new Constants.Value(fromString(String.valueOf(parsed)));

        throw new MarshalException(String.format(
                "Expected a short value, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        return Objects.toString(getSerializer().deserialize(buffer), "\"\"");
    }

    @Override
    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.SMALLINT;
    }

    public TypeSerializer<Short> getSerializer()
    {
        return ShortSerializer.instance;
    }

    @Override
    public short toShort(ByteBuffer value)
    {
        return ByteBufferUtil.toShort(value);
    }

    @Override
    public int toInt(ByteBuffer value)
    {
        return toShort(value);
    }

    @Override
    public ByteBuffer add(NumberType<?> leftType, ByteBuffer left, NumberType<?> rightType, ByteBuffer right)
    {
        return ByteBufferUtil.bytes((short) (leftType.toShort(left) + rightType.toShort(right)));
    }

    public ByteBuffer substract(NumberType<?> leftType, ByteBuffer left, NumberType<?> rightType, ByteBuffer right)
    {
        return ByteBufferUtil.bytes((short) (leftType.toShort(left) - rightType.toShort(right)));
    }

    public ByteBuffer multiply(NumberType<?> leftType, ByteBuffer left, NumberType<?> rightType, ByteBuffer right)
    {
        return ByteBufferUtil.bytes((short) (leftType.toShort(left) * rightType.toShort(right)));
    }

    public ByteBuffer divide(NumberType<?> leftType, ByteBuffer left, NumberType<?> rightType, ByteBuffer right)
    {
        return ByteBufferUtil.bytes((short) (leftType.toShort(left) / rightType.toShort(right)));
    }

    public ByteBuffer mod(NumberType<?> leftType, ByteBuffer left, NumberType<?> rightType, ByteBuffer right)
    {
        return ByteBufferUtil.bytes((short) (leftType.toShort(left) % rightType.toShort(right)));
    }

    public ByteBuffer negate(ByteBuffer input)
    {
        return ByteBufferUtil.bytes((short) -toShort(input));
    }

    @Override
    public ByteBuffer abs(ByteBuffer input)
    {
        return ByteBufferUtil.bytes((short) Math.abs(toShort(input)));
    }

    @Override
    public ByteBuffer exp(ByteBuffer input)
    {
        return ByteBufferUtil.bytes((short) Math.exp(toShort(input)));
    }

    @Override
    public ByteBuffer log(ByteBuffer input)
    {
        return ByteBufferUtil.bytes((short) Math.log(toShort(input)));
    }

    @Override
    public ByteBuffer log10(ByteBuffer input)
    {
        return ByteBufferUtil.bytes((short) Math.log10(toShort(input)));
    }

    @Override
    public ByteBuffer round(ByteBuffer input)
    {
        return ByteBufferUtil.clone(input);
    }

    @Override
    public ByteBuffer getMaskedValue()
    {
        return MASKED_VALUE;
    }
}
