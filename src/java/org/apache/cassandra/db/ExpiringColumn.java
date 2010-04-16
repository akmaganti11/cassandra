/**
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

package org.apache.cassandra.db;

import java.security.MessageDigest;
import java.io.IOException;

import org.apache.log4j.Logger;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.DataOutputBuffer;

/**
 * Alternative to Column that have an expiring time.
 * ExpiringColumn is immutable (as Column is).
 *
 * Note that ExpiringColumn does not override Column.getMarkedForDeleteAt,
 * which means that it's in the somewhat unintuitive position of being deleted (after its expiration)
 * without having a time-at-which-it-became-deleted.  (Because ttl is a server-side measurement,
 * we can't mix it with the timestamp field, which is client-supplied and whose resolution we
 * can't assume anything about.)
 */
public class ExpiringColumn extends Column
{
    private static Logger logger = Logger.getLogger(ExpiringColumn.class);

    private final int localExpirationTime;
    private final int timeToLive;

    public ExpiringColumn(byte[] name, byte[] value, long timestamp, int timeToLive)
    {
      this(name, value, timestamp, timeToLive, (int) (System.currentTimeMillis() / 1000) + timeToLive);
    }

    public ExpiringColumn(byte[] name, byte[] value, long timestamp, int timeToLive, int localExpirationTime)
    {
        super(name, value, timestamp);
        assert timeToLive != 0;
        assert localExpirationTime != 0;
        this.timeToLive = timeToLive;
        this.localExpirationTime = localExpirationTime;
    }

    public int getTimeToLive()
    {
        return timeToLive;
    }

    @Override
    public boolean isMarkedForDelete()
    {
        return (int) (System.currentTimeMillis() / 1000 ) > localExpirationTime;
    }

    @Override
    public int size()
    {
        /*
         * An expired column adds to a Column : 
         *    4 bytes for the localExpirationTime
         *  + 4 bytes for the timeToLive
        */
        return super.size() + DBConstants.intSize_ + DBConstants.intSize_;
    }

    @Override
    public void updateDigest(MessageDigest digest)
    {
        digest.update(name());
        digest.update(value());
        DataOutputBuffer buffer = new DataOutputBuffer();
        try
        {
            buffer.writeLong(timestamp());
            buffer.writeByte(ColumnSerializer.EXPIRATION_MASK);
            buffer.writeInt(timeToLive);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        digest.update(buffer.getData(), 0, buffer.getLength());
    }

    @Override
    public int getLocalDeletionTime()
    {
        return localExpirationTime;
    }

    @Override
    public String getString(AbstractType comparator)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(super.getString(comparator));
        sb.append("!");
        sb.append(timeToLive);
        return sb.toString();
    }
}
