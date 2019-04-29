/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cognitree.kronos.scheduler.store.mongo.codecs;

import com.cognitree.kronos.scheduler.model.Job;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

public class JobStatusCodec implements Codec<Job.Status> {

    @Override
    public void encode(BsonWriter writer, Job.Status value, EncoderContext encoderContext) {
        writer.writeString(value.name());
    }

    @Override
    public Job.Status decode(BsonReader reader, DecoderContext decoderContext) {
        return Job.Status.valueOf(reader.readString());
    }

    @Override
    public Class<Job.Status> getEncoderClass() {
        return Job.Status.class;
    }
}
