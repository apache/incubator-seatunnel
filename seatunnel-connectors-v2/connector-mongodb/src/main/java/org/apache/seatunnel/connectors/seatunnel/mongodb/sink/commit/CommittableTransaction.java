/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.mongodb.sink.commit;

import org.bson.BsonDocument;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.TransactionBody;
import com.mongodb.client.model.WriteModel;

import java.io.Serializable;
import java.util.List;

public class CommittableTransaction implements TransactionBody<Integer>, Serializable {

    protected final MongoCollection<BsonDocument> collection;

    protected List<WriteModel<BsonDocument>> bufferedDocuments;

    public CommittableTransaction(
            MongoCollection<BsonDocument> collection, List<WriteModel<BsonDocument>> documents) {
        this.collection = collection;
        this.bufferedDocuments = documents;
    }

    @Override
    public Integer execute() {
        BulkWriteResult bulkWriteResult = collection.bulkWrite(bufferedDocuments);
        return bulkWriteResult.getInserts().size();
    }
}
