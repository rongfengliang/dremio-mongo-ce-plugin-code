package com.dremio.plugins.mongo.planning;

import com.dremio.plugins.mongo.connection.MongoConnection;
import com.dremio.plugins.mongo.metadata.MongoChunk;
import com.dremio.plugins.mongo.metadata.MongoCollection;
import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;

public class MongoSubpartitioner {
   private final MongoClient client;

   public MongoSubpartitioner(MongoConnection connection) {
      this.client = connection.getClient();
   }

   public List<MongoSubpartitioner.Range> getPartitions(MongoCollection collection, int recordsPerCollection) {
      MongoDatabase database = this.client.getDatabase(collection.getDatabase());
      com.mongodb.client.MongoCollection<Document> documents = database.getCollection(collection.getCollection());
      long totalDocs = documents.count();
      ArrayList ranges;
      if (totalDocs < (long)recordsPerCollection) {
         ranges = Lists.newArrayList();
         ranges.add(new MongoSubpartitioner.Range(new MinKey(), new MaxKey()));
         return ranges;
      } else {
         ranges = Lists.newArrayList();
         Object previous = null;
         int i = 1;

         while(true) {
            Object val = this.getValueCondition(documents, i * recordsPerCollection);
            MongoSubpartitioner.Range r = new MongoSubpartitioner.Range(previous == null ? new MinKey() : previous, val);
            previous = val;
            ranges.add(r);
            if (val instanceof MaxKey) {
               return ranges;
            }

            ++i;
         }
      }
   }

   private Object getValueCondition(com.mongodb.client.MongoCollection<Document> collection, int skip) {
      Document o = (Document)collection.find().projection(new Document("_id", 1)).sort(new Document("_id", 1)).skip(skip).first();
      return o == null ? new MaxKey() : o.get("_id");
   }

   public class Range {
      private final Object min;
      private final Object max;

      public Range(Object min, Object max) {
         this.min = min;
         this.max = max;
      }

      public String toString() {
         return "Range [min=" + this.min + ", max=" + this.max + "]";
      }

      public MongoChunk getAsChunk(MongoChunk baseChunk) {
         MongoChunk c = new MongoChunk(baseChunk.getChunkLocList());
         if (!(this.min instanceof MinKey)) {
            c.setMinFilters(new Document("_id", this.min));
         }

         if (!(this.max instanceof MaxKey)) {
            c.setMaxFilters(new Document("_id", this.max));
         }

         return c;
      }
   }
}
