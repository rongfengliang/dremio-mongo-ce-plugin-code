package com.dremio.plugins.mongo.metadata;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.store.StoragePluginUtils;
import com.dremio.mongo.proto.MongoReaderProto;
import com.dremio.plugins.mongo.MongoDocumentHelper;
import com.dremio.plugins.mongo.connection.MongoConnection;
import com.dremio.plugins.mongo.planning.MongoSubpartitioner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoChunks implements Iterable<MongoChunk> {
   private static final Integer SELECT = 1;
   private static final Logger logger = LoggerFactory.getLogger(MongoChunks.class);
   private final MongoTopology topology;
   private final MongoCollection collection;
   private List<MongoChunk> chunks;
   private final MongoReaderProto.CollectionType type;

   public MongoChunks(MongoCollection collection, MongoConnection connection, MongoTopology topology, int subpartitionSize, String pluginName) {
      this.collection = collection;
      this.topology = topology;
      logger.debug("Start init");
      Stopwatch watch = Stopwatch.createStarted();

      try {
         String collectionName = collection.getCollection();
         MongoDatabase database = connection.getDatabase(collection.getDatabase());
         com.mongodb.client.MongoCollection<Document> collections = database.getCollection(collection.getCollection());
         if (!topology.isSharded()) {
            List<MongoChunk> chunks = new ArrayList();
            this.addChunk(chunks, connection, subpartitionSize, (MongoTopology.ReplicaSet)topology.iterator().next());
            this.chunks = chunks;
            this.type = MongoReaderProto.CollectionType.SUB_PARTITIONED;
         } else {
            if (isShardedCollection(database, collectionName) && topology.canConnectToMongods()) {
               boolean assignmentPerShard = !topology.canGetChunks() || isHashed(collections) || this.isCompoundKey(connection);
               if (!assignmentPerShard) {
                  this.chunks = this.handleShardedCollection(connection);
                  this.type = MongoReaderProto.CollectionType.RANGE_PARTITION;
                  return;
               }

               List<MongoChunk> chunks = new ArrayList();
               Iterator var12 = topology.iterator();

               while(var12.hasNext()) {
                  MongoTopology.ReplicaSet replicaSet = (MongoTopology.ReplicaSet)var12.next();
                  this.addChunk(chunks, connection, subpartitionSize, replicaSet);
               }

               this.type = MongoReaderProto.CollectionType.NODE_PARTITION;
               this.chunks = chunks;
               return;
            }

            this.type = MongoReaderProto.CollectionType.SINGLE_PARTITION;
            this.chunks = handleSimpleCase(connection);
            return;
         }
      } catch (RuntimeException var17) {
         throw StoragePluginUtils.message(UserException.dataReadError(var17), pluginName, "Failure while attempting to retrieve Mongo read information for collection %s.", new Object[]{collection}).build(logger);
      } finally {
         logger.debug("Mongo chunks retrieved in {} ms.", watch.elapsed(TimeUnit.MILLISECONDS));
      }

   }

   public MongoReaderProto.CollectionType getCollectionType() {
      return this.type;
   }

   private void addChunk(List<MongoChunk> listToFill, MongoConnection client, int subpartitionSize, MongoTopology.ReplicaSet replicaSet) {
      if (subpartitionSize > 0) {
         MongoSubpartitioner p = new MongoSubpartitioner(client);
         int index = 0;
         Iterator var7 = p.getPartitions(this.collection, subpartitionSize).iterator();

         while(var7.hasNext()) {
            MongoSubpartitioner.Range r = (MongoSubpartitioner.Range)var7.next();
            MongoChunk prototypeChunk = replicaSet.getAsChunk(index++);
            listToFill.add(r.getAsChunk(prototypeChunk));
         }
      } else {
         listToFill.add(replicaSet.getAsChunk(0));
      }

   }

   private static List<MongoChunk> handleSimpleCase(MongoConnection connection) {
      List<MongoChunk> chunks = new ArrayList();
      chunks.add(MongoChunk.newWithAddress(connection.getAddress()));
      return chunks;
   }

   private static boolean isHashed(com.mongodb.client.MongoCollection<Document> collection) {
      try {
         MongoCursor indexIterator = collection.listIndexes().iterator();

         while(indexIterator.hasNext()) {
            Document oneIndex = (Document)indexIterator.next();
            Iterator var3 = ((Document)oneIndex.get("key")).entrySet().iterator();

            while(var3.hasNext()) {
               Entry<String, Object> oneKey = (Entry)var3.next();
               if (oneKey.getValue() instanceof String && "hashed".equalsIgnoreCase((String)oneKey.getValue())) {
                  return true;
               }
            }
         }

         return false;
      } catch (Exception var5) {
         logger.error("Could not get list of indices for collection, defaulting to hashed mode");
         return true;
      }
   }

   private static boolean isShardedCollection(MongoDatabase db, String collectionName) {
      Document stats = MongoDocumentHelper.runMongoCommand(db, new Document("collStats", collectionName));
      return stats == null ? false : stats.getBoolean("sharded", false);
   }

   private boolean isCompoundKey(MongoConnection client) {
      FindIterable<Document> chunks = this.getChunkCollection(client);
      Document firstChunk = (Document)chunks.first();
      Document minMap = (Document)firstChunk.get("min");
      Document maxMap = (Document)firstChunk.get("max");
      return minMap.size() > 1 || maxMap.size() > 1;
   }

   private FindIterable<Document> getChunkCollection(MongoConnection client) {
      assert this.topology.canGetChunks() && this.topology.canConnectToMongods();

      MongoDatabase db = client.getDatabase("config");
      com.mongodb.client.MongoCollection<Document> chunksCollection = db.getCollection("chunks");
      Document filter = new Document("ns", this.collection.toName());
      Document projection = (new Document()).append("shard", SELECT).append("min", SELECT).append("max", SELECT);
      return chunksCollection.find(filter).projection(projection);
   }

   private List<MongoChunk> handleShardedCollection(MongoConnection connection) {
      List<MongoChunk> chunks = new ArrayList();
      MongoCursor<Document> iterator = this.getChunkCollection(connection).iterator();
      Map<String, MongoChunk> addresses = Maps.newHashMap();
      Iterator var5 = this.topology.iterator();

      MongoChunk chunk;
      while(var5.hasNext()) {
         MongoTopology.ReplicaSet sets = (MongoTopology.ReplicaSet)var5.next();
         chunk = sets.getAsChunk(0);
         addresses.put(sets.getName(), chunk);
      }

      while(iterator.hasNext()) {
         Document chunkObj = (Document)iterator.next();
         String shardName = (String)chunkObj.get("shard");
         chunk = new MongoChunk(((MongoChunk)addresses.get(shardName)).getChunkLocList());
         Document minMap = (Document)chunkObj.get("min");
         chunk.setMinFilters(minMap);
         Document maxMap = (Document)chunkObj.get("max");
         chunk.setMaxFilters(maxMap);
         chunks.add(chunk);
      }

      return (List)(chunks.isEmpty() ? handleSimpleCase(connection) : chunks);
   }

   public Iterator<MongoChunk> iterator() {
      return Iterators.unmodifiableIterator(this.chunks.iterator());
   }

   public int size() {
      return this.chunks.size();
   }

   public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("MongoChunks for ");
      sb.append(this.collection.toName());
      sb.append("\n");
      Iterator var2 = this.iterator();

      while(var2.hasNext()) {
         MongoChunk c = (MongoChunk)var2.next();
         sb.append("\t");
         sb.append(c.toString());
         sb.append("\n");
      }

      return sb.toString();
   }
}
