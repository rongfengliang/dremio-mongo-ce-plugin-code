package com.dremio.plugins.mongo.metadata;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SampleMutator;
import com.dremio.exec.store.StoragePluginUtils;
import com.dremio.mongo.proto.MongoReaderProto;
import com.dremio.plugins.mongo.connection.MongoConnectionManager;
import com.dremio.plugins.mongo.execution.BsonRecordReader;
import com.dremio.plugins.mongo.execution.MongoRecordReader;
import com.dremio.plugins.mongo.planning.MongoPipeline;
import com.dremio.plugins.mongo.planning.MongoSubScanSpec;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.memory.BufferAllocator;
import org.bson.Document;

public class MongoTable implements DatasetHandle {
   private final MongoCollection collection;
   private final MongoConnectionManager manager;
   private final EntityPath entityPath;
   private final SabotContext context;
   private final MongoTopology topology;
   private final int subpartitionSize;
   private DatasetMetadata datasetMetadata;
   private List<PartitionChunk> partitionChunks;

   public MongoTable(SabotContext context, int subpartitionSize, EntityPath entityPath, MongoCollection collection, MongoConnectionManager manager, MongoTopology topology) {
      this.context = context;
      this.entityPath = entityPath;
      this.subpartitionSize = subpartitionSize;
      this.collection = collection;
      this.manager = manager;
      this.topology = topology;
   }

   private void buildIfNecessary(BatchSchema oldSchema) throws ConnectorException {
      if (this.partitionChunks == null) {
         MongoDatabase db = this.manager.getMetadataClient().getDatabase(this.collection.getDatabase());
         com.mongodb.client.MongoCollection<Document> docs = db.getCollection(this.collection.getCollection());
         long count = docs.count();

         BatchSchema schema;
         try {
            schema = this.getSampledSchema(oldSchema);
         } catch (Exception var10) {
            Throwables.throwIfUnchecked(var10);
            throw new ConnectorException(var10);
         }

         MongoChunks chunks = new MongoChunks(this.collection, this.manager.getFirstConnection(), this.topology, this.subpartitionSize, (String)this.entityPath.getComponents().get(0));
         this.partitionChunks = new ArrayList();
         Iterator var8 = chunks.iterator();

         while(var8.hasNext()) {
            MongoChunk chunk = (MongoChunk)var8.next();
            this.partitionChunks.add(chunk.toSplit());
         }

         MongoReaderProto.MongoTableXattr extended = MongoReaderProto.MongoTableXattr.newBuilder().setDatabase(this.collection.getDatabase()).setCollection(this.collection.getCollection()).setType(chunks.getCollectionType()).build();
         DatasetStats var10001 = DatasetStats.of(count, ScanCostFactor.MONGO.getFactor());
         Objects.requireNonNull(extended);
         this.datasetMetadata = DatasetMetadata.of(var10001, schema, extended::writeTo);
      }
   }

   private BatchSchema getSampledSchema(BatchSchema oldSchema) throws Exception {
      MongoSubScanSpec spec = new MongoSubScanSpec(this.collection.getDatabase(), this.collection.getCollection(), (List)null, (String)null, (String)null, MongoPipeline.createMongoPipeline((List)null, false));
      int fetchSize = (int)this.context.getOptionManager().getOption(ExecConstants.TARGET_BATCH_RECORDS_MAX);
      ImmutableList<SchemaPath> columns = ImmutableList.of(SchemaPath.getSimplePath("*"));
      BufferAllocator sampleAllocator = this.context.getAllocator().newChildAllocator("mongo-sample-alloc", 0L, Long.MAX_VALUE);
      Throwable var6 = null;

      try {
         OperatorContextImpl operatorContext = new OperatorContextImpl(this.context.getConfig(), sampleAllocator, this.context.getOptionManager(), fetchSize);
         Throwable var8 = null;

         try {
            MongoRecordReader reader = new MongoRecordReader(this.manager, operatorContext, spec, columns, columns, operatorContext.getManagedBuffer(), true, fetchSize, oldSchema);
            Throwable var10 = null;

            try {
               SampleMutator mutator = new SampleMutator(sampleAllocator);

               try {
                  if (oldSchema != null) {
                     oldSchema.materializeVectors(GroupScan.ALL_COLUMNS, mutator);
                  }

                  while(true) {
                     try {
                        reader.setup(mutator);
                        reader.next();
                     } catch (BsonRecordReader.ChangedScaleException var50) {
                        mutator.close();
                        mutator = new SampleMutator(sampleAllocator);
                        continue;
                     }

                     mutator.getContainer().buildSchema(SelectionVectorMode.NONE);
                     BatchSchema batchSchema = mutator.getContainer().getSchema();
                     if (batchSchema.getFieldCount() == 0) {
                        throw StoragePluginUtils.message(UserException.dataReadError(), (String)this.entityPath.getComponents().get(0), "The table %s has no rows or columns", new Object[]{this.entityPath.getName()}).build();
                     }

                     BatchSchema var13 = batchSchema;
                     return var13;
                  }
               } finally {
                  mutator.close();
               }
            } catch (Throwable var52) {
               var10 = var52;
               throw var52;
            } finally {
               $closeResource(var10, reader);
            }
         } catch (Throwable var54) {
            var8 = var54;
            throw var54;
         } finally {
            $closeResource(var8, operatorContext);
         }
      } catch (Throwable var56) {
         var6 = var56;
         throw var56;
      } finally {
         if (sampleAllocator != null) {
            $closeResource(var6, sampleAllocator);
         }

      }
   }

   @VisibleForTesting
   public int getSubpartitionSize() {
      return this.subpartitionSize;
   }

   public EntityPath getDatasetPath() {
      return this.entityPath;
   }

   public DatasetMetadata getDatasetMetadata(BatchSchema oldSchema) throws ConnectorException {
      this.buildIfNecessary(oldSchema);
      return this.datasetMetadata;
   }

   public PartitionChunkListing listPartitionChunks(BatchSchema oldSchema) throws ConnectorException {
      this.buildIfNecessary(oldSchema);
      return () -> {
         return this.partitionChunks.iterator();
      };
   }

   // $FF: synthetic method
   private static void $closeResource(Throwable x0, AutoCloseable x1) {
      if (x0 != null) {
         try {
            x1.close();
         } catch (Throwable var3) {
            x0.addSuppressed(var3);
         }
      } else {
         x1.close();
      }

   }
}
