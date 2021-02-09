package com.dremio.plugins.mongo.execution;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.exec.vector.complex.fn.JsonReader;
import com.dremio.options.OptionManager;
import com.dremio.plugins.mongo.connection.MongoConnection;
import com.dremio.plugins.mongo.connection.MongoConnectionManager;
import com.dremio.plugins.mongo.planning.MongoPipeline;
import com.dremio.plugins.mongo.planning.MongoSubScanSpec;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.metrics.MongoStats.Metric;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.complex.impl.VectorContainerWriter;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.bson.BsonBinaryReader;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.ByteBuf;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.io.ByteBufferBsonInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoRecordReader extends AbstractRecordReader {
   private static final Logger logger = LoggerFactory.getLogger(MongoRecordReader.class);
   private static final BsonDocumentCodec DOCUMENT_CODEC = new BsonDocumentCodec();
   private final List<SchemaPath> sanitizedColumns;
   private MongoCollection<RawBsonDocument> collection;
   private MongoCursor<RawBsonDocument> cursor;
   private final MongoConnectionManager manager;
   private JsonReader jsonReader;
   private BsonRecordReader bsonReader;
   private VectorContainerWriter writer;
   private final MongoPipeline pipeline;
   private final ArrowBuf managedbuf;
   private boolean localRead = false;
   private final boolean enableAllTextMode;
   private final boolean readNumbersAsDouble;
   private final boolean isBsonRecordReader;
   public static final int NUM_RECORDS_FOR_SETUP = 10;
   private final MongoRecordReader.MongoReaderStats mongoReaderStats = new MongoRecordReader.MongoReaderStats();
   private Map<String, Integer> decimalScales;

   public MongoRecordReader(MongoConnectionManager manager, OperatorContext context, MongoSubScanSpec subScanSpec, List<SchemaPath> projectedColumns, List<SchemaPath> sanitizedColumns, ArrowBuf managedBuf, boolean isSingleThread, int target, BatchSchema fullSchema) {
      super(context, projectedColumns);
      this.manager = manager;
      MongoPipeline tempPipeline = subScanSpec.getPipeline();
      if (!isSingleThread) {
         tempPipeline = tempPipeline.applyMinMaxFilter(subScanSpec.getMinFiltersAsDocument(), subScanSpec.getMaxFiltersAsDocument());
      }

      OptionManager options = context.getOptions();
      this.numRowsPerBatch = target;
      this.pipeline = tempPipeline;
      this.managedbuf = managedBuf;
      this.enableAllTextMode = options.getOption("store.mongo.all_text_mode").getBoolVal();
      this.readNumbersAsDouble = options.getOption("store.mongo.read_numbers_as_double").getBoolVal();
      this.isBsonRecordReader = options.getOption("store.mongo.bson.record.reader").getBoolVal();
      this.sanitizedColumns = sanitizedColumns;
      logger.debug("BsonRecordReader is enabled? " + this.isBsonRecordReader);
      this.init(subScanSpec, isSingleThread);
      if (fullSchema != null) {
         Map<String, Integer> values = new HashMap();
         Iterator var13 = fullSchema.iterator();

         while(var13.hasNext()) {
            Field field = (Field)var13.next();
            if (ArrowTypeID.Decimal == field.getType().getTypeID()) {
               values.put(field.getName(), ((Decimal)field.getType()).getScale());
            }
         }

         this.decimalScales = Collections.unmodifiableMap(values);
      }

   }

   protected Collection<SchemaPath> transformColumns(Collection<SchemaPath> projectedColumns) {
      Set<SchemaPath> transformed = Sets.newLinkedHashSet();
      if (!this.isStarQuery()) {
         transformed.addAll(projectedColumns);
      } else {
         transformed.add(AbstractRecordReader.STAR_COLUMN);
      }

      return transformed;
   }

   private void init(MongoSubScanSpec subScanSpec, boolean isSingle) {
      MongoConnection client = null;
      if (isSingle) {
         client = this.manager.getReadClient();
      } else {
         List<String> hosts = subScanSpec.getHosts();
         List<ServerAddress> addresses = Lists.newArrayList();
         Iterator var6 = hosts.iterator();

         while(var6.hasNext()) {
            String host = (String)var6.next();
            addresses.add(new ServerAddress(host));
         }

         client = this.manager.getReadClients(addresses);
      }

      MongoDatabase db = client.getDatabase(subScanSpec.getDbName());
      this.collection = db.getCollection(subScanSpec.getCollectionName(), RawBsonDocument.class);
   }

   public void setup(OutputMutator output) throws ExecutionSetupException {
      int numRead = 0;
      int numRowsPerBatchOriginal = this.numRowsPerBatch;

      try {
         this.writer = new VectorContainerWriter(output);
         if (this.decimalScales != null) {
            FieldWriter fieldWriter = (FieldWriter)this.writer.rootAsStruct();
            Iterator var5 = this.decimalScales.entrySet().iterator();

            while(var5.hasNext()) {
               Entry<String, Integer> fieldNameScale = (Entry)var5.next();
               fieldWriter.decimal((String)fieldNameScale.getKey(), (Integer)fieldNameScale.getValue(), 38);
            }
         }

         int sizeLimit = Math.toIntExact(this.context.getOptions().getOption(ExecConstants.LIMIT_FIELD_SIZE_BYTES));
         int maxLeafLimit = Math.toIntExact(this.context.getOptions().getOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX));
         if (!this.sanitizedColumns.isEmpty()) {
            if (this.isBsonRecordReader) {
               this.bsonReader = new BsonRecordReader(this.managedbuf, Lists.newArrayList(this.sanitizedColumns), sizeLimit, maxLeafLimit, this.readNumbersAsDouble, this.decimalScales);
               logger.debug("Initialized BsonRecordReader.");
            } else {
               this.jsonReader = new JsonReader(this.managedbuf, Lists.newArrayList(this.sanitizedColumns), sizeLimit, maxLeafLimit, this.enableAllTextMode, false, this.readNumbersAsDouble);
               logger.debug(" Initialized JsonRecordReader.");
            }
         }

         this.numRowsPerBatch = 10;

         try {
            numRead = this.next();
         } catch (BsonRecordReader.ChangedScaleException var10) {
            this.cursor = null;
            this.decimalScales = this.bsonReader.getDecimalScales();
            throw var10;
         }
      } finally {
         this.cursor = null;
         this.updateStats((long)(-numRead), -this.mongoReaderStats.numBytesRead);
         this.writer.reset();
         this.numRowsPerBatch = numRowsPerBatchOriginal;
      }

   }

   public int next() {
      if (this.cursor == null) {
         Stopwatch watch = Stopwatch.createStarted();
         this.startWait();

         try {
            this.cursor = this.pipeline.getCursor(this.collection, this.numRowsPerBatch);
         } finally {
            this.stopWait();
         }

         logger.debug("Took {} ms to get cursor", watch.elapsed(TimeUnit.MILLISECONDS));
      }

      this.writer.allocate();
      this.writer.reset();
      int docCount = 0;
      long numBytesRead = 0L;
      Stopwatch watch = Stopwatch.createStarted();

      try {
         for(; docCount < this.numRowsPerBatch; ++docCount) {
            this.startWait();

            try {
               if (!this.cursor.hasNext()) {
                  break;
               }
            } finally {
               this.stopWait();
            }

            this.writer.setPosition(docCount);
            this.startWait();

            RawBsonDocument rawBsonDocument;
            try {
               rawBsonDocument = (RawBsonDocument)this.cursor.next();
            } finally {
               this.stopWait();
            }

            ByteBuf buffer = rawBsonDocument.getByteBuffer();
            numBytesRead += (long)buffer.remaining();
            if (this.isBsonRecordReader) {
               BsonBinaryReader rawBsonReader = new BsonBinaryReader(new ByteBufferBsonInput(buffer));
               Throwable var9 = null;

               BsonDocument bsonDocument;
               try {
                  bsonDocument = DOCUMENT_CODEC.decode(rawBsonReader, DecoderContext.builder().build());
               } catch (Throwable var43) {
                  var9 = var43;
                  throw var43;
               } finally {
                  if (var9 != null) {
                     try {
                        rawBsonReader.close();
                     } catch (Throwable var42) {
                        var9.addSuppressed(var42);
                     }
                  } else {
                     rawBsonReader.close();
                  }

               }

               if (!this.sanitizedColumns.isEmpty()) {
                  this.bsonReader.write(this.writer, new BsonDocumentReader(bsonDocument));
               }
            } else if (!this.sanitizedColumns.isEmpty()) {
               String doc = rawBsonDocument.toJson();
               this.jsonReader.setSource(doc.getBytes(Charsets.UTF_8));
               this.jsonReader.write(this.writer);
            }
         }

         this.writer.setValueCount(docCount);
         logger.debug("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), docCount);
         this.updateStats((long)docCount, numBytesRead);
         return docCount;
      } catch (IOException var48) {
         String msg = "Failure while reading document. - Parser was at record: " + (docCount + 1);
         logger.error(msg, var48);
         throw new RuntimeException(msg, var48);
      }
   }

   private void updateStats(long docCount, long numBytesRead) {
      if (this.mongoReaderStats != null) {
         this.mongoReaderStats.numBytesRead = numBytesRead;
         this.mongoReaderStats.numRecordsRead = docCount;
         if (this.localRead) {
            this.mongoReaderStats.numRecordsReadLocal = docCount;
         } else {
            this.mongoReaderStats.numRecordsReadRemote = docCount;
         }
      }

   }

   private void startWait() {
      if (this.context != null && this.context.getStats() != null) {
         this.context.getStats().startWait();
      }

   }

   private void stopWait() {
      if (this.context != null && this.context.getStats() != null) {
         this.context.getStats().stopWait();
      }

   }

   public void close() {
      if (this.mongoReaderStats != null && this.context != null && this.context.getStats() != null) {
         this.context.getStats().setLongStat(Metric.TOTAL_RECORDS_READ, this.mongoReaderStats.numRecordsRead);
         this.context.getStats().setLongStat(Metric.NUM_LOCAL_RECORDS_READ, this.mongoReaderStats.numRecordsReadLocal);
         this.context.getStats().setLongStat(Metric.NUM_REMOTE_RECORDS_READ, this.mongoReaderStats.numRecordsReadRemote);
         this.context.getStats().setLongStat(Metric.TOTAL_BYTES_READ, this.mongoReaderStats.numBytesRead);
      }

   }

   private static class MongoReaderStats {
      private long numRecordsRead;
      private long numRecordsReadLocal;
      private long numRecordsReadRemote;
      private long numBytesRead;

      private MongoReaderStats() {
      }

      // $FF: synthetic method
      MongoReaderStats(Object x0) {
         this();
      }
   }
}
