package com.dremio.plugins.mongo;

import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetHandleListing;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.extensions.SupportsListingDatasets;
import com.dremio.exec.catalog.CurrentSchemaOption;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.plugins.mongo.connection.MongoConnectionManager;
import com.dremio.plugins.mongo.metadata.MongoCollection;
import com.dremio.plugins.mongo.metadata.MongoCollections;
import com.dremio.plugins.mongo.metadata.MongoTable;
import com.dremio.plugins.mongo.metadata.MongoTopology;
import com.dremio.plugins.mongo.metadata.MongoVersion;
import com.dremio.plugins.mongo.planning.MongoRulesFactory;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.SourceState.SourceStatus;
import com.dremio.service.namespace.capabilities.BooleanCapability;
import com.dremio.service.namespace.capabilities.BooleanCapabilityValue;
import com.dremio.service.namespace.capabilities.CapabilityValue;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.protostuff.ByteString;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.StreamSupport;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;

public class MongoStoragePlugin implements StoragePlugin, SupportsListingDatasets {
   public static final BooleanCapability MONGO_3_2_FEATURES = new BooleanCapability("support_mongo_3_2_features", false);
   public static final BooleanCapability MONGO_3_4_FEATURES = new BooleanCapability("support_mongo_3_4_features", false);
   public static final BooleanCapability MONGO_3_6_FEATURES = new BooleanCapability("support_mongo_3_6_features", false);
   private final MongoConnectionManager manager;
   private final SabotContext context;
   private final String name;
   private final MongoConf config;
   private MongoVersion version;
   private boolean supportsMongo3_4;
   private boolean supportsMongo3_6;

   public MongoStoragePlugin(MongoConf config, SabotContext context, String name) {
      this.version = MongoVersion.MIN_MONGO_VERSION;
      this.supportsMongo3_4 = false;
      this.supportsMongo3_6 = false;
      this.context = context;
      this.config = config;
      this.name = name;
      this.manager = new MongoConnectionManager(config, name);
   }

   public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig config) {
      return true;
   }

   public DatasetHandleListing listDatasetHandles(GetDatasetOption... options) {
      MongoCollections collections = this.manager.getMetadataClient().getCollections();
      MongoTopology topology = this.manager.getTopology(collections.canDBStats(), collections.getAuthenticationDB());
      return () -> {
         return StreamSupport.stream(collections.spliterator(), false).map((input) -> {
            EntityPath entityPath = new EntityPath(ImmutableList.of(this.name, input.getDatabase(), input.getCollection()));
            return (DatasetHandle)this.getDatasetInternal(entityPath, topology).get();
         }).iterator();
      };
   }

   public Optional<DatasetHandle> getDatasetHandle(EntityPath datasetPath, GetDatasetOption... options) {
      List<String> components = datasetPath.getComponents();
      if (components.size() != 3) {
         return Optional.empty();
      } else {
         MongoCollections collections = this.manager.getMetadataClient().getCollections();
         MongoTopology topology = this.manager.getTopology(collections.canDBStats(), collections.getAuthenticationDB());
         return this.getDatasetInternal(datasetPath, topology);
      }
   }

   public DatasetMetadata getDatasetMetadata(DatasetHandle datasetHandle, PartitionChunkListing chunkListing, GetMetadataOption... options) throws ConnectorException {
      BatchSchema oldSchema = CurrentSchemaOption.getSchema(options);
      return ((MongoTable)datasetHandle.unwrap(MongoTable.class)).getDatasetMetadata(oldSchema);
   }

   public PartitionChunkListing listPartitionChunks(DatasetHandle datasetHandle, ListPartitionChunkOption... options) throws ConnectorException {
      BatchSchema oldSchema = CurrentSchemaOption.getSchema(options);
      return ((MongoTable)datasetHandle.unwrap(MongoTable.class)).listPartitionChunks(oldSchema);
   }

   public boolean containerExists(EntityPath key) {
      List<String> components = key.getComponents();
      if (components.size() != 2) {
         return false;
      } else {
         String database = (String)components.get(1);
         return StreamSupport.stream(this.manager.getMetadataClient().getCollections().spliterator(), false).anyMatch((input) -> {
            return input.getDatabase().equalsIgnoreCase(database);
         });
      }
   }

   public void start() {
      this.version = this.manager.connect();
      this.supportsMongo3_4 = (this.version.getMajor() > 3 || this.version.getMajor() == 3 && this.version.getMinor() >= 4) && (this.version.getCompatibilityMajor() > 3 || this.version.getCompatibilityMajor() == 3 && this.version.getCompatibilityMinor() >= 4);
      this.supportsMongo3_6 = (this.version.getMajor() > 3 || this.version.getMajor() == 3 && this.version.getMinor() >= 6) && (this.version.getCompatibilityMajor() > 3 || this.version.getCompatibilityMajor() == 3 && this.version.getCompatibilityMinor() >= 6);
   }

   @VisibleForTesting
   Optional<DatasetHandle> getDatasetInternal(EntityPath entityPath, MongoTopology topology) {
      List<String> components = entityPath.getComponents();
      if (components.size() != 3) {
         return Optional.empty();
      } else {
         MongoCollection desiredCollection = new MongoCollection((String)components.get(1), (String)components.get(2));
         return Optional.of(new MongoTable(this.context, this.config.subpartitionSize, entityPath, desiredCollection, this.manager, topology));
      }
   }

   public SourceCapabilities getSourceCapabilities() {
      return new SourceCapabilities(new CapabilityValue[]{new BooleanCapabilityValue(MONGO_3_2_FEATURES, this.version.enableNewFeatures()), new BooleanCapabilityValue(MONGO_3_4_FEATURES, this.supportsMongo3_4), new BooleanCapabilityValue(MONGO_3_6_FEATURES, this.supportsMongo3_6)});
   }

   public DatasetConfig createDatasetConfigFromSchema(DatasetConfig oldConfig, BatchSchema newSchema) {
      Preconditions.checkNotNull(oldConfig);
      Preconditions.checkNotNull(newSchema);
      BatchSchema merge;
      if (DatasetHelper.getSchemaBytes(oldConfig) == null) {
         merge = newSchema;
      } else {
         List<Field> oldFields = new ArrayList();
         CalciteArrowHelper.fromDataset(oldConfig).forEach((f) -> {
            oldFields.add(this.updateFieldTypeToTimestamp(f));
         });
         List<Field> newFields = new ArrayList();
         newSchema.forEach((f) -> {
            newFields.add(this.updateFieldTypeToTimestamp(f));
         });
         merge = (new BatchSchema(oldFields)).merge(new BatchSchema(newFields));
      }

      DatasetConfig newConfig = (DatasetConfig)DATASET_CONFIG_SERIALIZER.deserialize((byte[])DATASET_CONFIG_SERIALIZER.serialize(oldConfig));
      newConfig.setRecordSchema(ByteString.copyFrom(merge.serialize()));
      return newConfig;
   }

   public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
      return MongoRulesFactory.class;
   }

   public SourceState getState() {
      try {
         MongoVersion probeVersion = this.manager.validateConnection();
         if (probeVersion.enableNewFeatures() != this.version.enableNewFeatures()) {
            return SourceState.badState("Dremio needs to be restarted due to differences in the Mongo software version.", new String[]{"Mongo software version was modified at level that requires Dremio to be restarted (one or more nodes moved across the 3.2.0 version horizon)."});
         } else {
            return probeVersion.enableNewFeatures() ? new SourceState(SourceStatus.good, "", Collections.singletonList(probeVersion.getVersionInfo())) : new SourceState(SourceStatus.warn, "", Collections.singletonList(probeVersion.getVersionWarning()));
         }
      } catch (Exception var2) {
         return SourceState.badState("Could not connect to MongoDB host. Check your MongoDB database credentials and network settings", var2);
      }
   }

   public MongoConnectionManager getManager() {
      return this.manager;
   }

   public ViewTable getView(List<String> arg0, SchemaConfig arg1) {
      return null;
   }

   public void close() throws Exception {
      this.manager.close();
   }

   private Field updateFieldTypeToTimestamp(Field field) {
      List<Field> children = new ArrayList();
      FieldType type;
      if (field.getType().getTypeID() == ArrowTypeID.Union && field.getChildren().size() == 2 && ((Field)field.getChildren().get(0)).getType().getTypeID() == ArrowTypeID.Date && ((Field)field.getChildren().get(1)).getType().getTypeID() == ArrowTypeID.Timestamp) {
         type = new FieldType(field.isNullable(), new Timestamp(TimeUnit.MILLISECOND, (String)null), field.getDictionary(), field.getMetadata());
      } else {
         Iterator var4 = field.getChildren().iterator();

         while(var4.hasNext()) {
            Field child = (Field)var4.next();
            children.add(this.updateFieldTypeToTimestamp(child));
         }

         if (field.getType().getTypeID() == ArrowTypeID.Date) {
            type = new FieldType(field.isNullable(), new Timestamp(TimeUnit.MILLISECOND, (String)null), field.getDictionary(), field.getMetadata());
         } else {
            type = field.getFieldType();
         }
      }

      return new Field(field.getName(), type, children);
   }
}
