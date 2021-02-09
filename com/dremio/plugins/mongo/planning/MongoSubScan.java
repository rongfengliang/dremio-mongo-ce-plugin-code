package com.dremio.plugins.mongo.planning;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.physical.base.SubScanWithProjection;
import com.dremio.exec.planner.fragment.MinorDataReader;
import com.dremio.exec.planner.fragment.MinorDataWriter;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonTypeName("mongo-shard-read")
public class MongoSubScan extends SubScanWithProjection {
   static final Logger logger = LoggerFactory.getLogger(MongoSubScan.class);
   private static final String CHUNKS_ATTRIBUTE_KEY = "mongo-shard-read-chunks";
   private final StoragePluginId pluginId;
   private final List<SchemaPath> columns;
   private final List<SchemaPath> sanitizedColumns;
   private final boolean singleFragment;
   @JsonIgnore
   private List<MongoSubScanSpec> chunkScanSpecList;

   public MongoSubScan(OpProps props, StoragePluginId pluginId, List<MongoSubScanSpec> chunkScanSpecList, List<SchemaPath> columns, List<SchemaPath> sanitizedColumns, boolean singleFragment, List<String> tableSchemaPath, BatchSchema fullSchema) throws ExecutionSetupException {
      super(props, fullSchema, tableSchemaPath, columns);
      this.pluginId = pluginId;
      this.columns = columns;
      this.sanitizedColumns = sanitizedColumns;
      this.chunkScanSpecList = chunkScanSpecList;
      this.singleFragment = singleFragment;
      if (chunkScanSpecList != null) {
         Preconditions.checkArgument(!singleFragment || chunkScanSpecList.size() == 1);
      }

   }

   @JsonCreator
   public MongoSubScan(@JsonProperty("props") OpProps props, @JsonProperty("pluginId") StoragePluginId pluginId, @JsonProperty("columns") List<SchemaPath> columns, @JsonProperty("sanitizedColumns") List<SchemaPath> sanitizedColumns, @JsonProperty("singleFragment") boolean singleFragment, @JsonProperty("tableSchemaPath") List<String> tableSchemaPath, @JsonProperty("fullSchema") BatchSchema fullSchema) throws ExecutionSetupException {
      this(props, pluginId, (List)null, columns, sanitizedColumns, singleFragment, tableSchemaPath, fullSchema);
   }

   public StoragePluginId getPluginId() {
      return this.pluginId;
   }

   public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
      return physicalVisitor.visitSubScan(this, value);
   }

   public boolean isSingleFragment() {
      return this.singleFragment;
   }

   public List<SchemaPath> getColumns() {
      return this.columns;
   }

   public List<SchemaPath> getSanitizedColumns() {
      return this.sanitizedColumns;
   }

   public List<MongoSubScanSpec> getChunkScanSpecList() {
      return this.chunkScanSpecList;
   }

   public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
      Preconditions.checkArgument(children.isEmpty());
      return new MongoSubScan(this.props, this.pluginId, this.chunkScanSpecList, this.columns, this.sanitizedColumns, this.singleFragment, (List)Iterables.getOnlyElement(this.getReferencedTables()), this.getFullSchema());
   }

   public int getOperatorType() {
      return 37;
   }

   public Iterator<PhysicalOperator> iterator() {
      return ImmutableList.of().iterator();
   }

   public void collectMinorSpecificAttrs(MinorDataWriter writer) throws Exception {
      writer.writeJsonEntry(this.getProps(), "mongo-shard-read-chunks", new MongoSubScanSpecList(this.chunkScanSpecList));
   }

   public void populateMinorSpecificAttrs(MinorDataReader reader) throws Exception {
      MongoSubScanSpecList list = (MongoSubScanSpecList)reader.readJsonEntry(this.getProps(), "mongo-shard-read-chunks", MongoSubScanSpecList.class);
      this.chunkScanSpecList = list.getSpecs();
      Preconditions.checkArgument(!this.singleFragment || this.chunkScanSpecList.size() == 1);
   }
}
