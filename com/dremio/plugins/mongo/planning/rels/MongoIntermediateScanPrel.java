package com.dremio.plugins.mongo.planning.rels;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.ScanPrelBase;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.TableMetadata;
import com.dremio.mongo.proto.MongoReaderProto;
import com.dremio.plugins.mongo.planning.MongoPipeline;
import com.dremio.plugins.mongo.planning.MongoPipelineOperators;
import com.dremio.plugins.mongo.planning.MongoScanSpec;
import com.dremio.plugins.mongo.planning.rules.MongoImplementor;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.bson.Document;

public class MongoIntermediateScanPrel extends ScanPrelBase implements MongoRel {
   public MongoIntermediateScanPrel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, TableMetadata tableMetadata, List<SchemaPath> projectedColumns, double observedRowcountAdjustment) {
      super(cluster, traits(cluster, table.getRowCount(), tableMetadata.getSplitCount(), traitSet, tableMetadata), table, tableMetadata.getStoragePluginId(), tableMetadata, projectedColumns, observedRowcountAdjustment);
   }

   private static RelTraitSet traits(RelOptCluster cluster, double rowCount, int splitCount, RelTraitSet traitSet, TableMetadata tableMetadata) {
      PlannerSettings settings = PrelUtil.getPlannerSettings(cluster.getPlanner());
      boolean smallInput = rowCount < (double)settings.getSliceTarget();

      MongoReaderProto.MongoTableXattr extendedAttributes;
      try {
         extendedAttributes = MongoReaderProto.MongoTableXattr.parseFrom(tableMetadata.getReadDefinition().getExtendedProperty().asReadOnlyByteBuffer());
      } catch (InvalidProtocolBufferException var10) {
         throw Throwables.propagate(var10);
      }

      DistributionTrait distribution;
      if (settings.isMultiPhaseAggEnabled() && !settings.isSingleMode() && !smallInput && splitCount > 1 && extendedAttributes.getType() != MongoReaderProto.CollectionType.SINGLE_PARTITION) {
         distribution = DistributionTrait.ANY;
      } else {
         distribution = DistributionTrait.SINGLETON;
      }

      return traitSet.plus(distribution);
   }

   public StoragePluginId getPluginId() {
      return this.pluginId;
   }

   public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new MongoIntermediateScanPrel(this.getCluster(), traitSet, this.getTable(), this.tableMetadata, this.getProjectedColumns(), this.observedRowcountAdjustment);
   }

   public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator physicalPlanCreator) throws IOException {
      throw new UnsupportedOperationException();
   }

   public MongoIntermediateScanPrel cloneWithProject(List<SchemaPath> projection) {
      return new MongoIntermediateScanPrel(this.getCluster(), this.getTraitSet(), this.table, this.tableMetadata, projection, this.observedRowcountAdjustment);
   }

   public BatchSchema getSchema(FunctionLookupContext context) {
      LinkedHashSet<SchemaPath> paths = new LinkedHashSet();
      Iterator var3 = this.getProjectedColumns().iterator();

      while(var3.hasNext()) {
         SchemaPath p = (SchemaPath)var3.next();
         paths.add(SchemaPath.getSimplePath(p.getRootSegment().getPath()));
      }

      return this.tableMetadata.getSchema().maskAndReorder(ImmutableList.copyOf(paths));
   }

   public MongoScanSpec implement(MongoImplementor impl) {
      MongoScanSpec spec = new MongoScanSpec((String)this.getTableMetadata().getName().getPathComponents().get(1), (String)this.getTableMetadata().getName().getPathComponents().get(2), MongoPipeline.createMongoPipeline((List)null, false));
      List<SchemaPath> projectedColumns = this.getProjectedColumns();
      if (projectedColumns != null && !projectedColumns.equals(GroupScan.ALL_COLUMNS)) {
         Document columnsDoc = new Document();
         List<SchemaPath> newColumns = new ArrayList();
         Iterator var6 = projectedColumns.iterator();

         SchemaPath col;
         while(var6.hasNext()) {
            col = (SchemaPath)var6.next();
            SchemaPath newCol = SchemaPath.getSimplePath(col.getRootSegment().getPath());
            if (!newColumns.contains(newCol)) {
               newColumns.add(newCol);
            }
         }

         var6 = newColumns.iterator();

         while(var6.hasNext()) {
            col = (SchemaPath)var6.next();
            columnsDoc.put(col.getAsUnescapedPath(), 1);
         }

         return spec.plusPipeline(Collections.singletonList(new Document(MongoPipelineOperators.PROJECT.getOperator(), columnsDoc)), false);
      } else {
         return spec;
      }
   }
}
