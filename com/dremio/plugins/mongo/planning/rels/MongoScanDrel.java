package com.dremio.plugins.mongo.planning.rels;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.TableMetadata;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

public class MongoScanDrel extends ScanRelBase implements Rel {
   private final List<String> sanitizedColumns;

   public MongoScanDrel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, StoragePluginId pluginId, TableMetadata tableMetadata, List<SchemaPath> projectedColumns, List<String> sanitizedColumns, double observedRowcountAdjustment) {
      super(cluster, traitSet, table, pluginId, tableMetadata, projectedColumns, observedRowcountAdjustment);
      this.sanitizedColumns = sanitizedColumns;
   }

   public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new MongoScanDrel(this.getCluster(), traitSet, this.table, this.pluginId, this.tableMetadata, this.getProjectedColumns(), this.sanitizedColumns, this.observedRowcountAdjustment);
   }

   public ScanRelBase cloneWithProject(List<SchemaPath> projection) {
      return new MongoScanDrel(this.getCluster(), this.traitSet, this.table, this.pluginId, this.tableMetadata, projection, this.sanitizedColumns, this.observedRowcountAdjustment);
   }
}
