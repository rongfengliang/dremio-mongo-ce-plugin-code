package com.dremio.plugins.mongo.planning.rels;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.physical.CustomPrel;
import com.dremio.exec.planner.physical.LeafPrel;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.store.TableMetadata;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;
import com.dremio.plugins.mongo.planning.MongoGroupScan;
import com.dremio.plugins.mongo.planning.MongoScanSpec;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Options
public class MongoScanPrel extends TableScan implements LeafPrel, CustomPrel {
   private static final Logger logger = LoggerFactory.getLogger(MongoScanPrel.class);
   public static final LongValidator RESERVE = new PositiveLongValidator("planner.op.scan.mongo.reserve_bytes", Long.MAX_VALUE, 1000000L);
   public static final LongValidator LIMIT = new PositiveLongValidator("planner.op.scan.mongo.limit_bytes", Long.MAX_VALUE, Long.MAX_VALUE);
   private final Prel input;
   private MongoScanSpec spec;
   private TableMetadata tableMetadata;
   private List<SchemaPath> columns;
   private List<SchemaPath> sanitizedColumns;
   private BatchSchema schema;
   private double estimatedRowCount;
   private boolean isSingleFragment;

   public MongoScanPrel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, Prel input) {
      super(cluster, traitSet, table);
      this.input = input;
      this.rowType = input.getRowType();
   }

   public Prel getOriginPrel() {
      return this.input;
   }

   public double estimateRowCount(RelMetadataQuery mq) {
      return mq.getRowCount(this.input);
   }

   public RelWriter explainTerms(RelWriter pw) {
      return super.explainTerms(pw).item("MongoQuery", this.spec.getMongoQuery());
   }

   public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
      return new MongoGroupScan(creator.props(this, this.tableMetadata.getUser(), this.schema, RESERVE, LIMIT), this.spec, this.tableMetadata, this.columns, this.sanitizedColumns, this.schema, (long)this.estimatedRowCount, this.isSingleFragment);
   }

   public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
      return logicalVisitor.visitLeaf(this, value);
   }

   public SelectionVectorMode[] getSupportedEncodings() {
      return SelectionVectorMode.DEFAULT;
   }

   public SelectionVectorMode getEncoding() {
      return SelectionVectorMode.NONE;
   }

   public boolean needsFinalColumnReordering() {
      return false;
   }

   public Iterator<Prel> iterator() {
      return Collections.emptyIterator();
   }

   public int getMaxParallelizationWidth() {
      return this.tableMetadata.getSplitCount();
   }

   public int getMinParallelizationWidth() {
      return 1;
   }

   public DistributionAffinity getDistributionAffinity() {
      return this.tableMetadata.getStoragePluginId().getCapabilities().getCapability(SourceCapabilities.REQUIRES_HARD_AFFINITY) ? DistributionAffinity.HARD : DistributionAffinity.SOFT;
   }

   public void createScanOperator(MongoScanSpec spec, TableMetadata tableMetadata, List<SchemaPath> columns, List<SchemaPath> sanitizedColumns, BatchSchema schema, double estimatedRowCount, boolean isSingleFragment) {
      this.spec = spec;
      this.tableMetadata = tableMetadata;
      this.columns = columns;
      this.sanitizedColumns = sanitizedColumns;
      this.schema = schema;
      this.estimatedRowCount = estimatedRowCount;
      this.isSingleFragment = isSingleFragment;
   }
}
