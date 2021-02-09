package com.dremio.plugins.mongo.planning.rels;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.calcite.logical.SampleCrel;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.common.MoreRelOptUtil.SubsetRemover;
import com.dremio.exec.planner.physical.LimitPrel;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.SinglePrel;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.planner.sql.handlers.PrelFinalizable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;
import com.dremio.plugins.mongo.planning.MongoConvention;
import com.dremio.plugins.mongo.planning.MongoScanSpec;
import com.dremio.plugins.mongo.planning.rules.MongoImplementor;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Options
public class MongoIntermediatePrel extends SinglePrel implements PrelFinalizable, MongoRel {
   private static final Logger logger = LoggerFactory.getLogger(MongoIntermediatePrel.class);
   public static final LongValidator RESERVE = new PositiveLongValidator("planner.op.mongo.reserve_bytes", Long.MAX_VALUE, 1000000L);
   public static final LongValidator LIMIT = new PositiveLongValidator("planner.op.mongo.limit_bytes", Long.MAX_VALUE, Long.MAX_VALUE);
   private static final long SAMPLE_SIZE_DENOMINATOR = 10L;
   private final StoragePluginId pluginId;
   private final FunctionLookupContext functionLookupContext;
   private final MongoIntermediateScanPrel scan;

   public MongoIntermediatePrel(RelTraitSet traitSet, RelNode input, FunctionLookupContext functionLookupContext, MongoIntermediateScanPrel scan, StoragePluginId pluginId) {
      super(input.getCluster(), traitSet, input);
      Preconditions.checkArgument(input.getTraitSet().getTrait(ConventionTraitDef.INSTANCE) == MongoConvention.INSTANCE);
      this.input = input;
      this.pluginId = pluginId;
      this.functionLookupContext = functionLookupContext;
      this.scan = scan;
      this.rowType = input.getRowType();
   }

   public StoragePluginId getPluginId() {
      return this.pluginId;
   }

   public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
      throw new UnsupportedOperationException("Must be finalized before retrieving physical operator.");
   }

   public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      Preconditions.checkArgument(inputs.size() == 1, "must have one input: %s", inputs);
      return new MongoIntermediatePrel(traitSet, (RelNode)inputs.get(0), this.functionLookupContext, this.scan, this.pluginId);
   }

   public MongoIntermediatePrel withNewInput(MongoRel input) {
      return new MongoIntermediatePrel(input.getTraitSet().replace(Prel.PHYSICAL), input, this.functionLookupContext, this.scan, this.pluginId);
   }

   protected Object clone() throws CloneNotSupportedException {
      return this.copy(this.getTraitSet(), this.getInputs());
   }

   public MongoIntermediateScanPrel getScan() {
      return this.scan;
   }

   public SelectionVectorMode getEncoding() {
      return SelectionVectorMode.NONE;
   }

   public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
      throw new UnsupportedOperationException("This needs to be finalized before using a PrelVisitor.");
   }

   public boolean needsFinalColumnReordering() {
      return false;
   }

   public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
      return planner.getCostFactory().makeZeroCost();
   }

   public Prel finalizeRel() {
      MongoRel mongoTree = (MongoRel)this.getInput().accept(new SubsetRemover());
      MongoImplementor implementor = new MongoImplementor(this.pluginId);
      MongoScanSpec spec = implementor.visitChild(0, mongoTree);
      BatchSchema schema = mongoTree.getSchema(this.functionLookupContext);
      List<SchemaPath> columns = new ArrayList();
      Iterator var6 = this.rowType.getFieldNames().iterator();

      while(var6.hasNext()) {
         String colNames = (String)var6.next();
         columns.add(SchemaPath.getSimplePath(colNames));
      }

      List<SchemaPath> sanitizedColumns = new ArrayList();
      Iterator var18 = MongoColumnNameSanitizer.sanitizeColumnNames(this.rowType).getFieldNames().iterator();

      while(var18.hasNext()) {
         String sanitizedColumn = (String)var18.next();
         sanitizedColumns.add(SchemaPath.getSimplePath(sanitizedColumn));
      }

      double estimatedRowCount = this.getCluster().getMetadataQuery().getRowCount(this);
      PlannerSettings settings = (PlannerSettings)this.getCluster().getPlanner().getContext().unwrap(PlannerSettings.class);
      boolean smallInput = estimatedRowCount < (double)settings.getSliceTarget();
      boolean isSingleFragment = !settings.isMultiPhaseAggEnabled() || settings.isSingleMode() || smallInput || this.scan.getTableMetadata().getSplitCount() == 1;
      MongoScanPrel mongoPrel = new MongoScanPrel(this.getCluster(), this.getTraitSet(), this.scan.getTable(), mongoTree);
      mongoPrel.createScanOperator(spec, this.scan.getTableMetadata(), columns, sanitizedColumns, schema, estimatedRowCount, isSingleFragment);
      Object mongoWithLimit;
      if (!implementor.hasSample() && !implementor.hasLimit()) {
         mongoWithLimit = mongoPrel;
      } else {
         PlannerSettings plannerSettings = PrelUtil.getPlannerSettings(this.getCluster().getPlanner());
         long fetchSize = implementor.getLimitSize();
         if (implementor.hasSample()) {
            fetchSize = Math.min(fetchSize, SampleCrel.getSampleSizeAndSetMinSampleSize(plannerSettings, 10L));
         }

         mongoWithLimit = PrelUtil.addLimitPrel(mongoPrel, fetchSize);
      }

      if (!implementor.needsLimitZero()) {
         return (Prel)mongoWithLimit;
      } else {
         RexBuilder b = this.getCluster().getRexBuilder();
         return new LimitPrel(this.getCluster(), ((Prel)mongoWithLimit).getTraitSet(), (RelNode)mongoWithLimit, b.makeBigintLiteral(BigDecimal.ZERO), b.makeBigintLiteral(BigDecimal.ZERO));
      }
   }

   public MongoScanSpec implement(MongoImplementor impl) {
      throw new UnsupportedOperationException();
   }

   public BatchSchema getSchema(FunctionLookupContext context) {
      throw new UnsupportedOperationException();
   }
}
