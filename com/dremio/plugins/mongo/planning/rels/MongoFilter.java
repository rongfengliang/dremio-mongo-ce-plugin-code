package com.dremio.plugins.mongo.planning.rels;

import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.common.FilterRelBase;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.plugins.mongo.MongoStoragePlugin;
import com.dremio.plugins.mongo.planning.MongoConvention;
import com.dremio.plugins.mongo.planning.MongoPipelineOperators;
import com.dremio.plugins.mongo.planning.MongoScanSpec;
import com.dremio.plugins.mongo.planning.rules.FindQueryGenerator;
import com.dremio.plugins.mongo.planning.rules.MatchExpressionConverter;
import com.dremio.plugins.mongo.planning.rules.MongoImplementor;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.bson.Document;

public class MongoFilter extends FilterRelBase implements MongoRel {
   private final boolean needsCollation;

   public MongoFilter(RelTraitSet traits, RelNode child, RexNode condition, boolean needsCollation) {
      super((Convention)traits.getTrait(ConventionTraitDef.INSTANCE), child.getCluster(), traits, child, condition);

      assert this.getConvention() instanceof MongoConvention;

      this.needsCollation = needsCollation;
   }

   public MongoScanSpec implement(MongoImplementor impl) {
      MongoScanSpec childSpec = impl.visitChild(0, this.getInput());
      SourceCapabilities capabilities = impl.getPluginId().getCapabilities();
      boolean isMongo32Enabled = capabilities.getCapability(MongoStoragePlugin.MONGO_3_2_FEATURES);
      boolean isMongo34Enabled = capabilities.getCapability(MongoStoragePlugin.MONGO_3_4_FEATURES);
      boolean isMongo36Enabled = capabilities.getCapability(MongoStoragePlugin.MONGO_3_6_FEATURES);
      BatchSchema schema = this.getInput() instanceof MongoIntermediateScanPrel ? ((MongoIntermediateScanPrel)this.getInput()).getBatchSchema() : null;
      Document filterToAdd;
      if (isMongo36Enabled) {
         FindQueryGenerator generator = new FindQueryGenerator(schema, this.getInput().getRowType());
         filterToAdd = new Document(MongoPipelineOperators.MATCH.getOperator(), this.condition.accept(generator));
         return childSpec.plusPipeline(Collections.singletonList(filterToAdd), this.needsCollation);
      } else {
         Object filterExpr = this.condition.accept(new MatchExpressionConverter(schema, this.getInput().getRowType(), isMongo32Enabled, isMongo34Enabled));
         filterToAdd = new Document(MongoPipelineOperators.MATCH.getOperator(), filterExpr);
         return childSpec.plusPipeline(Collections.singletonList(filterToAdd), this.needsCollation);
      }
   }

   public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
      return new MongoFilter(traitSet, input, condition, this.needsCollation);
   }

   protected Object clone() throws CloneNotSupportedException {
      return this.copy(this.getTraitSet(), this.getInputs());
   }

   public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
      throw new UnsupportedOperationException();
   }

   public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
      return super.computeSelfCost(planner, mq).multiplyBy(0.1D);
   }

   public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
      return logicalVisitor.visitPrel(this, value);
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
      return PrelUtil.iter(new RelNode[]{this.getInput()});
   }

   public BatchSchema getSchema(FunctionLookupContext context) {
      MongoRel child = (MongoRel)this.getInput();
      return child.getSchema(context);
   }
}
