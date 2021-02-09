package com.dremio.plugins.mongo.planning.rels;

import com.dremio.exec.expr.ExpressionTreeMaterializer;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.common.ProjectRelBase;
import com.dremio.exec.planner.logical.ParseContext;
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
import com.dremio.plugins.mongo.planning.rules.MongoImplementor;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.bson.Document;

public class MongoProject extends ProjectRelBase implements MongoRel {
   private final RelDataType sanitizedRowType;
   private final boolean needsCollation;

   public MongoProject(RelTraitSet traits, RelNode input, List<? extends RexNode> projects, RelDataType rowType, boolean needsCollation) {
      super(MongoConvention.INSTANCE, input.getCluster(), traits, input, projects, rowType);
      this.sanitizedRowType = MongoColumnNameSanitizer.sanitizeColumnNames(rowType);
      this.needsCollation = needsCollation;
   }

   public final RelDataType getSanitizedRowType() {
      return this.sanitizedRowType;
   }

   public MongoScanSpec implement(MongoImplementor impl) {
      MongoScanSpec childSpec = impl.visitChild(0, this.getInput());
      boolean isMongo32Enabled = impl.getPluginId().getCapabilities().getCapability(MongoStoragePlugin.MONGO_3_2_FEATURES);
      boolean isMongo34Enabled = impl.getPluginId().getCapabilities().getCapability(MongoStoragePlugin.MONGO_3_4_FEATURES);
      Document projectDoc = new Document();
      RelNode input = this.getInput();
      boolean needsCollation = false;
      List<String> outputNames = this.getSanitizedRowType().getFieldNames();

      for(int i = 0; i < this.getProjects().size(); ++i) {
         String outputName = (String)outputNames.get(i);
         RexNode expr = (RexNode)this.getProjects().get(i);
         if (!(expr instanceof RexInputRef)) {
            throw new IllegalStateException("Mongo aggregation framework support has been removed. Non RexInputRef projection used.");
         }

         String inputName = MongoColumnNameSanitizer.sanitizeColumnName((String)input.getRowType().getFieldNames().get(((RexInputRef)expr).getIndex()));
         if (!inputName.equals(outputName)) {
            throw new IllegalStateException(String.format("Mongo aggregation framework support has been removed. InputName=%s, OutputName=%s.", inputName, outputName));
         }

         projectDoc.put(outputName, 1);
      }

      MongoScanSpec newSpec = childSpec.plusPipeline(Collections.singletonList(new Document(MongoPipelineOperators.PROJECT.getOperator(), projectDoc)), needsCollation);
      if (newSpec.getPipeline().isOnlyTrivialProjectOrFilter() && input instanceof MongoIntermediateScanPrel && newSpec.getPipeline().getProjectAsDocument().entrySet().size() == input.getTable().getRowType().getFieldCount()) {
         return new MongoScanSpec(newSpec.getDbName(), newSpec.getCollectionName(), newSpec.getPipeline().newWithoutProject());
      } else {
         return newSpec;
      }
   }

   public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
      return new MongoProject(traitSet, input, projects, rowType, this.needsCollation);
   }

   protected Object clone() throws CloneNotSupportedException {
      return this.copy(this.getTraitSet(), this.getInputs());
   }

   public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
      throw new UnsupportedOperationException();
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

   public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
      return super.computeSelfCost(planner, mq).multiplyBy(0.1D);
   }

   public BatchSchema getSchema(FunctionLookupContext context) {
      MongoRel child = (MongoRel)this.getInput();
      BatchSchema childSchema = child.getSchema(context);
      ParseContext parseContext = new ParseContext(PrelUtil.getSettings(this.getCluster()));
      return ExpressionTreeMaterializer.materializeFields(this.getProjectExpressions(parseContext), childSchema, context).setSelectionVectorMode(childSchema.getSelectionVectorMode()).build();
   }
}
