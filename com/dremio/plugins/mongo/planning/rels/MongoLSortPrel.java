package com.dremio.plugins.mongo.planning.rels;

import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import java.io.IOException;
import java.util.Iterator;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

public class MongoLSortPrel extends Sort implements Prel {
   public MongoLSortPrel(RelOptCluster cluster, RelTraitSet traits, RelNode child, RelCollation collation) {
      super(cluster, traits, child, collation);
   }

   public Sort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset, RexNode fetch) {
      return new MongoLSortPrel(this.getCluster(), traitSet, newInput, this.collation);
   }

   protected Object clone() throws CloneNotSupportedException {
      return this.copy(this.getTraitSet(), this.getInputs());
   }

   public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
      return planner.getCostFactory().makeInfiniteCost();
   }

   public Iterator<Prel> iterator() {
      throw new UnsupportedOperationException();
   }

   public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
      throw new UnsupportedOperationException();
   }

   public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
      throw new UnsupportedOperationException();
   }

   public SelectionVectorMode[] getSupportedEncodings() {
      throw new UnsupportedOperationException();
   }

   public SelectionVectorMode getEncoding() {
      throw new UnsupportedOperationException();
   }

   public boolean needsFinalColumnReordering() {
      throw new UnsupportedOperationException();
   }
}
