package com.dremio.plugins.mongo.planning.rules;

import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.plugins.mongo.planning.MongoConvention;
import com.dremio.plugins.mongo.planning.rels.MongoIntermediatePrel;
import com.dremio.plugins.mongo.planning.rels.MongoIntermediateScanPrel;
import com.dremio.plugins.mongo.planning.rels.MongoScanDrel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;

public class MongoScanPrule extends RelOptRule {
   private final FunctionLookupContext lookupContext;

   public MongoScanPrule(FunctionLookupContext lookupContext) {
      super(RelOptHelper.any(MongoScanDrel.class), "MongoScanPrule");
      this.lookupContext = lookupContext;
   }

   public void onMatch(RelOptRuleCall call) {
      MongoScanDrel logicalScan = (MongoScanDrel)call.rel(0);
      MongoIntermediateScanPrel physicalScan = new MongoIntermediateScanPrel(logicalScan.getCluster(), logicalScan.getTraitSet().replace(MongoConvention.INSTANCE), logicalScan.getTable(), logicalScan.getTableMetadata(), logicalScan.getProjectedColumns(), logicalScan.getObservedRowcountAdjustment());
      RelNode converted = new MongoIntermediatePrel(physicalScan.getTraitSet().replace(Prel.PHYSICAL), physicalScan, this.lookupContext, physicalScan, physicalScan.getPluginId());
      call.transformTo(converted);
   }
}
