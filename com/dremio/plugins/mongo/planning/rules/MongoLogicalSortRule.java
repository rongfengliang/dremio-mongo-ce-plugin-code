package com.dremio.plugins.mongo.planning.rules;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.logical.SortRel;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.options.OptionManager;
import com.dremio.plugins.mongo.planning.rels.MongoLSortPrel;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;

public class MongoLogicalSortRule extends RuleWithOption {
   public static final MongoLogicalSortRule INSTANCE = new MongoLogicalSortRule();

   public MongoLogicalSortRule() {
      super(operand(SortRel.class, any()), "MongoLogicalSort");
   }

   public boolean matches(RelOptRuleCall call) {
      SortRel sort = (SortRel)call.rel(0);
      return CollationFilterChecker.hasCollationFilter(sort) ? false : AbstractMongoConverterRule.sortAllowed(sort.getCollation());
   }

   public void onMatch(RelOptRuleCall call) {
      SortRel sort = (SortRel)call.rel(0);
      RelNode newInput = convert(sort.getInput(), sort.getInput().getTraitSet().replace(Prel.PHYSICAL).simplify());
      MongoLSortPrel newSort = new MongoLSortPrel(sort.getCluster(), newInput.getTraitSet().replace(sort.getCollation()).replace(DistributionTrait.SINGLETON), newInput, sort.getCollation());
      call.transformTo(newSort);
   }

   public boolean isEnabled(OptionManager options) {
      return options.getOption(ExecConstants.MONGO_RULES_SORT);
   }
}
