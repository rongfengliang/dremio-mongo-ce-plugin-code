package com.dremio.plugins.mongo.planning;

import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.store.StoragePluginRulesFactory.StoragePluginTypeRulesFactory;
import com.dremio.plugins.mongo.planning.rules.MongoFilterRule;
import com.dremio.plugins.mongo.planning.rules.MongoLogicalSortRule;
import com.dremio.plugins.mongo.planning.rules.MongoProjectRule;
import com.dremio.plugins.mongo.planning.rules.MongoScanDrule;
import com.dremio.plugins.mongo.planning.rules.MongoScanPrule;
import com.dremio.plugins.mongo.planning.rules.RuleWithOption;
import com.google.common.base.Functions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.calcite.plan.RelOptRule;

public class MongoRulesFactory extends StoragePluginTypeRulesFactory {
   private static final ImmutableList<RuleWithOption> PHYSICAL_RULES;

   public Set<RelOptRule> getRules(OptimizerRulesContext context, PlannerPhase phase, SourceType type) {
      RuleWithOption.OptionPredicate predicate = new RuleWithOption.OptionPredicate(context.getPlannerSettings().getOptions());
      switch(phase) {
      case LOGICAL:
         return ImmutableSet.of(MongoScanDrule.INSTANCE);
      case PHYSICAL:
         return FluentIterable.from(PHYSICAL_RULES).filter(predicate).transform(Functions.identity()).append(new RelOptRule[]{new MongoScanPrule(context.getFunctionRegistry())}).toSet();
      default:
         return ImmutableSet.of();
      }
   }

   static {
      PHYSICAL_RULES = ImmutableList.of(MongoFilterRule.INSTANCE, MongoProjectRule.INSTANCE, MongoLogicalSortRule.INSTANCE);
   }
}
