package com.dremio.plugins.mongo.planning.rules;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.record.BatchSchema;
import com.dremio.plugins.mongo.MongoStoragePlugin;
import com.dremio.plugins.mongo.planning.MongoConvention;
import com.dremio.plugins.mongo.planning.rels.MongoFilter;
import com.dremio.plugins.mongo.planning.rels.MongoIntermediatePrel;
import com.dremio.plugins.mongo.planning.rels.MongoRel;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;

public class MongoFilterRule extends AbstractMongoConverterRule<FilterPrel> {
   public static final MongoFilterRule INSTANCE = new MongoFilterRule();

   private MongoFilterRule() {
      super(FilterPrel.class, "MongoFilterRule", ExecConstants.MONGO_RULES_FILTER, false);
   }

   public MongoRel convert(RelOptRuleCall call, FilterPrel filter, StoragePluginId pluginId, RelNode inputToFilterRel) {
      SourceCapabilities capabilities = pluginId.getCapabilities();
      boolean isMongo3_6Enabled = capabilities.getCapability(MongoStoragePlugin.MONGO_3_6_FEATURES);
      BatchSchema schema = ((MongoIntermediatePrel)call.rel(1)).getScan().getBatchSchema();
      if (isMongo3_6Enabled) {
         try {
            FindQueryGenerator generator = new FindQueryGenerator(schema, filter.getRowType());
            filter.getCondition().accept(generator);
            return new MongoFilter(filter.getTraitSet().replace(MongoConvention.INSTANCE), inputToFilterRel, filter.getCondition(), generator.needsCollation);
         } catch (Exception var14) {
            return null;
         }
      } else {
         boolean isMongo3_2Enabled = capabilities.getCapability(MongoStoragePlugin.MONGO_3_2_FEATURES);
         boolean isMongo3_4Enabled = capabilities.getCapability(MongoStoragePlugin.MONGO_3_4_FEATURES);
         ProjectExtractor projectExtractor = new ProjectExtractor(filter.getRowType(), filter.getCluster().getTypeFactory(), isMongo3_2Enabled, isMongo3_4Enabled);
         RexNode newFilterCond = (RexNode)filter.getCondition().accept(projectExtractor);
         MatchExpressionConverter expressionConverter = new MatchExpressionConverter(schema, projectExtractor.getNewRecordType(), isMongo3_2Enabled, isMongo3_4Enabled);
         newFilterCond.accept(expressionConverter);
         boolean needsCollation;
         if (isMongo3_4Enabled) {
            needsCollation = expressionConverter.needsCollation();
         } else {
            needsCollation = false;
         }

         return projectExtractor.hasNewProjects() ? null : new MongoFilter(filter.getTraitSet().replace(MongoConvention.INSTANCE), inputToFilterRel, newFilterCond, needsCollation);
      }
   }

   public boolean matches(RelOptRuleCall call) {
      MongoIntermediatePrel prel = (MongoIntermediatePrel)call.rel(1);
      SourceCapabilities capabilities = prel.getPluginId().getCapabilities();
      boolean isMongo3_6Enabled = capabilities.getCapability(MongoStoragePlugin.MONGO_3_6_FEATURES);
      FilterPrel filter = (FilterPrel)call.rel(0);
      BatchSchema schema = prel.getScan().getBatchSchema();
      if (isMongo3_6Enabled) {
         try {
            filter.getCondition().accept(new FindQueryGenerator(schema, filter.getRowType()));
            return true;
         } catch (Exception var11) {
            return false;
         }
      } else {
         boolean isMongo3_2Enabled = capabilities.getCapability(MongoStoragePlugin.MONGO_3_2_FEATURES);
         boolean isMongo3_4Enabled = capabilities.getCapability(MongoStoragePlugin.MONGO_3_4_FEATURES);

         try {
            ProjectExtractor projectExtractor = new ProjectExtractor(filter.getRowType(), filter.getCluster().getTypeFactory(), isMongo3_2Enabled, isMongo3_4Enabled);
            if (projectExtractor.hasNewProjects()) {
               return false;
            } else {
               RexNode rexNode = (RexNode)filter.getCondition().accept(projectExtractor);
               rexNode.accept(new MatchExpressionConverter(schema, projectExtractor.getNewRecordType(), isMongo3_2Enabled, isMongo3_4Enabled));
               return true;
            }
         } catch (Exception var12) {
            return false;
         }
      }
   }
}
