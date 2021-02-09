package com.dremio.plugins.mongo.planning.rules;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.options.OptionManager;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.plugins.mongo.planning.MongoConvention;
import com.dremio.plugins.mongo.planning.rels.MongoIntermediatePrel;
import com.dremio.plugins.mongo.planning.rels.MongoRel;
import java.util.Iterator;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractMongoConverterRule<R extends RelNode> extends RuleWithOption {
   protected final Logger logger;
   private final BooleanValidator isRuleEnabledValidator;
   private final boolean checkCollationFilter;

   protected AbstractMongoConverterRule(Class<R> clazz, String description, BooleanValidator isRuleEnabledValidator, boolean checkCollationFilter) {
      this(clazz, Prel.PHYSICAL, description, isRuleEnabledValidator, checkCollationFilter);
   }

   protected AbstractMongoConverterRule(Class<R> clazz, Convention inputConvention, String description, BooleanValidator isRuleEnabledValidator, boolean checkCollationFilter) {
      super(operand(clazz, inputConvention, some(operand(MongoIntermediatePrel.class, any()), new RelOptRuleOperand[0])), description);
      this.logger = LoggerFactory.getLogger(this.getClass());
      this.isRuleEnabledValidator = isRuleEnabledValidator;
      this.checkCollationFilter = checkCollationFilter;
   }

   static boolean sortAllowed(RelCollation collation) {
      Iterator var1 = collation.getFieldCollations().iterator();

      RelFieldCollation c;
      do {
         if (!var1.hasNext()) {
            return true;
         }

         c = (RelFieldCollation)var1.next();
      } while((c.direction != Direction.ASCENDING || c.nullDirection != NullDirection.LAST) && (c.direction != Direction.DESCENDING || c.nullDirection != NullDirection.FIRST) && c.direction != Direction.CLUSTERED);

      return false;
   }

   public static RelTraitSet withMongo(RelNode node) {
      return node.getTraitSet().replace(MongoConvention.INSTANCE);
   }

   public void onMatch(RelOptRuleCall call) {
      R main = call.rel(0);
      if (!this.checkCollationFilter || !CollationFilterChecker.hasCollationFilter(main)) {
         MongoIntermediatePrel child = (MongoIntermediatePrel)call.rel(1);
         MongoRel rel = this.convert(call, main, child.getPluginId(), child.getInput());
         if (rel != null) {
            call.transformTo(child.withNewInput(rel));
         }

      }
   }

   public abstract MongoRel convert(RelOptRuleCall var1, R var2, StoragePluginId var3, RelNode var4);

   public boolean isEnabled(OptionManager options) {
      return options == null ? true : options.getOption(this.isRuleEnabledValidator);
   }
}
