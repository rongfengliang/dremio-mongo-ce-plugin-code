package com.dremio.plugins.mongo.planning.rules;

import com.dremio.options.OptionManager;
import com.google.common.base.Predicate;
import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.tools.RelBuilderFactory;

public abstract class RuleWithOption extends RelOptRule {
   public RuleWithOption(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description) {
      super(operand, relBuilderFactory, description);
   }

   public RuleWithOption(RelOptRuleOperand operand, String description) {
      super(operand, description);
   }

   public RuleWithOption(RelOptRuleOperand operand) {
      super(operand);
   }

   public abstract boolean isEnabled(OptionManager var1);

   public static class DelegatingRuleWithOption extends RuleWithOption {
      private final RelOptRule delegate;
      private final Predicate<OptionManager> predicate;

      public DelegatingRuleWithOption(RelOptRule delegate, Predicate<OptionManager> predicate) {
         super(delegate.getOperand(), delegate.relBuilderFactory, delegate.toString());
         this.delegate = delegate;
         this.predicate = predicate;
      }

      public boolean isEnabled(OptionManager options) {
         return this.predicate.apply(options);
      }

      public void onMatch(RelOptRuleCall call) {
         this.delegate.onMatch(call);
      }

      public RelOptRuleOperand getOperand() {
         return this.delegate.getOperand();
      }

      public List<RelOptRuleOperand> getOperands() {
         return this.delegate.getOperands();
      }

      public int hashCode() {
         return this.delegate.hashCode();
      }

      public boolean equals(Object obj) {
         return this.delegate.equals(obj);
      }

      protected boolean equals(RelOptRule that) {
         return this.delegate.equals(that);
      }

      public boolean matches(RelOptRuleCall call) {
         return this.delegate.matches(call);
      }

      public Convention getOutConvention() {
         return this.delegate.getOutConvention();
      }

      public RelTrait getOutTrait() {
         return this.delegate.getOutTrait();
      }
   }

   public static class OptionPredicate implements Predicate<RuleWithOption> {
      private final OptionManager options;

      public OptionPredicate(OptionManager options) {
         this.options = options;
      }

      public boolean apply(RuleWithOption input) {
         return input.isEnabled(this.options);
      }
   }
}
