package com.dremio.plugins.mongo.planning.rules;

import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.physical.FilterPrel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;

public class CollationFilterChecker extends StatelessRelShuttleImpl {
   private boolean hasCollationFilter = false;

   public RelNode visit(LogicalFilter filter) {
      if (null == filter.getCondition().accept(new CollationFilterChecker.CollationRexFilterChecker())) {
         this.hasCollationFilter = true;
         return null;
      } else {
         return super.visit(filter);
      }
   }

   public RelNode visit(RelNode other) {
      if (other instanceof FilterPrel && null == ((FilterPrel)other).getCondition().accept(new CollationFilterChecker.CollationRexFilterChecker())) {
         this.hasCollationFilter = true;
         return null;
      } else {
         return super.visit(other);
      }
   }

   public boolean hasCollationFilter() {
      return this.hasCollationFilter;
   }

   public static boolean hasCollationFilter(RelNode node) {
      CollationFilterChecker checker = new CollationFilterChecker();
      node.accept(checker);
      return checker.hasCollationFilter();
   }

   static boolean hasCollationFilter(RexNode rexCall, RexNode rexLiteral) {
      return checkForCollationCast(rexCall) && rexLiteral instanceof RexLiteral && rexLiteral.getType().getSqlTypeName().getFamily() == SqlTypeFamily.NUMERIC;
   }

   public static boolean checkForCollationCast(RexNode node) {
      if (node instanceof RexCall) {
         RexCall call = (RexCall)node;
         if (call.getOperator().getKind() == SqlKind.CAST && call.getType().getSqlTypeName().getFamily() == SqlTypeFamily.NUMERIC) {
            return ((RexNode)call.getOperands().get(0)).getType().getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER;
         }
      }

      return false;
   }

   public static class CollationRexFilterChecker extends RexShuttle {
      private boolean hasCollationFilter = false;

      public RexNode visitCall(RexCall call) {
         switch(call.getOperator().getKind()) {
         case AND:
            if (this.visitCall((RexCall)call.operands.get(0)) == null && this.visitCall((RexCall)call.operands.get(1)) == null) {
               this.hasCollationFilter = true;
               return null;
            }
            break;
         case GREATER_THAN:
         case GREATER_THAN_OR_EQUAL:
         case LESS_THAN:
         case LESS_THAN_OR_EQUAL:
         case EQUALS:
         case NOT_EQUALS:
            boolean hasFilter = false;
            if (call.operands.get(0) instanceof RexCall && call.operands.get(1) instanceof RexLiteral) {
               hasFilter = CollationFilterChecker.hasCollationFilter((RexNode)call.operands.get(0), (RexNode)call.operands.get(1));
            } else if (call.operands.get(1) instanceof RexCall && call.operands.get(0) instanceof RexLiteral) {
               hasFilter = CollationFilterChecker.hasCollationFilter((RexNode)call.operands.get(1), (RexNode)call.operands.get(0));
            }

            if (hasFilter) {
               this.hasCollationFilter = true;
               return null;
            }
         }

         this.hasCollationFilter = false;
         return super.visitCall(call);
      }

      boolean hasCollationFilter() {
         return this.hasCollationFilter;
      }
   }
}
