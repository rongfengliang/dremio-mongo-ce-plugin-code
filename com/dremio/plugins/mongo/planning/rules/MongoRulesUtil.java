package com.dremio.plugins.mongo.planning.rules;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

public final class MongoRulesUtil {
   private MongoRulesUtil() {
   }

   public static boolean isInputRef(RexNode node) {
      if (node instanceof RexInputRef) {
         return true;
      } else if (node instanceof RexFieldAccess) {
         return isInputRef(((RexFieldAccess)node).getReferenceExpr());
      } else {
         if (node instanceof RexCall) {
            RexCall call = (RexCall)node;
            if (call.getOperator() == SqlStdOperatorTable.ITEM) {
               List<RexNode> operands = call.getOperands();
               if (isInputRef((RexNode)operands.get(0)) && isStringLiteral((RexNode)operands.get(1)) || isInputRef((RexNode)operands.get(1)) && isStringLiteral((RexNode)operands.get(0))) {
                  return true;
               }
            }
         }

         return false;
      }
   }

   public static boolean isLiteral(RexNode node) {
      return node instanceof RexLiteral;
   }

   public static boolean isStringLiteral(RexNode node) {
      return node instanceof RexLiteral && node.getType().getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER;
   }

   public static String getCompoundFieldName(RexNode node, List<RelDataTypeField> fields) {
      if (node instanceof RexInputRef) {
         int index = ((RexInputRef)node).getIndex();
         RelDataTypeField field = (RelDataTypeField)fields.get(index);
         return field.getName();
      } else if (node instanceof RexFieldAccess) {
         RexFieldAccess rexFieldAccess = (RexFieldAccess)node;
         return getCompoundFieldName(rexFieldAccess.getReferenceExpr(), fields) + "." + rexFieldAccess.getField().getName();
      } else {
         if (node instanceof RexCall) {
            RexCall call = (RexCall)node;
            if (call.getOperator() == SqlStdOperatorTable.ITEM) {
               List<RexNode> operands = call.getOperands();
               if (isInputRef((RexNode)operands.get(0)) && isLiteral((RexNode)operands.get(1))) {
                  return getCompoundFieldName((RexNode)operands.get(0), fields) + "." + RexLiteral.stringValue((RexNode)operands.get(1));
               }

               if (isInputRef((RexNode)operands.get(1)) && isLiteral((RexNode)operands.get(0))) {
                  return getCompoundFieldName((RexNode)operands.get(1), fields) + "." + RexLiteral.stringValue((RexNode)operands.get(0));
               }
            } else if (call.getOperator() == SqlStdOperatorTable.CAST) {
               return getCompoundFieldName((RexNode)call.getOperands().get(0), fields);
            }
         }

         throw new RuntimeException("Unexpected RexNode received: " + node);
      }
   }

   static boolean checkForUnneededCast(RexNode expr) {
      if (expr instanceof RexCall) {
         RexCall arg2Call = (RexCall)expr;
         RelDataType inputType = ((RexNode)arg2Call.getOperands().get(0)).getType();
         if (arg2Call.getOperator().getName().equalsIgnoreCase("CAST") && (arg2Call.getType().equals(inputType) || inputType.getSqlTypeName().equals(SqlTypeName.ANY) || arg2Call.getType().getSqlTypeName().equals(SqlTypeName.ANY) || arg2Call.getType().getFamily().equals(inputType.getFamily()))) {
            return true;
         }
      }

      return false;
   }

   static boolean isSupportedCast(RexNode node) {
      return checkForUnneededCast(node) || CollationFilterChecker.checkForCollationCast(node);
   }
}
