package com.dremio.plugins.mongo.planning.rules;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.expr.fn.impl.RegexpUtil;
import com.dremio.exec.record.BatchSchema;
import com.dremio.plugins.mongo.planning.MongoFunctions;
import com.dremio.plugins.mongo.planning.MongoPipelineOperators;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @deprecated */
@Deprecated
public class MatchExpressionConverter extends ProjectExpressionConverter {
   private static final Logger logger = LoggerFactory.getLogger(MatchExpressionConverter.class);
   public static final String UNSUPPORTED_REX_NODE_ERROR = "Cannot convert RexNode to equivalent MongoDB match expression.";

   public MatchExpressionConverter(BatchSchema schema, RelDataType dataType, boolean mongo3_2enabled, boolean mongo3_4enabled) {
      super(schema, dataType, mongo3_2enabled, mongo3_4enabled);
   }

   public Document visitInputRef(RexInputRef inputRef) {
      int index = inputRef.getIndex();
      RelDataTypeField field = (RelDataTypeField)this.recordType.getFieldList().get(index);
      return new Document(field.getName(), true);
   }

   public Object visitLiteral(RexLiteral literal) {
      return this.getLiteralDocument(literal).get(MongoFunctions.LITERAL.getMongoOperator());
   }

   public Object visitCall(RexCall call) {
      String funcName = call.getOperator().getName().toLowerCase();
      MongoFunctions operator = MongoFunctions.getMongoOperator(funcName);
      if (!operator.canUseInStage(MongoPipelineOperators.MATCH)) {
         throw UserException.planError().message("Found mongo operator " + operator.getMongoOperator() + ", which cannot be used in mongo pipeline, " + MongoPipelineOperators.MATCH.getOperator(), new Object[0]).build(logger);
      } else {
         switch(operator) {
         case AND:
         case OR:
            return this.handleGenericFunction(call, funcName);
         case NOT:
         case EQUAL:
         case NOT_EQUAL:
         case GREATER:
         case GREATER_OR_EQUAL:
         case LESS:
         case LESS_OR_EQUAL:
            return this.handleGenericMatchFunction(call, funcName);
         case REGEX:
            return this.handleLikeFunction(call);
         default:
            return this.visitUnknown(call);
         }
      }
   }

   public static boolean checkForUnneededCast(RexNode expr) {
      if (expr instanceof RexCall) {
         RexCall arg2Call = (RexCall)expr;
         RelDataType inputType = ((RexNode)arg2Call.getOperands().get(0)).getType();
         if (arg2Call.getOperator().getName().equalsIgnoreCase("CAST") && (arg2Call.getType().equals(inputType) || inputType.getSqlTypeName().equals(SqlTypeName.ANY) || arg2Call.getType().getSqlTypeName().equals(SqlTypeName.ANY) || arg2Call.getType().getFamily().equals(inputType.getFamily()))) {
            return true;
         }
      }

      return false;
   }

   private Object handleGenericMatchFunction(RexCall call, String funcName) {
      List<RexNode> operands = call.getOperands();

      assert operands.size() == 2;

      assert MongoRulesUtil.isInputRef((RexNode)operands.get(0)) || MongoRulesUtil.isInputRef((RexNode)operands.get(1)) || MongoRulesUtil.isSupportedCast((RexNode)operands.get(0)) || MongoRulesUtil.isSupportedCast((RexNode)operands.get(1));

      boolean firstArgInputRef = MongoRulesUtil.isInputRef((RexNode)operands.get(0)) || MongoRulesUtil.isSupportedCast((RexNode)operands.get(0));

      RexNode colRef;
      RexNode otherArgValue;
      Object otherArg;
      try {
         if (!firstArgInputRef) {
            colRef = (RexNode)operands.get(1);
            otherArgValue = (RexNode)operands.get(0);
         } else {
            colRef = (RexNode)operands.get(0);
            otherArgValue = (RexNode)operands.get(1);
         }

         this.leftFieldType = this.getFieldType(colRef);
         otherArg = otherArgValue.accept(this);
      } finally {
         this.leftFieldType = null;
      }

      MongoFunctions mongoOperator = MongoFunctions.getMongoOperator(funcName);
      if (mongoOperator == null) {
         throw new RuntimeException("Encountered a function that cannot be converted to MongoOperator, " + funcName);
      } else if (!mongoOperator.canUseInStage(MongoPipelineOperators.MATCH)) {
         throw UserException.planError().message("Found mongo operator " + mongoOperator.getMongoOperator() + ", which cannot be used in mongo pipeline, " + MongoPipelineOperators.MATCH.getOperator(), new Object[0]).build(logger);
      } else {
         String mongoFunction;
         if (!firstArgInputRef) {
            if (!mongoOperator.canFlip()) {
               throw new RuntimeException("Encountered a function that cannot swap the left and right operads, " + funcName);
            }

            mongoFunction = mongoOperator.getFlipped();
         } else {
            mongoFunction = mongoOperator.getMongoOperator();
         }

         if (CollationFilterChecker.hasCollationFilter(colRef, otherArgValue) || isCollationEquality(call, colRef, otherArgValue)) {
            otherArg = otherArg.toString();
            this.needsCollation = true;
         }

         String compoundFieldName = MongoRulesUtil.getCompoundFieldName(colRef, this.recordType.getFieldList());
         return new Document(compoundFieldName, new Document(mongoFunction, otherArg));
      }
   }

   private Object handleLikeFunction(RexCall call) {
      List<RexNode> operands = call.getOperands();

      assert operands.size() == 2;

      RexInputRef arg1;
      RexLiteral arg2;
      if (!(operands.get(0) instanceof RexInputRef)) {
         arg2 = (RexLiteral)operands.get(0);
         arg1 = (RexInputRef)operands.get(1);
      } else {
         arg2 = (RexLiteral)operands.get(1);
         arg1 = (RexInputRef)operands.get(0);
      }

      String sqlRegex = arg2.toString();
      sqlRegex = sqlRegex.substring(1, sqlRegex.length() - 1);
      String regex = RegexpUtil.sqlToRegexLike(sqlRegex);
      int index = arg1.getIndex();
      RelDataTypeField field = (RelDataTypeField)this.recordType.getFieldList().get(index);
      return new Document(field.getName(), new Document(MongoFunctions.REGEX.getMongoOperator(), regex.toString()));
   }

   protected String visitUnknown(RexNode o) {
      throw UserException.planError().message("Cannot convert RexNode to equivalent MongoDB match expression.RexNode Class: %s, RexNode Digest: %s", new Object[]{o.getClass().getName(), o.toString()}).build(logger);
   }

   private static boolean isCollationEquality(RexCall call, RexNode colRef, RexNode otherValue) {
      return call.isA(SqlKind.EQUALS) && otherValue instanceof RexLiteral && colRef.getType().getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER && otherValue.getType().getSqlTypeName().getFamily() == SqlTypeFamily.NUMERIC;
   }
}
