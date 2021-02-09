package com.dremio.plugins.mongo.planning.rules;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.expr.fn.impl.RegexpUtil;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.plugins.mongo.planning.MongoFunctions;
import com.dremio.plugins.mongo.planning.MongoPipelineOperators;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FindQueryGenerator extends RexVisitorImpl<Document> {
   private static final Logger logger = LoggerFactory.getLogger(FindQueryGenerator.class);
   public static final String UNSUPPORTED_REX_NODE_ERROR = "Cannot convert RexNode to equivalent MongoDB match expression.";
   protected final RelDataType recordType;
   protected final BatchSchema schema;
   protected CompleteType leftFieldType;
   protected boolean needsCollation = false;

   public FindQueryGenerator(BatchSchema schema, RelDataType recordType) {
      super(true);
      this.schema = schema;
      this.recordType = recordType;
      this.leftFieldType = null;
   }

   public Document visitInputRef(RexInputRef inputRef) {
      return this.visitUnknown(inputRef);
   }

   public Document visitLiteral(RexLiteral literal) {
      return this.visitUnknown(literal);
   }

   public Document visitCall(RexCall call) {
      String funcName = call.getOperator().getName();
      MongoFunctions operator = MongoFunctions.getMongoOperator(funcName);
      switch(operator) {
      case NOT:
         return this.handleNotFunction(call, funcName);
      case AND:
      case OR:
         return this.handleGenericFunction(call, funcName);
      case EQUAL:
      case NOT_EQUAL:
      case GREATER:
      case GREATER_OR_EQUAL:
      case LESS:
      case LESS_OR_EQUAL:
         return this.handleFindQueryComparison(call, funcName);
      case IFNULL:
         return this.handleIsNullFunction(call, true);
      case IFNOTNULL:
         return this.handleIsNullFunction(call, false);
      case REGEX:
         return this.handleLikeFunction(call);
      default:
         return this.getNodeAsAggregateExprDoc(call);
      }
   }

   public Document visitFieldAccess(RexFieldAccess fieldAccess) {
      return this.visitUnknown(fieldAccess);
   }

   private Document handleFindQueryComparison(RexCall call, String funcName) {
      List<RexNode> operands = call.getOperands();
      Preconditions.checkArgument(operands.size() == 2);
      RexNode firstOp = (RexNode)operands.get(0);
      RexNode secondOp = (RexNode)operands.get(1);
      if (secondOp instanceof RexLiteral && (MongoFunctions.getMongoOperator(funcName) == MongoFunctions.EQUAL || MongoFunctions.getMongoOperator(funcName) == MongoFunctions.NOT_EQUAL) && ((RexLiteral)secondOp).getValue() == null) {
         throw new RuntimeException("Equality comparison operators are invalid when used with NULL, use operator IS NULL or IS NOT NULL instead.");
      } else if (!MongoRulesUtil.isInputRef(firstOp) && !MongoRulesUtil.isSupportedCast(firstOp) && !MongoRulesUtil.isInputRef(secondOp) && !MongoRulesUtil.isSupportedCast(secondOp)) {
         return this.getNodeAsAggregateExprDoc(call);
      } else {
         boolean firstArgInputRef = MongoRulesUtil.isInputRef(firstOp) || MongoRulesUtil.isSupportedCast(firstOp);

         RexNode colRef;
         RexNode otherArgValue;
         Object otherArg;
         label156: {
            Document var10;
            try {
               if (!firstArgInputRef) {
                  colRef = secondOp;
                  otherArgValue = firstOp;
               } else {
                  colRef = firstOp;
                  otherArgValue = secondOp;
               }

               this.leftFieldType = this.getFieldType(colRef);
               if (otherArgValue instanceof RexLiteral) {
                  otherArg = RexToFilterDocumentUtils.getMongoFormattedLiteral((RexLiteral)otherArgValue, this.leftFieldType);
                  break label156;
               }

               var10 = this.getNodeAsAggregateExprDoc(call);
            } finally {
               this.leftFieldType = null;
            }

            return var10;
         }

         MongoFunctions mongoOperator = MongoFunctions.getMongoOperator(funcName);
         if (mongoOperator == null) {
            throw new RuntimeException("Encountered a function that cannot be converted to MongoOperator, " + funcName);
         } else {
            Preconditions.checkState(mongoOperator.canUseInStage(MongoPipelineOperators.MATCH));
            String mongoFunction;
            if (!firstArgInputRef) {
               if (!mongoOperator.canFlip()) {
                  throw new RuntimeException("Encountered a function that cannot swap the left and right operands, " + funcName);
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
            Document filter = new Document(compoundFieldName, new Document(mongoFunction, otherArg));
            return addNotNullCheck(filter, compoundFieldName);
         }
      }
   }

   private Document handleLikeFunction(RexCall call) {
      List<RexNode> operands = call.getOperands();

      assert operands.size() == 2;

      RexNode firstOp = (RexNode)operands.get(0);
      if (MongoRulesUtil.isInputRef(firstOp)) {
         RexLiteral secondOp = (RexLiteral)operands.get(1);
         String sqlRegex = secondOp.toString();
         sqlRegex = sqlRegex.substring(1, sqlRegex.length() - 1);
         String regex = RegexpUtil.sqlToRegexLike(sqlRegex);
         String compoundFieldName = MongoRulesUtil.getCompoundFieldName(firstOp, this.recordType.getFieldList());
         return new Document(compoundFieldName, new Document(MongoFunctions.REGEX.getMongoOperator(), regex));
      } else {
         return this.visitUnknown(call);
      }
   }

   protected Document visitUnknown(RexNode o) {
      throw new RuntimeException(String.format("Cannot convert RexNode to equivalent MongoDB match expression. RexNode Class: %s, RexNode Digest: %s", o.getClass().getName(), o.toString()));
   }

   protected CompleteType getFieldType(RexNode curNode) {
      if (this.schema == null) {
         return null;
      } else {
         String compoundName = MongoRulesUtil.getCompoundFieldName(curNode, this.recordType.getFieldList());
         SchemaPath compoundPath = SchemaPath.getCompoundPath(compoundName.split("\\."));
         TypedFieldId fieldId = this.schema.getFieldId(compoundPath);
         return null != fieldId ? fieldId.getFinalType() : null;
      }
   }

   protected Document handleNotFunction(RexCall call, String functionName) {
      return ((RexNode)call.getOperands().get(0)).getKind().equals(SqlKind.LIKE) ? this.visitUnknown(call) : this.handleGenericFunction(call, functionName);
   }

   protected Document handleGenericFunction(RexCall call, String functionName) {
      String funcName = MongoFunctions.convertToMongoFunction(functionName);
      Object[] args = new Object[call.getOperands().size()];

      for(int i = 0; i < call.getOperands().size(); ++i) {
         args[i] = this.formatArgForMongoOperator((RexNode)call.getOperands().get(i));
      }

      return RexToFilterDocumentUtils.constructOperatorDocument(funcName, args);
   }

   private Object formatArgForMongoOperator(RexNode arg) {
      if (arg instanceof RexInputRef) {
         int columnRefIndex = ((RexInputRef)arg).getIndex();
         RelDataTypeField field = (RelDataTypeField)this.recordType.getFieldList().get(columnRefIndex);
         Preconditions.checkArgument(field.getType().getSqlTypeName() == SqlTypeName.BOOLEAN);
         return new Document(field.getName(), true);
      } else {
         try {
            return arg.accept(this);
         } catch (RuntimeException var4) {
            return this.getNodeAsAggregateExprDoc(arg);
         }
      }
   }

   private Document getNodeAsAggregateExprDoc(RexNode node) {
      AggregateExpressionGenerator aggExprGenerator = new AggregateExpressionGenerator(this.schema, this.recordType);
      Document result = new Document("$expr", node.accept(aggExprGenerator));
      if (aggExprGenerator.needsCollation()) {
         this.needsCollation = true;
      }

      return result;
   }

   private static boolean isCollationEquality(RexCall call, RexNode colRef, RexNode otherValue) {
      return call.isA(SqlKind.EQUALS) && otherValue instanceof RexLiteral && colRef.getType().getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER && otherValue.getType().getSqlTypeName().getFamily() == SqlTypeFamily.NUMERIC;
   }

   private static Document addNotNullCheck(Document filter, String mongoFormattedColReference) {
      Document notNullFilter = new Document(mongoFormattedColReference, new Document(MongoFunctions.NOT_EQUAL.getMongoOperator(), (Object)null));
      return new Document(MongoFunctions.AND.getMongoOperator(), Arrays.asList(notNullFilter, filter));
   }

   private Document handleIsNullFunction(RexCall call, boolean isNull) {
      RexNode firstOp = (RexNode)call.getOperands().get(0);
      if (MongoRulesUtil.isInputRef(firstOp)) {
         String mongoFunction = isNull ? MongoFunctions.EQUAL.getMongoOperator() : MongoFunctions.NOT_EQUAL.getMongoOperator();
         String compoundFieldName = MongoRulesUtil.getCompoundFieldName(firstOp, this.recordType.getFieldList());
         return new Document(compoundFieldName, new Document(mongoFunction, (Object)null));
      } else {
         return this.getNodeAsAggregateExprDoc(call);
      }
   }
}
