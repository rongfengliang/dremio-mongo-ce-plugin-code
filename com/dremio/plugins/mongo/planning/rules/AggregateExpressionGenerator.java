package com.dremio.plugins.mongo.planning.rules;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.plugins.mongo.planning.MongoFunctions;
import com.dremio.plugins.mongo.planning.rels.MongoColumnNameSanitizer;
import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.Pair;
import org.bson.Document;

public class AggregateExpressionGenerator extends RexVisitorImpl<Document> {
   public static final String UNSUPPORTED_REX_NODE_ERROR = "Cannot convert RexNode to equivalent MongoDB project expression.";
   private static final Document NULL_LITERAL_EXPR;
   protected final RelDataType recordType;
   protected final BatchSchema schema;
   protected CompleteType leftFieldType;
   protected boolean needsCollation = false;

   public AggregateExpressionGenerator(BatchSchema schema, RelDataType recordType) {
      super(true);
      this.schema = schema;
      this.recordType = recordType;
      this.leftFieldType = null;
   }

   public boolean needsCollation() {
      return this.needsCollation;
   }

   public Document visitInputRef(RexInputRef inputRef) {
      return this.visitUnknown(inputRef);
   }

   public Document visitLiteral(RexLiteral literal) {
      return this.visitUnknown(literal);
   }

   public Document visitCall(RexCall call) {
      String funcName = call.getOperator().getName();
      MongoFunctions mongoFuncName = MongoFunctions.getMongoOperator(funcName);
      switch(mongoFuncName) {
      case EXTRACT:
         return this.handleExtractFunction(call);
      case IFNULL:
         return this.handleIsNullFunction(call, false);
      case IFNOTNULL:
         return this.handleIsNullFunction(call, true);
      case SUBSTR:
         return this.handleSubStrFunction(call);
      case TRUNC:
         return this.handleTruncFunction(call);
      case CASE:
         return this.handleCaseFunction(call);
      case LOG:
         if (call.getOperands().size() == 1) {
            return this.handleGenericFunction(call, "ln");
         }

         return this.handleGenericFunction(call, funcName);
      case ITEM:
         return this.visitUnknown(call);
      default:
         return this.handleGenericFunction(call, funcName);
      }
   }

   protected Document handleGenericFunction(RexCall call, String functionName) {
      String funcName = MongoFunctions.convertToMongoFunction(functionName);
      boolean handleNull = false;
      Object[] args;
      Document doc;
      switch(call.op.kind) {
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
      case NOT_EQUALS:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
      case EQUALS:
         handleNull = true;
         args = null;
         if (call.operands.get(0) instanceof RexCall && call.operands.get(1) instanceof RexLiteral) {
            args = this.getCollationFilterArgs((RexCall)call.operands.get(0), (RexLiteral)call.operands.get(1));
         } else if (call.operands.get(1) instanceof RexCall && call.operands.get(0) instanceof RexLiteral) {
            args = this.getCollationFilterArgs((RexCall)call.operands.get(1), (RexLiteral)call.operands.get(0));
         }

         if (args != null) {
            this.needsCollation = true;
            doc = RexToFilterDocumentUtils.constructOperatorDocument(funcName, args);
            return addNotNullCheck(doc, args[0]);
         }
      default:
         args = call.getOperands().stream().map(this::formatArgForMongoOperator).toArray();
         doc = RexToFilterDocumentUtils.constructOperatorDocument(funcName, args);
         if (handleNull) {
            Preconditions.checkArgument(args.length > 1, "Null check only needed for operators with at least one argument.");
            return addNotNullCheck(doc, args[0]);
         } else {
            return doc;
         }
      }
   }

   private Object[] getCollationFilterArgs(RexCall rexCall, RexLiteral rexLiteral) {
      return CollationFilterChecker.hasCollationFilter(rexCall, rexLiteral) ? new Object[]{this.formatArgForMongoOperator((RexNode)rexCall.getOperands().get(0)), rexLiteral.getValue3().toString()} : null;
   }

   private Object handleCastFunction(RexCall call) {
      Preconditions.checkArgument(call.getOperands().size() == 1, "Cast expects a single argument");
      if (MongoRulesUtil.checkForUnneededCast(call)) {
         return this.formatArgForMongoOperator((RexNode)call.getOperands().get(0));
      } else {
         throw new RuntimeException("Cannot handle anything but unnecessary casts");
      }
   }

   private Document handleExtractFunction(RexCall call) {
      String unit = ((RexLiteral)call.getOperands().get(0)).getValue().toString().toLowerCase();
      String funcName = MongoFunctions.convertToMongoFunction("extract_" + unit);
      Object arg = this.formatArgForMongoOperator((RexNode)call.getOperands().get(1));
      Document extractFn = RexToFilterDocumentUtils.constructOperatorDocument(funcName, arg);
      Document isNotNullExpr = RexToFilterDocumentUtils.constructOperatorDocument(MongoFunctions.NOT_EQUAL.getMongoOperator(), arg, NULL_LITERAL_EXPR);
      return RexToFilterDocumentUtils.constructOperatorDocument(MongoFunctions.CASE.getMongoOperator(), isNotNullExpr, extractFn, NULL_LITERAL_EXPR);
   }

   private Document handleIsNullFunction(RexCall call, boolean reverse) {
      return RexToFilterDocumentUtils.constructOperatorDocument(reverse ? MongoFunctions.GREATER.getMongoOperator() : MongoFunctions.LESS_OR_EQUAL.getMongoOperator(), this.formatArgForMongoOperator((RexNode)call.getOperands().get(0)), NULL_LITERAL_EXPR);
   }

   private Document handleSubStrFunction(RexCall call) {
      List<RexNode> operands = call.getOperands();
      Object stringToSubstr = this.formatArgForMongoOperator((RexNode)operands.get(0));
      Object zeroBasedStartIndex = RexToFilterDocumentUtils.constructOperatorDocument(MongoFunctions.MAX.getMongoOperator(), RexToFilterDocumentUtils.constructOperatorDocument(MongoFunctions.SUBTRACT.getMongoOperator(), this.formatArgForMongoOperator((RexNode)operands.get(1)), 1), 0);
      Object numberOfChars;
      if (operands.size() > 2) {
         numberOfChars = this.formatArgForMongoOperator((RexNode)operands.get(2));
      } else {
         numberOfChars = -1;
      }

      return RexToFilterDocumentUtils.constructOperatorDocument(MongoFunctions.SUBSTR.getMongoOperator(), stringToSubstr, zeroBasedStartIndex, numberOfChars);
   }

   private Object handleItemFunctionRoot(RexCall call) {
      Pair<Object, Boolean> result = this.handleItemFunctionRecursive(call);
      return (Boolean)result.right ? result.left : "$" + result.left;
   }

   private Object handleFieldAccessRoot(RexFieldAccess call) {
      Pair<Object, Boolean> result = this.handleFieldAccessRecursive(call);
      return (Boolean)result.right ? result.left : "$" + result.left;
   }

   private Pair<Object, Boolean> handleItemFunctionRecursive(RexCall call) {
      RexNode leftInput = (RexNode)call.getOperands().get(0);
      boolean hitArrayIndex = false;
      Preconditions.checkArgument(leftInput instanceof RexFieldAccess || leftInput instanceof RexInputRef || leftInput instanceof RexCall);
      Object leftInputObject;
      if (leftInput instanceof RexInputRef) {
         RexInputRef left = (RexInputRef)leftInput;
         leftInputObject = this.recordType.getFieldNames().get(left.getIndex());
      } else if (leftInput instanceof RexFieldAccess) {
         Pair<Object, Boolean> result = this.handleFieldAccessRecursive((RexFieldAccess)leftInput);
         leftInputObject = result.left;
         hitArrayIndex = (Boolean)result.right;
      } else {
         RexCall leftFunc = (RexCall)leftInput;
         Preconditions.checkArgument(leftFunc.getOperator() == SqlStdOperatorTable.ITEM);
         Pair<Object, Boolean> result = this.handleItemFunctionRecursive(leftFunc);
         leftInputObject = result.left;
         hitArrayIndex = (Boolean)result.right;
      }

      RexNode rightInput = (RexNode)call.getOperands().get(1);
      if (!(rightInput instanceof RexLiteral)) {
         throw new RuntimeException("Error converting item expression. The second argument not a literal or varchar or number");
      } else {
         RexLiteral rightLit = (RexLiteral)rightInput;
         switch(rightInput.getType().getSqlTypeName()) {
         case DECIMAL:
         case INTEGER:
            Integer val = ((BigDecimal)rightLit.getValue()).intValue();
            if (hitArrayIndex) {
               assert leftInputObject instanceof Document : "Expected a Document for array selection, but got " + leftInputObject.getClass().getName();

               return new Pair(RexToFilterDocumentUtils.constructOperatorDocument(MongoFunctions.ARRAYELEMAT.getMongoOperator(), leftInputObject, val), true);
            } else {
               assert leftInputObject instanceof String : "Expected a String for array selection, but got " + leftInputObject.getClass().getName();

               return new Pair(RexToFilterDocumentUtils.constructOperatorDocument(MongoFunctions.ARRAYELEMAT.getMongoOperator(), "$" + leftInputObject, val), true);
            }
         case CHAR:
         case VARCHAR:
            if (hitArrayIndex) {
               throw new RuntimeException("Need to implement more than one array index in a complex column reference");
            } else {
               assert leftInputObject instanceof String : "Expected a String for array selection with second argument as varchar";

               String rightLitString = (String)rightLit.getValue2();
               if (rightLitString.startsWith("$")) {
                  throw new RuntimeException("Mongo aggregation pipeline does not support reference to fields that start with '$', field " + rightLitString);
               }

               return new Pair(String.format("%s.%s", leftInputObject, rightLitString), hitArrayIndex);
            }
         default:
            throw new RuntimeException("error converting item expression, second argument not a literal or varchar or number");
         }
      }
   }

   private Pair<Object, Boolean> handleFieldAccessRecursive(RexFieldAccess fieldAccess) {
      RexNode leftInput = fieldAccess.getReferenceExpr();
      boolean hitArrayIndex = false;
      Preconditions.checkArgument(leftInput instanceof RexFieldAccess || leftInput instanceof RexInputRef || leftInput instanceof RexCall);
      Object leftInputObject;
      if (leftInput instanceof RexInputRef) {
         RexInputRef left = (RexInputRef)leftInput;
         leftInputObject = this.recordType.getFieldNames().get(left.getIndex());
      } else if (leftInput instanceof RexFieldAccess) {
         Pair<Object, Boolean> result = this.handleFieldAccessRecursive((RexFieldAccess)leftInput);
         leftInputObject = result.left;
         hitArrayIndex = (Boolean)result.right;
      } else {
         RexCall leftFunc = (RexCall)leftInput;
         Preconditions.checkArgument(leftFunc.getOperator() == SqlStdOperatorTable.ITEM);
         Pair<Object, Boolean> result = this.handleItemFunctionRecursive(leftFunc);
         leftInputObject = result.left;
         hitArrayIndex = (Boolean)result.right;
      }

      RelDataTypeField field = fieldAccess.getField();
      if (hitArrayIndex) {
         throw new RuntimeException("Need to implement more than one array index in a complex column reference");
      } else {
         Preconditions.checkArgument(leftInputObject instanceof String, "Expected a String for array selection with second argument as varchar");
         String rightLitString = field.getName();
         if (rightLitString.startsWith("$")) {
            throw new RuntimeException("Mongo aggregation pipeline does not support reference to fields that start with '$', field " + rightLitString);
         } else {
            return new Pair(String.format("%s.%s", leftInputObject, rightLitString), hitArrayIndex);
         }
      }
   }

   private Document handleTruncFunction(RexCall call) {
      if (call.getOperands().size() > 1) {
         throw new RuntimeException("Method 'trunc(x, y)' is not supported. Only 'trunc(x)' (which strips the decimal part) is supported");
      } else {
         return RexToFilterDocumentUtils.constructOperatorDocument(MongoFunctions.TRUNC.getMongoOperator(), this.formatArgForMongoOperator((RexNode)call.getOperands().get(0)));
      }
   }

   private Document handleCaseFunction(RexCall call) {
      Preconditions.checkArgument(call.getOperands().size() % 2 == 1, "Number of arguments to a case function should be an odd numbered.");
      return (Document)this.handleCaseFunctionHelper(call.getOperands(), 0);
   }

   private Object handleCaseFunctionHelper(List<RexNode> operands, int start) {
      return start == operands.size() - 1 ? this.formatArgForMongoOperator((RexNode)operands.get(start)) : RexToFilterDocumentUtils.constructOperatorDocument(MongoFunctions.CASE.getMongoOperator(), this.formatArgForMongoOperator((RexNode)operands.get(start)), this.formatArgForMongoOperator((RexNode)operands.get(start + 1)), this.handleCaseFunctionHelper(operands, start + 2));
   }

   public Document visitDynamicParam(RexDynamicParam dynamicParam) {
      return this.visitUnknown(dynamicParam);
   }

   public Document visitRangeRef(RexRangeRef rangeRef) {
      return this.visitUnknown(rangeRef);
   }

   public Document visitFieldAccess(RexFieldAccess fieldAccess) {
      return this.visitUnknown(fieldAccess);
   }

   public Document visitLocalRef(RexLocalRef localRef) {
      return this.visitUnknown(localRef);
   }

   public Document visitOver(RexOver over) {
      return this.visitUnknown(over);
   }

   public Document visitCorrelVariable(RexCorrelVariable correlVariable) {
      return this.visitUnknown(correlVariable);
   }

   protected Document visitUnknown(RexNode o) {
      throw new RuntimeException(String.format("Cannot convert RexNode to equivalent MongoDB project expression.RexNode Class: %s, RexNode Digest: %s", o.getClass().getName(), o.toString()));
   }

   private Object formatArgForMongoOperator(RexNode arg) {
      if (arg instanceof RexInputRef) {
         int index = ((RexInputRef)arg).getIndex();
         RelDataTypeField field = (RelDataTypeField)this.recordType.getFieldList().get(index);
         return "$" + MongoColumnNameSanitizer.sanitizeColumnName(field.getName());
      } else {
         if (arg instanceof RexCall) {
            RexCall call = (RexCall)arg;
            SqlOperator function = call.getOperator();
            if (function == SqlStdOperatorTable.CAST) {
               return this.handleCastFunction(call);
            }

            if (function == SqlStdOperatorTable.ITEM) {
               return this.handleItemFunctionRoot(call);
            }
         } else {
            if (arg instanceof RexLiteral) {
               return RexToFilterDocumentUtils.getMongoFormattedLiteral((RexLiteral)arg, (CompleteType)null);
            }

            if (arg instanceof RexFieldAccess) {
               return this.handleFieldAccessRoot((RexFieldAccess)arg);
            }
         }

         return arg.accept(this);
      }
   }

   private static Document addNotNullCheck(Document comparisonFilter, Object firstArg) {
      Document nullFilter = RexToFilterDocumentUtils.constructOperatorDocument(MongoFunctions.NOT_EQUAL.getMongoOperator(), firstArg, null);
      return RexToFilterDocumentUtils.constructOperatorDocument(MongoFunctions.convertToMongoFunction(SqlKind.AND.sql), comparisonFilter, nullFilter);
   }

   static {
      NULL_LITERAL_EXPR = new Document(MongoFunctions.LITERAL.getMongoOperator(), (Object)null);
   }
}
