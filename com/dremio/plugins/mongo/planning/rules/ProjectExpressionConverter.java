package com.dremio.plugins.mongo.planning.rules;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.plugins.mongo.planning.MongoFunctions;
import com.dremio.plugins.mongo.planning.MongoPipelineOperators;
import com.dremio.plugins.mongo.planning.rels.MongoColumnNameSanitizer;
import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @deprecated */
@Deprecated
public class ProjectExpressionConverter extends RexVisitorImpl<Object> {
   private static final Logger logger = LoggerFactory.getLogger(ProjectExpressionConverter.class);
   public static final String UNSUPPORTED_REX_NODE_ERROR = "Cannot convert RexNode to equivalent MongoDB project expression.";
   private static final Document NULL_LITERAL_EXPR;
   protected final RelDataType recordType;
   protected final boolean enableMongo3_2;
   protected final boolean enableMongo3_4;
   protected final BatchSchema schema;
   protected CompleteType leftFieldType;
   protected boolean needsCollation;

   public ProjectExpressionConverter(RelDataType recordType, boolean enableMongo3_2, boolean enableMongo3_4) {
      this((BatchSchema)null, recordType, enableMongo3_2, enableMongo3_4);
   }

   public ProjectExpressionConverter(BatchSchema schema, RelDataType recordType, boolean enableMongo3_2, boolean enableMongo3_4) {
      super(true);
      this.needsCollation = false;
      this.schema = schema;
      this.recordType = recordType;
      this.enableMongo3_2 = enableMongo3_2;
      this.enableMongo3_4 = enableMongo3_4;
      this.leftFieldType = null;
   }

   public boolean needsCollation() {
      return this.needsCollation && this.enableMongo3_4;
   }

   public Object visitInputRef(RexInputRef inputRef) {
      int index = inputRef.getIndex();
      RelDataTypeField field = (RelDataTypeField)this.recordType.getFieldList().get(index);
      return "$" + MongoColumnNameSanitizer.sanitizeColumnName(field.getName());
   }

   public Object visitFieldAccess(RexFieldAccess fieldAccess) {
      Pair<Object, Boolean> result = this.handleFieldAccess(fieldAccess, false);
      return (Boolean)result.right ? result.left : "$" + result.left;
   }

   private Pair<Object, Boolean> handleFieldAccess(RexFieldAccess fieldAccess, boolean hitArrayIndex) {
      RexNode leftInput = fieldAccess.getReferenceExpr();
      Object leftInputObject;
      if (leftInput instanceof RexInputRef) {
         RexInputRef left = (RexInputRef)leftInput;
         leftInputObject = this.recordType.getFieldNames().get(left.getIndex());
      } else {
         Pair result;
         if (leftInput instanceof RexCall) {
            RexCall leftFunc = (RexCall)leftInput;
            if (leftFunc.getOperator() != SqlStdOperatorTable.ITEM) {
               throw new RuntimeException("Error");
            }

            result = this.handleItemFunction(leftFunc, false);
            leftInputObject = result.left;
            hitArrayIndex = (Boolean)result.right;
         } else {
            if (!(leftInput instanceof RexFieldAccess)) {
               throw new RuntimeException("left input to ITEM was not another ITEM, RexFieldAccess, or RexInputRef");
            }

            RexFieldAccess left = (RexFieldAccess)leftInput;
            result = this.handleFieldAccess(left, false);
            leftInputObject = result.left;
            hitArrayIndex = (Boolean)result.right;
         }
      }

      RelDataTypeField field = fieldAccess.getField();
      if (hitArrayIndex) {
         throw new RuntimeException("Need to implement more than one array index in a complex column reference");
      } else {
         assert leftInputObject instanceof String : "Expected a String for array selection with second argument as varchar";

         String rightLitString = field.getName();
         if (rightLitString.startsWith("$")) {
            throw new RuntimeException("Mongo aggregation pipeline does not support reference to fields that start with '$', field " + rightLitString);
         } else {
            String rightInputConvStr = "." + rightLitString;
            return new Pair(leftInputObject + rightInputConvStr, hitArrayIndex);
         }
      }
   }

   protected Document getLiteralDocument(RexLiteral literal) {
      if (null != this.leftFieldType && this.leftFieldType.isComplex()) {
         throw new IllegalArgumentException("Cannot push down values of unknown or complex type.");
      } else {
         String val;
         String litVal;
         if (!literal.getType().getSqlTypeName().equals(SqlTypeName.DATE) && !literal.getType().getSqlTypeName().equals(SqlTypeName.TIMESTAMP) && !literal.getType().getSqlTypeName().equals(SqlTypeName.TIME)) {
            if (null != this.leftFieldType && this.leftFieldType.isTemporal() && literal.getType().getSqlTypeName().equals(SqlTypeName.VARCHAR)) {
               litVal = literal.toString();
               if (2 < litVal.length() && '\'' == litVal.charAt(0) && '\'' == litVal.charAt(litVal.length() - 1)) {
                  litVal = litVal.substring(1, litVal.length() - 1);
               }

               val = "ISODate(\"" + (new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")).format(Timestamp.valueOf(litVal)) + "\")";
            } else if (null != this.leftFieldType && this.leftFieldType.isDecimal() && literal.getType().getSqlTypeName().equals(SqlTypeName.DECIMAL)) {
               val = "NumberDecimal(\"" + literal.toString() + "\")";
            } else {
               val = literal.toString();
            }
         } else {
            val = "ISODate(\"" + (new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")).format(Timestamp.valueOf(literal.toString())) + "\")";
         }

         litVal = "{ " + MongoFunctions.LITERAL.getMongoOperator() + " : " + val + " }";
         return Document.parse(litVal);
      }
   }

   public Object visitLiteral(RexLiteral literal) {
      return new Document(MongoFunctions.LITERAL.getMongoOperator(), this.getLiteralDocument(literal).get(MongoFunctions.LITERAL.getMongoOperator()));
   }

   public Object visitCall(RexCall call) {
      String funcName = call.getOperator().getName().toLowerCase();
      MongoFunctions functions = MongoFunctions.getMongoOperator(funcName);
      switch(functions) {
      case EXTRACT:
         return this.handleExtractFunction(call);
      case IFNULL:
         return this.handleIsNullFunction(call, false);
      case IFNOTNULL:
         return this.handleIsNullFunction(call, true);
      case SUBSTR:
         return this.handleSubStrFunction(call);
      case DIVIDE:
         return this.handleDivideFunction(call);
      case TRUNC:
         return this.handleTruncFunction(call);
      case CASE:
         return this.handleCaseFunction(call);
      case ITEM:
         return this.handleItemFunctionRoot(call);
      case CAST:
         return this.handleCastFunction(call);
      default:
         return this.handleGenericFunction(call, funcName);
      }
   }

   protected Object handleGenericFunction(RexCall call, String functionName) {
      String funcName = this.convertToMongoFunction(functionName);
      boolean handleNull = false;
      Object[] args;
      if (this.enableMongo3_4) {
         switch(call.op.kind) {
         case LESS_THAN:
         case LESS_THAN_OR_EQUAL:
         case NOT_EQUALS:
            handleNull = true;
         case GREATER_THAN:
         case GREATER_THAN_OR_EQUAL:
         case EQUALS:
            args = null;
            if (call.operands.get(0) instanceof RexCall && call.operands.get(1) instanceof RexLiteral) {
               args = this.getCollationFilterArgs((RexCall)call.operands.get(0), (RexLiteral)call.operands.get(1));
            } else if (call.operands.get(1) instanceof RexCall && call.operands.get(0) instanceof RexLiteral) {
               args = this.getCollationFilterArgs((RexCall)call.operands.get(1), (RexLiteral)call.operands.get(0));
            }

            if (args != null) {
               this.needsCollation = true;
               Document doc = this.constructJson(funcName, args);
               if (handleNull) {
                  Document nullFilter = this.constructJson(this.convertToMongoFunction(SqlKind.GREATER_THAN.sql), args[0], null);
                  doc = this.constructJson(this.convertToMongoFunction(SqlKind.AND.sql), doc, nullFilter);
               }

               return doc;
            }
         }
      }

      args = new Object[call.getOperands().size()];

      for(int i = 0; i < call.getOperands().size(); ++i) {
         args[i] = ((RexNode)call.getOperands().get(i)).accept(this);
      }

      return this.constructJson(funcName, args);
   }

   private Object[] getCollationFilterArgs(RexCall rexCall, RexLiteral rexLiteral) {
      if (CollationFilterChecker.hasCollationFilter(rexCall, rexLiteral)) {
         Object[] args = new Object[]{((RexNode)rexCall.getOperands().get(0)).accept(this), rexLiteral.getValue3().toString()};
         return args;
      } else {
         return null;
      }
   }

   private Object handleCastFunction(RexCall call) {
      Preconditions.checkArgument(call.getOperands().size() == 1, "Cast expects a single argument");
      if (MatchExpressionConverter.checkForUnneededCast(call)) {
         return ((RexNode)call.getOperands().get(0)).accept(this);
      } else {
         throw new RuntimeException("Cannot handle anything but unnecessary casts");
      }
   }

   private Object handleExtractFunction(RexCall call) {
      String unit = ((RexLiteral)call.getOperands().get(0)).getValue().toString().toLowerCase();
      String funcName = this.convertToMongoFunction("extract_" + unit);
      Object arg = ((RexNode)call.getOperands().get(1)).accept(this);
      Object extractFn = this.constructJson(funcName, arg);
      Object isNotNullExpr = this.constructJson(MongoFunctions.NOT_EQUAL.getMongoOperator(), arg, NULL_LITERAL_EXPR);
      Object condExpr = this.constructJson(MongoFunctions.CASE.getMongoOperator(), isNotNullExpr, extractFn, NULL_LITERAL_EXPR);
      return condExpr;
   }

   private Object handleIsNullFunction(RexCall call, boolean reverse) {
      return this.constructJson(reverse ? MongoFunctions.GREATER.getMongoOperator() : MongoFunctions.LESS_OR_EQUAL.getMongoOperator(), ((RexNode)call.getOperands().get(0)).accept(this), NULL_LITERAL_EXPR);
   }

   private Object handleDivideFunction(RexCall call) {
      return this.constructJson(MongoFunctions.DIVIDE.getMongoOperator(), ((RexNode)call.getOperands().get(0)).accept(this), ((RexNode)call.getOperands().get(1)).accept(this));
   }

   private Object handleSubStrFunction(RexCall call) {
      List<RexNode> operands = call.getOperands();
      Object arg0 = ((RexNode)operands.get(0)).accept(this);
      Object arg1 = ((RexNode)operands.get(1)).accept(this);
      Object arg2;
      if (operands.size() > 2) {
         arg2 = ((RexNode)operands.get(2)).accept(this);
      } else {
         arg2 = -1;
      }

      return this.constructJson(MongoFunctions.SUBSTR.getMongoOperator(), arg0, arg1, arg2);
   }

   private Object handleItemFunctionRoot(RexCall call) {
      Pair<Object, Boolean> result = this.handleItemFunction(call, false);
      return (Boolean)result.right ? result.left : "$" + result.left;
   }

   private Pair<Object, Boolean> handleItemFunction(RexCall call, boolean hitArrayIndex) {
      RexNode leftInput = (RexNode)call.getOperands().get(0);
      Object leftInputObject;
      if (leftInput instanceof RexInputRef) {
         RexInputRef left = (RexInputRef)leftInput;
         leftInputObject = this.recordType.getFieldNames().get(left.getIndex());
      } else {
         Pair result;
         if (leftInput instanceof RexCall) {
            RexCall leftFunc = (RexCall)leftInput;
            if (leftFunc.getOperator() != SqlStdOperatorTable.ITEM) {
               throw new RuntimeException("Error");
            }

            result = this.handleItemFunction(leftFunc, false);
            leftInputObject = result.left;
            hitArrayIndex = (Boolean)result.right;
         } else {
            if (!(leftInput instanceof RexFieldAccess)) {
               throw new RuntimeException("left input to ITEM was not another ITEM, RexFieldAccess, or RexInputRef");
            }

            RexFieldAccess leftFunc = (RexFieldAccess)leftInput;
            result = this.handleFieldAccess(leftFunc, false);
            leftInputObject = result.left;
            hitArrayIndex = (Boolean)result.right;
         }
      }

      RexNode rightInput = (RexNode)call.getOperands().get(1);
      if (rightInput instanceof RexLiteral) {
         RexLiteral rightLit = (RexLiteral)rightInput;
         switch(rightInput.getType().getSqlTypeName()) {
         case DECIMAL:
         case INTEGER:
            if (!this.enableMongo3_2) {
               throw new RuntimeException("Mongo version less than 3.2 does not support " + MongoFunctions.ARRAYELEMAT.getMongoOperator());
            } else {
               Integer val = ((BigDecimal)rightLit.getValue()).intValue();
               if (hitArrayIndex) {
                  assert leftInputObject instanceof Document : "Expected a Document for array selection, but got " + leftInputObject.getClass().getName();

                  return new Pair(this.constructJson(MongoFunctions.ARRAYELEMAT.getMongoOperator(), leftInputObject, val), true);
               } else {
                  assert leftInputObject instanceof String : "Expected a String for array selection, but got " + leftInputObject.getClass().getName();

                  return new Pair(this.constructJson(MongoFunctions.ARRAYELEMAT.getMongoOperator(), "$" + leftInputObject, val), true);
               }
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

               String rightInputConvStr = "." + rightLitString;
               return new Pair(leftInputObject + rightInputConvStr, hitArrayIndex);
            }
         default:
            throw new RuntimeException("error converting item expression, second argument not a literal or varchar or number");
         }
      } else {
         throw new RuntimeException("error converting item expression, second argument not a literal or varchar or number");
      }
   }

   private Object handleTruncFunction(RexCall call) {
      if (!this.enableMongo3_2) {
         throw UserException.planError().message("Mongo version less than 3.2 does not support " + MongoFunctions.TRUNC.getMongoOperator(), new Object[0]).build(logger);
      } else if (call.getOperands().size() > 1) {
         throw UserException.planError().message("Method 'trunc(x, y)' is not supported. Only 'trunc(x)' (which strips the decimal part) is supported", new Object[0]).build(logger);
      } else {
         return this.constructJson(MongoFunctions.TRUNC.getMongoOperator(), ((RexNode)call.getOperands().get(0)).accept(this));
      }
   }

   private Object handleCaseFunction(RexCall call) {
      Preconditions.checkArgument(call.getOperands().size() % 2 == 1, "Number of arguments to a case function should be an odd numbered.");
      return this.handleCaseFunctionHelper(call.getOperands(), 0);
   }

   private Object handleCaseFunctionHelper(List<RexNode> operands, int start) {
      return start == operands.size() - 1 ? ((RexNode)operands.get(start)).accept(this) : this.constructJson(MongoFunctions.CASE.getMongoOperator(), ((RexNode)operands.get(start)).accept(this), ((RexNode)operands.get(start + 1)).accept(this), this.handleCaseFunctionHelper(operands, start + 2));
   }

   private String convertToMongoFunction(String sqlFunction) {
      MongoFunctions mongoOp = MongoFunctions.getMongoOperator(sqlFunction);
      if (mongoOp != null && mongoOp.canUseInStage(MongoPipelineOperators.PROJECT)) {
         if (this.enableMongo3_2) {
            return mongoOp.getMongoOperator();
         }

         if (mongoOp.supportedInVersion("3.1")) {
            return mongoOp.getMongoOperator();
         }
      }

      throw UserException.planError().message("Unsupported function %s, Mongo 3.2 features enabled: %s", new Object[]{sqlFunction, this.enableMongo3_2}).build(logger);
   }

   private Document constructJson(String opName, Object... args) {
      return new Document(opName, Arrays.asList(args));
   }

   public Object visitDynamicParam(RexDynamicParam dynamicParam) {
      return this.visitUnknown(dynamicParam);
   }

   public Object visitRangeRef(RexRangeRef rangeRef) {
      return this.visitUnknown(rangeRef);
   }

   public Object visitLocalRef(RexLocalRef localRef) {
      return this.visitUnknown(localRef);
   }

   public Object visitOver(RexOver over) {
      return this.visitUnknown(over);
   }

   public Object visitCorrelVariable(RexCorrelVariable correlVariable) {
      return this.visitUnknown(correlVariable);
   }

   protected Object visitUnknown(RexNode o) {
      throw UserException.planError().message("Cannot convert RexNode to equivalent MongoDB project expression.RexNode Class: %s, RexNode Digest: %s", new Object[]{o.getClass().getName(), o.toString()}).build(logger);
   }

   protected CompleteType getFieldType(RexNode curNode) {
      if (this.schema == null) {
         return null;
      } else {
         String compoundName = MongoRulesUtil.getCompoundFieldName(curNode, this.recordType.getFieldList());
         SchemaPath compoundPath = SchemaPath.getCompoundPath(compoundName.split("\\."));
         if (null != compoundPath) {
            TypedFieldId fieldId = this.schema.getFieldId(compoundPath);
            if (null != fieldId) {
               return fieldId.getFinalType();
            }
         }

         return null;
      }
   }

   static {
      NULL_LITERAL_EXPR = new Document(MongoFunctions.LITERAL.getMongoOperator(), (Object)null);
   }
}
