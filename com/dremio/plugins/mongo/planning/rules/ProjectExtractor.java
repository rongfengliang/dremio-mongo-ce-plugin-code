package com.dremio.plugins.mongo.planning.rules;

import com.dremio.plugins.mongo.planning.MongoFunctions;
import com.google.common.collect.Lists;
import java.util.Iterator;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @deprecated */
@Deprecated
public class ProjectExtractor extends RexShuttle {
   private static final Logger logger = LoggerFactory.getLogger(ProjectExtractor.class);
   private final RelDataType recordType;
   private final RelDataTypeFactory dataTypeFactory;
   private final boolean mongo3_2enabled;
   private final boolean mongo3_4enabled;
   private final List<RexNode> projectColExprs = Lists.newArrayList();
   private final List<RelDataTypeField> projectCols = Lists.newArrayList();
   private int currentTempColIndex = 0;

   public ProjectExtractor(RelDataType recordType, RelDataTypeFactory dataTypeFactory, boolean isMongo3_2enabled, boolean isMongo3_4enabled) {
      this.recordType = recordType;
      this.dataTypeFactory = dataTypeFactory;
      this.mongo3_2enabled = isMongo3_2enabled;
      this.mongo3_4enabled = isMongo3_4enabled;
   }

   public RexNode visitCall(RexCall call) {
      String funcName = call.getOperator().getName().toLowerCase();
      MongoFunctions function = MongoFunctions.getMongoOperator(funcName);
      if (function != null) {
         if (function.isProjectOnly()) {
            return this.createProject(call);
         } else {
            List<RexNode> operands = call.getOperands();
            switch(function) {
            case NOT:
               return this.createProject(call);
            case EQUAL:
            case NOT_EQUAL:
            case GREATER:
            case GREATER_OR_EQUAL:
            case LESS:
            case LESS_OR_EQUAL:
               RexNode firstArg = (RexNode)operands.get(0);
               RexNode secondArg = (RexNode)operands.get(1);
               if (MongoRulesUtil.isSupportedCast(firstArg) && MongoRulesUtil.isLiteral(secondArg) || MongoRulesUtil.isLiteral(firstArg) && MongoRulesUtil.isSupportedCast(secondArg)) {
                  return call;
               }
            case REGEX:
               break;
            default:
               return super.visitCall(call);
            }

            assert operands.size() == 2;

            RexNode first = (RexNode)operands.get(0);
            RexNode second = (RexNode)operands.get(1);
            if (MongoRulesUtil.isInputRef(first) && MongoRulesUtil.isLiteral(second) || MongoRulesUtil.isInputRef(second) && MongoRulesUtil.isLiteral(first)) {
               return call;
            } else {
               return this.createProject(call);
            }
         }
      } else {
         return super.visitCall(call);
      }
   }

   private RexNode createProject(RexCall call) {
      ProjectExpressionConverter visitor = new ProjectExpressionConverter(this.recordType, this.mongo3_2enabled, this.mongo3_4enabled);
      call.accept(visitor);
      int fieldId = this.recordType.getFieldCount() + this.currentTempColIndex++;
      RelDataTypeField newProjectField = new RelDataTypeFieldImpl("__temp" + fieldId, fieldId, this.dataTypeFactory.createSqlType(SqlTypeName.ANY));
      this.projectColExprs.add(call);
      this.projectCols.add(newProjectField);
      return new RexInputRef(newProjectField.getIndex(), newProjectField.getType());
   }

   public boolean hasNewProjects() {
      return this.projectCols.size() > 0 && this.projectColExprs.size() > 0;
   }

   public RelDataType getNewRecordType() {
      List<RelDataTypeField> fields = Lists.newArrayList(this.recordType.getFieldList());
      fields.addAll(this.projectCols);
      return new RelRecordType(fields);
   }

   public List<RexNode> getOuterProjects() {
      List<RexNode> projectExpr = Lists.newArrayList();
      int index = 0;

      for(Iterator var3 = this.recordType.getFieldList().iterator(); var3.hasNext(); ++index) {
         RelDataTypeField field = (RelDataTypeField)var3.next();
         projectExpr.add(new RexInputRef(index, field.getType()));
      }

      return projectExpr;
   }

   public RelDataType getOuterProjectRecordType() {
      List<RelDataTypeField> fields = Lists.newArrayList();
      int index = 0;

      for(Iterator var3 = this.recordType.getFieldList().iterator(); var3.hasNext(); ++index) {
         RelDataTypeField field = (RelDataTypeField)var3.next();
         fields.add(new RelDataTypeFieldImpl(field.getName(), index, field.getType()));
      }

      return new RelRecordType(fields);
   }

   public List<RexNode> getProjects() {
      List<RexNode> projectExpr = Lists.newArrayList();
      Iterator var2 = this.recordType.getFieldList().iterator();

      while(var2.hasNext()) {
         RelDataTypeField field = (RelDataTypeField)var2.next();
         projectExpr.add(new RexInputRef(field.getIndex(), field.getType()));
      }

      projectExpr.addAll(this.projectColExprs);
      return projectExpr;
   }
}
