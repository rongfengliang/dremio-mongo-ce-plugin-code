package com.dremio.plugins.mongo.planning.rules;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.plugins.mongo.planning.rels.MongoProject;
import com.dremio.plugins.mongo.planning.rels.MongoRel;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

public class MongoProjectRule extends AbstractMongoConverterRule<ProjectPrel> {
   public static final MongoProjectRule INSTANCE = new MongoProjectRule();

   public MongoProjectRule() {
      super(ProjectPrel.class, "MongoProjectRule", ExecConstants.MONGO_RULES_PROJECT, true);
   }

   public MongoRel convert(RelOptRuleCall call, ProjectPrel project, StoragePluginId pluginId, RelNode input) {
      for(int projectFieldIndex = 0; projectFieldIndex < project.getProjects().size(); ++projectFieldIndex) {
         RexNode rexNode = (RexNode)project.getProjects().get(projectFieldIndex);
         if (!(rexNode instanceof RexInputRef)) {
            return null;
         }

         RexInputRef inputRef = (RexInputRef)rexNode;
         int indexOfChild = inputRef.getIndex();
         RelDataTypeField fieldInInput = (RelDataTypeField)project.getInput().getRowType().getFieldList().get(indexOfChild);
         if (fieldInInput.getName().endsWith("*") || !((RelDataTypeField)project.getRowType().getFieldList().get(projectFieldIndex)).equals(fieldInInput)) {
            return null;
         }
      }

      return new MongoProject(withMongo(project), input, project.getProjects(), project.getRowType(), CollationFilterChecker.hasCollationFilter(project.getInput()));
   }
}
