package com.dremio.plugins.mongo.planning.rels;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;

public final class MongoColumnNameSanitizer {
   private static final String DOLLAR_FIELDNAME_PREFIX = "MongoRename_";
   private static final int DOLLAR_FIELDNAME_PREFIX_LENGTH = 12;

   private MongoColumnNameSanitizer() {
   }

   public static RelDataType sanitizeColumnNames(RelDataType inputRowType) {
      boolean needRenaming = false;
      List<String> currentFieldNames = inputRowType.getFieldNames();
      List<String> newFieldNames = new ArrayList(currentFieldNames.size());
      Iterator var4 = currentFieldNames.iterator();

      while(var4.hasNext()) {
         String currentName = (String)var4.next();
         if (currentName.charAt(0) == '$') {
            newFieldNames.add("MongoRename_" + currentName);
            needRenaming = true;
         } else {
            newFieldNames.add(currentName);
         }
      }

      if (!needRenaming) {
         return inputRowType;
      } else {
         List<RelDataTypeField> currentFields = inputRowType.getFieldList();
         List<RelDataTypeField> newFields = new ArrayList(newFieldNames.size());

         for(int i = 0; i != newFieldNames.size(); ++i) {
            newFields.add(new RelDataTypeFieldImpl((String)newFieldNames.get(i), i, ((RelDataTypeField)currentFields.get(i)).getType()));
         }

         return new RelRecordType(newFields);
      }
   }

   public static String sanitizeColumnName(String columnName) {
      return columnName.charAt(0) == '$' ? "MongoRename_" + columnName : columnName;
   }

   public static String unsanitizeColumnName(String columnName) {
      return columnName.startsWith("MongoRename_") ? columnName.substring(12) : columnName;
   }
}
