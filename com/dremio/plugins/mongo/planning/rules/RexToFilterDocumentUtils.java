package com.dremio.plugins.mongo.planning.rules;

import com.dremio.common.expression.CompleteType;
import com.dremio.plugins.mongo.planning.MongoFunctions;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.bson.Document;

public final class RexToFilterDocumentUtils {
   private static final DateTimeFormatter ISO_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

   private RexToFilterDocumentUtils() {
   }

   static Document constructOperatorDocument(String opName, Object... args) {
      return new Document(opName, Arrays.asList(args));
   }

   static Object getMongoFormattedLiteral(RexLiteral literal, CompleteType typeOfFieldBeingCompared) {
      return getLiteralDocument(literal, typeOfFieldBeingCompared).get(MongoFunctions.LITERAL.getMongoOperator());
   }

   static Document getLiteralDocument(RexLiteral literal, CompleteType typeOfFieldBeingCompared) {
      if (null != typeOfFieldBeingCompared && typeOfFieldBeingCompared.isComplex()) {
         throw new IllegalArgumentException("Cannot push down values of unknown or complex type.");
      } else {
         SqlTypeName literalSqlType = literal.getType().getSqlTypeName();
         String val;
         if (!literalSqlType.equals(SqlTypeName.DATE) && !literalSqlType.equals(SqlTypeName.TIMESTAMP) && !literalSqlType.equals(SqlTypeName.TIME)) {
            if (null != typeOfFieldBeingCompared && typeOfFieldBeingCompared.isTemporal() && literal.getType().getSqlTypeName().equals(SqlTypeName.VARCHAR)) {
               String litVal = literal.toString();
               if (2 < litVal.length() && '\'' == litVal.charAt(0) && '\'' == litVal.charAt(litVal.length() - 1)) {
                  litVal = litVal.substring(1, litVal.length() - 1);
               }

               val = formatDateTimeLiteralAsISODateString(litVal);
            } else if (null != typeOfFieldBeingCompared && typeOfFieldBeingCompared.isDecimal() && literal.getType().getSqlTypeName().equals(SqlTypeName.DECIMAL)) {
               val = "NumberDecimal(\"" + literal.toString() + "\")";
            } else {
               val = literal.toString();
            }
         } else {
            val = formatDateTimeLiteralAsISODateString(literal.toString());
         }

         return Document.parse(String.format("{ \"%s\": %s }", MongoFunctions.LITERAL.getMongoOperator(), val));
      }
   }

   private static String formatDateTimeLiteralAsISODateString(String dateTimeLiteralText) {
      LocalDateTime ldt = Timestamp.valueOf(dateTimeLiteralText).toLocalDateTime();
      return String.format("ISODate(\"%s\")", ISO_DATE_FORMATTER.format(ldt));
   }
}
