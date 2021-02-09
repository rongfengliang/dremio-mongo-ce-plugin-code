package com.dremio.plugins.mongo.connection;

import com.dremio.ssl.SSLHelper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoClientOptions.Builder;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import javax.net.ssl.SSLContext;

public final class MongoClientOptionsHelper {
   private MongoClientOptionsHelper() {
   }

   public static MongoClientOptions newMongoClientOptions(MongoClientURI mongoURI) {
      Builder builder = new Builder(mongoURI.getOptions());
      if (mongoURI.getOptions().isSslEnabled()) {
         String connection = mongoURI.getURI();
         URI uri = null;

         try {
            uri = new URI(connection);
         } catch (URISyntaxException var8) {
            throw new RuntimeException("Cannot decode properly URI " + connection, var8);
         }

         ListMultimap<String, String> parameters = getParameters(uri);
         boolean sslInvalidHostnameAllowed = Boolean.parseBoolean(getLastValue(parameters, "sslInvalidHostnameAllowed"));
         builder.sslInvalidHostNameAllowed(sslInvalidHostnameAllowed);
         boolean sslInvalidCertificateAllowed = Boolean.parseBoolean(getLastValue(parameters, "sslInvalidCertificateAllowed"));
         if (sslInvalidCertificateAllowed) {
            SSLContext sslContext = SSLHelper.newAllTrustingSSLContext("TLS");
            builder.socketFactory(sslContext.getSocketFactory());
         }
      }

      return builder.build();
   }

   private static String getLastValue(ListMultimap<String, String> parameters, String key) {
      List<String> list = parameters.get(key);
      return list != null && !list.isEmpty() ? (String)list.get(list.size() - 1) : null;
   }

   private static ListMultimap<String, String> getParameters(URI uri) {
      String query = uri.getQuery();
      ListMultimap<String, String> parameters = ArrayListMultimap.create();
      String[] items = query.split("&");
      String[] var4 = items;
      int var5 = items.length;

      for(int var6 = 0; var6 < var5; ++var6) {
         String item = var4[var6];
         String[] keyValue = item.split("=");
         if (keyValue.length != 0) {
            String key = urlDecode(keyValue[0]);
            String value = keyValue.length > 1 ? urlDecode(keyValue[1]) : null;
            parameters.put(key, value);
         }
      }

      return parameters;
   }

   private static String urlDecode(String s) {
      try {
         return URLDecoder.decode(s, StandardCharsets.UTF_8.name());
      } catch (UnsupportedEncodingException var2) {
         throw new RuntimeException("UTF-8 encoding not supported", var2);
      }
   }
}
