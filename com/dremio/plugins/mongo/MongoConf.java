package com.dremio.plugins.mongo;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.AuthenticationType;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.Host;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.protostuff.Tag;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Provider;

@SourceType(
   value = "MONGO",
   label = "MongoDB",
   uiConfig = "mongo-layout.json"
)
public class MongoConf extends ConnectionConf<MongoConf, MongoStoragePlugin> {
   @Tag(1)
   public String username;
   @Tag(2)
   @Secret
   public String password;
   @Tag(3)
   public List<Host> hostList;
   @Tag(4)
   @JsonIgnore
   public String replicateSet;
   @Tag(5)
   @NotMetadataImpacting
   @DisplayMetadata(
      label = "Encrypt connection"
   )
   public boolean useSsl = false;
   @Tag(6)
   public List<Property> propertyList;
   @Tag(7)
   public AuthenticationType authenticationType;
   @Tag(8)
   @DisplayMetadata(
      label = "Authentication Database"
   )
   public String authDatabase;
   @Tag(9)
   @NotMetadataImpacting
   @DisplayMetadata(
      label = "Auth Timeout (millis)"
   )
   public int authenticationTimeoutMillis;
   @Tag(10)
   @JsonIgnore
   public List<String> databases;
   @Tag(11)
   @DisplayMetadata(
      label = "Read from secondaries only"
   )
   public boolean secondaryReadsOnly;
   @Tag(12)
   @NotMetadataImpacting
   @DisplayMetadata(
      label = "Subpartition Size"
   )
   public int subpartitionSize;

   public MongoConf() {
      this.authenticationType = AuthenticationType.ANONYMOUS;
      this.authenticationTimeoutMillis = 2000;
      this.databases = new ArrayList();
      this.secondaryReadsOnly = false;
      this.subpartitionSize = 0;
   }

   public MongoStoragePlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
      return new MongoStoragePlugin(this, context, name);
   }
}
