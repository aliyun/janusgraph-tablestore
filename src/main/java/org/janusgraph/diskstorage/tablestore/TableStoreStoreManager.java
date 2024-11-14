// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.tablestore;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.StoreMetaData;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.common.DistributedStoreManager;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;
import org.janusgraph.util.system.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static org.janusgraph.diskstorage.Backend.LOCK_STORE_SUFFIX;
import static org.janusgraph.diskstorage.Backend.INDEXSTORE_NAME;
import static org.janusgraph.diskstorage.Backend.SYSTEM_TX_LOG_NAME;
import static org.janusgraph.diskstorage.Backend.EDGESTORE_NAME;
import static org.janusgraph.diskstorage.Backend.SYSTEM_MGMT_LOG_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.DROP_ON_CLEAR;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.IDS_STORE_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.SYSTEM_PROPERTIES_STORE_NAME;


/**
 * Storage Manager for HBase
 *
 * @author Dan LaRocque &lt;dalaro@hopcount.org&gt;
 */
@PreInitializeConfigOptions
public class TableStoreStoreManager extends DistributedStoreManager implements KeyColumnValueStoreManager {

    private static final Logger logger = LoggerFactory.getLogger(TableStoreStoreManager.class);

    public static final ConfigNamespace HBASE_NS =
        new ConfigNamespace(GraphDatabaseConfiguration.STORAGE_NS, "hbase", "HBase storage options");


    public static final ConfigOption<Boolean> SKIP_SCHEMA_CHECK =
        new ConfigOption<>(HBASE_NS, "skip-schema-check",
            "Assume that JanusGraph's HBase table and column families already exist. " +
                "When this is true, JanusGraph will not check for the existence of its table/CFs, " +
                "nor will it attempt to create them under any circumstances.  This is useful " +
                "when running JanusGraph without HBase admin privileges.",
            ConfigOption.Type.MASKABLE, false);

    public static final ConfigOption<String> HBASE_TABLE =
        new ConfigOption<>(HBASE_NS, "table",
            "The name of the table JanusGraph will use.  When " + ConfigElement.getPath(SKIP_SCHEMA_CHECK) +
                " is false, JanusGraph will automatically create this table if it does not already exist." +
                " If this configuration option is not provided but graph.graphname is, the table will be set" +
                " to that value.",
            ConfigOption.Type.LOCAL, "janusgraph");


    public static final int PORT_DEFAULT = 2181;  // Not used. Just for the parent constructor.

    public static final TimestampProviders PREFERRED_TIMESTAMPS = TimestampProviders.MILLI;

    public static final ConfigNamespace HBASE_CONFIGURATION_NAMESPACE =
        new ConfigNamespace(HBASE_NS, "ext", "Overrides for hbase-{site,default}.xml options", true);

    public static final byte[] defaultColumnFamilyNameBytes = Bytes.toBytes("s");

    // Immutable instance fields
    private final String tableNamePrefix;

    private final Connection cnx;
    private final boolean skipSchemaCheck;

    private final org.apache.hadoop.conf.Configuration hconf;

    private static final ConcurrentHashMap<TableStoreStoreManager, Throwable> openManagers = new ConcurrentHashMap<>();

    // Mutable instance state
    private final ConcurrentMap<String, TableStoreKeyColumnValueStore> openStores;

    private final BiMap<String, String> shortCfNameMap;

    public TableStoreStoreManager(org.janusgraph.diskstorage.configuration.Configuration config) throws BackendException {
        super(config, PORT_DEFAULT);
        shortCfNameMap = createShortCfMap(config);

        this.tableNamePrefix = determineTableNamePrefix(config);
        if(tableNamePrefix.contains("_")) {
            throw new PermanentBackendException(String.format("tableNamePrefix %s contains invalid characters '_'",tableNamePrefix));
        }
        this.skipSchemaCheck = config.get(SKIP_SCHEMA_CHECK);


        /* This static factory calls HBaseConfiguration.addHbaseResources(),
         * which in turn applies the contents of hbase-default.xml and then
         * applies the contents of hbase-site.xml.
         */
        hconf = HBaseConfiguration.create();

        // Copy a subset of our commons config into a Hadoop config
        int keysLoaded=0;
        Map<String,Object> configSub = config.getSubset(HBASE_CONFIGURATION_NAMESPACE);
        for (Map.Entry<String,Object> entry : configSub.entrySet()) {
            logger.info("HBase configuration: setting {}={}", entry.getKey(), entry.getValue());
            if (entry.getValue()==null) continue;
            hconf.set(entry.getKey(), entry.getValue().toString());
            keysLoaded++;
        }

        logger.debug("HBase configuration: set a total of {} configuration values", keysLoaded);

        // Special case for STORAGE_HOSTS
        if (config.has(GraphDatabaseConfiguration.STORAGE_HOSTS)) {
            String zkQuorumKey = "hbase.zookeeper.quorum";
            String csHostList = Joiner.on(",").join(config.get(GraphDatabaseConfiguration.STORAGE_HOSTS));
            hconf.set(zkQuorumKey, csHostList);
            logger.info("Copied host list from {} to {}: {}", GraphDatabaseConfiguration.STORAGE_HOSTS, zkQuorumKey, csHostList);
        }

        // Special case for STORAGE_PORT
        if (config.has(GraphDatabaseConfiguration.STORAGE_PORT)) {
            String zkPortKey = "hbase.zookeeper.property.clientPort";
            Integer zkPort = config.get(GraphDatabaseConfiguration.STORAGE_PORT);
            hconf.set(zkPortKey, zkPort.toString());
            logger.info("Copied Zookeeper Port from {} to {}: {}", GraphDatabaseConfiguration.STORAGE_PORT, zkPortKey, zkPort);
        }

        try {
            this.cnx = ConnectionFactory.createConnection(hconf);
        } catch (IOException e) {
            throw new PermanentBackendException(e);
        }

        if (logger.isTraceEnabled()) {
            openManagers.put(this, new Throwable("Manager Opened"));
            dumpOpenManagers();
        }

        logger.debug("Dumping HBase config key=value pairs");
        for (Map.Entry<String, String> entry : hconf) {
            logger.debug("[HBaseConfig] " + entry.getKey() + "=" + entry.getValue());
        }
        logger.debug("End of HBase config key=value pairs");

        openStores = new ConcurrentHashMap<>();
    }

    @Override
    public Deployment getDeployment() {
        return Deployment.REMOTE;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + tableNamePrefix;
    }

    public void dumpOpenManagers() {
        int estimatedSize = openManagers.size();
        logger.trace("---- Begin open HBase store manager list ({} managers) ----", estimatedSize);
        for (TableStoreStoreManager m : openManagers.keySet()) {
            logger.trace("Manager {} opened at:", m, openManagers.get(m));
        }
        logger.trace("----   End open HBase store manager list ({} managers)  ----", estimatedSize);
    }

    @Override
    public void close() {
        openStores.clear();
        if (logger.isTraceEnabled())
            openManagers.remove(this);
        IOUtils.closeQuietly(cnx);
    }

    @Override
    public StoreFeatures getFeatures() {
        Configuration c = GraphDatabaseConfiguration.buildGraphConfiguration();
        StandardStoreFeatures.Builder fb = new StandardStoreFeatures.Builder()
            .orderedScan(true).unorderedScan(true).batchMutation(true)
            .multiQuery(true).distributed(true).keyOrdered(true).storeTTL(true)
            .cellTTL(false).timestamps(true).preferredTimestamps(PREFERRED_TIMESTAMPS)
            .optimisticLocking(true).keyConsistent(c).localKeyPartition(false);
        return fb.build();
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        Long putTimestamp = null;
        Long delTimestamp = null;
        MaskedTimestamp commitTime = null;
        if (assignTimestamp) {
            commitTime = new MaskedTimestamp(txh);
            putTimestamp = commitTime.getAdditionTime(times);
            delTimestamp = commitTime.getDeletionTime(times);
        }
        // In case of an addition and deletion with identical timestamps, the
        // deletion tombstone wins.
        // https://hbase.apache.org/book/versions.html#d244e4250
        final Map<TableName, Map<StaticBuffer, Pair<List<Put>, List<Delete>>>> commandsPerTable =
            convertToCommands(mutations, putTimestamp, delTimestamp);

        for (Map.Entry<TableName, Map<StaticBuffer, Pair<List<Put>, List<Delete>>>> entry : commandsPerTable.entrySet()) {
            TableName tableName = entry.getKey();
            Map<StaticBuffer, Pair<List<Put>, List<Delete>>> commandsPerKey = entry.getValue();

            final List<Row> batch = new ArrayList<>(commandsPerKey.size()); // actual batch operation

            // convert sorted commands into representation required for 'batch' operation
            for (Pair<List<Put>, List<Delete>> commands : commandsPerKey.values()) {
                if (commands.getFirst() != null && !commands.getFirst().isEmpty())
                    batch.addAll(commands.getFirst());
                if (commands.getSecond() != null && !commands.getSecond().isEmpty())
                    batch.addAll(commands.getSecond());
            }

            try {
                Table table = null;

                try {
                    table = cnx.getTable(tableName);
                    table.batch(batch, new Object[batch.size()]);
                } finally {
                    IOUtils.closeQuietly(table);
                }
            } catch (IOException | InterruptedException e) {
                throw new TemporaryBackendException(e);
            }
        }

        if (commitTime != null) {
            sleepAfterWrite(commitTime);
        }
    }

    @Override
    public KeyColumnValueStore openDatabase(String storeName, StoreMetaData.Container metaData) throws BackendException {
        // HBase does not support retrieving cell-level TTL by the client.
        Preconditions.checkArgument(!storageConfig.has(GraphDatabaseConfiguration.STORE_META_TTL, storeName)
            || !storageConfig.get(GraphDatabaseConfiguration.STORE_META_TTL, storeName));

        TableStoreKeyColumnValueStore store = openStores.get(storeName);

        if (store == null) {
            final String tableNameSuffix = getTableNameSuffixForStoreName(storeName);
            TableName tableName = getTableName(tableNameSuffix);
            TableStoreKeyColumnValueStore newStore = new TableStoreKeyColumnValueStore(this, cnx, tableName, storeName);

            store = openStores.putIfAbsent(storeName, newStore); // nothing bad happens if we loose to other thread

            if (store == null) {
                if (!skipSchemaCheck) {
                    int tableTTLInSeconds = -1;
                    if (metaData.contains(StoreMetaData.TTL)) {
                        tableTTLInSeconds = metaData.get(StoreMetaData.TTL);
                    }
                    ensureTableExists(tableName, tableTTLInSeconds);
                }

                store = newStore;
            }
        }

        return store;
    }

    @Override
    public StoreTransaction beginTransaction(final BaseTransactionConfig config) {
        return new TableStoreTransaction(config);
    }

    @Override
    public String getName() {
        return getClass().getSimpleName() + tableNamePrefix;
    }

    /**
     * Deletes the specified table with all its columns.
     * ATTENTION: Invoking this method will delete the table if it exists and therefore causes data loss.
     */
    @Override
    public void clearStorage() throws BackendException {
        String regex = "^" + tableNamePrefix+ "_.+$";
        try (Admin adm = getAdminInterface()) {
            if (this.storageConfig.get(DROP_ON_CLEAR)) {
                adm.deleteTables(regex);
            }else {
                TableName[] tableNames = adm.listTableNames(regex);
                for (TableName tableName : tableNames) {
                    adm.truncateTable(tableName,false);
                }
            }

        } catch (IOException e)
        {
            throw new TemporaryBackendException(e);
        }
    }

    @Override
    public boolean exists() throws BackendException {
        String regex = "^" + tableNamePrefix+ "_.+$";
        try (Admin adm = getAdminInterface()) {
            TableName[] tableNames = adm.listTableNames(regex);
            return null != tableNames && tableNames.length > 0;
        } catch (IOException e)
        {
            throw new TemporaryBackendException(e);
        }
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
    }

    private TableDescriptor ensureTableExists(TableName tableName, int ttlInSeconds) throws BackendException {
        Admin adm = null;

        TableDescriptor desc;

        try { // Create our table, if necessary
            adm = getAdminInterface();

            if (adm.tableExists(tableName)) {
                desc = adm.getDescriptor(tableName);
            } else {
                desc = createTable(tableName, ttlInSeconds, adm);
            }
        } catch (IOException e) {
            throw new TemporaryBackendException(e);
        } finally {
            IOUtils.closeQuietly(adm);
        }

        return desc;
    }

    private TableDescriptor createTable(TableName tableName, int ttlInSeconds, Admin adm) throws IOException {

        TableDescriptorBuilder desc = TableDescriptorBuilder.newBuilder(tableName);
        ColumnFamilyDescriptorBuilder columnDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(defaultColumnFamilyNameBytes);
        setCFOptions(columnDescriptor, ttlInSeconds);
        desc.setColumnFamily(columnDescriptor.build());
        TableDescriptor td = desc.build();
        adm.createTable(td);
        return td;
    }

    private void setCFOptions(ColumnFamilyDescriptorBuilder columnDescriptor, int ttlInSeconds) {
        if (ttlInSeconds > 0)
            columnDescriptor.setTimeToLive(ttlInSeconds);
    }

    /**
     * Convert JanusGraph internal Mutation representation into HBase native commands.
     *
     * @param mutations    Mutations to convert into HBase commands.
     * @param putTimestamp The timestamp to use for Put commands.
     * @param delTimestamp The timestamp to use for Delete commands.
     * @return Commands sorted by key converted from JanusGraph internal representation.
     * @throws org.janusgraph.diskstorage.PermanentBackendException
     */
    @VisibleForTesting
    Map<TableName, Map<StaticBuffer, Pair<List<Put>, List<Delete>>>> convertToCommands(Map<String, Map<StaticBuffer, KCVMutation>> mutations,
                                                                 final Long putTimestamp,
                                                                 final Long delTimestamp) throws PermanentBackendException {
        // A map of rowkey to commands (list of Puts, Delete)

        Map<TableName, Map<StaticBuffer, Pair<List<Put>, List<Delete>>>> commandsPerTable = new HashMap<>();
        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> entry : mutations.entrySet()) {
            final Map<StaticBuffer, Pair<List<Put>, List<Delete>>> commandsPerKey = new HashMap<>();
            String tableNameSuffix = getTableNameSuffixForStoreName(entry.getKey());
            TableName tableName = getTableName(tableNameSuffix);
            for (Map.Entry<StaticBuffer, KCVMutation> m : entry.getValue().entrySet()) {
                final byte[] key = m.getKey().as(StaticBuffer.ARRAY_FACTORY);
                KCVMutation mutation = m.getValue();

                Pair<List<Put>, List<Delete>> commands = commandsPerKey.get(m.getKey());

                // The first time we go through the list of input <rowkey, KCVMutation>,
                // create the holder for a particular rowkey
                if (commands == null) {
                    commands = new Pair<>();
                    // List of all the Puts for this rowkey, including the ones without TTL and with TTL.
                    final List<Put> putList = new ArrayList<>();
                    commands.setFirst(putList);
                    final List<Delete> deleteList = new ArrayList<>();
                    commands.setSecond(deleteList);
                    commandsPerKey.put(m.getKey(), commands);
                }

                if (mutation.hasDeletions()) {
                    Delete d = delTimestamp != null ? new Delete(key,delTimestamp) : new Delete(key);
                    int columnCount = 0;
                    for (StaticBuffer b : mutation.getDeletions()) {
                        addColumnToDelete(d, b.as(StaticBuffer.ARRAY_FACTORY), delTimestamp);
                        columnCount++;
                        if (columnCount >= 1024) {
                            commands.getSecond().add(d);
                            columnCount = 0;
                            d = delTimestamp != null ? new Delete(key,delTimestamp) : new Delete(key);
                        }
                    }
                    if(!d.isEmpty()) {
                        commands.getSecond().add(d);
                    }
                }

                if (mutation.hasAdditions()) {
                    // All the entries (column cells) with the rowkey use this one Put, except the ones with TTL.
                    Put putColumnsWithoutTtl = putTimestamp != null ? new Put(key, putTimestamp) : new Put(key);
                    // At the end of this loop, there will be one Put entry in the commands.getFirst() list that
                    // contains all additions without TTL set, and possible multiple Put entries for columns
                    // that have TTL set.
                    int columnCount = 0;
                    for (Entry e : mutation.getAdditions()) {

                        // Deal with TTL within the entry (column cell) first
                        // HBase cell level TTL is actually set at the Mutation/Put level.
                        // Therefore we need to construct a new Put for each entry (column cell) with TTL.
                        // We can not combine them because column cells within the same rowkey may:
                        // 1. have no TTL
                        // 2. have TTL
                        // 3. have different TTL
                        final Integer ttl = (Integer) e.getMetaData().get(EntryMetaData.TTL);
                        if (null != ttl && ttl > 0) {
                            // Create a new Put
                            Put putColumnWithTtl = putTimestamp != null ? new Put(key, putTimestamp) : new Put(key);
                            addColumnToPut(putColumnWithTtl, putTimestamp, e);
                            // Convert ttl from second (JanusGraph TTL) to milliseconds (HBase TTL)
                            // @see JanusGraphManagement#setTTL(JanusGraphSchemaType, Duration)
                            // HBase supports cell-level TTL for versions 0.98.6 and above.
                            (putColumnWithTtl).setTTL(TimeUnit.SECONDS.toMillis((long)ttl));
                            // commands.getFirst() is the list of Puts for this rowkey. Add this
                            // Put column with TTL to the list.
                            commands.getFirst().add(putColumnWithTtl);
                        } else {
                            addColumnToPut(putColumnsWithoutTtl, putTimestamp, e);
                            columnCount++;
                            if (columnCount >= 1024){
                                commands.getFirst().add(putColumnsWithoutTtl);
                                columnCount = 0;
                                putColumnsWithoutTtl = putTimestamp != null ? new Put(key, putTimestamp) : new Put(key);
                            }
                        }

                    }
                    // If there were any mutations without TTL set, add them to commands.getFirst()
                    if (!putColumnsWithoutTtl.isEmpty()) {
                        commands.getFirst().add(putColumnsWithoutTtl);
                    }
                }
            }
            commandsPerTable.put(tableName,commandsPerKey);
        }

        return commandsPerTable;
    }

    private void addColumnToDelete(Delete d, byte[] qualifier, Long delTimestamp) {
        byte[] encodedColumn = TableStoreColumnBuilder.encodeColumn(qualifier);
        if (delTimestamp != null) {
            d.addColumns(defaultColumnFamilyNameBytes, encodedColumn, delTimestamp);
        } else {
            d.addColumns(defaultColumnFamilyNameBytes, encodedColumn);
        }
    }

    private void addColumnToPut(Put p, Long putTimestamp, Entry e) {
        final byte[] qualifier = e.getColumnAs(StaticBuffer.ARRAY_FACTORY);
        byte[] encodedColumn = TableStoreColumnBuilder.encodeColumn(qualifier);
        final byte[] value = e.getValueAs(StaticBuffer.ARRAY_FACTORY);
        if (putTimestamp != null) {
            p.addColumn(defaultColumnFamilyNameBytes, encodedColumn, putTimestamp, value);
        } else {
            p.addColumn(defaultColumnFamilyNameBytes, encodedColumn, value);
        }
    }

    private TableName getTableName(String tableNameSuffix) {
        String tableNameStr = tableNamePrefix + "_" + tableNameSuffix;
        return TableName.valueOf(tableNameStr);
    }

    private Admin getAdminInterface() {
        try {
            return cnx.getAdmin();
        } catch (IOException e) {
            throw new JanusGraphException(e);
        }
    }

    private String determineTableNamePrefix(Configuration config) {
        if ((!config.has(HBASE_TABLE)) && (config.has(GRAPH_NAME))) {
            return config.get(GRAPH_NAME);
        }
        return config.get(HBASE_TABLE);
    }

    @VisibleForTesting
    protected org.apache.hadoop.conf.Configuration getHBaseConf() {
        return hconf;
    }

    public static BiMap<String, String> createShortCfMap(Configuration config) {
        return ImmutableBiMap.<String, String>builder()
                .put(INDEXSTORE_NAME, "g")
                .put(INDEXSTORE_NAME + LOCK_STORE_SUFFIX, "h")
                .put(config.get(IDS_STORE_NAME), "i")
                .put(EDGESTORE_NAME, "e")
                .put(EDGESTORE_NAME + LOCK_STORE_SUFFIX, "f")
                .put(SYSTEM_PROPERTIES_STORE_NAME, "s")
                .put(SYSTEM_PROPERTIES_STORE_NAME + LOCK_STORE_SUFFIX, "t")
                .put(SYSTEM_MGMT_LOG_NAME, "m")
                .put(SYSTEM_TX_LOG_NAME, "l")
                .build();
    }

    public static String shortenCfName(BiMap<String, String> shortCfNameMap, String longName) throws PermanentBackendException {
        final String s;
        if (shortCfNameMap.containsKey(longName)) {
            s = shortCfNameMap.get(longName);
            Preconditions.checkNotNull(s);
            logger.debug("Substituted default CF name \"{}\" with short form \"{}\" to reduce HBase KeyValue size", longName, s);
        } else {
            if (shortCfNameMap.containsValue(longName)) {
                String fmt = "Must use CF long-form name \"%s\" instead of the short-form name \"%s\"";
                String msg = String.format(fmt, shortCfNameMap.inverse().get(longName), longName);
                throw new PermanentBackendException(msg);
            }
            s = longName;
            logger.debug("Kept default CF name \"{}\" because it has no associated short form", s);
        }
        return s;
    }
    private String getTableNameSuffixForStoreName(String storeName) throws PermanentBackendException {
        return shortenCfName(shortCfNameMap, storeName);
    }

}


