package org.embulk.output;

import com.google.common.base.Optional;
import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.DeleteResult;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.FieldType;
import com.sforce.soap.partner.GetUserInfoResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.UpsertResult;
import com.sforce.soap.partner.fault.ApiFault;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.embulk.config.TaskReport;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvListWriter;
import org.supercsv.io.ICsvListWriter;
import org.supercsv.prefs.CsvPreference;

public class SalesforceOutputPlugin
        implements OutputPlugin
{
    protected static Logger logger;
    private static PartnerConnection client = null;
    private static Map<String, String> externalIdToObjectNameMap = null;
        
    public interface PluginTask
            extends Task
    {
        @Config("username")
        public String getUsername();

        @Config("password")
        public String getPassword();

        @Config("login_endpoint")
        @ConfigDefault("\"https://login.salesforce.com\"")
        public Optional<String> getLoginEndpoint();
        
        @Config("sobject")
        public String getSObject();
        
        @Config("upsert_key")
        @ConfigDefault("null")
        public Optional<String> getUpsertKey();
        
        @Config("batch_size")
        @ConfigDefault("200")
        public Integer getBatchSize();
        
        @Config("action")
        @ConfigDefault("\"insert\"")
        public Optional<String> getAction();
        
        @Config("version")
        @ConfigDefault("34.0")
        public Optional<String> getVersion();
        
        @Config("result_dir")
        @ConfigDefault("null")
        public Optional<String> getResultDir();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config,
            Schema schema, int taskCount,
            OutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        logger = Exec.getLogger(getClass());
        
        if (task.getResultDir().isPresent() && task.getResultDir().get() != null) {
            File resultDir = new File(task.getResultDir().get());
            if (!resultDir.exists() || !resultDir.isDirectory()) {
                logger.error("{} is not exist or is not directory.", task.getResultDir().get());
                throw new RuntimeException(task.getResultDir().get() + " is not exist or is not directory.");
            }
        }
        
        final String username = task.getUsername();
        final String password = task.getPassword();
        final String loginEndpoint = task.getLoginEndpoint().get();
        try {
            if (client == null) {
                ConnectorConfig connectorConfig = new ConnectorConfig();
                connectorConfig.setUsername(username);
                connectorConfig.setPassword(password);
                connectorConfig.setAuthEndpoint(loginEndpoint + "/services/Soap/u/" +task.getVersion().get() + "/");

                client = Connector.newConnection(connectorConfig);
                GetUserInfoResult userInfo = client.getUserInfo();
                logger.info("login successful with {}", userInfo.getUserName());
                externalIdToObjectNameMap = new HashMap<>();
                DescribeSObjectResult describeResult = client.describeSObject(task.getSObject());
                for (Field field : describeResult.getFields()) {
                    if (field.getType() == FieldType.reference) {
                        externalIdToObjectNameMap.put(field.getRelationshipName(), field.getReferenceTo()[0]);
                    }
                }
            }
        } catch(ConnectionException ex) {
            logger.error("Login error. Please check your credentials.");
            throw new RuntimeException(ex);
        }
            
        control.run(task.dump());
        return Exec.newConfigDiff();
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            OutputPlugin.Control control)
    {
        throw new UnsupportedOperationException("salesforce output plugin does not support resuming");
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<TaskReport> successTaskReports)
    {
        logger.info("logout");
        try {
            if (client != null) {
                client.logout();
            }
        } catch (ConnectionException ex) {}
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, Schema schema, int taskIndex)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        
        PageReader reader = new PageReader(schema);
        return new SalesforcePageOutput(reader, client, task);
    }

    public class SalesforcePageOutput
            implements TransactionalPageOutput
    {
        private final String dateSuffix = new SimpleDateFormat("yyyyMMddhhmmssSSS").format(new Date());
    
        private final PageReader pageReader;
        private final PartnerConnection client;
        private final int batchSize;
        private List<SObject> records;
        private final String upsertKey;
        private final String sobject;
        private final String action;
        private final String resultDir;
        private Integer numOfSuccess = 0;
        private Integer numOfError = 0;

        public SalesforcePageOutput(final PageReader pageReader, 
                PartnerConnection client, PluginTask task)
        {
            this.pageReader = pageReader;
            this.client = client;
            this.batchSize = task.getBatchSize();
            this.upsertKey = task.getUpsertKey().isPresent() ? task.getUpsertKey().get() : null;
            this.sobject = task.getSObject();
            this.action = task.getAction().isPresent() ? task.getAction().get() : null;
            this.resultDir = task.getResultDir().isPresent() ? task.getResultDir().get() : null;
            this.records = new ArrayList<>();
        }

        @Override
        public void add(Page page)
        {
            try {
                pageReader.setPage(page);
                while (pageReader.nextRecord()) {
                    final SObject record = new SObject();
                    record.setType(this.sobject);
                    
                    pageReader.getSchema().visitColumns(new ColumnVisitor() {
                        @Override
                        public void doubleColumn(Column column) {
                            columnWithReferenceCheck(column.getName(), pageReader.getDouble(column));
                        }
                        @Override
                        public void timestampColumn(Column column) {
                            DateTime dt = new DateTime(pageReader.getTimestamp(column).getEpochSecond()*1000);
                            Calendar cal = Calendar.getInstance();
                            cal.clear();
                            cal.setTimeZone(dt.getZone().toTimeZone());
                            cal.set(dt.getYear(), dt.getMonthOfYear()-1, dt.getDayOfMonth(), 
                                    dt.getHourOfDay(), dt.getMinuteOfHour(), dt.getSecondOfMinute());
                            record.addField(column.getName(), cal);
                        }
                        @Override
                        public void stringColumn(Column column) {
                            columnWithReferenceCheck(column.getName(), pageReader.getString(column));
                        }
                        @Override
                        public void longColumn(Column column) {
                            columnWithReferenceCheck(column.getName(), pageReader.getLong(column));
                        }
                        @Override
                        public void booleanColumn(Column column) {
                            record.addField(column.getName(), pageReader.getBoolean(column));
                        }

                        private void columnWithReferenceCheck(String name, Object value) {
                            if (name.indexOf('.') > 0) {
                                String[] tokens = name.split("\\.");
                                String referencesFieldName = tokens[0];
                                String externalIdFieldName = tokens[1];

                                SObject sObjRef = new SObject();
                                String refFieldApiName = referencesFieldName.replaceAll("__R", "__r");
                                if (externalIdToObjectNameMap.containsKey(refFieldApiName)) {
                                    sObjRef.setType(externalIdToObjectNameMap.get(refFieldApiName));
                                } else {
                                    throw new ConfigException("Invalid Relationship Name '" + refFieldApiName + "'");
                                }
                                sObjRef.addField(externalIdFieldName, value);
                                record.addField(referencesFieldName, sObjRef);
                            } else {
                                record.addField(name, value);
                            }
                        }

                    });
                    this.records.add(record);
                    
                    if (this.records.size() >= this.batchSize) {
                        this.action(this.records);
                        logger.info("Number of processed records: {}", this.numOfSuccess + this.numOfError);
                    }
                }
                
                if (!this.records.isEmpty()) {
                    this.action(this.records);
                    logger.info("Number of processed records: {}", this.numOfSuccess + this.numOfError);
                }
            } catch (ConfigException ex) {
                logger.error("Configuration Error: {}", ex.getMessage());
            } catch (ApiFault ex) {
                logger.error("API Error: {}", ex.getExceptionMessage());
            } catch (ConnectionException ex) {
                logger.error("Connection Error: {}", ex.getMessage());
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public void finish()
        {
            
        }

        @Override
        public void close()
        {
            
        }

        @Override
        public void abort()
        {
        }

        @Override
        public TaskReport commit()
        {
            return Exec.newTaskReport();
        }
        
        private void action(List<SObject> records) throws ConnectionException, IOException{
            switch(this.action){
                case "insert":
                    SaveResult[] insertResults = client.create(
                            (SObject[])this.records.toArray(new SObject[0]));
                    countSaveResults(insertResults);
                    if (resultDir != null) {
                        ResultWrapper resultWrapper = new ResultWrapper(insertResults, null, null);
                        createResultsFiles(records, resultWrapper);
                    }
                    break;
                case "upsert":
                    UpsertResult[] upsertResults = client.upsert(
                            this.upsertKey, (SObject[])this.records.toArray(new SObject[0]));
                    countUpsertResults(upsertResults);
                    if (resultDir != null) {
                        ResultWrapper resultWrapper = new ResultWrapper(null, upsertResults, null);
                        createResultsFiles(records, resultWrapper);
                    }
                    break;
                case "update":
                    SaveResult[] updateResults = client.update(
                            (SObject[])this.records.toArray(new SObject[0]));
                    countSaveResults(updateResults);
                    if (resultDir != null) {
                        ResultWrapper resultWrapper = new ResultWrapper(updateResults, null, null);
                        createResultsFiles(records, resultWrapper);
                    }
                    break;
                case "delete":
                    List<String> ids = new ArrayList<>();
                    for (SObject sobj : this.records) {
                        ids.add(sobj.getId());
                    }
                    DeleteResult[] deleteResults = client.delete(ids.toArray(new String[0]));
                    countDeleteResults(deleteResults);
                    if (resultDir != null) {
                        ResultWrapper resultWrapper = new ResultWrapper(null, null, deleteResults);
                        createResultsFiles(records, resultWrapper);
                    }
                    break;
            }
            this.records = new ArrayList<>();
        }
        
        private void countSaveResults(SaveResult[] saveResults) {
            for (SaveResult saveResult : saveResults) {
                if (saveResult.isSuccess()) {
                    this.numOfSuccess++;
                } else {
                    this.numOfError++;
                }
            }
        }
    
        private void countUpsertResults(UpsertResult[] upsertResults) {
            for (UpsertResult upsertResult : upsertResults) {
                if (upsertResult.isSuccess()) {
                    this.numOfSuccess++;
                } else {
                    this.numOfError++;
                }
            }
        }

        private void countDeleteResults(DeleteResult[] deleteResults) {
            for (DeleteResult deleteResult : deleteResults) {
                if (deleteResult.isSuccess()) {
                    this.numOfSuccess++;
                } else {
                    this.numOfError++;
                }
            }
        }
        
        private List<String> createSuccessHeader() {
            final List<String> successHeader = new ArrayList<>();
            successHeader.add("Id");
            for (Column col : pageReader.getSchema().getColumns()) {
                successHeader.add(col.getName());
            }
            return successHeader;
        }
        
        private List<String> createErrorHeader() {
            final List<String> errorHeader = new ArrayList<>();
            for (Column col : pageReader.getSchema().getColumns()) {
                errorHeader.add(col.getName());
            }
            errorHeader.add("Error");
            return errorHeader;
        }

        private void createResultsFiles(List<SObject> records, ResultWrapper resultWrapper) throws IOException{
            ICsvListWriter successListWriter = null;
            ICsvListWriter errorListWriter = null;
            try {
                String successFileName = this.resultDir + "/success_" + dateSuffix + ".csv";
                Boolean isExistSuccessFile = new File(successFileName).exists();
                successListWriter = new CsvListWriter(new FileWriter(successFileName, true), 
                        CsvPreference.STANDARD_PREFERENCE);
                if (!isExistSuccessFile) {
                    successListWriter.write(createSuccessHeader());
                }
                
                String errorFileName = this.resultDir + "/error_" + dateSuffix + ".csv";
                Boolean isExistErrorFile = new File(errorFileName).exists();
                errorListWriter = new CsvListWriter(new FileWriter(errorFileName, true), 
                        CsvPreference.STANDARD_PREFERENCE);
                if (!isExistErrorFile) {
                    errorListWriter.write(createErrorHeader());
                }
                    
                CellProcessor[] processors = new CellProcessor[pageReader.getSchema().getColumns().size() + 1];
                ArrayList<ArrayList<String>> errorValues = new ArrayList<>();
                for (Integer i = 0, imax = records.size(); i < imax; i++) {
                    SObject record = records.get(i);

                    List<String> values = new ArrayList<>();
                    if (resultWrapper.getIsSuccess(i)) {
                        values.add(resultWrapper.getId(i));
                    }
                    for (Column col : pageReader.getSchema().getColumns()) {
                        Object obj = record.getSObjectField(col.getName());
                        if (obj != null) {
                            if (obj instanceof Calendar) {
                                DateTime dt = new DateTime((Calendar)obj);
                                values.add(dt.toString("yyyy-MM-dd'T'hh:mm:ssZZ"));
                            } else {
                                values.add(obj.toString());
                            }
                        } else {
                            values.add("");
                        }
                    }
                    if (!resultWrapper.getIsSuccess(i)) {
                        StringBuilder sb = new StringBuilder();
                        for (com.sforce.soap.partner.Error err : resultWrapper.getErrors(i)) {
                            if (sb.length() > 0) {
                                sb.append(";");
                            }
                            sb.append(err.getStatusCode())
                              .append(":")
                              .append(err.getMessage());
                        }

                        values.add(sb.toString());
                    }
                    if (resultWrapper.getIsSuccess(i)) {
                        successListWriter.write(values);
                    } else {
                        errorListWriter.write(values);
                    }
                }
            } finally {
                if(successListWriter != null ) {
                    successListWriter.close();
                }
                if(errorListWriter != null ) {
                    errorListWriter.close();
                }
            }
        }
    }
    
    public class ResultWrapper {
        private final SaveResult[] saveResult;
        private final UpsertResult[] upsertResult;
        private final DeleteResult[] deleteResult;
        
        public ResultWrapper(SaveResult[] saveResults, 
                UpsertResult[] upsertResults, DeleteResult[] deleteResults) {
            this.saveResult = saveResults;
            this.upsertResult = upsertResults;
            this.deleteResult = deleteResults;
        }
        
        public Boolean isSaveResult() {
            return this.saveResult != null;
        }
        
        public Boolean isUpsertResult() {
            return this.upsertResult != null;
        }
        
        public Boolean isDeleteResult() {
            return this.deleteResult != null;
        }
        
        public String getId(Integer index) {
            if (this.isSaveResult()) {
                return this.saveResult[index].getId();
            } else if (this.isUpsertResult()) {
                return this.upsertResult[index].getId();
            } else if (this.isDeleteResult()) {
                return this.deleteResult[index].getId();
            }
            return null;
        }
        
        public Boolean getIsSuccess(Integer index) {
            if (this.isSaveResult()) {
                return this.saveResult[index].isSuccess();
            } else if (this.isUpsertResult()) {
                return this.upsertResult[index].isSuccess();
            } else if (this.isDeleteResult()) {
                return this.deleteResult[index].isSuccess();
            }
            return null;
        }
        
        public Boolean getIsCreated(Integer index) {
            if (this.isUpsertResult()) {
                return this.upsertResult[index].isCreated();
            }
            return null;
        }
        
        public com.sforce.soap.partner.Error[] getErrors(Integer index) {
            if (this.isSaveResult()) {
                return this.saveResult[index].getErrors();
            } else if (this.isUpsertResult()) {
                return this.upsertResult[index].getErrors();
            } else if (this.isDeleteResult()) {
                return this.deleteResult[index].getErrors();
            }
            return null;
        }
    }
    
}
