/*
 * Copyright 2020-2022 University of Oxford and Health and Social Care Information Centre, also known as NHS Digital
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package uk.ac.ox.softeng.maurodatamapper.plugins.database

import uk.ac.ox.softeng.maurodatamapper.api.exception.ApiBadRequestException
import uk.ac.ox.softeng.maurodatamapper.api.exception.ApiException
import uk.ac.ox.softeng.maurodatamapper.core.authority.AuthorityService
import uk.ac.ox.softeng.maurodatamapper.core.container.Folder
import uk.ac.ox.softeng.maurodatamapper.core.model.CatalogueItem
import uk.ac.ox.softeng.maurodatamapper.datamodel.DataModel
import uk.ac.ox.softeng.maurodatamapper.datamodel.DataModelType
import uk.ac.ox.softeng.maurodatamapper.datamodel.facet.SummaryMetadata
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.DataClass
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.DataClassService
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.DataElement
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.DataElementService
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.datatype.DataType
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.datatype.DataTypeService
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.datatype.EnumerationType
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.datatype.EnumerationTypeService
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.datatype.PrimitiveTypeService
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.datatype.ReferenceTypeService
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.datatype.enumeration.EnumerationValue
import uk.ac.ox.softeng.maurodatamapper.datamodel.provider.DefaultDataTypeProvider
import uk.ac.ox.softeng.maurodatamapper.datamodel.provider.importer.DataModelImporterProviderService
import uk.ac.ox.softeng.maurodatamapper.plugins.database.calculation.CalculationStrategy
import uk.ac.ox.softeng.maurodatamapper.plugins.database.calculation.SamplingStrategy
import uk.ac.ox.softeng.maurodatamapper.plugins.database.query.QueryStringProvider
import uk.ac.ox.softeng.maurodatamapper.plugins.database.summarymetadata.AbstractIntervalHelper
import uk.ac.ox.softeng.maurodatamapper.plugins.database.summarymetadata.DateIntervalHelper
import uk.ac.ox.softeng.maurodatamapper.plugins.database.summarymetadata.SummaryMetadataHelper
import uk.ac.ox.softeng.maurodatamapper.security.User
import uk.ac.ox.softeng.maurodatamapper.util.Utils

import grails.util.Pair
import groovy.json.JsonOutput
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.commons.text.WordUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.util.regex.Pattern

@Slf4j
@CompileStatic
abstract class AbstractDatabaseDataModelImporterProviderService<S extends DatabaseDataModelImporterProviderServiceParameters>
    extends DataModelImporterProviderService<S> {

    static final String DATABASE_NAMESPACE = 'uk.ac.ox.softeng.maurodatamapper.plugins.database'
    static final String IS_NOT_NULL_CONSTRAINT = 'IS NOT NULL'

    public static final Logger SQL_LOGGER = LoggerFactory.getLogger('uk.ac.ox.softeng.maurodatamapper.plugins.database.sql')

    @Autowired
    DataClassService dataClassService

    @Autowired
    DataElementService dataElementService

    @Autowired
    EnumerationTypeService enumerationTypeService

    @Autowired
    PrimitiveTypeService primitiveTypeService

    @Autowired
    ReferenceTypeService referenceTypeService

    @Autowired
    AuthorityService authorityService

    @Autowired
    DataTypeService dataTypeService

    String schemaNameColumnName = 'table_schema'
    String dataTypeColumnName = 'data_type'
    String tableNameColumnName = 'table_name'
    String columnNameColumnName = 'column_name'
    String tableCatalogColumnName = 'table_catalog'
    String columnIsNullableColumnName = 'is_nullable'

    Collection<String> coreColumns = [
        schemaNameColumnName,
        dataTypeColumnName,
        tableNameColumnName,
        columnNameColumnName,
        tableCatalogColumnName,
    ]

    final QueryStringProvider queryStringProvider = createQueryStringProvider()

    abstract DefaultDataTypeProvider getDefaultDataTypeProvider()

    abstract QueryStringProvider createQueryStringProvider()

    @Override
    Boolean canImportMultipleDomains() {
        true
    }

    @Override
    DataModel importModel(User currentUser, DatabaseDataModelImporterProviderServiceParameters parameters)
        throws ApiException, ApiBadRequestException {
        final List<DataModel> imported = importModels(currentUser, parameters)
        imported ? imported.first() : null
    }

    @Override
    List<DataModel> importModels(User currentUser, DatabaseDataModelImporterProviderServiceParameters parameters)
        throws ApiException, ApiBadRequestException {
        final List<String> databaseNames = parameters.databaseNames.split(',').toList()
        log.info 'Importing {} DataModel(s)', databaseNames.size()

        final List<DataModel> dataModels = []
        databaseNames.each {String databaseName ->
            List<DataModel> importedModels = importDataModelsFromParameters(currentUser, databaseName, parameters as S)
            dataModels.addAll(importedModels)
        }
        dataModels
    }

    /**
     * Return the metadata namespace to be used when adding metadata to a column (DataElement).
     * Subclasses may override in order to provide a specific profile for column metadata.
     * @return String
     */
    String namespaceColumn() {
        namespace
    }

    /**
     * Return the metadata namespace to be used when adding metadata to a table (DataClass).
     * Subclasses may override in order to provide a specific profile for table metadata.
     * @return String
     */
    String namespaceTable() {
        namespace
    }

    /**
     * Return the metadata namespace to be used when adding metadata to a schema (DataClass).
     * Subclasses may override in order to provide a specific profile for schema metadata.
     * @return String
     */
    String namespaceSchema() {
        namespace
    }

    /**
     * Return the metadata namespace to be used when adding metadata to a database (DataModel).
     * Subclasses may override in order to provide a specific profile for database metadata.
     * @return String
     */
    String namespaceDatabase() {
        DATABASE_NAMESPACE
    }

    SamplingStrategy createSamplingStrategy(String schema, String table, S parameters) {
        new SamplingStrategy(schema, table)
    }

    CalculationStrategy createCalculationStrategy(S parameters) {
        new CalculationStrategy(parameters)
    }

    PreparedStatement prepareCoreStatement(Connection connection, S parameters) {
        connection.prepareStatement(getQueryStringProvider().databaseStructureQueryString)
    }

    List<DataModel> importDataModelsFromParameters(User currentUser, String databaseName, S parameters)
        throws ApiException, ApiBadRequestException {
        String modelName = databaseName
        if (!parameters.isMultipleDataModelImport()) modelName = parameters.modelName ?: modelName
        modelName = parameters.dataModelNameSuffix ? "${modelName}_${parameters.dataModelNameSuffix}" : modelName

        log.info 'Importing DataModel with from database {} with name {}', databaseName, modelName
        try {
            final Connection connection = getConnection(databaseName, parameters)
            final PreparedStatement preparedStatement = prepareCoreStatement(connection, parameters)
            final List<Map<String, Object>> results = executeStatement(preparedStatement)
            preparedStatement.close()

            log.debug 'Size of results from statement {}', results.size()
            if (!results) {
                log.warn 'No results from database statement, therefore nothing to import for {}.', modelName
                return []
            }

            final List<DataModel> dataModels = importAndUpdateDataModelsFromResults(
                currentUser, databaseName, parameters, Folder.get(parameters.folderId), modelName, results, connection)
            connection.close()
            dataModels
        } catch (SQLException e) {
            log.error 'Something went wrong executing statement while importing {}: {}', modelName, e.message
            throw new ApiBadRequestException('DIS03', 'Cannot execute statement', e)
        }
    }

    List<DataModel> importAndUpdateDataModelsFromResults(User currentUser, String databaseName, S parameters, Folder folder, String modelName,
                                                         List<Map<String, Object>> results, Connection connection) throws ApiException, SQLException {
        final DataModel dataModel = importDataModelFromResults(currentUser, folder, modelName, parameters.databaseDialect, results,
                                                               parameters.shouldImportSchemasAsDataClasses(),
                                                               parameters.getListOfTableRegexesToIgnore())
        if (parameters.dataModelNameSuffix) dataModel.aliasesString = databaseName

        if (parameters.detectEnumerations || parameters.calculateSummaryMetadata) {
            updateDataModelWithEnumerationsAndSummaryMetadata(currentUser, parameters, dataModel, connection)
        }

        updateDataModelWithDatabaseSpecificInformation(dataModel, connection)
        [dataModel]
    }

    DataModel importDataModelFromResults(User user, Folder folder, String modelName, String dialect, List<Map<String, Object>> results,
                                         boolean importSchemaAsDataClass, List<Pattern> tableRegexesToIgnore) throws ApiException {
        final DataModel dataModel = new DataModel(createdBy: user.emailAddress, label: modelName, type: DataModelType.DATA_ASSET, folder: folder,
                                                  authority: authorityService.getDefaultAuthority())
        addAliasIfSuitable(dataModel)
        dataModel.addToMetadata(namespace: namespaceDatabase(), key: 'dialect', value: dialect, createdBy: user.emailAddress)

        // Add any default datatypes provided by the implementing service
        if (defaultDataTypeProvider) {
            log.debug("Adding ${defaultDataTypeProvider.displayName} default DataTypes")
            dataTypeService.addDefaultListOfDataTypesToDataModel(dataModel, defaultDataTypeProvider.defaultListOfDataTypes)
        }

        results.each {Map<String, Object> row ->
            String tableName = row[tableNameColumnName] as String

            // If the tablename matches any of the ignore regexes then skip
            if (tableRegexesToIgnore.any {tableName.matches(it)}) return

            final DataType dataType = primitiveTypeService.findOrCreateDataTypeForDataModel(dataModel, row[dataTypeColumnName] as String, null, user)
            DataClass tableDataClass

            if (importSchemaAsDataClass) {
                DataClass schemaDataClass = dataClassService.findOrCreateDataClass(dataModel, row[schemaNameColumnName] as String, null, user)
                addAliasIfSuitable(schemaDataClass)
                tableDataClass = dataClassService.findOrCreateDataClass(schemaDataClass, tableName, null, user)
            } else {
                tableDataClass = dataClassService.findOrCreateDataClass(dataModel, tableName, null, user)
            }

            addAliasIfSuitable(tableDataClass)
            final int minMultiplicity = isColumnNullable(row[columnIsNullableColumnName] as String) ? 0 : 1
            final DataElement dataElement = dataElementService.findOrCreateDataElementForDataClass(
                tableDataClass, row[columnNameColumnName] as String, null, user, dataType, minMultiplicity, 1)
            addAliasIfSuitable(dataElement)
            row.findAll {String column, data ->
                data && !(column in coreColumns)
            }.each {String column, data ->
                dataElement.addToMetadata(namespace: namespaceColumn(), key: column, value: data.toString(), createdBy: user.emailAddress)
            }
        }

        dataModel
    }

    void updateDataModelWithEnumerationsAndSummaryMetadata(User user, S parameters, DataModel dataModel, Connection connection) {
        log.debug('Starting enumeration and summary metadata detection')
        long startTime = System.currentTimeMillis()
        CalculationStrategy calculationStrategy = createCalculationStrategy(parameters)
        dataModel.childDataClasses.sort().each {DataClass schemaClass ->
            schemaClass.dataClasses.sort().each {DataClass tableClass ->
                log.trace('Checking {}.{} for possible enumerations and summary metadata', schemaClass.label, tableClass.label)
                SamplingStrategy samplingStrategy = createSamplingStrategy(schemaClass.label, tableClass.label, parameters)
                if (samplingStrategy.requiresTableType()) {
                    samplingStrategy.tableType = getTableType(connection, tableClass.label, schemaClass.label, dataModel.label)
                }
                try {
                    // If SS needs the approx count then make the query, this can take a long time hence the reason to check if we need it
                    samplingStrategy.approxCount = samplingStrategy.requiresApproxCount() ? getApproxCount(connection, tableClass.label, schemaClass.label) : -1
                    if (samplingStrategy.dataExists()) {
                        calculateEnumerationsAndSummaryMetadata(dataModel, schemaClass, tableClass, calculationStrategy, samplingStrategy, connection, user)
                    } else {
                        log.warn('Not calculating enumerations and summary metadata in {}.{} as the table contains no data', schemaClass.label, tableClass.label)
                    }

                } catch (SQLException exception) {
                    log.warn('Could not perform enumeration or summary metadata detection on {}.{} because of {}', schemaClass.label, tableClass.label, exception.message)
                }
            }
        }

        log.debug('Finished enumeration and summary metadata detection in {}', Utils.timeTaken(startTime))
    }

    /**
     * Updates DataModel with custom information which is Database specific.
     * Default is to do nothing
     * @param dataModel DataModel to update
     * @param connection Connection to database
     */
    void updateDataModelWithDatabaseSpecificInformation(DataModel dataModel, Connection connection) throws ApiException, SQLException {
        addStandardConstraintInformation dataModel, connection
        addPrimaryKeyAndUniqueConstraintInformation dataModel, connection
        addIndexInformation dataModel, connection
        addForeignKeyInformation dataModel, connection
        addMetadata dataModel, connection
    }

    void calculateEnumerationsAndSummaryMetadata(DataModel dataModel, DataClass schemaClass, DataClass tableClass,
                                                 CalculationStrategy calculationStrategy, SamplingStrategy samplingStrategy,
                                                 Connection connection, User user) {

        log.debug('Calculating enumerations and summary metadata using {}', samplingStrategy)
        tableClass.dataElements.sort().each {DataElement dataElement ->
            DataType dt = dataElement.dataType

            //Enumeration detection
            if (calculationStrategy.shouldDetectEnumerations(dataElement.label, dt, samplingStrategy.approxCount)) {
                if (samplingStrategy.canDetectEnumerationValues()) {
                    boolean isEnumeration = detectEnumerationsForDataElement(calculationStrategy, samplingStrategy, connection,
                                                                             dataModel, schemaClass, tableClass, dataElement, user)

                    if (isEnumeration && calculationStrategy.computeSummaryMetadata) {
                        if (samplingStrategy.canComputeSummaryMetadata()) {
                            computeSummaryMetadataForEnumerations(calculationStrategy, samplingStrategy, connection, schemaClass, tableClass, dataElement, user)
                        } else {
                            logNotCalculatingSummaryMetadata(samplingStrategy, dataElement)
                        }
                    }
                } else {
                    logNotDetectingEnumerationValues(samplingStrategy, dataElement)
                }
            }

            //Summary metadata on dates and numbers
            else if (calculationStrategy.shouldComputeSummaryData(dataElement.label, dt)) {
                if (samplingStrategy.canComputeSummaryMetadata()) {
                    computeSummaryMetadataForDatesAndNumbers(calculationStrategy, samplingStrategy, connection, schemaClass, tableClass, dataElement, user)
                } else {
                    logNotCalculatingSummaryMetadata(samplingStrategy, dataElement)
                }
            }
        }
    }

    boolean detectEnumerationsForDataElement(CalculationStrategy calculationStrategy, SamplingStrategy samplingStrategy, Connection connection,
                                             DataModel dataModel, DataClass schemaClass, DataClass tableClass, DataElement dataElement, User user) {
        logEnumerationDetection(samplingStrategy, dataElement)

        // Make 1 call to get the distinct values, then use the size of the that results to tell if its actually an ET
        final List<Map<String, Object>> results = getDistinctColumnValues(connection, calculationStrategy, samplingStrategy, dataElement.label,
                                                                          tableClass.label, schemaClass?.label)
        if (calculationStrategy.isEnumerationType(results.size())) {

            EnumerationType enumerationType = enumerationTypeService.findOrCreateDataTypeForDataModel(dataModel, dataElement.label, dataElement.label, user)
            replacePrimitiveTypeWithEnumerationType(dataElement, dataElement.dataType, enumerationType, results)
            return true
        }
        false
    }

    void computeSummaryMetadataForEnumerations(CalculationStrategy calculationStrategy, SamplingStrategy samplingStrategy, Connection connection,
                                               DataClass schemaClass, DataClass tableClass, DataElement dataElement, User user) {
        logSummaryMetadataDetection(samplingStrategy, dataElement, 'enumeration')
        //Count enumeration values
        Map<String, Long> enumerationValueDistribution =
            getEnumerationValueDistribution(connection, samplingStrategy, dataElement.label, tableClass.label, schemaClass?.label)
        if (enumerationValueDistribution) {
            String description = 'Enumeration Value Distribution'
            if (samplingStrategy.useSamplingForSummaryMetadata()) {
                description = "Estimated Enumeration Value Distribution (calculated by sampling ${samplingStrategy.getSummaryMetadataSamplePercentage()}% of rows)"
            }
            SummaryMetadata enumerationSummaryMetadata =
                SummaryMetadataHelper.createSummaryMetadataFromMap(user, dataElement.label, description, calculationStrategy.calculationDateTime,
                                                                   enumerationValueDistribution)
            dataElement.addToSummaryMetadata(enumerationSummaryMetadata)

            SummaryMetadata enumerationSummaryMetadataOnTable =
                SummaryMetadataHelper.createSummaryMetadataFromMap(user, dataElement.label, description, calculationStrategy.calculationDateTime,
                                                                   enumerationValueDistribution)
            tableClass.addToSummaryMetadata(enumerationSummaryMetadataOnTable)
        }
    }

    void computeSummaryMetadataForDatesAndNumbers(CalculationStrategy calculationStrategy, SamplingStrategy samplingStrategy, Connection connection,
                                                  DataClass schemaClass, DataClass tableClass, DataElement dataElement, User user) {
        logSummaryMetadataDetection(samplingStrategy, dataElement, 'date or numeric')
        Pair minMax = getMinMaxColumnValues(connection, samplingStrategy, dataElement.label, tableClass.label, schemaClass?.label)

        //aValue is the MIN, bValue is the MAX. If they are not null then calculate the range etc...
        if (minMax.aValue != null && minMax.bValue != null) {
            DataType dt = dataElement.dataType
            AbstractIntervalHelper intervalHelper = calculationStrategy.getIntervalHelper(dt, minMax)
            log.trace('Summary Metadata computation using {}', intervalHelper)
            Map<String, Long> valueDistribution = getColumnRangeDistribution(connection, samplingStrategy, dt, intervalHelper, dataElement.label,
                                                                             tableClass.label, schemaClass?.label)
            if (valueDistribution) {
                String description = 'Value Distribution'
                if (samplingStrategy.useSamplingForSummaryMetadata()) {
                    description = "Estimated Value Distribution (calculated by sampling ${samplingStrategy.getSummaryMetadataSamplePercentage()}% of rows)"
                }

                // Dates can produce a lot of buckets and a lot of empty buckets
                if (calculationStrategy.isColumnForDateSummary(dt) && (intervalHelper as DateIntervalHelper).needToMergeOrRemoveEmptyBuckets) {
                    log.debug('Need to merge or remove empty buckets from value distribution with current bucket size {}', valueDistribution.size())
                    switch (calculationStrategy.dateBucketHandling) {
                        case CalculationStrategy.BucketHandling.MERGE:
                            valueDistribution = mergeDateBuckets(valueDistribution, false)
                            break
                        case CalculationStrategy.BucketHandling.MERGE_RELATIVE_SMALL:
                            valueDistribution = mergeDateBuckets(valueDistribution, true)
                            break
                        case CalculationStrategy.BucketHandling.REMOVE:
                            // Remove all buckets with no counts
                            log.trace('Removing date buckets with no counts')
                            valueDistribution.removeAll {k, v -> !v}
                            break
                    }
                    log.debug('Value distribution bucket size reduced to {}', valueDistribution.size())
                }

                SummaryMetadata summaryMetadata =
                    SummaryMetadataHelper.createSummaryMetadataFromMap(user, dataElement.label, description, calculationStrategy.calculationDateTime,
                                                                       valueDistribution)
                dataElement.addToSummaryMetadata(summaryMetadata)

                SummaryMetadata summaryMetadataOnTable =
                    SummaryMetadataHelper.createSummaryMetadataFromMap(user, dataElement.label, description, calculationStrategy.calculationDateTime,
                                                                       valueDistribution)
                tableClass.addToSummaryMetadata(summaryMetadataOnTable)
            }
        }
    }

    void addStandardConstraintInformation(DataModel dataModel, Connection connection) throws ApiException, SQLException {
        if (!queryStringProvider.standardConstraintInformationQueryString()) return
        dataModel.childDataClasses.each {DataClass schemaClass ->
            final List<Map<String, Object>> results = executePreparedStatement(
                dataModel, schemaClass, connection, queryStringProvider.standardConstraintInformationQueryString())
            results.each {Map<String, Object> row ->
                final DataClass tableClass = schemaClass.findDataClass(row.table_name as String)
                if (!tableClass) return

                final String constraint = row.check_clause && (row.check_clause as String).contains(IS_NOT_NULL_CONSTRAINT) ?
                                          IS_NOT_NULL_CONSTRAINT : null
                if (constraint && constraint != IS_NOT_NULL_CONSTRAINT) {
                    log.warn 'Unhandled constraint {}', constraint
                }
            }
        }
    }

    void addPrimaryKeyAndUniqueConstraintInformation(DataModel dataModel, Connection connection) throws ApiException, SQLException {
        if (!queryStringProvider.primaryKeyAndUniqueConstraintInformationQueryString()) return
        dataModel.childDataClasses.each {DataClass schemaClass ->
            final List<Map<String, Object>> results = executePreparedStatement(
                dataModel, schemaClass, connection, queryStringProvider.primaryKeyAndUniqueConstraintInformationQueryString())
            results
                .groupBy {it.constraint_name}
                .each {constraintName, List<Map<String, Object>> rows ->
                    final Map<String, Object> firstRow = rows.head()
                    final DataClass tableClass = schemaClass.findDataClass(firstRow.table_name as String)
                    if (!tableClass) return

                    final String constraintTypeName = (firstRow.constraint_type as String).toLowerCase().replaceAll(/ /, '_')
                    final String constraintTypeColumns = rows.size() == 1 ?
                                                         firstRow.column_name :
                                                         rows.sort {it.ordinal_position}.collect {it.column_name}.join(', ')
                    final String constraintKeyName = firstRow.constraint_name.toString()

                    tableClass.addToMetadata(namespaceTable(), "${constraintTypeName}_name", constraintKeyName, dataModel.createdBy)
                    tableClass.addToMetadata(namespaceTable(), "${constraintTypeName}_columns", constraintTypeColumns, dataModel.createdBy)

                    rows.each {Map<String, Object> row ->
                        final DataElement columnElement = tableClass.findDataElement(row.column_name as String)
                        if (columnElement) {
                            columnElement.addToMetadata(namespaceColumn(), (row.constraint_type as String).toLowerCase(), row.ordinal_position as String, dataModel.createdBy)
                        }
                    }
                }
        }
    }

    void addIndexInformation(DataModel dataModel, Connection connection) throws ApiException, SQLException {
        if (!queryStringProvider.indexInformationQueryString) return
        dataModel.childDataClasses.each {DataClass schemaClass ->
            addIndexInformation(dataModel, schemaClass, connection)
        }
    }

    void addIndexInformation(DataModel dataModel, DataClass schemaClass, Connection connection) throws ApiException, SQLException {
        final List<Map<String, Object>> results = executePreparedStatement(dataModel, schemaClass, connection, queryStringProvider.indexInformationQueryString)

        results.groupBy {it.table_name}.each {tableName, rows ->
            final DataClass tableClass = schemaClass ? schemaClass.findDataClass(tableName as String) : dataModel.dataClasses.find {(it.label == tableName as String)}
            if (!tableClass) {
                log.warn 'Could not add indexes as DataClass for table {} does not exist', tableName
                return
            }

            List<Map> indexes = rows.collect {row ->
                [name          : (row.index_name as String).trim(),
                 columns       : (row.column_names as String).trim(),
                 primaryIndex  : getBooleanValue(row.primary_index),
                 uniqueIndex   : getBooleanValue(row.unique_index),
                 clusteredIndex: getBooleanValue(row.clustered),
                ]
            } as List<Map>

            tableClass.addToMetadata(namespaceTable(), 'indexes', JsonOutput.prettyPrint(JsonOutput.toJson(indexes)), dataModel.createdBy)
        }
    }

    void addForeignKeyInformation(DataModel dataModel, Connection connection) throws ApiException, SQLException {
        if (!queryStringProvider.foreignKeyInformationQueryString) return
        dataModel.childDataClasses.each {DataClass schemaClass ->
            addForeignKeyInformation(dataModel, schemaClass, connection)
        }
    }

    void addForeignKeyInformation(DataModel dataModel, DataClass schemaClass, Connection connection) throws ApiException, SQLException {
        final List<Map<String, Object>> results = executePreparedStatement(dataModel, schemaClass, connection, queryStringProvider.foreignKeyInformationQueryString)
        results.each {Map<String, Object> row ->
            final DataClass foreignTableClass = dataModel.dataClasses.find {DataClass dataClass -> dataClass.label == row.reference_table_name}
            DataType dataType

            if (foreignTableClass) {
                dataType = referenceTypeService.findOrCreateDataTypeForDataModel(
                    dataModel, "${foreignTableClass.label}Type", "Linked to DataElement [${row.reference_column_name}]",
                    dataModel.createdBy, foreignTableClass)
                dataModel.addToDataTypes dataType
            } else {
                dataType = primitiveTypeService.findOrCreateDataTypeForDataModel(
                    dataModel, "${row.reference_table_name}Type",
                    "Missing link to foreign key table [${row.reference_table_name}.${row.reference_column_name}]",
                    dataModel.createdBy)
            }

            final DataClass tableClass = schemaClass ? schemaClass.findDataClass(row.table_name as String) : dataModel.dataClasses.find {it.label == row.table_name as String}
            final DataElement columnElement = tableClass.findDataElement(row.column_name as String)
            columnElement.dataType = dataType
            columnElement.addToMetadata(namespaceColumn(), "foreign_key_name", row.constraint_name as String, dataModel.createdBy)
            columnElement.addToMetadata(namespaceColumn(), "foreign_key_columns", row.reference_column_name as String, dataModel.createdBy)
        }

    }

    /**
     * Subclasses may override this method and use vendor specific queries to add metadata / extended properties /
     * comments to the DataModel and its constituent parts.
     * @param dataModel
     * @param connection
     */
    void addMetadata(DataModel dataModel, Connection connection) {
    }

    Connection getConnection(String databaseName, S parameters) throws ApiException, ApiBadRequestException {
        try {
            parameters.getDataSource(databaseName).getConnection(parameters.databaseUsername, parameters.databasePassword)
        } catch (SQLException e) {
            log.error 'Cannot connect to database [{}]: {}', parameters.getUrl(databaseName), e.message
            throw new ApiBadRequestException('DIS02', "Cannot connect to database [${parameters.getUrl(databaseName)}]", e)
        }
    }

    List<Map<String, Object>> executePreparedStatement(DataModel dataModel, Connection connection,
                                                       String queryString) {
        executePreparedStatement(dataModel, null, connection, queryString)
    }


    List<Map<String, Object>> executePreparedStatement(DataModel dataModel, DataClass schemaClass, Connection connection,
                                                       String queryString) throws ApiException, SQLException {
        List<Map<String, Object>> results = null
        try {
            final PreparedStatement preparedStatement = connection.prepareStatement(queryString)
            preparedStatement.setString(1, schemaClass?.label ?: dataModel.label)
            results = executeStatement(preparedStatement)
            preparedStatement.close()
        } catch (SQLException e) {
            if (e.message.contains('Invalid object name \'information_schema.table_constraints\'')) {
                log.warn 'No table_constraints available for {}', dataModel.label
            } else throw e
        }
        results
    }

    /**
     * Get table type (BASE TABLE or VIEW) for an object
     * @param connection
     * @param tableName
     * @param schemaName
     * @return
     */
    String getTableType(Connection connection, String tableName, String schemaName, String modelName) {

        String tableType = ""
        final PreparedStatement preparedStatement = connection.prepareStatement(queryStringProvider.tableTypeQueryString())
        preparedStatement.setString(1, modelName)
        preparedStatement.setString(2, schemaName)
        preparedStatement.setString(3, tableName)
        List<Map<String, Object>> results = executeStatement(preparedStatement)

        if (results && results[0].table_type != null) {
            tableType = (String) results[0].table_type
        }

        return tableType
    }

    /**
     * Iterate candidate query strings and return the first not-null approximate count found.
     * If there are no not-null results then return 0. We do this in case queries return null
     * due to a lack of statistics.
     * @param connection
     * @param tableName
     * @param schemaName
     * @return
     */
    Long getApproxCount(Connection connection, String tableName, String schemaName = null) {
        log.trace("Starting getApproxCouunt query for ${tableName}")
        long startTime = System.currentTimeMillis()
        Long approxCount = 0
        List<String> queryStrings = queryStringProvider.approxCountQueryString(tableName, schemaName)
        for (String queryString : queryStrings) {
            SQL_LOGGER.trace('\n{}', queryString)
            PreparedStatement preparedStatement = connection.prepareStatement(queryString)
            List<Map<String, Object>> results = executeStatement(preparedStatement)

            if (results && results[0].approx_count != null) {
                approxCount = (Long) results[0].approx_count
                break
            }
        }
        long tt = System.currentTimeMillis() - startTime
        if (tt > 1000) log.warn('Finished getApproxCount query for {} in {}', tableName, Utils.timeTaken(startTime))
        log.trace('Finished getApproxCount query for {} in {}', tableName, Utils.timeTaken(startTime))

        return approxCount
    }

    private List<Map<String, Object>> getDistinctColumnValues(Connection connection, CalculationStrategy calculationStrategy, SamplingStrategy samplingStrategy,
                                                              String columnName, String tableName, String schemaName) {
        log.trace("Starting getDistinctColumnValues query for ${tableName}.${columnName}")
        long startTime = System.currentTimeMillis()
        String queryString = queryStringProvider.distinctColumnValuesQueryString(calculationStrategy, samplingStrategy, columnName, tableName, schemaName)
        SQL_LOGGER.trace('\n{}', queryString)
        final PreparedStatement preparedStatement = connection.prepareStatement(queryString)
        final List<Map<String, Object>> results = executeStatement(preparedStatement)
        log.trace("Finished getDistinctColumnValues query for ${tableName}.${columnName} in {}", Utils.timeTaken(startTime))
        results
    }

    private Pair getMinMaxColumnValues(Connection connection, SamplingStrategy samplingStrategy, String columnName, String tableName, String schemaName) {
        log.trace("Starting getMinMaxColumnValues query for ${tableName}.${columnName}")
        long startTime = System.currentTimeMillis()
        String queryString = queryStringProvider.minMaxColumnValuesQueryString(samplingStrategy, columnName, tableName, schemaName)
        SQL_LOGGER.trace('\n{}', queryString)
        final PreparedStatement preparedStatement = connection.prepareStatement(queryString)
        final List<Map<String, Object>> results = executeStatement(preparedStatement)

        log.trace("Finished getMinMaxColumnValues query for ${tableName}.${columnName} in {}", Utils.timeTaken(startTime))

        new Pair(results[0].min_value, results[0].max_value)
    }

    private Map<String, Long> getColumnRangeDistribution(Connection connection, SamplingStrategy samplingStrategy,
                                                         DataType dataType, AbstractIntervalHelper intervalHelper,
                                                         String columnName, String tableName, String schemaName) {
        log.trace("Starting getColumnRangeDistribution query for ${tableName}.${columnName}")
        long startTime = System.currentTimeMillis()
        String queryString = queryStringProvider.columnRangeDistributionQueryString(samplingStrategy, dataType, intervalHelper, columnName, tableName, schemaName)
        SQL_LOGGER.trace('\n{}', queryString)
        final PreparedStatement preparedStatement = connection.prepareStatement(queryString)
        List<Map<String, Object>> results = executeStatement(preparedStatement)
        preparedStatement.close()
        log.trace("Finished getColumnRangeDistribution query for ${tableName}.${columnName} in {}", Utils.timeTaken(startTime))

        results.collectEntries {
            [(it.interval_label): it.interval_count]
        }
    }

    private Map<String, Long> getEnumerationValueDistribution(Connection connection, SamplingStrategy samplingStrategy,
                                                              String columnName, String tableName, String schemaName) {
        log.trace("Starting getEnumerationValueDistribution query for ${tableName}.${columnName}")
        long startTime = System.currentTimeMillis()
        String queryString = queryStringProvider.enumerationValueDistributionQueryString(samplingStrategy, columnName, tableName, schemaName)
        SQL_LOGGER.trace('\n{}', queryString)
        final PreparedStatement preparedStatement = connection.prepareStatement(queryString)
        List<Map<String, Object>> results = executeStatement(preparedStatement)
        preparedStatement.close()
        log.trace("Finished getEnumerationValueDistribution query for ${tableName}.${columnName} in {}", Utils.timeTaken(startTime))

        results.collectEntries {
            // Convert a null key to string 'NULL'. There is a risk of collision with a string value 'NULL'
            // from the database.
            it.enumeration_value != null ? [(it.enumeration_value): it.enumeration_count] : ['NULL': it.enumeration_count]
        }
    }

    private void logSummaryMetadataDetection(SamplingStrategy samplingStrategy, DataElement de, String type) {
        if (samplingStrategy.useSamplingForSummaryMetadata()) {
            log.debug('Performing {} summary metadata detection for column {} (calculated by sampling {}% of rows)', type, de.label,
                      samplingStrategy.getSummaryMetadataSamplePercentage())
        } else {
            log.debug('Performing {} summary metadata detection for column {}', type, de.label)
        }
    }

    private void logEnumerationDetection(SamplingStrategy samplingStrategy, DataElement de) {
        if (samplingStrategy.useSamplingForEnumerationValues()) {
            log.debug('Performing enumeration detection for column {} (calculated by sampling {}% of rows)', de.label, samplingStrategy.getEnumerationValueSamplePercentage())
        } else {
            log.debug('Performing enumeration detection for column {}', de.label)
        }
    }

    private void logNotCalculatingSummaryMetadata(SamplingStrategy samplingStrategy, DataElement de) {
        log.warn(
            'Not calculating summary metadata for {} as rowcount {} is above threshold {} and we cannot use sampling',
            de.label, samplingStrategy.approxCount, samplingStrategy.smThreshold)
    }

    private void logNotDetectingEnumerationValues(SamplingStrategy samplingStrategy, DataElement de) {
        log.warn(
            'Not detecting enumerations for {} as rowcount {} is above threshold {} and we cannot use sampling',
            de.label, samplingStrategy.approxCount, samplingStrategy.evThreshold)
    }

    private void replacePrimitiveTypeWithEnumerationType(DataElement de, DataType primitiveType, EnumerationType enumerationType,
                                                         List<Map<String, Object>> results) {
        log.debug('Replacing {} with an EnumerationType', de.label)
        results.each {
            String val = (it.distinct_value as String)?.trim()
            //null is not a value, so skip it
            if (val) {
                enumerationType.addToEnumerationValues(new EnumerationValue(key: val, value: val))
            }
        }

        primitiveType.removeFromDataElements(de)
        de.dataType = enumerationType
    }


    private Map<String, Long> mergeDateBuckets(Map<String, Long> valueDistribution, boolean mergeRelativeSmallBuckets) {
        log.trace('Merging date buckets')
        Map<Pair<Integer, Integer>, Long> processing = valueDistribution.collectEntries {yearRange, value ->
            String[] split = yearRange.split('-')
            [new Pair<>(Integer.parseInt(split[0].trim()), Integer.parseInt(split[1].trim())), value]
        }
        List<Long> values = valueDistribution.collect {it.value}.findAll()
        long mergeValue = 0
        if (mergeRelativeSmallBuckets && values.min() > 0) {
            // check for a sensible merge value
            // if we have lots of values that are massive we should merge tiny values along with 0s otherwise the barcharts will look empty
            List<Integer> digitCount = values.collect {it.toString().length()}.sort()
            int size = digitCount.size()
            int medianPoint = size % 2 == 0 ? (size / 2).intValue() : (size / 2).round().intValue()
            int avgDigits = (digitCount.average() as Number).intValue()
            int median = digitCount[medianPoint]
            mergeValue = median > avgDigits ? Math.pow(10, avgDigits - 1).toLong() : Math.pow(10, median).toLong()
        }

        Map<Pair<Integer, Integer>, Long> merged = [:]
        processing.each {range, value ->
            def previous = merged.find {r, v -> r.bValue == range.aValue}
            if (previous) {
                if (mergeValue && previous.value < mergeValue && value < mergeValue) {
                    merged.remove(previous.key)
                    merged[new Pair<>(previous.key.aValue, range.bValue)] = previous.value + value
                } else if (previous?.value == 0 && value == 0) {
                    merged.remove(previous.key)
                    merged[new Pair<>(previous.key.aValue, range.bValue)] = 0L
                } else {
                    merged[range] = value
                }
            } else {
                merged[range] = value
            }
        }

        merged.collectEntries {range, value ->
            ["${range.aValue} - ${range.bValue}".toString(), value]
        } as Map<String, Long>
    }

    private Map<String, Map<String, List<String>>> getTableNamesToImport(S parameters) {
        if (!parameters.onlyImportTables) return [:]
        boolean multipleDatabaseImport = parameters.isMultipleDataModelImport()
        List<String[]> tableNames = parameters.onlyImportTables.split(',').collect {it.split(/\./)}

        Map<String, Map<String, List<String>>> mappedTableNames
        if (multipleDatabaseImport) {
            if (tableNames.any {it.size() != 3}) {
                log.warn('Attempt to only import specific tables but using multi-database import and names are not in database.schema.table format')
                return [:]
            }
            mappedTableNames = [:]
        } else {
            if (tableNames.any {it.size() < 2}) {
                log.warn('Attempt to only import specific tables but not enough information provided and names are not in (database.)schema.table format')
                return [:]
            }
            mappedTableNames = [default: [:]] as Map<String, Map<String, List<String>>>
        }

        tableNames.each {parts ->
            Map<String, List<String>> dbListing
            List<String> schemaListing
            if (parts.size() == 3) {
                dbListing = mappedTableNames.getOrDefault(parts[0], [:])
                schemaListing = dbListing.getOrDefault(parts[1], [])
                schemaListing.add(parts[2])
            } else {
                dbListing = mappedTableNames.default
                schemaListing = dbListing.getOrDefault(parts[0], [])
                schemaListing.add(parts[1])
            }
        }

        mappedTableNames
    }

    static List<Map<String, Object>> executeStatement(PreparedStatement preparedStatement) throws ApiException, SQLException {
        final List<Map<String, Object>> results = new ArrayList(50)
        final ResultSet resultSet = preparedStatement.executeQuery()
        final ResultSetMetaData resultSetMetaData = resultSet.metaData
        final int columnCount = resultSetMetaData.columnCount

        while (resultSet.next()) {
            final Map<String, Object> row = new HashMap(columnCount)
            (1..columnCount).each {int i ->
                row[resultSetMetaData.getColumnLabel(i).toLowerCase()] = resultSet.getObject(i)
            }
            results << row
        }
        resultSet.close()
        results
    }

    static void addAliasIfSuitable(CatalogueItem catalogueItem) {
        String alias = WordUtils.capitalizeFully(catalogueItem.label.split('_').join(' '))
        if (alias.toLowerCase() != catalogueItem.label.toLowerCase()) {
            catalogueItem.aliases.add(alias)
        }
    }

    static boolean isColumnNullable(String nullableColumnValue) {
        nullableColumnValue.toLowerCase() == 'yes'
    }


    static boolean getBooleanValue(def value) {
        value.toString().toBoolean()
    }

}
