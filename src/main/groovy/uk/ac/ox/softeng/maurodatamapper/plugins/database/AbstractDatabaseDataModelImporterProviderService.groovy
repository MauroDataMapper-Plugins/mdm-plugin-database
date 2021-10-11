/*
 * Copyright 2020-2021 University of Oxford and Health and Social Care Information Centre, also known as NHS Digital
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

import grails.util.Pair
import uk.ac.ox.softeng.maurodatamapper.api.exception.ApiBadRequestException
import uk.ac.ox.softeng.maurodatamapper.api.exception.ApiException
import uk.ac.ox.softeng.maurodatamapper.core.authority.AuthorityService
import uk.ac.ox.softeng.maurodatamapper.core.container.Folder
import uk.ac.ox.softeng.maurodatamapper.datamodel.DataModel
import uk.ac.ox.softeng.maurodatamapper.datamodel.DataModelType
import uk.ac.ox.softeng.maurodatamapper.datamodel.facet.SummaryMetadata
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.DataClass
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.DataClassService
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.DataElement
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.DataElementService
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.datatype.DataType
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.datatype.EnumerationType
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.datatype.EnumerationTypeService
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.datatype.enumeration.EnumerationValue
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.datatype.PrimitiveTypeService
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.datatype.ReferenceTypeService
import uk.ac.ox.softeng.maurodatamapper.datamodel.provider.importer.DataModelImporterProviderService
import uk.ac.ox.softeng.maurodatamapper.plugins.database.summarymetadata.AbstractIntervalHelper
import uk.ac.ox.softeng.maurodatamapper.plugins.database.summarymetadata.DateIntervalHelper
import uk.ac.ox.softeng.maurodatamapper.plugins.database.summarymetadata.DecimalIntervalHelper
import uk.ac.ox.softeng.maurodatamapper.plugins.database.summarymetadata.IntegerIntervalHelper
import uk.ac.ox.softeng.maurodatamapper.plugins.database.summarymetadata.LongIntervalHelper
import uk.ac.ox.softeng.maurodatamapper.plugins.database.summarymetadata.SummaryMetadataHelper
import uk.ac.ox.softeng.maurodatamapper.security.User

import groovy.json.JsonOutput
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Autowired
import uk.ac.ox.softeng.maurodatamapper.util.Utils

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException


@Slf4j
@CompileStatic
abstract class AbstractDatabaseDataModelImporterProviderService<S extends DatabaseDataModelImporterProviderServiceParameters>
    extends DataModelImporterProviderService<S> {

    static final String DATABASE_NAMESPACE = 'uk.ac.ox.softeng.maurodatamapper.plugins.database'
    static final String IS_NOT_NULL_CONSTRAINT = 'IS NOT NULL'
    static final Integer MAX_ENUMERATIONS = 20
    static final Integer DEFAULT_SAMPLE_THRESHOLD = 0
    static final BigDecimal DEFAULT_SAMPLE_PERCENTAGE = 1

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

    SamplingStrategy getSamplingStrategy(S parameters) {
        new SamplingStrategy()
    }

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

    /**
     * Must return a String which will be queryable by schema name,
     * and return a row with the following elements:
     *  * table_name
     *  * check_clause (the constraint information)
     * @return Query string for standard constraint information
     */
    String standardConstraintInformationQueryString = '''
            SELECT
              tc.table_name,
              cc.check_clause
            FROM information_schema.table_constraints tc
              INNER JOIN information_schema.check_constraints cc ON tc.constraint_name = cc.constraint_name
            WHERE tc.constraint_schema = ?;
            '''.stripIndent()

    /**
     * Must return a String which will be queryable by schema name,
     * and return a row with the following elements:
     *  * constraint_name
     *  * table_name
     *  * constraint_type (primary_key or unique)
     *  * column_name
     *  * ordinal_position
     * @return Query string for primary key and unique constraint information
     */
    String primaryKeyAndUniqueConstraintInformationQueryString = '''
            SELECT
              tc.constraint_name,
              tc.table_name,
              tc.constraint_type,
              kcu.column_name,
              kcu.ordinal_position
            FROM
              information_schema.table_constraints AS tc
              LEFT JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name
            WHERE
              tc.constraint_schema = ?
              AND constraint_type NOT IN ('FOREIGN KEY', 'CHECK');
            '''.stripIndent()

    /**
     * Must return a String which will be queryable by schema name,
     * and return a row with the following elements:
     *  * table_name
     *  * index_name
     *  * unique_index (boolean)
     *  * primary_index (boolean)
     *  * clustered (boolean)
     *  * column_names
     * @return Query string for index information
     */
    abstract String getIndexInformationQueryString()

    /**
     * Must return a String which will be queryable by schema name,
     * and return a row with the following elements:
     *  * constraint_name
     *  * table_name
     *  * column_name
     *  * reference_table_name
     *  * reference_column_name
     * @return Query string for foreign key information
     */
    abstract String getForeignKeyInformationQueryString()

    abstract String getDatabaseStructureQueryString()

    /**
     * Must return a String which will be queryable by column name, table name
     * and optionally schema name, and return a row with the following columns:
     *  * count
     *
     * @return Query string for count of distinct values in a column
     */
    String countDistinctColumnValuesQueryString(String columnName, String tableName, String schemaName = null) {
        String schemaIdentifier = schemaName ? "${escapeIdentifier(schemaName)}." : ""
        "SELECT COUNT(DISTINCT(${escapeIdentifier(columnName)})) AS count FROM ${schemaIdentifier}${escapeIdentifier(tableName)}"
    }

    /**
     * Must return a String which will be queryable by column name, table name
     * and optionally schema name, and return a row with the following columns:
     *  * count
     * and preferably apply a sampling strategy.
     *
     * The base method returns a query that does not use any sampling; subclasses which
     * support sampling should override with vendor specific SQL.
     *
     * @return Query string for count of distinct values in a column
     */
    String countDistinctColumnValuesQueryString(SamplingStrategy samplingStrategy, String columnName, String tableName, String schemaName = null) {
        countDistinctColumnValuesQueryString(columnName, tableName, schemaName)
    }

    /**
     * Must return a String which will be queryable by column name, table name
     * and optionally schema name, and return rows with the following columns:
     *  * distinct_value
     *
     * @return Query string for distinct values in a column
     */
    String distinctColumnValuesQueryString(String columnName, String tableName, String schemaName = null) {
        String schemaIdentifier = schemaName ? "${escapeIdentifier(schemaName)}." : ""
        "SELECT DISTINCT(${escapeIdentifier(columnName)}) AS distinct_value FROM ${schemaIdentifier}${escapeIdentifier(tableName)}"
    }

    /**
     * Must return a String which will be queryable by column name, table name
     * and optionally schema name, and return rows with the following columns:
     *  * distinct_value
     *
     * and preferably apply a sampling strategy.
     *
     * The base method returns a query that does not use any sampling; subclasses which
     * support sampling should override with vendor specific SQL.
     *
     * @return Query string for distinct values in a column
     */
    String distinctColumnValuesQueryString(SamplingStrategy samplingStrategy, String columnName, String tableName, String schemaName = null) {
        distinctColumnValuesQueryString(columnName, tableName, schemaName)
    }

    /**
     * Escape an identifier. Subclasses can override and using vendor specific syntax.
     * @param identifier
     * @return The escaped identifier
     */
    String escapeIdentifier(String identifier) {
        identifier
    }

    /**
     * Does the dataType represent a column that should be checked as a possible enumeration?
     * Subclasses can override and use database specific types e.g char/varchar or
     * character/character varying
     * @param dataType
     * @return boolean
     */
    boolean isColumnPossibleEnumeration(DataType dataType) {
        false
    }

    /**
     * Does the dataType represent a column that should be summarised as a date?
     * Subclasses can override and use database specific types.
     * @param dataType
     * @return boolean
     */
    boolean isColumnForDateSummary(DataType dataType) {
        false
    }

    /**
     * Does the dataType represent a column that should be summarised as a decimal?
     * Subclasses can override and use database specific types.
     * @param dataType
     * @return boolean
     */
    boolean isColumnForDecimalSummary(DataType dataType) {
        false
    }

    /**
     * Does the dataType represent a column that should be summarised as an integer?
     * Subclasses can override and use database specific types.
     * @param dataType
     * @return boolean
     */
    boolean isColumnForIntegerSummary(DataType dataType) {
        false
    }

    /**
     * Does the dataType represent a column that should be summarised as a long?
     * Subclasses can override and use database specific types.
     * @param dataType
     * @return boolean
     */
    boolean isColumnForLongSummary(DataType dataType) {
        false
    }

    /**
     * Must return a List of Strings, each of which will be queryable by table name
     * and optionally schema name, and return rows with the following elements:
     *  * approx_count
     *
     * The base implementation returns a single COUNT(*) query, which is vendor neutral,
     * returns an exact row count, and is likely to be slow on large tables. Subclasses should
     * override this method and push to the front of the list alternative faster implementations
     * for an approximate count, using vendor specific queries as appropriate. Some such
     * queries may return null values (for example if statistics have not been calculated
     * on the relevant table); if a query does return a null count, the next query
     * in the list is executed.
     *
     * @return Query string for approximate count of rows in a table
     */
    List<String> approxCountQueryString(String tableName, String schemaName = null) {
        String schemaIdentifier = schemaName ? "${escapeIdentifier(schemaName)}." : ""
        [
            "SELECT COUNT(*) AS approx_count FROM ${schemaIdentifier}${escapeIdentifier(tableName)}".toString()
        ]
    }

    /**
     * Must return a String which will be queryable by table name, schema name, and model name,
     * and return one row with the following elements:
     *  * table_type
     *
     *
     * @return Query string for table type
     */
    String tableTypeQueryString() {
        "SELECT TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = ? AND TABLE_SCHEMA = ? AND TABLE_NAME = ?"
    }

    /**
     * Must return a String which will be queryable by column name, table name
     * and optionally schema name, and return rows with the following elements:
     *  * min_value
     *  * max_value
     *
     * @return Query string for distinct values in a column
     */
    String minMaxColumnValuesQueryString(String columnName, String tableName, String schemaName = null) {
        String schemaIdentifier = schemaName ? "${escapeIdentifier(schemaName)}." : ""
        "SELECT MIN(${escapeIdentifier(columnName)}) AS min_value, MAX(${escapeIdentifier(columnName)}) AS max_value FROM ${schemaIdentifier}${escapeIdentifier(tableName)}"
    }

    /**
     * Must return a String which will be queryable by column name, table name
     * and optionally schema name, and return rows with the following elements:
     *  * min_value
     *  * max_value
     *
     * and use an appropriate sampling strategy.
     *
     * The base method does not use sampling, and should be overridden by subclasses where possible.
     * @return Query string for distinct values in a column
     */
    String minMaxColumnValuesQueryString(SamplingStrategy samplingStrategy, String columnName, String tableName, String schemaName = null) {
        minMaxColumnValuesQueryString(columnName, tableName, schemaName)
    }

    /**
     * Must return a String which will be queryable by table name
     * and column name, and return rows with the following elements:
     *  * interval_start
     *  * interval_end
     *  * interval_count i.e count of values in the range interval_start to interval_end
     *
     *  interval_start is inclusive. interval_end is exclusive. interval_count is the
     *  count of values in the interval. Rows must be ordered by interval_start ascending.
     *
     *  Subclasses must implement this method using vendor specific SQL as necessary.
     *
     * @return Query string for count by interval
     */
    abstract String columnRangeDistributionQueryString(DataType dataType,
                                                       AbstractIntervalHelper intervalHelper,
                                                       String columnName, String tableName, String schemaName)


    String columnRangeDistributionQueryString(SamplingStrategy samplingStrategy, DataType dataType,
                                              AbstractIntervalHelper intervalHelper,
                                              String columnName, String tableName, String schemaName)  {
        columnRangeDistributionQueryString(dataType, intervalHelper, columnName, tableName, schemaName)
    }

    boolean isColumnNullable(String nullableColumnValue) {
        nullableColumnValue.toLowerCase() == 'yes'
    }

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

    @SuppressWarnings('unused')
    PreparedStatement prepareCoreStatement(Connection connection, S parameters) {
        connection.prepareStatement(databaseStructureQueryString)
    }

    DataModel importDataModelFromResults(User user, Folder folder, String modelName, String dialect, List<Map<String, Object>> results,
                                         boolean importSchemaAsDataClass) throws ApiException {
        final DataModel dataModel = new DataModel(createdBy: user.emailAddress, label: modelName, type: DataModelType.DATA_ASSET, folder: folder,
                                                  authority: authorityService.getDefaultAuthority())
        dataModel.addToMetadata(namespace: DATABASE_NAMESPACE, key: 'dialect', value: dialect, createdBy: user.emailAddress)

        results.each {Map<String, Object> row ->
            final DataType dataType = primitiveTypeService.findOrCreateDataTypeForDataModel(dataModel, row[dataTypeColumnName] as String, null, user)
            DataClass tableDataClass

            if (importSchemaAsDataClass) {
                DataClass schemaDataClass = dataClassService.findOrCreateDataClass(dataModel, row[schemaNameColumnName] as String, null, user)
                tableDataClass = dataClassService.findOrCreateDataClass(schemaDataClass, row[tableNameColumnName] as String, null, user)
            } else {
                tableDataClass = dataClassService.findOrCreateDataClass(dataModel, row[tableNameColumnName] as String, null, user)
            }

            final int minMultiplicity = isColumnNullable(row[columnIsNullableColumnName] as String) ? 0 : 1
            final DataElement dataElement = dataElementService.findOrCreateDataElementForDataClass(
                tableDataClass, row[columnNameColumnName] as String, null, user, dataType, minMultiplicity, 1)

            row.findAll {String column, data ->
                data && !(column in coreColumns)
            }.each {String column, data ->
                dataElement.addToMetadata(namespace: namespace, key: column, value: data.toString(), createdBy: user.emailAddress)
            }
        }

        dataModel
    }

    List<DataModel> importAndUpdateDataModelsFromResults(User currentUser, String databaseName, S parameters, Folder folder, String modelName,
                                                         List<Map<String, Object>> results, Connection connection) throws ApiException, SQLException {
        final DataModel dataModel = importDataModelFromResults(currentUser, folder, modelName, parameters.databaseDialect, results,
                                                               parameters.shouldImportSchemasAsDataClasses())
        if (parameters.dataModelNameSuffix) dataModel.aliasesString = databaseName

        if (parameters.detectEnumerations) {
            updateDataModelWithEnumerations(currentUser, parameters, parameters.maxEnumerations ?: MAX_ENUMERATIONS, dataModel, connection)
        }

        if (parameters.calculateSummaryMetadata) {
            updateDataModelWithSummaryMetadata(currentUser, parameters, dataModel, connection)
        }

        updateDataModelWithDatabaseSpecificInformation(dataModel, connection)
        [dataModel]
    }

    void updateDataModelWithEnumerations(User user, S parameters, int maxEnumerations, DataModel dataModel, Connection connection) {
        log.info('Starting enumeration detection')
        long startTime = System.currentTimeMillis()
        dataModel.childDataClasses.each { DataClass schemaClass ->
            schemaClass.dataClasses.each { DataClass tableClass ->
                SamplingStrategy samplingStrategy = getSamplingStrategy(parameters)
                samplingStrategy.approxCount = getApproxCount(connection, tableClass.label, schemaClass.label)
                samplingStrategy.tableType = getTableType(connection, tableClass.label, schemaClass.label, dataModel.label)

                if (!samplingStrategy.canSample() && samplingStrategy.approxCount > samplingStrategy.threshold) {
                    log.info("Not calculating enumerations for ${samplingStrategy.tableType} ${tableClass.label} with approx rowcount ${samplingStrategy.approxCount} and threshold ${samplingStrategy.threshold}")
                } else {
                    tableClass.dataElements.each { DataElement de ->
                        DataType primitiveType = de.dataType
                        if (isColumnPossibleEnumeration(primitiveType)) {
                            int countDistinct = getCountDistinctColumnValues(connection, samplingStrategy, de.label, tableClass.label, schemaClass.label)
                            if (countDistinct > 0 && countDistinct <= maxEnumerations) {
                                EnumerationType enumerationType = enumerationTypeService.findOrCreateDataTypeForDataModel(dataModel, de.label, de.label, user)

                                final List<Map<String, Object>> results = getDistinctColumnValues(connection, samplingStrategy, de.label, tableClass.label, schemaClass.label)

                                replacePrimitiveTypeWithEnumerationType(dataModel, de, primitiveType, enumerationType, results)
                            }
                        }
                    }
                }
            }
        }
        log.info('Finished enumeration detection in {}', Utils.timeTaken(startTime))
    }

    void replacePrimitiveTypeWithEnumerationType(DataModel dataModel, DataElement de, DataType primitiveType, EnumerationType enumerationType, List<Map<String, Object>> results) {
        results.each {
            //null is not a value, so skip it
            if (it.distinct_value != null) {
                enumerationType.addToEnumerationValues(new EnumerationValue(key: it.distinct_value, value: it.distinct_value))
            }
        }

        primitiveType.removeFromDataElements(de)

        de.dataType = enumerationType

        if (primitiveType.dataElements.size() == 0 ) {
            dataModel.removeFromPrimitiveTypes(primitiveType)
        }
    }

    /**
     * Compute a value distribution for relevant columns and store as summary metadata.
     * @param user
     * @param dataModel
     * @param connection
     */
    void updateDataModelWithSummaryMetadata(User user, S parameters, DataModel dataModel, Connection connection) {
        log.info('Starting summary metadata')
        long startTime = System.currentTimeMillis()
        dataModel.childDataClasses.each { DataClass schemaClass ->
            schemaClass.dataClasses.each { DataClass tableClass ->
                SamplingStrategy samplingStrategy = getSamplingStrategy(parameters)
                samplingStrategy.approxCount = getApproxCount(connection, tableClass.label, schemaClass.label)
                samplingStrategy.tableType = getTableType(connection, tableClass.label, schemaClass.label, dataModel.label)

                if (!samplingStrategy.canSample() && samplingStrategy.approxCount > samplingStrategy.threshold) {
                    log.info("Not calculating summary metadata for ${samplingStrategy.tableType} ${tableClass.label} with approx rowcount ${samplingStrategy.approxCount} and threshold ${samplingStrategy.threshold}")
                } else {
                    tableClass.dataElements.each { DataElement de ->
                        DataType dt = de.dataType
                        if (isColumnForDateSummary(dt) || isColumnForDecimalSummary(dt) || isColumnForIntegerSummary(dt) || isColumnForLongSummary(dt)) {
                            Pair minMax = getMinMaxColumnValues(connection, samplingStrategy, de.label, tableClass.label, schemaClass.label)

                            //aValue is the MIN, bValue is the MAX. If they are not null then calculate the range etc...
                            if (!(minMax.aValue == null) && !(minMax.bValue == null)) {
                                AbstractIntervalHelper intervalHelper = getIntervalHelper(dt, minMax)

                                Map<String, Long> valueDistribution = getColumnRangeDistribution(connection, samplingStrategy, dt, intervalHelper, de.label, tableClass.label, schemaClass.label)
                                if (valueDistribution) {
                                    String description = 'Value Distribution';
                                    if (samplingStrategy.useSampling()) {
                                        description = "Estimated Value Distribution (calculated by sampling ${samplingStrategy.percentage}% of rows)"
                                    }
                                    SummaryMetadata summaryMetadata = SummaryMetadataHelper.createSummaryMetadataFromMap(user, de.label, description, valueDistribution)
                                    de.addToSummaryMetadata(summaryMetadata);
                                }
                            }
                        }
                    }
                }
            }
        }
        log.info('Finished summary metadata in {}', Utils.timeTaken(startTime))
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

    void addStandardConstraintInformation(DataModel dataModel, Connection connection) throws ApiException, SQLException {
        if (!standardConstraintInformationQueryString) return
        dataModel.childDataClasses.each {DataClass schemaClass ->
            final List<Map<String, Object>> results = executePreparedStatement(
                dataModel, schemaClass, connection, standardConstraintInformationQueryString)
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
        if (!primaryKeyAndUniqueConstraintInformationQueryString) return
        dataModel.childDataClasses.each {DataClass schemaClass ->
            final List<Map<String, Object>> results = executePreparedStatement(
                dataModel, schemaClass, connection, primaryKeyAndUniqueConstraintInformationQueryString)
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

                    tableClass.addToMetadata(namespace, "${constraintTypeName}_name", constraintKeyName, dataModel.createdBy)
                    tableClass.addToMetadata(namespace, "${constraintTypeName}_columns", constraintTypeColumns, dataModel.createdBy)

                    rows.each {Map<String, Object> row ->
                        final DataElement columnElement = tableClass.findDataElement(row.column_name as String)
                        if (columnElement) {
                            columnElement.addToMetadata(namespace, (row.constraint_type as String).toLowerCase(), row.ordinal_position as String, dataModel.createdBy)
                        }
                    }
                }
        }
    }

    void addIndexInformation(DataModel dataModel, Connection connection) throws ApiException, SQLException {
        if (!indexInformationQueryString) return
        dataModel.childDataClasses.each {DataClass schemaClass ->
            final List<Map<String, Object>> results = executePreparedStatement(dataModel, schemaClass, connection, indexInformationQueryString)

            results.groupBy {it.table_name}.each {tableName, rows ->
                final DataClass tableClass = schemaClass.findDataClass(tableName as String)
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

                tableClass.addToMetadata(namespace, 'indexes', JsonOutput.prettyPrint(JsonOutput.toJson(indexes)), dataModel.createdBy)
            }
        }
    }

    void addForeignKeyInformation(DataModel dataModel, Connection connection) throws ApiException, SQLException {
        if (!foreignKeyInformationQueryString) return
        dataModel.childDataClasses.each {DataClass schemaClass ->
            final List<Map<String, Object>> results = executePreparedStatement(dataModel, schemaClass, connection, foreignKeyInformationQueryString)
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

                final DataClass tableClass = schemaClass.findDataClass(row.table_name as String)
                final DataElement columnElement = tableClass.findDataElement(row.column_name as String)
                columnElement.dataType = dataType
                columnElement.addToMetadata(namespace, "foreign_key_name", row.constraint_name as String, dataModel.createdBy)
                columnElement.addToMetadata(namespace, "foreign_key_columns", row.reference_column_name as String, dataModel.createdBy)
            }
        }
    }

    /**
     * Subclasses may override this method and use vendor specific queries to add metadata / extended properties /
     * comments to the DataModel and its constituent parts.
     * @param dataModel
     * @param connection
     */
    void addMetadata(DataModel dataModel, Connection connection) {
        return
    }

    Connection getConnection(String databaseName, S parameters) throws ApiException, ApiBadRequestException {
        try {
            parameters.getDataSource(databaseName).getConnection(parameters.databaseUsername, parameters.databasePassword)
        } catch (SQLException e) {
            log.error 'Cannot connect to database [{}]: {}', parameters.getUrl(databaseName), e.message
            throw new ApiBadRequestException('DIS02', "Cannot connect to database [${parameters.getUrl(databaseName)}]", e)
        }
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

    private List<DataModel> importDataModelsFromParameters(User currentUser, String databaseName, S parameters)
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

    private List<Map<String, Object>> executePreparedStatement(DataModel dataModel, DataClass schemaClass, Connection connection,
                                                               String queryString) throws ApiException, SQLException {
        List<Map<String, Object>> results = null
        try {
            final PreparedStatement preparedStatement = connection.prepareStatement(queryString)
            preparedStatement.setString(1, schemaClass.label)
            results = executeStatement(preparedStatement)
            preparedStatement.close()
        } catch (SQLException e) {
            if (e.message.contains('Invalid object name \'information_schema.table_constraints\'')) {
                log.warn 'No table_constraints available for {}', dataModel.label
            } else throw e
        }
        results
    }

    static boolean getBooleanValue(def value) {
        value.toString().toBoolean()
    }

    int getCountDistinctColumnValues(Connection connection, SamplingStrategy samplingStrategy, String columnName, String tableName, String schemaName = null) {
        log.info("Starting getCountDistinctColumnValues query for ${tableName}.${columnName}")
        long startTime = System.currentTimeMillis()
        String queryString = countDistinctColumnValuesQueryString(samplingStrategy, columnName, tableName, schemaName)
        final PreparedStatement preparedStatement = connection.prepareStatement(queryString)
        final List<Map<String, Object>> results = executeStatement(preparedStatement)
        log.info("Finished getCountDistinctColumnValues query for ${tableName}.${columnName} in {}", Utils.timeTaken(startTime))
        (int) results[0].count
    }

    private List<Map<String, Object>> getDistinctColumnValues(Connection connection, SamplingStrategy samplingStrategy, String columnName, String tableName, String schemaName = null) {
        log.info("Starting getDistinctColumnValues query for ${tableName}.${columnName}")
        long startTime = System.currentTimeMillis()
        String queryString = distinctColumnValuesQueryString(samplingStrategy, columnName, tableName, schemaName)
        final PreparedStatement preparedStatement = connection.prepareStatement(queryString)
        final List<Map<String, Object>> results = executeStatement(preparedStatement)
        log.info("Finished getDistinctColumnValues query for ${tableName}.${columnName} in {}", Utils.timeTaken(startTime))
        results
    }

    private Pair getMinMaxColumnValues(Connection connection, SamplingStrategy samplingStrategy, String columnName, String tableName, String schemaName = null) {
        log.info("Starting getMinMaxColumnValues query for ${tableName}.${columnName}")
        long startTime = System.currentTimeMillis()
        String queryString = minMaxColumnValuesQueryString(samplingStrategy, columnName, tableName, schemaName)
        final PreparedStatement preparedStatement = connection.prepareStatement(queryString)
        final List<Map<String, Object>> results = executeStatement(preparedStatement)

        log.info("Finished getMinMaxColumnValues query for ${tableName}.${columnName} in {}", Utils.timeTaken(startTime))

        new Pair(results[0].min_value, results[0].max_value)
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
    private Long getApproxCount(Connection connection, String tableName, String schemaName = null) {
        log.info("Starting getApproxCouunt query for ${tableName}")
        long startTime = System.currentTimeMillis()
        Long approxCount = 0
        List<String> queryStrings = approxCountQueryString(tableName, schemaName)
        for (String queryString: queryStrings) {
            PreparedStatement preparedStatement = connection.prepareStatement(queryString)
            List<Map<String, Object>> results = executeStatement(preparedStatement)

            if (results && results[0].approx_count != null) {
                approxCount =  (Long) results[0].approx_count
                break
            }
        }

        log.info("Finished getApproxCount query for ${tableName} in {}", Utils.timeTaken(startTime))

        return approxCount
    }

    /**
     * Get table type (BASE TABLE or VIEW) for an object
     * @param connection
     * @param tableName
     * @param schemaName
     * @return
     */
    private String getTableType(Connection connection, String tableName, String schemaName, String modelName) {

        String tableType = ""
        String queryString = tableTypeQueryString()
        final PreparedStatement preparedStatement = connection.prepareStatement(queryString)
        preparedStatement.setString(1, modelName)
        preparedStatement.setString(2, schemaName)
        preparedStatement.setString(3, tableName)
        List<Map<String, Object>> results = executeStatement(preparedStatement)

        if (results && results[0].table_type != null) {
            tableType =  (String) results[0].table_type
        }

        return tableType
    }

    private AbstractIntervalHelper getIntervalHelper(DataType dt, Pair minMax) {
        if (isColumnForLongSummary(dt)) {
            return new LongIntervalHelper((Long) minMax.aValue, (Long) minMax.bValue)
        } else if (isColumnForIntegerSummary(dt)) {
            return new IntegerIntervalHelper((Integer) minMax.aValue, (Integer) minMax.bValue)
        } else if (isColumnForDateSummary(dt)) {
            return new DateIntervalHelper(((java.util.Date) minMax.aValue).toLocalDateTime(), ((java.util.Date) minMax.bValue).toLocalDateTime())
        } else if (isColumnForDecimalSummary(dt)) {
            return new DecimalIntervalHelper((BigDecimal) minMax.aValue, (BigDecimal) minMax.bValue)
        }
    }

    private Map<String, Long> getColumnRangeDistribution(Connection connection, SamplingStrategy samplingStrategy,
                                                            DataType dataType, AbstractIntervalHelper intervalHelper,
                                                            String columnName, String tableName, String schemaName = null) {
        log.info("Starting getColumnRangeDistribution query for ${tableName}.${columnName}")
        long startTime = System.currentTimeMillis()
        String queryString = columnRangeDistributionQueryString(samplingStrategy, dataType, intervalHelper, columnName, tableName, schemaName)

        final PreparedStatement preparedStatement = connection.prepareStatement(queryString)
        List<Map<String, Object>> results = executeStatement(preparedStatement)
        preparedStatement.close()
        log.info("Finished getColumnRangeDistribution query for ${tableName}.${columnName} in {}", Utils.timeTaken(startTime))

        results.collectEntries{
            [(it.interval_label): it.interval_count]
        }
    }
}
