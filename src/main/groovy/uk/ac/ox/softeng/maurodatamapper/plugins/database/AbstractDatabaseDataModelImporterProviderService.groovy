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
import uk.ac.ox.softeng.maurodatamapper.plugins.database.summarymetadata.AbstractIntervalHelper
import uk.ac.ox.softeng.maurodatamapper.plugins.database.summarymetadata.DateIntervalHelper
import uk.ac.ox.softeng.maurodatamapper.plugins.database.summarymetadata.SummaryMetadataHelper
import uk.ac.ox.softeng.maurodatamapper.security.User
import uk.ac.ox.softeng.maurodatamapper.util.Utils

import grails.util.Pair
import groovy.json.JsonOutput
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
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

    SamplingStrategy getSamplingStrategy(String schema, String table, S parameters) {
        new SamplingStrategy(schema, table)
    }

    CalculationStrategy getCalculationStrategy(S parameters) {
        new CalculationStrategy(parameters)
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

    abstract DefaultDataTypeProvider getDefaultDataTypeProvider()

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
    @Deprecated
    String countDistinctColumnValuesQueryString(SamplingStrategy samplingStrategy, String columnName, String tableName, String schemaName = null) {
        String schemaIdentifier = schemaName ? "${escapeIdentifier(schemaName)}." : ""
        "SELECT COUNT(DISTINCT(${escapeIdentifier(columnName)})) AS count FROM ${schemaIdentifier}${escapeIdentifier(tableName)}" +
        samplingStrategy.samplingClause(SamplingStrategy.Type.ENUMERATION_VALUES) +
        "WHERE ${escapeIdentifier(columnName)} <> ''"
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
     * Optimisation can also occur by vendor specific SQL limiting the return to the max allowed EVs + 1,
     * this will mean the system gets the first (e.g 21) entries. If the max allowed values is 20 then the size of 21 will stop it from being an ET
     * it will also be faster as its only checking for 21 distinct values then returning a result.
     *
     * @return Query string for distinct values in a column
     */
    String distinctColumnValuesQueryString(CalculationStrategy calculationStrategy, SamplingStrategy samplingStrategy, String columnName, String tableName,
                                           String schemaName = null) {
        String schemaIdentifier = schemaName ? "${escapeIdentifier(schemaName)}." : ""
        "SELECT DISTINCT(${escapeIdentifier(columnName)}) AS distinct_value FROM ${schemaIdentifier}${escapeIdentifier(tableName)}" +
        samplingStrategy.samplingClause(SamplingStrategy.Type.ENUMERATION_VALUES) +
        "WHERE ${escapeIdentifier(columnName)} <> ''"
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
     * and use an appropriate sampling strategy.
     *
     * The base method does not use sampling, and should be overridden by subclasses where possible.
     * @return Query string for distinct values in a column
     */
    String minMaxColumnValuesQueryString(SamplingStrategy samplingStrategy, String columnName, String tableName, String schemaName = null) {
        String schemaIdentifier = schemaName ? "${escapeIdentifier(schemaName)}." : ""
        """SELECT MIN(${escapeIdentifier(columnName)}) AS min_value, 
MAX(${escapeIdentifier(columnName)}) AS max_value 
FROM ${schemaIdentifier}${escapeIdentifier(tableName)} ${samplingStrategy.samplingClause(SamplingStrategy.Type.SUMMARY_METADATA)}
WHERE ${escapeIdentifier(columnName)} IS NOT NULL""".stripIndent()
    }

    /**
     * Must return a String which will be queryable by table name
     * and column name, and return rows with the following elements:
     *  * enumeration_value
     *  * enumeration_count
     *
     *  Subclasses must implement this method using vendor specific SQL as necessary.
     *
     * @return Query string for count grouped by enumeration value
     */
    String enumerationValueDistributionQueryString(SamplingStrategy samplingStrategy,
                                                   String columnName,
                                                   String tableName,
                                                   String schemaName) {

        """
        SELECT ${escapeIdentifier(schemaName)}.${escapeIdentifier(tableName)}.${escapeIdentifier(columnName)} AS enumeration_value,
        COUNT(*) AS enumeration_count
        FROM ${escapeIdentifier(schemaName)}.${escapeIdentifier(tableName)} 
        ${samplingStrategy.samplingClause(SamplingStrategy.Type.SUMMARY_METADATA)}
        GROUP BY ${escapeIdentifier(schemaName)}.${escapeIdentifier(tableName)}.${escapeIdentifier(columnName)}
        ORDER BY ${escapeIdentifier(schemaName)}.${escapeIdentifier(tableName)}.${escapeIdentifier(columnName)}
        """.stripIndent()
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
    abstract String columnRangeDistributionQueryString(SamplingStrategy samplingStrategy, DataType dataType,
                                                       AbstractIntervalHelper intervalHelper,
                                                       String columnName, String tableName, String schemaName)

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
                                         boolean importSchemaAsDataClass, List<Pattern> tableRegexesToIgnore) throws ApiException {
        final DataModel dataModel = new DataModel(createdBy: user.emailAddress, label: modelName, type: DataModelType.DATA_ASSET, folder: folder,
                                                  authority: authorityService.getDefaultAuthority())
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
                tableDataClass = dataClassService.findOrCreateDataClass(schemaDataClass, tableName, null, user)
            } else {
                tableDataClass = dataClassService.findOrCreateDataClass(dataModel, tableName, null, user)
            }

            final int minMultiplicity = isColumnNullable(row[columnIsNullableColumnName] as String) ? 0 : 1
            final DataElement dataElement = dataElementService.findOrCreateDataElementForDataClass(
                tableDataClass, row[columnNameColumnName] as String, null, user, dataType, minMultiplicity, 1)

            row.findAll {String column, data ->
                data && !(column in coreColumns)
            }.each {String column, data ->
                dataElement.addToMetadata(namespace: namespaceColumn(), key: column, value: data.toString(), createdBy: user.emailAddress)
            }
        }

        dataModel
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

    void updateDataModelWithEnumerationsAndSummaryMetadata(User user, S parameters, DataModel dataModel, Connection connection) {
        log.debug('Starting enumeration and summary metadata detection')
        long startTime = System.currentTimeMillis()
        CalculationStrategy calculationStrategy = getCalculationStrategy(parameters)
        dataModel.childDataClasses.each {DataClass schemaClass ->
            schemaClass.dataClasses.each {DataClass tableClass ->
                log.trace('Checking {}.{} for possible enumerations and summary metadata', schemaClass.label, tableClass.label)
                SamplingStrategy samplingStrategy = getSamplingStrategy(schemaClass.label, tableClass.label, parameters)
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

    void calculateEnumerationsAndSummaryMetadata(DataModel dataModel, DataClass schemaClass, DataClass tableClass,
                                                 CalculationStrategy calculationStrategy, SamplingStrategy samplingStrategy,
                                                 Connection connection, User user) {

        log.debug('Calculating enumerations and summary metadata using {}', samplingStrategy)
        tableClass.dataElements.each {DataElement de ->
            DataType dt = de.dataType

            //Enumeration detection
            if (calculationStrategy.shouldDetectEnumerations(de.label, dt, samplingStrategy.approxCount)) {
                if (samplingStrategy.canDetectEnumerationValues()) {
                    logEnumerationDetection(samplingStrategy, de)

                    // Make 1 call to get the distinct values, then use the size of the that results to tell if its actually an ET
                    final List<Map<String, Object>> results = getDistinctColumnValues(connection, calculationStrategy, samplingStrategy, de.label,
                                                                                      tableClass.label, schemaClass.label)
                    if (calculationStrategy.isEnumerationType(results.size())) {
                        log.debug('Converting {} to an EnumerationType', de.label)
                        EnumerationType enumerationType = enumerationTypeService.findOrCreateDataTypeForDataModel(dataModel, de.label, de.label, user)
                        replacePrimitiveTypeWithEnumerationType(dataModel, de, dt, enumerationType, results)
                    }

                    if (calculationStrategy.computeSummaryMetadata) {
                        if (samplingStrategy.canComputeSummaryMetadata()) {
                            logSummaryMetadataDetection(samplingStrategy, de, 'enumeration')
                            //Count enumeration values
                            Map<String, Long> enumerationValueDistribution =
                                getEnumerationValueDistribution(connection, samplingStrategy, de.label, tableClass.label, schemaClass.label)
                            if (enumerationValueDistribution) {
                                String description = 'Enumeration Value Distribution'
                                if (samplingStrategy.useSamplingForSummaryMetadata()) {
                                    description = "Estimated Enumeration Value Distribution (calculated by sampling ${samplingStrategy.smPercentage}% of rows)"
                                }
                                SummaryMetadata enumerationSummaryMetadata =
                                    SummaryMetadataHelper.createSummaryMetadataFromMap(user, de.label, description, enumerationValueDistribution)
                                de.addToSummaryMetadata(enumerationSummaryMetadata)

                                SummaryMetadata enumerationSummaryMetadataOnTable =
                                    SummaryMetadataHelper.createSummaryMetadataFromMap(user, de.label, description, enumerationValueDistribution)
                                tableClass.addToSummaryMetadata(enumerationSummaryMetadataOnTable)
                            }
                        } else {
                            logNotCalculatingSummaryMetadata(samplingStrategy, de)
                        }
                    }
                } else {
                    logNotDetectingEnumerationValues(samplingStrategy, de)
                }
            }

            //Summary metadata on dates and numbers
            else if (calculationStrategy.shouldComputeSummaryData(de.label, dt)) {
                if (samplingStrategy.canComputeSummaryMetadata()) {
                    logSummaryMetadataDetection(samplingStrategy, de, 'date or numeric')
                    Pair minMax = getMinMaxColumnValues(connection, samplingStrategy, de.label, tableClass.label, schemaClass.label)

                    //aValue is the MIN, bValue is the MAX. If they are not null then calculate the range etc...
                    if (minMax.aValue != null && minMax.bValue != null) {
                        AbstractIntervalHelper intervalHelper = calculationStrategy.getIntervalHelper(dt, minMax)
                        log.trace('Summary Metadata computation using {}', intervalHelper)
                        Map<String, Long> valueDistribution = getColumnRangeDistribution(connection, samplingStrategy, dt, intervalHelper, de.label,
                                                                                         tableClass.label, schemaClass.label)
                        if (valueDistribution) {
                            String description = 'Value Distribution'
                            if (samplingStrategy.useSamplingForSummaryMetadata()) {
                                description = "Estimated Value Distribution (calculated by sampling ${samplingStrategy.smPercentage}% of rows)"
                            }

                            // Dates can produce a lot of buckets and a lot of empty buckets
                            if (calculationStrategy.isColumnForDateSummary(dt) && (intervalHelper as DateIntervalHelper).needToMergeOrRemoveEmptyBuckets) {
                                log.debug('Need to merge or remove empty buckets from value distribution with current bucket size {}', valueDistribution.size())
                                switch (calculationStrategy.dateBucketHandling) {
                                    case CalculationStrategy.BucketHandling.MERGE:
                                        valueDistribution = mergeDateBuckets(valueDistribution)
                                        break
                                    case CalculationStrategy.BucketHandling.REMOVE:
                                        // Remove all buckets with no counts
                                        log.trace('Removing date buckets with no counts')
                                        valueDistribution.removeAll {k, v -> !v}
                                        break
                                }
                                log.debug('Value distribution bucket size reduced to {}', valueDistribution.size())
                            }

                            SummaryMetadata summaryMetadata = SummaryMetadataHelper.createSummaryMetadataFromMap(user, de.label, description, valueDistribution)
                            de.addToSummaryMetadata(summaryMetadata)

                            SummaryMetadata summaryMetadataOnTable =
                                SummaryMetadataHelper.createSummaryMetadataFromMap(user, de.label, description, valueDistribution)
                            tableClass.addToSummaryMetadata(summaryMetadataOnTable)
                        }
                    }
                } else {
                    logNotCalculatingSummaryMetadata(samplingStrategy, de)
                }
            }
        }
    }

    Map<String, Long> mergeDateBuckets(Map<String, Long> valueDistribution) {
        log.trace('Merging date buckets')
        Map<Pair<Integer, Integer>, Long> processing = valueDistribution.collectEntries {yearRange, value ->
            String[] split = yearRange.split('-')
            [new Pair<>(Integer.parseInt(split[0].trim()), Integer.parseInt(split[1].trim())), value]
        }

        Map<Pair<Integer, Integer>, Long> merged = [:]
        processing.each {range, value ->
            def previous = merged.find {r, v -> r.bValue == range.aValue}
            if (previous?.value == 0 && value == 0) {
                merged.remove(previous.key)
                merged[new Pair<>(previous.key.aValue, range.bValue)] = 0L
            } else {
                merged[range] = value
            }
        }

        merged.collectEntries {range, value ->
            ["${range.aValue} - ${range.bValue}".toString(), value]
        } as Map<String, Long>
    }

    void replacePrimitiveTypeWithEnumerationType(DataModel dataModel, DataElement de, DataType primitiveType, EnumerationType enumerationType,
                                                 List<Map<String, Object>> results) {
        results.each {
            //null is not a value, so skip it
            if (it.distinct_value != null) {
                enumerationType.addToEnumerationValues(new EnumerationValue(key: it.distinct_value, value: it.distinct_value))
            }
        }

        primitiveType.removeFromDataElements(de)
        de.dataType = enumerationType
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

                tableClass.addToMetadata(namespaceTable(), 'indexes', JsonOutput.prettyPrint(JsonOutput.toJson(indexes)), dataModel.createdBy)
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
                columnElement.addToMetadata(namespaceColumn(), "foreign_key_name", row.constraint_name as String, dataModel.createdBy)
                columnElement.addToMetadata(namespaceColumn(), "foreign_key_columns", row.reference_column_name as String, dataModel.createdBy)
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

    // Faster to get the results with 1 query just to get all the distinct values rather than 1 query to get the count and 1 to get the values
    // Optimisation can also occur by vendor specific SQL limiting the return to the max allowed EVs + 1,
    // this will mean the system gets the first (e.g 21) entries. If the max allowed values is 20 then the size of 21 will stop it from being an ET
    // it will also be faster as its only checking for 21 distinct values then returning a result.
    @Deprecated
    int getCountDistinctColumnValues(Connection connection, SamplingStrategy samplingStrategy, String columnName, String tableName, String schemaName = null) {
        log.trace("Starting getCountDistinctColumnValues query for ${tableName}.${columnName}")
        long startTime = System.currentTimeMillis()
        String queryString = countDistinctColumnValuesQueryString(samplingStrategy, columnName, tableName, schemaName)
        SQL_LOGGER.debug('\n{}', queryString)
        final PreparedStatement preparedStatement = connection.prepareStatement(queryString)
        final List<Map<String, Object>> results = executeStatement(preparedStatement)
        log.trace("Finished getCountDistinctColumnValues query for ${tableName}.${columnName} in {}", Utils.timeTaken(startTime))
        (int) results[0].count
    }

    private List<Map<String, Object>> getDistinctColumnValues(Connection connection, CalculationStrategy calculationStrategy, SamplingStrategy samplingStrategy,
                                                              String columnName, String tableName,
                                                              String schemaName = null) {
        log.trace("Starting getDistinctColumnValues query for ${tableName}.${columnName}")
        long startTime = System.currentTimeMillis()
        String queryString = distinctColumnValuesQueryString(calculationStrategy, samplingStrategy, columnName, tableName, schemaName)
        SQL_LOGGER.trace('\n{}', queryString)
        final PreparedStatement preparedStatement = connection.prepareStatement(queryString)
        final List<Map<String, Object>> results = executeStatement(preparedStatement)
        log.trace("Finished getDistinctColumnValues query for ${tableName}.${columnName} in {}", Utils.timeTaken(startTime))
        results
    }

    private Pair getMinMaxColumnValues(Connection connection, SamplingStrategy samplingStrategy, String columnName, String tableName, String schemaName = null) {
        log.trace("Starting getMinMaxColumnValues query for ${tableName}.${columnName}")
        long startTime = System.currentTimeMillis()
        String queryString = minMaxColumnValuesQueryString(samplingStrategy, columnName, tableName, schemaName)
        SQL_LOGGER.trace('\n{}', queryString)
        final PreparedStatement preparedStatement = connection.prepareStatement(queryString)
        final List<Map<String, Object>> results = executeStatement(preparedStatement)

        log.trace("Finished getMinMaxColumnValues query for ${tableName}.${columnName} in {}", Utils.timeTaken(startTime))

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
        log.trace("Starting getApproxCouunt query for ${tableName}")
        long startTime = System.currentTimeMillis()
        Long approxCount = 0
        List<String> queryStrings = approxCountQueryString(tableName, schemaName)
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
            tableType = (String) results[0].table_type
        }

        return tableType
    }


    private Map<String, Long> getColumnRangeDistribution(Connection connection, SamplingStrategy samplingStrategy,
                                                         DataType dataType, AbstractIntervalHelper intervalHelper,
                                                         String columnName, String tableName, String schemaName = null) {
        log.trace("Starting getColumnRangeDistribution query for ${tableName}.${columnName}")
        long startTime = System.currentTimeMillis()
        String queryString = columnRangeDistributionQueryString(samplingStrategy, dataType, intervalHelper, columnName, tableName, schemaName)
        SQL_LOGGER.trace('\n{}', queryString)
        final PreparedStatement preparedStatement = connection.prepareStatement(queryString)
        List<Map<String, Object>> results = executeStatement(preparedStatement)
        preparedStatement.close()
        log.trace("Finished getColumnRangeDistribution query for ${tableName}.${columnName} in {}", Utils.timeTaken(startTime))

        results.collectEntries {
            [(it.interval_label): it.interval_count]
        }
    }

    protected Map<String, Long> getEnumerationValueDistribution(Connection connection, SamplingStrategy samplingStrategy,
                                                                String columnName, String tableName, String schemaName = null) {
        log.trace("Starting getEnumerationValueDistribution query for ${tableName}.${columnName}")
        long startTime = System.currentTimeMillis()
        String queryString = enumerationValueDistributionQueryString(samplingStrategy, columnName, tableName, schemaName)
        log.debug('{}', queryString)
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
                      samplingStrategy.smPercentage)
        } else {
            log.debug('Performing {} summary metadata detection for column {}', type, de.label)
        }
    }

    private void logEnumerationDetection(SamplingStrategy samplingStrategy, DataElement de) {
        if (samplingStrategy.useSamplingForEnumerationValues()) {
            log.debug('Performing enumeration detection for column {} (calculated by sampling {}% of rows)', de.label, samplingStrategy.evPercentage)
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
}
