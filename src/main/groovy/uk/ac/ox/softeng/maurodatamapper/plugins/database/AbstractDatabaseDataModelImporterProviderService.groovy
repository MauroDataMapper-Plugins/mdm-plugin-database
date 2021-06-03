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

import uk.ac.ox.softeng.maurodatamapper.api.exception.ApiBadRequestException
import uk.ac.ox.softeng.maurodatamapper.api.exception.ApiException
import uk.ac.ox.softeng.maurodatamapper.core.container.Folder
import uk.ac.ox.softeng.maurodatamapper.datamodel.DataModel
import uk.ac.ox.softeng.maurodatamapper.datamodel.DataModelType
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.DataClass
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.DataClassService
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.DataElement
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.DataElementService
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.datatype.DataType
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.datatype.PrimitiveTypeService
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.datatype.ReferenceTypeService
import uk.ac.ox.softeng.maurodatamapper.datamodel.provider.importer.DataModelImporterProviderService
import uk.ac.ox.softeng.maurodatamapper.security.User

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Autowired

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

    @Autowired
    DataClassService dataClassService

    @Autowired
    DataElementService dataElementService

    @Autowired
    PrimitiveTypeService primitiveTypeService

    @Autowired
    ReferenceTypeService referenceTypeService

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
        final DataModel dataModel = dataModelService.createAndSaveDataModel(user, folder, DataModelType.DATA_ASSET, modelName, null, null, null).tap {
            addToMetadata(namespace: DATABASE_NAMESPACE, key: 'dialect', value: dialect, createdBy: user.emailAddress)
        }

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
                dataElement.addToMetadata(namespace, column, data.toString(), user)
            }
        }

        dataModel
    }

    List<DataModel> importAndUpdateDataModelsFromResults(User currentUser, String databaseName, S parameters, Folder folder, String modelName,
                                                         List<Map<String, Object>> results, Connection connection) throws ApiException, SQLException {
        final DataModel dataModel = importDataModelFromResults(currentUser, folder, modelName, parameters.databaseDialect, results,
                                                               parameters.shouldImportSchemasAsDataClasses())
        if (parameters.dataModelNameSuffix) dataModel.aliasesString = databaseName
        updateDataModelWithDatabaseSpecificInformation(dataModel, connection)
        [dataModel]
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
            results.each {Map<String, Object> row ->
                final DataClass tableClass = schemaClass.findDataClass(row.table_name as String)
                if (!tableClass) {
                    log.warn 'Could not add {} as DataClass for table {} does not exist', row.index_name, row.table_name
                    return
                }

                String indexType = row.primary_index ? 'primary_index' : row.unique_index ? 'unique_index' : 'index'
                indexType = row.clustered ? "clustered_${indexType}" : indexType
                tableClass.addToMetadata(namespace, "${indexType}_name", row.index_name as String, dataModel.createdBy)
                tableClass.addToMetadata(namespace, "${indexType}_columns", row.column_names as String, dataModel.createdBy)
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
}
