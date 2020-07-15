package uk.ac.ox.softeng.maurodatamapper.plugin.database

import uk.ac.ox.softeng.maurodatamapper.api.exception.ApiBadRequestException
import uk.ac.ox.softeng.maurodatamapper.api.exception.ApiException
import uk.ac.ox.softeng.maurodatamapper.core.container.Folder
import uk.ac.ox.softeng.maurodatamapper.datamodel.DataModel
import uk.ac.ox.softeng.maurodatamapper.datamodel.DataModelService
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

import org.springframework.beans.factory.annotation.Autowired

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException

@CompileStatic
@Slf4j
abstract class AbstractDatabaseDataModelImporterProviderService<T extends DatabaseDataModelImporterProviderServiceParameters>
    extends DataModelImporterProviderService<T> {

    private static final String DATABASE_NAMESPACE = 'uk.ac.ox.softeng.maurodatamapper.plugin.database'
    private static final String IS_NOT_NULL_CONSTRAINT = 'IS NOT NULL'

    @Autowired
    DataModelService dataModelService

    @Autowired
    DataClassService dataClassService

    @Autowired
    DataElementService dataElementService

    @Autowired
    PrimitiveTypeService primitiveTypeService

    @Autowired
    ReferenceTypeService referenceTypeService

    /**
     * Must return a String which will be queryable by schema name,
     * and return a row with the following elements:
     *  * table_name
     *  * check_clause (the constraint information)
     * @return
     */
    String getStandardConstraintInformationQueryString() {
        '''
SELECT
  tc.table_name,
  cc.check_clause
FROM information_schema.table_constraints tc
  INNER JOIN information_schema.check_constraints cc ON tc.constraint_name = cc.constraint_name
WHERE tc.constraint_schema = ?;
'''
    }

    /**
     * Must return a String which will be queryable by schema name,
     * and return a row with the following elements:
     *  * constraint_name
     *  * table_name
     *  * constraint_type (primary_key or unique)
     *  * column_name
     *  * ordinal_position
     * @return
     */
    String getPrimaryKeyAndUniqueConstraintInformationQueryString() {
        '''
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
  '''
    }

    /**
     * Must return a String which will be queryable by schema name,
     * and return a row with the following elements:
     *  * table_name
     *  * index_name
     *  * unique_index (boolean)
     *  * primary_index (boolean)
     *  * clustered (boolean)
     *  * column_names
     * @return
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
     * @return
     */
    abstract String getForeignKeyInformationQueryString()

    abstract String getDatabaseStructureQueryString()

    List<String> getCoreColumns() {
        [getSchemaNameColumnName(),
         getDataTypeColumnName(),
         getTableNameColumnName(),
         getColumnNameColumnName(),
         getTableCatalogColumnName(),]
    }

    String getSchemaNameColumnName() {
        'table_schema'
    }

    String getDataTypeColumnName() {
        'data_type'
    }

    String getTableNameColumnName() {
        'table_name'
    }

    String getColumnNameColumnName() {
        'column_name'
    }

    String getTableCatalogColumnName() {
        'table_catalog'
    }

    String getColumnIsNullableColumnName() {
        'is_nullable'
    }

    boolean isColumnNullable(String nullableColumnValue) {
        nullableColumnValue.toLowerCase() == 'yes'
    }

    @Override
    Boolean canImportMultipleDomains() {
        true
    }

    Connection getConnection(String databaseName, T params) throws ApiException, ApiBadRequestException {
        try {
            params.getDataSource(databaseName).getConnection(params.databaseUsername, params.databasePassword)
        } catch (SQLException e) {
            log.error 'Cannot connect to database [{}]: {}', params.getUrl(databaseName), e.message
            throw new ApiBadRequestException('DIS02', "Cannot connect to database [${params.getUrl(databaseName)}]", e)
        }
    }

    @Override
    DataModel importDataModel(User currentUser, T params) throws ApiException, ApiBadRequestException {
        importDataModels(currentUser, params.databaseNames, params).head()
    }

    @Override
    List<DataModel> importDataModels(User currentUser, T params) throws ApiException, ApiBadRequestException {
        final List<String> databaseNames = params.databaseNames.split(',').toList()
        log.info 'Importing {} DataModel(s)', databaseNames.size()

        final List<DataModel> dataModels = []
        databaseNames.each { String databaseName -> dataModels.addAll importDataModels(currentUser, databaseName, params) }
        dataModels
    }

    List<DataModel> importDataModels(User currentUser, String databaseName, T params) throws ApiException, ApiBadRequestException {
        String modelName = databaseName
        if (!params.isMultipleDataModelImport()) modelName = params.modelName ?: modelName
        modelName = params.dataModelNameSuffix ? "${modelName}_${params.dataModelNameSuffix}" : modelName

        try {
            final Connection connection = getConnection(databaseName, params)
            final PreparedStatement preparedStatement = connection.prepareStatement(databaseStructureQueryString)
            final StatementExecutionResults results = executeStatement(preparedStatement)
            preparedStatement.close()

            log.debug 'Size of results from statement {}', results.size()
            if (results.isEmpty()) {
                log.warn 'No results from database statement, therefore nothing to import for {}.', modelName
                return []
            }

            final List<DataModel> dataModels = importAndUpdateDataModelsFromResults(
                    currentUser, databaseName, params, Folder.get(params.folderId), modelName, results, connection)
            connection.close()
            dataModels
        } catch (SQLException e) {
            log.error 'Something went wrong executing statement while importing {} : {}', modelName, e.message
            throw new ApiBadRequestException('DIS03', 'Cannot execute statement', e)
        }
    }

    DataModel importDataModelFromResults(
            User user, Folder folder, String modelName, String dialect, StatementExecutionResults results, boolean importSchemaAsDataClass = true)
            throws ApiException {
        final DataModel dataModel = new DataModel(createdBy: user, label: modelName, type: DataModelType.DATA_ASSET, folder: folder)
        dataModel.addCreatedEdit user
        dataModel.addToMetadata namespace: DATABASE_NAMESPACE, key: 'dialect', value: dialect

        results.each { StatementExecutionResultsRow row ->
            final DataType dataType = primitiveTypeService.findOrCreateDataTypeForDataModel(dataModel, row(dataTypeColumnName), null, user)

            DataClass tableDataClass = null
            if (importSchemaAsDataClass) {
                DataClass schemaDataClass = dataClassService.findOrCreateDataClass(dataModel, row(schemaNameColumnName), null, user)
                tableDataClass = dataClassService.findOrCreateDataClass(schemaDataClass, row(tableNameColumnName), null, user)
            } else {
                tableDataClass = dataClassService.findOrCreateDataClass(dataModel, row(tableNameColumnName), null, user)
            }

            final int minMultiplicity = isColumnNullable(row(columnIsNullableColumnName)) ? 0 : 1
            final DataElement dataElement = dataElementService.findOrCreateDataElementForDataClass(
                    tableDataClass, row(columnNameColumnName), null, user, dataType, minMultiplicity, 1)

            row.findAll { String column, Object data ->
                data && !(column in coreColumns)
            }.each { String column, Object data ->
                dataElement.addToMetadata(namespace, column, data.toString(), user)
            }
        }

        dataModel
    }

    List<DataModel> importAndUpdateDataModelsFromResults(
            User currentUser, String databaseName, T params, Folder folder, String modelName, StatementExecutionResults results,
            Connection connection) throws ApiException, SQLException {
        final DataModel dataModel = importDataModelFromResults(currentUser, folder, modelName, params.databaseDialect, results)
        if (params.dataModelNameSuffix) dataModel.aliasesString = databaseName
        updateDataModelWithDatabaseSpecificInformation dataModel, connection
        [dataModel]
    }

    /**
     * Updates DataModel with custom information which is Database specific.
     * Default is to do nothing
     * @param dataModel DataModel to update
     * @param connection Connection to database
     * @return
     */
    void updateDataModelWithDatabaseSpecificInformation(DataModel dataModel, Connection connection) throws ApiException, SQLException {
        addStandardConstraintInformation dataModel, connection
        addPrimaryKeyAndUniqueConstraintInformation dataModel, connection
        addIndexInformation dataModel, connection
        addForeignKeyInformation dataModel, connection
    }

    void addStandardConstraintInformation(DataModel dataModel, Connection connection) throws ApiException, SQLException {
        if (standardConstraintInformationQueryString == null) return

        dataModel.childDataClasses.each { DataClass schemaClass ->
            final StatementExecutionResults results = executePreparedStatement(
                    dataModel, schemaClass, connection, standardConstraintInformationQueryString)
            results.each { StatementExecutionResultsRow row ->
                final DataClass tableClass = schemaClass.findDataClass(row.table_name as String)
                if (tableClass == null) return

                final String constraint = row.check_clause && (row.check_clause as String).contains(IS_NOT_NULL_CONSTRAINT) ?
                                          IS_NOT_NULL_CONSTRAINT : null
                if (constraint && constraint != IS_NOT_NULL_CONSTRAINT) {
                    log.warn 'Unhandled constraint {}', constraint
                }
            }
        }
    }

    void addPrimaryKeyAndUniqueConstraintInformation(DataModel dataModel, Connection connection) throws ApiException, SQLException {
        if (primaryKeyAndUniqueConstraintInformationQueryString == null) return

        dataModel.childDataClasses.each { DataClass schemaClass ->
            final StatementExecutionResults results = executePreparedStatement(
                    dataModel, schemaClass, connection, primaryKeyAndUniqueConstraintInformationQueryString)
            results.groupBy { it.constraint_name }.each { _, List<StatementExecutionResultsRow> rows ->
                final StatementExecutionResultsRow firstRow = rows.head()
                final DataClass tableClass = schemaClass.findDataClass(firstRow.table_name as String)
                if (tableClass == null) return

                final String constraintTypeName = (firstRow.constraint_type as String).toLowerCase().replaceAll(/ /, '_')
                final String constraintTypeValue = rows.size() == 1 ?
                                                   firstRow.column_name : rows.sort { it.ordinal_position }.collect { it.column_name }.join(', ')
                tableClass.addToMetadata namespace, "$constraintTypeName[${firstRow.constraint_name}]", constraintTypeValue, dataModel.createdBy

                rows.each { StatementExecutionResultsRow row ->
                    final DataElement columnElement = tableClass.findDataElement(row.column_name as String)
                    if (columnElement) {
                        columnElement.addToMetadata(
                                namespace, (row.constraint_type as String).toLowerCase(), row.ordinal_position as String, dataModel.createdBy)
                    }
                }
            }
        }
    }

    void addIndexInformation(DataModel dataModel, Connection connection) throws ApiException, SQLException {
        if (indexInformationQueryString == null) return

        dataModel.childDataClasses.each { DataClass schemaClass ->
            final StatementExecutionResults results = executePreparedStatement(dataModel, schemaClass, connection, indexInformationQueryString)
            results.each { StatementExecutionResultsRow row ->
                final DataClass tableClass = schemaClass.findDataClass(row.table_name as String)
                if (tableClass == null) {
                    log.warn 'Could not add {} as DataClass for table {} does not exist', row.index_name, row.table_name
                    return
                }

                String indexType = row.primary_index ? 'primary_index' : row.unique_index ? 'unique_index' : 'index'
                indexType = row.clustered ? "clustered_$indexType" : indexType
                tableClass.addToMetadata namespace, "$indexType[${row.index_name}]", row.column_names as String, dataModel.createdBy
            }
        }
    }

    void addForeignKeyInformation(DataModel dataModel, Connection connection) throws ApiException, SQLException {
        if (foreignKeyInformationQueryString == null) return

        dataModel.childDataClasses.each { DataClass schemaClass ->
            final StatementExecutionResults results = executePreparedStatement(dataModel, schemaClass, connection, foreignKeyInformationQueryString)
            results.each { StatementExecutionResultsRow row ->
                final DataClass foreignTableClass = dataModel.dataClasses.find { DataClass dataClass -> dataClass.label == row.reference_table_name }

                DataType dataType = null
                if (foreignTableClass) {
                    dataType = referenceTypeService.findOrCreateDataTypeForDataModel(
                            dataModel, "${foreignTableClass.label}Type", "Linked to DataElement [${row.reference_column_name}]",
                            dataModel.createdBy as User, foreignTableClass)
                    dataModel.addToDataTypes dataType
                } else {
                    dataType = primitiveTypeService.findOrCreateDataTypeForDataModel(
                            dataModel, "${row.reference_table_name}Type",
                            "Missing link to foreign key table [${row.reference_table_name}.${row.reference_column_name}]",
                            dataModel.createdBy as User
                    )
                }

                final DataClass tableClass = schemaClass.findDataClass(row.table_name as String)
                final DataElement columnElement = tableClass.findDataElement(row.column_name as String)
                columnElement.dataType = dataType
                columnElement.addToMetadata(
                        namespace, "foreign_key[${row.constraint_name}]", row.reference_column_name as String, dataModel.createdBy)
            }
        }
    }

    StatementExecutionResults executePreparedStatement(
            DataModel dataModel, DataClass schemaClass, Connection connection, String queryString) throws ApiException, SQLException {
        StatementExecutionResults results = null
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
        results as StatementExecutionResults
    }

    StatementExecutionResults executeStatement(PreparedStatement preparedStatement) throws ApiException, SQLException {
        final StatementExecutionResults results = new ArrayList(50) as StatementExecutionResults

        final ResultSet resultSet = preparedStatement.executeQuery()
        final ResultSetMetaData resultSetMetaData = resultSet.metaData
        final int columnCount = resultSetMetaData.columnCount

        while (resultSet.next()) {
            final StatementExecutionResultsRow row = new HashMap(columnCount) as StatementExecutionResultsRow
            (1..columnCount).each { int i ->
                row[resultSetMetaData.getColumnName(i).toLowerCase()] = resultSet.getObject(i)
            }
            results.add(row)
        }
        resultSet.close()

        results as StatementExecutionResults
    }

    private static trait StatementExecutionResultsRow implements Map<String, Object> {
        String call(String columnName) {
            this[columnName] as String
        }
    }

    private static trait StatementExecutionResults implements List<StatementExecutionResultsRow> {
        @Override
        abstract boolean add(StatementExecutionResultsRow statementExecutionResultsRow)
    }
}
