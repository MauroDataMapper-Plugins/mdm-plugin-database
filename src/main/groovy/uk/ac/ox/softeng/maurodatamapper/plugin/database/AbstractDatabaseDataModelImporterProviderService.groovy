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
import javax.sql.DataSource

@CompileStatic
@Slf4j
abstract class AbstractDatabaseDataModelImporterProviderService<T extends DatabaseDataModelImporterProviderServiceParameters>
    extends DataModelImporterProviderService<T> {

    static final String DATABASE_NAMESPACE = 'uk.ac.ox.softeng.maurodatamapper.plugin.database'
    static final String IS_NOT_NULL_CONSTRAINT = 'IS NOT NULL'

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
    static String getStandardConstraintInformationQueryString() {
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
    static String getPrimaryKeyAndUniqueConstraintInformationQueryString() {
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

    static List<String> getCoreColumns() {
        [getSchemaNameColumnName(),
         getDataTypeColumnName(),
         getTableNameColumnName(),
         getColumnNameColumnName(),
         getTableCatalogColumnName(),]
    }

    static String getSchemaNameColumnName() {
        'table_schema'
    }

    static String getDataTypeColumnName() {
        'data_type'
    }

    static String getTableNameColumnName() {
        'table_name'
    }

    static String getColumnNameColumnName() {
        'column_name'
    }

    static String getTableCatalogColumnName() {
        'table_catalog'
    }

    static String getColumnIsNullableColumnName() {
        'is_nullable'
    }

    static Boolean isColumnNullable(String nullableColumnValue) {
        nullableColumnValue.toLowerCase() == 'yes'
    }

    @Override
    Boolean canImportMultipleDomains() {
        true
    }

    Connection getConnection(String databaseName, T params) throws ApiException {
        DataSource dataSource
        try {
            dataSource = params.getDataSource(databaseName)
            return dataSource.getConnection(params.getDatabaseUsername(), params.getDatabasePassword())
        } catch (SQLException e) {
            log.error('Cannot connect to database [{}]: {}', params.getUrl(databaseName), e.getMessage())
            throw new ApiBadRequestException('DIS02', "Cannot connect to database [${params.getUrl(databaseName)}]", e)
        }
    }

    DataModel importDataModel(User currentUser, T params) {
        importDataModels(currentUser, params.databaseNames, params).first()
    }

    List<DataModel> importDataModels(User currentUser, T params) {
        List<String> databases = params.databaseNames.split(',').toList()
        List<DataModel> dataModels = []

        log.info('Importing {} DataModel/s', databases.size())

        databases.each { name ->
            dataModels.addAll(importDataModels(currentUser, name, params))
        }

        dataModels
    }

    List<DataModel> importDataModels(User currentUser, String databaseName, T params) throws ApiException, ApiBadRequestException {
        String modelName = params.isMultipleDataModelImport() ? databaseName : params.getModelName() ?: databaseName
        modelName = params.dataModelNameSuffix ? "${modelName}_${params.dataModelNameSuffix}" : modelName
        Folder folder = Folder.get(params.folderId)

        try {
            Connection connection = getConnection(databaseName, params)
            PreparedStatement st = connection.prepareStatement(getDatabaseStructureQueryString())
            List<Map<String, Object>> results = executeStatement(st)
            st.close()
            results

            log.debug('Size of results from statement {}', results.size())

            if (results.size() == 0) {
                log.warn('No results from database statement, therefore nothing to import for {}.', modelName)
                return []
            }

            List<DataModel> dataModels = importAndUpdateDataModelsFromResults(currentUser, databaseName, params,
                                                                              folder, modelName, results, connection)
            connection.close()
            dataModels
        } catch (SQLException e) {
            log.error('Something went wrong executing statement while importing {} : {}', modelName, e.message)
            throw new ApiBadRequestException('DIS03', 'Cannot execute statement', e)
        }
    }

    DataModel importDataModelFromResults(User user, Folder folder, String modelName, String dialect,
                                         List<Map<String, Object>> results, boolean importSchemaAsDataClass = true) throws ApiException {
        final DataModel dataModel = new DataModel(createdBy: user, label: modelName, type: DataModelType.DATA_ASSET, folder: folder)
        dataModel.addCreatedEdit(user)
        dataModel.addToMetadata(namespace: DATABASE_NAMESPACE, key: 'dialect', value: dialect)

        for (Map<String, Object> row : results) {
            String dataTypeName = (String) row[getDataTypeColumnName()]
            DataType dataType = primitiveTypeService.findOrCreateDataTypeForDataModel(dataModel, dataTypeName, null, user)

            String tableName = (String) row[getTableNameColumnName()]
            DataClass tableDataClass
            if (importSchemaAsDataClass) {
                String schemaName = (String) row[getSchemaNameColumnName()]

                DataClass schemaDataClass = dataClassService.findOrCreateDataClass(dataModel, schemaName, null, user)
                tableDataClass = dataClassService.findOrCreateDataClass(schemaDataClass, tableName, null, user)
            } else {
                tableDataClass = dataClassService.findOrCreateDataClass(dataModel, tableName, null, user)
            }

            String columnName = (String) row[getColumnNameColumnName()]
            String isNullable = (String) row[getColumnIsNullableColumnName()]
            Integer min = isColumnNullable(isNullable) ? 0 : 1
            DataElement de = dataElementService.findOrCreateDataElementForDataClass(tableDataClass, columnName, null, user, dataType,
                                                                                    min, 1)

            row.findAll { col, data ->
                data && !(col in coreColumns)
            }.each { col, data ->
                de.addToMetadata(namespace, col, data.toString(), user)
            }
        }
        dataModel
    }

    List<DataModel> importAndUpdateDataModelsFromResults(
        User currentUser, String databaseName, T params, Folder folder, String modelName, List<Map<String, Object>> results, Connection connection)
        throws ApiException, SQLException {
        DataModel dataModel = importDataModelFromResults(currentUser, folder, modelName, params.databaseDialect, results)
        if (params.dataModelNameSuffix) dataModel.aliasesString = databaseName

        updateDataModelWithDatabaseSpecificInformation(dataModel, connection)
        [dataModel]
    }

    /**
     * Updates DataModel with custom information which is Database specific.
     * Default is to do nothing
     * @param dataModel DataModel to update
     * @param connection Connection to database
     * @return
     */
    void updateDataModelWithDatabaseSpecificInformation(DataModel dataModel, Connection connection) {
        addStandardConstraintInformation(dataModel, connection)
        addPrimaryKeyAndUniqueConstraintInformation(dataModel, connection)
        addIndexInformation(dataModel, connection)
        addForeignKeyInformation(dataModel, connection)
    }

    void addStandardConstraintInformation(DataModel dataModel, Connection connection) throws ApiException, SQLException {
        if (!getStandardConstraintInformationQueryString()) return

        dataModel.childDataClasses.each { schemaClass ->
            executePreparedStatement(dataModel, schemaClass, connection, this.&getStandardConstraintInformationQueryString).each { row ->
                DataClass tableClass = schemaClass.findDataClass(row.table_name as String)

                if (tableClass) {
                    String checkClause = row.check_clause
                    String constraint = checkClause && checkClause.contains(IS_NOT_NULL_CONSTRAINT) ? IS_NOT_NULL_CONSTRAINT : null

                    if (constraint && constraint != IS_NOT_NULL_CONSTRAINT) {
                        // String columnName = checkClause.replace(/ ${constraint}/, '')
                        // DataElement columnElement = tableClass.findChildDataElement(columnName)
                        log.warn('Unhandled constraint {}', constraint)
                    }
                }
            }
        }
    }

    void addPrimaryKeyAndUniqueConstraintInformation(DataModel dataModel, Connection connection) throws ApiException, SQLException {
        if (!getPrimaryKeyAndUniqueConstraintInformationQueryString()) return

        dataModel.childDataClasses.each { schemaClass ->
            executePreparedStatement(dataModel, schemaClass, connection, this.&getPrimaryKeyAndUniqueConstraintInformationQueryString)
                .groupBy { it.constraint_name }
                .each { constraintName, rows ->
                    Map firstRow = rows.first()
                    String value = rows.size() == 1 ? firstRow.column_name : rows.sort { it.ordinal_position }.collect { it.column_name }.join(', ')
                    DataClass tableClass = schemaClass.findDataClass(firstRow.table_name as String)

                    if (tableClass) {
                        String constraintTypeName = (firstRow.constraint_type as String).toLowerCase().replaceAll(/ /, '_')

                        tableClass.addToMetadata(namespace,
                                                 "${constraintTypeName}[${firstRow.constraint_name}]",
                                                 value, dataModel.createdBy)

                        rows.each { row ->
                            DataElement columnElement = tableClass.findDataElement(row.column_name as String)
                            if (columnElement) {
                                columnElement.addToMetadata(namespace, (row.constraint_type as String).toLowerCase(),
                                                            row.ordinal_position as String, dataModel.createdBy)
                            }
                        }
                    }
                }
        }
    }

    void addIndexInformation(DataModel dataModel, Connection connection) throws ApiException, SQLException {
        if (!getIndexInformationQueryString()) return

        dataModel.childDataClasses.each { schemaClass ->
            executePreparedStatement(dataModel, schemaClass, connection, this.&getIndexInformationQueryString).each { row ->
                DataClass tableClass = schemaClass.findDataClass(row.table_name as String)

                if (tableClass) {
                    String indexType = row.primary_index ? 'primary_index' : row.unique_index ? 'unique_index' : 'index'
                    indexType = row.clustered ? "clustered_${indexType}" : indexType

                    tableClass.addToMetadata(namespace, "${indexType}[${row.index_name}]", row.column_names as String, dataModel.createdBy)
                } else log.warn('Could not add {} as DataClass for table {} does not exist', row.index_name, row.table_name)
            }
        }
    }

    void addForeignKeyInformation(DataModel dataModel, Connection connection) throws ApiException, SQLException {
        if (!getForeignKeyInformationQueryString()) return

        dataModel.childDataClasses.each { schemaClass ->
            executePreparedStatement(dataModel, schemaClass, connection, this.&getForeignKeyInformationQueryString).each { row ->
                DataClass foreignTableClass = dataModel.dataClasses.find { it.label == row.reference_table_name }
                DataType dataType

                if (foreignTableClass) {
                    dataType = referenceTypeService.findOrCreateDataTypeForDataModel(
                        dataModel, "${foreignTableClass.label}Type", "Linked to DataElement [${row.reference_column_name}]",
                        dataModel.createdBy as User, foreignTableClass)

                    dataModel.addToDataTypes(dataType)
                } else {
                    dataType = primitiveTypeService.findOrCreateDataTypeForDataModel(
                        dataModel, "${row.reference_table_name}Type",
                        "Missing link to foreign key table [${row.reference_table_name}.${row.reference_column_name}]",
                        dataModel.createdBy as User
                    )
                }

                DataClass tableClass = schemaClass.findDataClass(row.table_name as String)
                DataElement columnElement = tableClass.findDataElement(row.column_name as String)
                columnElement.dataType = dataType
                columnElement.addToMetadata(namespace, "foreign_key[${row.constraint_name}]",
                                            row.reference_column_name as String, dataModel.createdBy)
            }
        }
    }

    List<Map<String, Object>> executePreparedStatement(
        DataModel dataModel, DataClass schemaClass, Connection connection, Closure<String> queryStringGetter)
        throws ApiException, SQLException {
        List<Map<String, Object>> results = []
        try {
            PreparedStatement st = connection.prepareStatement(queryStringGetter())
            st.setString(1, schemaClass.label)
            results = executeStatement(st)
            st.close()
        } catch (SQLException e) {
            if (e.message.contains('Invalid object name \'information_schema.table_constraints\'')) {
                log.warn('No table_constraints available for {}', dataModel.label)
            } else throw e
        }
        results
    }

    static List<Map<String, Object>> executeStatement(PreparedStatement preparedStatement) throws ApiException {
        List list = new ArrayList(50)
        ResultSet rs = preparedStatement.executeQuery()
        ResultSetMetaData md = rs.getMetaData()
        int columns = md.getColumnCount()
        while (rs.next()) {
            Map row = new HashMap(columns)
            for (int i = 1; i <= columns; ++i) {
                row[md.getColumnName(i).toLowerCase()] = rs.getObject(i)
            }
            list.add(row)
        }
        rs.close()
        list
    }
}
