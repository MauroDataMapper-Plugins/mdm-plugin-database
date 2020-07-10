package uk.ac.ox.softeng.maurodatamapper.plugin.database

import uk.ac.ox.softeng.maurodatamapper.api.exception.ApiBadRequestException
import uk.ac.ox.softeng.maurodatamapper.api.exception.ApiException
import uk.ac.ox.softeng.maurodatamapper.core.container.Folder
import uk.ac.ox.softeng.maurodatamapper.datamodel.DataModel
import uk.ac.ox.softeng.maurodatamapper.datamodel.DataModelService
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.DataClass
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.DataClassService
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.DataElement
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.DataElementService
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.datatype.DataType
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.datatype.PrimitiveTypeService
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.datatype.ReferenceTypeService
import uk.ac.ox.softeng.maurodatamapper.datamodel.provider.importer.DataModelImporterProviderService
import uk.ac.ox.softeng.maurodatamapper.security.CatalogueUser

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Autowired

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException
import javax.sql.DataSource

/**
 * @since 23/08/2017
 * TODO adei please work through this file inside intellij
 * all "red" line needs to be fixed
 * this will/must compile with @CompileStatic in place
 * as a start the @Slf4j will allow you to replace getLogger(). with log.
 * Also make sure the correct methods are overriden for the importing
 * check the extended class for whats not yet implemented
 */
@CompileStatic
@Slf4j
abstract class AbstractDatabaseDataModelImporterProviderService<P extends DatabaseDataModelImporterProviderServiceParameters>
    extends DataModelImporterProviderService<P> {

    public static final String IS_NOT_NULL_CONSTRAINT = 'IS NOT NULL'

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

    abstract String getDatabaseStructureQueryString()

    /**
     * Must return a String which will be queryable by schema name,
     * and return a row with the following elements:
     *
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
     *
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
     *
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
     *
     *  * constraint_name
     *  * table_name
     *  * column_name
     *  * reference_table_name
     *  * reference_column_name
     * @return
     */
    abstract String getForeignKeyInformationQueryString()

    String getColumnNameColumnName() {
        'column_name'
    }

    String getDataTypeColumnName() {
        'data_type'
    }

    String getSchemaNameColumnName() {
        'table_schema'
    }

    String getTableCatalogColumnName() {
        'table_catalog'
    }

    String getTableNameColumnName() {
        'table_name'
    }

    String getColumnIsNullableColumnName() {
        'is_nullable'
    }

    List<String> getCoreColumns() {
        [getSchemaNameColumnName(),
         getDataTypeColumnName(),
         getTableNameColumnName(),
         getColumnNameColumnName(),
         getTableCatalogColumnName(),]
    }

    @Override
    DataModel importDataModel(CatalogueUser currentUser, P params) {
        importDataModels(currentUser, params.databaseNames, params).first()
    }

    @Override
    List<DataModel> importDataModels(CatalogueUser currentUser, P params) {

        List<String> databases = params.databaseNames.split(',').toList()
        List<DataModel> dataModels = []

        getLogger().info('Importing {} DataModel/s', databases.size())

        databases.each {name ->
            dataModels.addAll(importDataModels(currentUser, name, params))
        }

        dataModels
    }

    @Override
    Boolean canImportMultipleDomains() {
        true
    }

    List<DataModel> importDataModels(CatalogueUser currentUser, String databaseName, P params) {
        String modelName = params.isMultipleDataModelImport() ? databaseName : params.getDataModelName() ?: databaseName
        modelName = params.dataModelNameSuffix ? "${modelName}_${params.dataModelNameSuffix}" : modelName
        Folder folder = Folder.get(params.folderId)

        try {
            Connection connection = getConnection(databaseName, params)
            List<Map<String, Object>> results = executeCoreStatement(connection, params)

            getLogger().debug('Size of results from statement {}', results.size())

            if (results.size() == 0) {
                getLogger().warn('No results from database statement, therefore nothing to import for {}.', modelName)
                return []
            }

            List<DataModel> dataModels = importAndUpdateDataModelsFromResults(currentUser, databaseName, params,
                                                                              folder, modelName, results, connection)
            connection.close()
            dataModels
        } catch (SQLException ex) {
            getLogger().error('Something went wrong executing statement while importing {} : {}', modelName, ex.message)
            throw new ApiBadRequestException('DIS03', 'Cannot execute statement', ex)
        }
    }

    List<DataModel> importAndUpdateDataModelsFromResults(CatalogueUser currentUser, String databaseName, P params, Folder folder,
                                                         String modelName, List<Map<String, Object>> results, Connection connection) {
        DataModel dataModel = importDataModelFromResults(currentUser, folder, modelName, params.databaseDialect, results)
        if (params.dataModelNameSuffix) dataModel.aliasesString = databaseName

        updateDataModelWithDatabaseSpecificInformation(dataModel, connection)
        [dataModel]
    }

    Connection getConnection(String databaseName, P params) throws ApiException {
        DataSource dataSource
        try {
            dataSource = params.getDataSource(databaseName)
            return dataSource.getConnection(params.getDatabaseUsername(), params.getDatabasePassword())
        } catch (SQLException e) {
            getLogger().error('Cannot connect to database [{}]: {}', params.getUrl(databaseName), e.getMessage())
            throw new ApiBadRequestException('DIS02', "Cannot connect to database [${params.getUrl(databaseName)}]", e)
        }
    }

    @SuppressWarnings(['unchecked', 'UnusedMethodParameter'])
    List<Map<String, Object>> executeCoreStatement(Connection connection, P params) throws ApiException {
        List<Map<String, Object>> results = []
        PreparedStatement st = prepareCoreStatement(connection, params)
        results = executeStatement(st)
        st.close()
        results
    }

    @SuppressWarnings(['unchecked', 'UnusedMethodParameter'])
    PreparedStatement prepareCoreStatement(Connection connection, P params) {
        connection.prepareStatement(getDatabaseStructureQueryString())
    }

    List<Map<String, Object>> executeStatement(PreparedStatement preparedStatement) throws ApiException {
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

    Boolean isColumnNullable(String nullableColumnValue) {
        nullableColumnValue.toLowerCase() == 'yes'
    }

    DataModel importDataModelFromResults(CatalogueUser catalogueUser, Folder folder, String modelName, String dialect,
                                         List<Map<String, Object>> results, boolean importSchemaAsDataClass = true) throws ApiException {

        final DataModel dataModel = dataModelService.createDatabase(catalogueUser, modelName, null, null, null, dialect,
                                                                    folder)

        for (Map<String, Object> row : results) {
            String dataTypeName = (String) row[getDataTypeColumnName()]
            DataType dataType = primitiveTypeService.findOrCreateDataTypeForDataModel(dataModel, dataTypeName, null, catalogueUser)

            String tableName = (String) row[getTableNameColumnName()]
            DataClass tableDataClass
            if (importSchemaAsDataClass) {
                String schemaName = (String) row[getSchemaNameColumnName()]

                DataClass schemaDataClass = dataClassService.findOrCreateDataClass(dataModel, schemaName, null, catalogueUser)
                tableDataClass = dataClassService.findOrCreateDataClass(schemaDataClass, tableName, null, catalogueUser)
            } else {
                tableDataClass = dataClassService.findOrCreateDataClass(dataModel, tableName, null, catalogueUser)
            }

            String columnName = (String) row[getColumnNameColumnName()]
            String isNullable = (String) row[getColumnIsNullableColumnName()]
            Integer min = isColumnNullable(isNullable) ? 0 : 1
            DataElement de = dataElementService.findOrCreateDataElementForDataClass(tableDataClass, columnName, null, catalogueUser, dataType,
                                                                                    min, 1)

            row.findAll {col, data ->
                data && !(col in coreColumns)
            }.each {col, data ->
                de.addToMetadata(namespace, col, data.toString(), catalogueUser)
            }
        }
        dataModel
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

    void addStandardConstraintInformation(DataModel dataModel, Connection connection) {

        if (!getStandardConstraintInformationQueryString()) return

        dataModel.childDataClasses.each {schemaClass ->
            List<Map<String, Object>> results = []

            try {
                PreparedStatement st = connection.prepareStatement(getStandardConstraintInformationQueryString())
                st.setString(1, schemaClass.label)
                results = executeStatement(st)
                st.close()
            } catch (SQLException ex) {
                if (ex.message.contains('Invalid object name \'information_schema.table_constraints\'')) {
                    logger.warn('No table_constraints available for {}', dataModel.label)
                } else throw ex
            }

            results.each {row ->

                DataClass tableClass = schemaClass.findChildDataClass(row.table_name as String)

                if (tableClass) {
                    String checkClause = row.check_clause
                    String constraint = extractConstraint(checkClause)

                    if (constraint && constraint != IS_NOT_NULL_CONSTRAINT) {
                        //                    String columnName = checkClause.replace(/ ${constraint}/, '')
                        //                    DataElement columnElement = tableClass.findChildDataElement(columnName)
                        logger.warn('Unhandled constraint {}', constraint)
                    }
                }
            }
        }
    }

    @SuppressWarnings("UnnecessaryCollectCall")
    void addPrimaryKeyAndUniqueConstraintInformation(DataModel dataModel, Connection connection) {

        if (!getPrimaryKeyAndUniqueConstraintInformationQueryString()) return

        dataModel.childDataClasses.each {schemaClass ->
            List<Map<String, Object>> results = []

            try {
                PreparedStatement st = connection.prepareStatement(getPrimaryKeyAndUniqueConstraintInformationQueryString())
                st.setString(1, schemaClass.label)
                results = executeStatement(st)
                st.close()
            } catch (SQLException ex) {
                if (ex.message.contains('Invalid object name \'information_schema.table_constraints\'')) {
                    logger.warn('No table_constraints available for {}', dataModel.label)
                } else throw ex
            }

            results.groupBy {it.constraint_name}.each {constraintName, rows ->
                Map firstRow = rows.first()
                String value = rows.size() == 1 ? firstRow.column_name : rows.sort {it.ordinal_position}.collect {it.column_name}.join(', ')
                DataClass tableClass = schemaClass.findChildDataClass(firstRow.table_name as String)

                if (tableClass) {

                    String constraintTypeName = (firstRow.constraint_type as String).toLowerCase().replaceAll(/ /, '_')

                    tableClass.addToMetadata(namespace,
                                             "${constraintTypeName}[${firstRow.constraint_name}]",
                                             value, dataModel.createdBy)

                    rows.each {row ->
                        DataElement columnElement = tableClass.findChildDataElement(row.column_name as String)
                        if (columnElement) {
                            columnElement.addToMetadata(namespace, (row.constraint_type as String).toLowerCase(),
                                                        row.ordinal_position as String, dataModel.createdBy)
                        }
                    }
                }
            }
        }
    }

    void addForeignKeyInformation(DataModel dataModel, Connection connection) {

        if (!getForeignKeyInformationQueryString()) return

        dataModel.childDataClasses.each {schemaClass ->
            List<Map<String, Object>> results = []

            PreparedStatement st = connection.prepareStatement(getForeignKeyInformationQueryString())
            st.setString(1, schemaClass.label)
            results = executeStatement(st)
            st.close()

            results.each {row ->

                DataClass foreignTableClass = dataModel.dataClasses.find {it.label == row.reference_table_name}
                DataType dataType

                if (foreignTableClass) {
                    dataType = referenceTypeService.findOrCreateDataTypeForDataModel(
                        dataModel, "${foreignTableClass.label}Type", "Linked to DataElement [${row.reference_column_name}]",
                        dataModel.createdBy, foreignTableClass)

                    dataModel.addToDataTypes(dataType)
                } else {
                    dataType = primitiveTypeService.findOrCreateDataTypeForDataModel(
                        dataModel, "${row.reference_table_name}Type",
                        "Missing link to foreign key table [${row.reference_table_name}.${row.reference_column_name}]",
                        dataModel.createdBy
                    )
                }

                DataClass tableClass = schemaClass.findChildDataClass(row.table_name as String)
                DataElement columnElement = tableClass.findChildDataElement(row.column_name as String)
                columnElement.dataType = dataType
                columnElement.addToMetadata(namespace, "foreign_key[${row.constraint_name}]",
                                            row.reference_column_name as String, dataModel.createdBy)
            }
        }
    }

    void addIndexInformation(DataModel dataModel, Connection connection) {

        if (!getIndexInformationQueryString()) return

        dataModel.childDataClasses.each {schemaClass ->
            List<Map<String, Object>> results = []

            PreparedStatement st = connection.prepareStatement(getIndexInformationQueryString())
            st.setString(1, schemaClass.label)
            results = executeStatement(st)
            st.close()

            results.each {row ->
                DataClass tableClass = schemaClass.findChildDataClass(row.table_name as String)

                if (tableClass) {
                    String indexType = row.primary_index ? 'primary_index' : row.unique_index ? 'unique_index' : 'index'
                    indexType = row.clustered ? "clustered_${indexType}" : indexType

                    tableClass.addToMetadata(namespace, "${indexType}[${row.index_name}]", row.column_names as String, dataModel.createdBy)
                } else logger.warn('Could not add {} as DataClass for table {} does not exist', row.index_name, row.table_name)
            }
        }
    }

    private static String extractConstraint(String checkClause) {
        if (checkClause && checkClause.contains(IS_NOT_NULL_CONSTRAINT)) return IS_NOT_NULL_CONSTRAINT
        null
    }
}
