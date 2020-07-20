package uk.ac.ox.softeng.maurodatamapper.plugin.database

import uk.ac.ox.softeng.maurodatamapper.core.provider.importer.parameter.config.ImportGroupConfig
import uk.ac.ox.softeng.maurodatamapper.core.provider.importer.parameter.config.ImportParameterConfig
import uk.ac.ox.softeng.maurodatamapper.datamodel.provider.importer.parameter.DataModelImporterProviderServiceParameters

import groovy.transform.CompileStatic
import groovy.transform.PackageScope

import java.sql.SQLException
import javax.sql.DataSource

@CompileStatic
abstract class DatabaseDataModelImporterProviderServiceParameters<K extends DataSource> extends DataModelImporterProviderServiceParameters {

    @ImportParameterConfig(
            displayName = 'DataModel Name Suffix',
            description = [
                    'A suffix to attach to the end of the auto-imported DataModel name.',
                    'This should only be used if the DataModel name property is not supplied.',
                    'The suffix will be appended in the form ${modelName}_${suffix}.'],
            order = 0,
            optional = true,
            group = @ImportGroupConfig(
                    name = 'DataModel',
                    order = 0
            ))
    String dataModelNameSuffix

    @ImportParameterConfig(
            displayName = 'Database Name(s)',
            description = [
                    'A comma separated list of names of the databases to connect to, the database name will be used as the DataModel name',
                    'unless the DataModel name option is supplied.',
                    'If multiple names supplied then DataModel name will be ignored and the database name will be used as the DataModel name,',
                    'and the same username and password will be used for all named databases.'],
            order = 1,
            group = @ImportGroupConfig(
                    name = 'Database Import Details',
                    order = 0
            ))
    String databaseNames

    @ImportParameterConfig(
            displayName = 'Database Host',
            description = 'The hostname of the server that is running the database.',
            order = 2,
            group = @ImportGroupConfig(
                    name = 'Database Connection Details',
                    order = 0
            ))
    String databaseHost

    @ImportParameterConfig(
            displayName = 'Database Port',
            description = [
                    'The port that the database is accessed through.',
                    'If not supplied then the default port for the specified type will be used.'],
            order = 3,
            optional = true,
            group = @ImportGroupConfig(
                    name = 'Database Connection Details',
                    order = 1
            ))
    int databasePort

    @ImportParameterConfig(
            displayName = 'Username',
            description = 'The username used to connect to the database.',
            order = 4,
            group = @ImportGroupConfig(
                    name = 'Database Connection Details',
                    order = 2
            ))
    String databaseUsername

    @ImportParameterConfig(
            displayName = 'Password',
            description = 'The password used to connect to the database.',
            order = 5,
            password = true,
            group = @ImportGroupConfig(
                    name = 'Database Connection Details',
                    order = 3
            ))
    String databasePassword

    @ImportParameterConfig(
            displayName = 'SSL',
            description = 'Whether SSL should be used to connect to the database.',
            order = 6,
            group = @ImportGroupConfig(
                    name = 'Database Connection Details',
                    order = 4
            ))
    boolean databaseSSL

    @PackageScope
    int getDatabasePort() {
        databasePort = databasePort ?: getDefaultPort()
        databasePort
    }

    boolean isMultipleDataModelImport() {
        databaseNames.contains ','
    }

    void populateFromProperties(Properties properties) {
        setFinalised true
        setImportAsNewDocumentationVersion false
        setFolderId UUID.randomUUID()

        databaseNames = properties.getProperty(properties.containsKey('import.database.name') ? 'import.database.name' : 'import.database.names')
        databaseHost = properties.getProperty 'import.database.host'
        databaseUsername = properties.getProperty 'import.database.username'
        databasePassword = properties.getProperty 'import.database.password'
        databaseSSL = properties.getProperty('import.database.ssl') as boolean

        try {
            databasePort = properties.getProperty('import.database.port') as int
        } catch (ignored) {
            databasePort = getDefaultPort()
        }
    }

    abstract K getDataSource(String databaseName) throws SQLException

    abstract String getUrl(String databaseName)

    abstract String getDatabaseDialect()

    abstract int getDefaultPort()
}
