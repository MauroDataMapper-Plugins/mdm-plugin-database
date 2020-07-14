package uk.ac.ox.softeng.maurodatamapper.plugin.database

import uk.ac.ox.softeng.maurodatamapper.core.provider.importer.parameter.config.ImportGroupConfig
import uk.ac.ox.softeng.maurodatamapper.core.provider.importer.parameter.config.ImportParameterConfig
import uk.ac.ox.softeng.maurodatamapper.datamodel.provider.importer.parameter.DataModelImporterProviderServiceParameters

import groovy.transform.CompileStatic

import java.sql.SQLException
import java.util.Properties
import java.util.UUID
import javax.sql.DataSource

@CompileStatic
abstract class DatabaseDataModelImporterProviderServiceParameters<K extends DataSource> extends DataModelImporterProviderServiceParameters {

    @ImportParameterConfig(
        displayName = 'DataModel name suffix',
        description = [
            'A suffix to attach to the end of the auto-imported DataModel name.',
            'This should only be used if the DataModel name property is not supplied.',
            'The suffix will be appended in the form ${modelName}_${suffix}.'
        ],
        order = 0,
        optional = true,
        group = @ImportGroupConfig(
            name = 'DataModel',
            order = 0
        )
    )
    String dataModelNameSuffix

    @ImportParameterConfig(
        displayName = 'Database Host',
        description = 'The hostname of the server that is running the database',
        order = 2,
        group = @ImportGroupConfig(
            name = 'Database Connection Details',
            order = 1
        )
    )
    String databaseHost

    @ImportParameterConfig(
        displayName = 'Database Name/s',
        description = [
            'A comma separated list of names of the databases to connect to, the database name will be used as the DataModel name ',
            'unless the DataModel name option is supplied.\n',
            'If multiple names supplied then DataModel name will be ignored and the database name will be used as the DataModel name, ',
            'and the same username and password will be used for all named databases.'
        ],
        order = 1,
        group = @ImportGroupConfig(
            name = 'Database Import Details',
            order = 2
        )
    )
    String databaseNames

    @ImportParameterConfig(
        displayName = 'Password',
        description = 'The password used to connect to the database.',
        password = true,
        order = 4,
        group = @ImportGroupConfig(
            name = 'Database Connection Details',
            order = 1
        )
    )
    String databasePassword

    @ImportParameterConfig(
        optional = true,
        displayName = 'Database Port',
        description = 'The port that the database is accessed through. If not supplied then the default port for the specified type will be used.',
        order = 2,
        group = @ImportGroupConfig(
            name = 'Database Connection Details',
            order = 1
        )
    )
    Integer databasePort

    @ImportParameterConfig(
        displayName = 'SSL',
        description = 'Whether SSL should be used to connect to the database.',
        order = 2,
        group = @ImportGroupConfig(
            name = 'Database Connection Details',
            order = 1
        )
    )
    Boolean databaseSSL

    @ImportParameterConfig(
        displayName = 'Username',
        description = 'The username used to connect to the database.',
        order = 3,
        group = @ImportGroupConfig(
            name = 'Database Connection Details',
            order = 1
        )
    )
    String databaseUsername

    abstract String getDatabaseDialect()

    abstract K getDataSource(String databaseName) throws SQLException

    abstract String getUrl(String databaseName)

    Integer getDatabasePort() {
        if (databasePort == null) databasePort = getDefaultPort()
        databasePort
    }

    boolean isMultipleDataModelImport() {
        databaseNames.contains ','
    }

    void populateFromProperties(Properties properties) {
        setFinalised true
        setImportAsNewDocumentationVersion false
        setFolderId UUID.randomUUID()

        databaseNames = properties.containsKey('import.database.name') ?
            properties.getProperty('import.database.name')
            : properties.getProperty('import.database.names')

        databaseHost = properties.getProperty 'import.database.host'
        databaseUsername = properties.getProperty 'import.database.username'
        databasePassword = properties.getProperty 'import.database.password'

        databaseSSL = Boolean.parseBoolean(properties.getProperty('import.database.ssl'))

        try {
            databasePort = Integer.parseInt(properties.getProperty('import.database.port'))
        } catch (NumberFormatException ignored) {
            databasePort = getDefaultPort()
        }
    }

    abstract int getDefaultPort()
}
