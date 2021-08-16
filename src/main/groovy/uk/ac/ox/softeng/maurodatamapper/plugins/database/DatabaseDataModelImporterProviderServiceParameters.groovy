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

import uk.ac.ox.softeng.maurodatamapper.core.provider.importer.parameter.config.ImportGroupConfig
import uk.ac.ox.softeng.maurodatamapper.core.provider.importer.parameter.config.ImportParameterConfig
import uk.ac.ox.softeng.maurodatamapper.datamodel.provider.importer.parameter.DataModelImporterProviderServiceParameters

import java.sql.SQLException
import javax.sql.DataSource

// @CompileStatic
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
            order = 2
        ))
    String databaseNames

    @ImportParameterConfig(
        displayName = 'Database Host',
        description = 'The hostname of the server that is running the database.',
        order = 2,
        group = @ImportGroupConfig(
            name = 'Database Connection Details',
            order = 1
        ))
    String databaseHost

    @ImportParameterConfig(
        displayName = 'Database Port',
        description = [
            'The port that the database is accessed through.',
            'If not supplied then the default port for the specified type will be used.'],
        order = 2,
        optional = true,
        group = @ImportGroupConfig(
            name = 'Database Connection Details',
            order = 1
        ))
    Integer databasePort

    @ImportParameterConfig(
        displayName = 'Username',
        description = 'The username used to connect to the database.',
        order = 3,
        group = @ImportGroupConfig(
            name = 'Database Connection Details',
            order = 1
        ))
    String databaseUsername

    @ImportParameterConfig(
        displayName = 'Password',
        description = 'The password used to connect to the database.',
        order = 4,
        password = true,
        group = @ImportGroupConfig(
            name = 'Database Connection Details',
            order = 1
        ))
    String databasePassword

    @ImportParameterConfig(
        displayName = 'SSL',
        description = 'Whether SSL should be used to connect to the database.',
        order = 2,
        group = @ImportGroupConfig(
            name = 'Database Connection Details',
            order = 1
        ))
    Boolean databaseSSL

    @ImportParameterConfig(
        displayName = 'Detect Enumerations',
        description = 'Whether to treat columns with small numbers of unique values as enumerations',
        order = 1,
        optional = true,
        group = @ImportGroupConfig(
                name = 'Configuration',
                order = 2
        )
    )
    Boolean detectEnumerations = false

    @ImportParameterConfig(
        displayName = 'Maximum Enumerations',
        description = 'The maximum number of unique values to be interpreted as a defined enumeration',
        order = 2,
        optional = true,
        group = @ImportGroupConfig(
                name = 'Configuration',
                order = 2
        )
    )
    Integer maxEnumerations = 20

    Integer getDatabasePort() {
        databasePort = databasePort ?: defaultPort
        databasePort
    }

    boolean isMultipleDataModelImport() {
        databaseNames.contains ','
    }

    void populateFromProperties(Properties properties) {
        finalised = true
        importAsNewDocumentationVersion = false
        folderId = UUID.randomUUID()

        databaseNames = properties.getProperty(properties.containsKey('import.database.name') ? 'import.database.name' : 'import.database.names')
        databaseHost = properties.getProperty 'import.database.host'
        databaseUsername = properties.getProperty 'import.database.username'
        databasePassword = properties.getProperty 'import.database.password'
        databaseSSL = properties.getProperty('import.database.ssl') as Boolean

        if (!maxEnumerations) {
            maxEnumerations = 20
        }

        try {
            databasePort = properties.getProperty('import.database.port') as Integer
        } catch (NumberFormatException ignored) {
            databasePort = defaultPort
        }
    }

    abstract K getDataSource(String databaseName) throws SQLException

    abstract String getUrl(String databaseName)

    abstract String getDatabaseDialect()

    abstract int getDefaultPort()

    boolean shouldImportSchemasAsDataClasses() {
        true
    }
}
