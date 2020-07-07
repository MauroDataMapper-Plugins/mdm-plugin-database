package uk.ac.ox.softeng.maurodatamapper.plugin.database;

import uk.ac.ox.softeng.maurodatamapper.core.spi.importer.parameter.DataModelImporterPluginParameters;
import uk.ac.ox.softeng.maurodatamapper.core.spi.importer.parameter.config.ImportGroupConfig;
import uk.ac.ox.softeng.maurodatamapper.core.spi.importer.parameter.config.ImportParameterConfig;

import java.sql.SQLException;
import java.util.Properties;
import java.util.UUID;
import javax.sql.DataSource;

/**
 * @since 23/08/2017
 */
public abstract class DatabaseImportParameters<K extends DataSource> extends DataModelImporterPluginParameters {

    @ImportParameterConfig(
        displayName = "DataModel name suffix",
        description =
            "A suffix to attach to the end of the auto-imported DataModel name. This should only be used if the DataModel name property is" +
            "not supplied. The suffix will be appended in the form ${modelName}_${suffix}.",
        order = 0,
        optional = true,
        group = @ImportGroupConfig(
            name = "DataModel",
            order = 0
        )
    )
    private String dataModelNameSuffix;

    @ImportParameterConfig(
        displayName = "Database Host",
        description = "The hostname of the server that is running the database",
        order = 2,
        group = @ImportGroupConfig(
            name = "Database Connection Details",
            order = 1
        )
    )
    private String databaseHost;

    @ImportParameterConfig(
        displayName = "Database Name/s",
        description = "A comma separated list of names of the databases to connect to, the database name will be used as the DataModel name " +
                      "unless the DataModel name option is supplied.\n" +
                      "If multiple names supplied then DataModel name will be ignored and the database name will be used as the DataModel name, " +
                      "and the same username and password will be used for all named databases.",
        order = 1,
        group = @ImportGroupConfig(
            name = "Database Import Details",
            order = 2
        )
    )
    private String databaseNames;

    @ImportParameterConfig(
        displayName = "Password",
        description = "The password used to connect to the database.",
        password = true,
        order = 4,
        group = @ImportGroupConfig(
            name = "Database Connection Details",
            order = 1
        )
    )
    private String databasePassword;

    @ImportParameterConfig(
        optional = true,
        displayName = "Database Port",
        description = "The port that the database is accessed through. If not supplied then the default port for the specified type will be used.",
        order = 2,
        group = @ImportGroupConfig(
            name = "Database Connection Details",
            order = 1
        )
    )
    private Integer databasePort;

    @ImportParameterConfig(
        displayName = "SSL",
        description = "Whether SSL should be used to connect to the database.",
        order = 2,
        group = @ImportGroupConfig(
            name = "Database Connection Details",
            order = 1
        )
    )
    private Boolean databaseSSL;

    @ImportParameterConfig(
        displayName = "Username",
        description = "The username used to connect to the database.",
        order = 3,
        group = @ImportGroupConfig(
            name = "Database Connection Details",
            order = 1
        )
    )
    private String databaseUsername;

    public String getDatabaseHost() {
        return databaseHost;
    }

    public void setDatabaseHost(String databaseHost) {
        this.databaseHost = databaseHost;
    }

    public String getDatabaseNames() {
        return databaseNames;
    }

    public String getDatabasePassword() {
        return databasePassword;
    }

    public void setDatabasePassword(String databasePassword) {
        this.databasePassword = databasePassword;
    }

    public void setDatabaseNames(String databaseNames) {
        this.databaseNames = databaseNames;
    }

    public void setDatabasePort(Integer databasePort) {
        this.databasePort = databasePort;
    }

    public Boolean getDatabaseSSL() {
        return databaseSSL;
    }

    public void setDatabaseSSL(Boolean databaseSSL) {
        this.databaseSSL = databaseSSL;
    }

    public String getDatabaseUsername() {
        return databaseUsername;
    }

    public void setDatabaseUsername(String databaseUsername) {
        this.databaseUsername = databaseUsername;
    }

    public String getDataModelNameSuffix() {
        return dataModelNameSuffix;
    }

    public void setDataModelNameSuffix(String dataModelNmaeSuffix) {
        this.dataModelNameSuffix = dataModelNmaeSuffix;
    }

    public abstract String getDatabaseDialect();

    public abstract K getDataSource(String databaseName) throws SQLException;

    public abstract String getUrl(String databaseName);

    protected Integer getDatabasePort() {
        if (databasePort == null) setDatabasePort(getDefaultPort());
        return databasePort;
    }

    public boolean isMultipleDataModelImport() {
        return databaseNames.contains(",");
    }

    public void populateFromProperties(Properties properties) {

        setFinalised(true);
        setImportAsNewDocumentationVersion(false);
        setFolderId(UUID.randomUUID());

        databaseHost = properties.getProperty("import.database.host");
        databaseNames = properties.containsKey("import.database.name") ? properties.getProperty("import.database.name") :
                        properties.getProperty("import.database.names");
        databasePassword = properties.getProperty("import.database.password");
        databaseUsername = properties.getProperty("import.database.username");

        databaseSSL = Boolean.parseBoolean(properties.getProperty("import.database.ssl"));

        try {
            databasePort = Integer.parseInt(properties.getProperty("import.database.port"));
        } catch (NumberFormatException ignored) {
            databasePort = getDefaultPort();
        }
    }

    public abstract int getDefaultPort();

    public void setDatabaseName(String databaseName) {
        databaseNames = databaseName;
    }

}
