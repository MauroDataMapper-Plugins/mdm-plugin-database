package uk.ac.ox.softeng.maurodatamapper.plugin.database

import uk.ac.ox.softeng.maurodatamapper.api.exception.ApiException
import uk.ac.ox.softeng.maurodatamapper.core.container.Folder
import uk.ac.ox.softeng.maurodatamapper.core.importer.ImporterService
import uk.ac.ox.softeng.maurodatamapper.core.provider.importer.parameter.FileParameter
import uk.ac.ox.softeng.maurodatamapper.core.provider.importer.parameter.ImporterProviderServiceParameters
import uk.ac.ox.softeng.maurodatamapper.datamodel.Application
import uk.ac.ox.softeng.maurodatamapper.datamodel.DataModel
import uk.ac.ox.softeng.maurodatamapper.datamodel.provider.exporter.JsonExporterService
import uk.ac.ox.softeng.maurodatamapper.datamodel.provider.importer.parameter.DataModelFileImporterProviderServiceParameters
import uk.ac.ox.softeng.maurodatamapper.security.CatalogueUser
import uk.ac.ox.softeng.maurodatamapper.security.User
import uk.ac.ox.softeng.maurodatamapper.util.Utils

import org.grails.orm.hibernate.HibernateDatastore
import org.grails.web.json.JSONObject
import org.springframework.context.ApplicationContext
import org.springframework.context.MessageSource
import org.springframework.http.HttpStatus
import org.springframework.orm.hibernate5.SessionHolder
import org.springframework.transaction.support.TransactionSynchronizationManager
import org.springframework.validation.Errors
import org.springframework.validation.FieldError
import org.springframework.validation.ObjectError

import grails.boot.GrailsApp
import grails.plugin.json.view.JsonViewTemplateEngine
import grails.views.WritableScriptTemplate
import grails.web.mime.MimeType
import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j

import java.security.SecureRandom
import javax.net.ssl.HttpsURLConnection
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManager
import javax.net.ssl.X509TrustManager

// @CompileStatic
@Slf4j
class RemoteDatabaseImporterService extends AbstractDatabaseDataModelImporterProviderService<DatabaseDataModelImporterProviderServiceParameters> {

    private static final JsonSlurper jsonSlurper = new JsonSlurper()
    private static final Map<String, String> endpoints = [
            LOGIN              : '/authentication/login',
            LOGOUT             : '/authentication/logout',
            DATAMODEL_IMPORTERS: '/public/plugins/dataModelImporters'
    ]

    private static ApplicationContext applicationContext

    @Override
    String getIndexInformationQueryString() {
        ''
    }

    @Override
    String getForeignKeyInformationQueryString() {
        ''
    }

    @Override
    String getDatabaseStructureQueryString() {
        ''
    }

    @Override
    String getDisplayName() {
        'Remote Database Importer'
    }

    @Override
    String getVersion() {
        '2.0.0-SNAPSHOT'
    }

    Object post(String url, byte[] bytes) {
        connect(openJsonConnection(url).tap {
            setDoOutput true
            outputStream.write bytes
        })
    }

    Object post(String host, String path, byte[] bytes) {
        post "${host}${path}", bytes
    }

    Object post(String host, String path, String message) {
        post host, path, message.bytes
    }

    Object post(String host, String path, ImporterProviderServiceParameters parameters) {
        post host, path, writeToJson(parameters).bytes
    }

    Object get(String url) {
        connect openJsonConnection(url)
    }

    Object get(String host, String path) {
        get "${host}${path}"
    }

    void performImportAndExport(Properties properties) {
        log.info 'Performing remote import and export of DataModel'
        try {
            CatalogueUser user = setupGorm()
            List<DataModel> importedModels = importDatabases(properties, user)
            if (!importedModels) {
                log.error 'Cannot import databases due to errors'
                return
            }
            sendModelsToMauroDataMapper properties, user, importedModels
        } catch (ApiException e) {
            log.error 'Failed to import and export database', e.cause ?: e
        } catch (e) {
            log.error 'Unhandled exception, failed to import and export database', e
        } finally {
            shutdownGorm()
        }
    }

    private List<DataModel> importDatabases(Properties properties, User user) {
        AbstractDatabaseDataModelImporterProviderService dbImporter = applicationContext.getBean(AbstractDatabaseDataModelImporterProviderService)
        log.info 'Importing Databases using {} (v{})', dbImporter.class.simpleName, dbImporter.version

        ImporterService importer = applicationContext.getBean(ImporterService)
        DatabaseDataModelImporterProviderServiceParameters dbImportParams = importer.createNewImporterProviderServiceParameters(dbImporter)
        dbImportParams.populateFromProperties properties

        Folder randomFolder = new Folder(label: 'random', createdBy: user)
        randomFolder.id = UUID.randomUUID()
        dbImportParams.folderId = randomFolder.id

        Errors errors = importer.validateParameters(dbImportParams, dbImporter.importerProviderServiceParametersClass)
        if (errors.hasErrors()) {
            outputErrors errors, applicationContext.getBean(MessageSource)
            return []
        }

        List<DataModel> dataModels = importer.importModels(user, dbImporter, dbImportParams)
        dataModels.each { DataModel dataModel ->
            dataModel.folder = randomFolder
            dataModel.validate()
        }
        if (dataModels.any { DataModel dataModel -> dataModel.hasErrors() }) {
            dataModels.findAll { DataModel dataModel -> dataModel.hasErrors() }.each {
                DataModel dataModel -> outputErrors dataModel.errors, applicationContext.getBean(MessageSource)
            }
            return []
        }

        dataModels
    }

    private void sendModelsToMauroDataMapper(Properties properties, User user, List<DataModel> dataModels) {
        log.info 'Sending DataModel to Mauro Data Mapper server'

        CookieHandler.setDefault(new CookieManager(null, CookiePolicy.ACCEPT_ALL)) // Ensure session is maintained throughout

        String host = properties.getProperty('export.server.url')
        host = !host.endsWith('/') ?: host - '/'
        host = host.endsWith('api') ?: "${host}/api"
        if (host.startsWith('https')) enableSslConnection()
        log.info 'Using Mauro Data Mapper server: {}', host

        log.info 'Logging into server as "{}"', properties.getProperty('server.username')
        Closure logInRequest = { ->
            post host, endpoints.LOGIN, "{\"username\": \"${properties.getProperty('server.username')}\",\
                                          \"password\": \"${properties.getProperty('server.password')}\"}"
        }
        if (!evalCheckNotNull(logInRequest, 'Could not log in', null, false)) return
        log.debug 'Logged in, now exporting DataModel'

        log.info 'Getting list of importers in server' // We need to do this to ensure we use the correct version of importer
        Closure<List<Map>> importersRequest = { -> get(host, endpoints.DATAMODEL_IMPORTERS) as List<Map> }
        List<Map> importers = evalCheckNotNull(importersRequest, 'No importers could be retrieved', host) as List<Map>
        if (!importers) return

        log.debug 'Getting Folder for DataModel'
        Closure<String> folderPathRequest = { -> properties.getProperty 'export.folder.path' }
        String folderPath = evalCheckNotNull(folderPathRequest, 'Property export.folder.path was not supplied', host)
        if (!folderPath) return

        Closure<JSONObject> folderJsonRequest = { -> get(host, "/folders/${URLEncoder.encode(folderPath, 'UTF-8')}") as JSONObject }
        JSONObject folderJson = evalCheckNotNull(folderJsonRequest, "No folder could be found matching path '${folderPath}'", host) as JSONObject
        if (!folderJson) return

        log.debug 'Importing/exporting DataModels to JSON'
        Map jsonImporter = importers.find { Map importer -> (importer as JSONObject).name == 'JsonImporterService' } as Map
        JsonExporterService jsonExporter = applicationContext.getBean(JsonExporterService)

        dataModels.each { DataModel dataModel ->
            log.info 'Using JSON importer {}.{} (v{})', jsonImporter.namespace, jsonImporter.name, jsonImporter.version
            post host, "/dataModels/import/${jsonImporter.namespace}/${jsonImporter.name}/${jsonImporter.version}",
                 new DataModelFileImporterProviderServiceParameters(
                         modelName: properties.getProperty('export.dataModel.name') ?: dataModel.label,
                         finalised: properties.getProperty('export.dataModel.finalised') ?: true,
                         importAsNewDocumentationVersion: true,
                         folderId: Utils.toUuid(folderJson.id as String),
                         importFile: new FileParameter(dataModel.label, MimeType.JSON.name,
                                                       jsonExporter.exportDataModel(user, dataModel).toByteArray()))
        }

        logout host
        log.info 'Successfully exported to remote server'
    }

    private evalCheckNotNull(Closure closure, String errorMessage, String host, boolean logoutIfFailure = true) {
        def value = closure()
        if (value) return value
        if (logoutIfFailure) logout host
        log.error 'Could not export to remote server: {}', errorMessage
        null
    }

    private CatalogueUser setupGorm() {
        log.info 'Starting Grails Application to handle GORM'

        ['grails.bootstrap.skip'     : 'true', // GORM properties
         'grails.env'                : 'custom',
         'server.port'               : '9000',
         'flyway.enabled'            : 'false',
         'dataSource.driverClassName': 'org.h2.Driver',
         'dataSource.dialect'        : 'org.hibernate.dialect.H2Dialect',
         'dataSource.username'       : 'sa',
         'dataSource.password'       : '',
         'dataSource.dbCreate'       : 'create-drop',
         'dataSource.url'            : 'jdbc:h2:mem:remoteDb;MVCC=TRUE;LOCK_TIMEOUT=10000;DB_CLOSE_ON_EXIT=TRUE',
        ].each { k, v -> System.setProperty(k, v) }

        applicationContext = GrailsApp.run(Application) // NB(adjl)
        HibernateDatastore hibernateDatastore = applicationContext.getBean(HibernateDatastore)
        TransactionSynchronizationManager.bindResource(hibernateDatastore.getSessionFactory(), new SessionHolder(hibernateDatastore.openSession()))

        new CatalogueUser().tap {
            emailAddress = 'databaseImporter@metadatacatalogue.com'
            firstName = 'Database'
            lastName = 'Importer'
            organisation = 'Oxford BRC Informatics'
            jobTitle = 'Worker'
        }
    }

    private void shutdownGorm() {
        log.debug 'Shutting down Grails Application'
        if (applicationContext) GrailsApp.exit(applicationContext)
    }

    private void outputErrors(Errors errors, MessageSource messageSource) {
        log.error 'Errors validating domain: {}', errors.objectName
        errors.allErrors.each { ObjectError error ->
            StringBuilder message = new StringBuilder(messageSource ? messageSource.getMessage(error, Locale.default)
                                                                    : "${error.defaultMessage} :: ${Arrays.asList(error.arguments)}")
            if (error instanceof FieldError) message.append(" :: [${error.field}]")
            log.error message.toString()
        }
    }

    private void logout(String host) {
        log.info 'Logging out'
        get host, endpoints.LOGOUT
    }

    private Object connect(HttpURLConnection connection) {
        log.debug 'Performing {} to {}', connection.requestMethod, connection.getURL()

        HttpStatus response = HttpStatus.valueOf(connection.responseCode)
        if (response.is2xxSuccessful()) {
            String body = connection.inputStream.text
            log.trace 'Success Response:\n{}', prettyPrint(body)
            try {
                return jsonSlurper.parseText(body)
            } catch (ignored) {
                return body
            }
        }

        log.error 'Could not {} to Mauro Data Mapper server at [{}]. Response: {} {}. Message: {}',
                  connection.requestMethod, connection.getURL(), response.value(), response.reasonPhrase,
                  prettyPrint(connection.errorStream?.text)

        null
    }

    private static HttpURLConnection openJsonConnection(String url) {
        new URL(url).openConnection().with { URLConnection connection ->
            setRequestProperty 'Accept', 'application/json'
            setRequestProperty 'Content-Type', 'application/json'
            connection as HttpURLConnection
        }
    }

    private static String writeToJson(ImporterProviderServiceParameters parameters) {
        StringWriter stringWriter = new StringWriter()
        String domainName = parameters.class.simpleName.uncapitalize()
        WritableScriptTemplate template = applicationContext.getBean(JsonViewTemplateEngine).resolveTemplate("/${domainName}/_${domainName}.gson")
        template.make([domainName: parameters]).writeTo(stringWriter)
        stringWriter.toString()
    }

    private static String prettyPrint(String json) {
        try {
            new JsonBuilder(jsonSlurper.parseText(json)).toPrettyString()
        } catch (ignored) {
            json
        }
    }

    private static void enableSslConnection() {
        SSLContext sslContext = SSLContext.getInstance('SSL')
        def trustAll = [getAcceptedIssuers: {}, checkClientTrusted: { a, b -> }, checkServerTrusted: { a, b -> }]
        sslContext.init(null, [trustAll as X509TrustManager] as TrustManager[], new SecureRandom())
        HttpsURLConnection.defaultSSLSocketFactory = sslContext.socketFactory
    }
}
