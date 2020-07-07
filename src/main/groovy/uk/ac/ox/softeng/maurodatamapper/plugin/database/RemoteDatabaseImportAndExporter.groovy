package uk.ac.ox.softeng.maurodatamapper.plugin.database

import uk.ac.ox.softeng.maurodatamapper.core.Application
import uk.ac.ox.softeng.maurodatamapper.core.api.exception.ApiException
import uk.ac.ox.softeng.maurodatamapper.core.catalogue.linkable.datamodel.DataModel
import uk.ac.ox.softeng.maurodatamapper.core.feature.Folder
import uk.ac.ox.softeng.maurodatamapper.core.gorm.UuidDomain
import uk.ac.ox.softeng.maurodatamapper.core.spi.importer.ImporterService
import uk.ac.ox.softeng.maurodatamapper.core.spi.importer.parameter.DataModelFileImporterPluginParameters
import uk.ac.ox.softeng.maurodatamapper.core.spi.importer.parameter.FileParameter
import uk.ac.ox.softeng.maurodatamapper.core.spi.importer.parameter.ImporterPluginParameters
import uk.ac.ox.softeng.maurodatamapper.core.spi.json.JsonExporterService
import uk.ac.ox.softeng.maurodatamapper.core.type.user.UserRole
import uk.ac.ox.softeng.maurodatamapper.core.user.CatalogueUser

import grails.boot.GrailsApp
import grails.plugin.json.view.JsonViewTemplateEngine
import grails.web.mime.MimeType
import groovy.json.JsonBuilder
import groovy.json.JsonException
import groovy.json.JsonSlurper
import org.grails.orm.hibernate.HibernateDatastore
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationContext
import org.springframework.context.MessageSource
import org.springframework.http.HttpStatus
import org.springframework.orm.hibernate5.SessionHolder
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionSynchronizationManager
import org.springframework.validation.Errors
import org.springframework.validation.FieldError

import java.lang.management.ManagementFactory
import java.lang.management.RuntimeMXBean
import java.security.SecureRandom
import javax.net.ssl.HttpsURLConnection
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManager
import javax.net.ssl.X509TrustManager

/**
 * @since 16/03/2018
 */
class RemoteDatabaseImportAndExporter {

    private static final Logger logger = LoggerFactory.getLogger(RemoteDatabaseImportAndExporter)

    private static ApplicationContext applicationContext
    private static PlatformTransactionManager transactionManager
    private static final JsonSlurper jsonSlurper = new JsonSlurper()

    private static final Map<String, String> gormProperties = [
        'grails.bootstrap.skip'     : 'true',
        'grails.env'                : 'custom',
        'server.port'               : '9000',
        'flyway.enabled'            : 'false',
        'dataSource.driverClassName': 'org.h2.Driver',
        'dataSource.dialect'        : 'org.hibernate.dialect.H2Dialect',
        'dataSource.username'       : 'sa',
        'dataSource.password'       : '',
        'dataSource.dbCreate'       : 'create-drop',
        'dataSource.url'            : 'jdbc:h2:mem:remoteDb;MVCC=TRUE;LOCK_TIMEOUT=10000;DB_CLOSE_ON_EXIT=TRUE',
    ]

    void outputRuntimeArgs() {
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        List<String> arguments = runtimeMxBean.getInputArguments();
        logger.warn("Running with JVM args:\n  " + arguments.join('\n  '))
    }

    void performImportAndExport(Properties loadedProperties) {
        logger.info('Performing remote import and export of DataModel')
        try {

            CatalogueUser exportUser = setupGorm()

            List<DataModel> importedModels = importDatabases(loadedProperties, exportUser)

            if (!importedModels) {
                logger.error('Cannot import databases due to errors')
                return
            }
            sendModelsToMetadataCatalogue(loadedProperties, exportUser, importedModels)
        } catch (ApiException ex) {
            logger.error('Failed to import and export database', ex.cause ?: ex)
        }
        catch (Exception ex) {
            logger.error('Unhandled exception, failed to import and export database', ex)
        } finally {
            shutdownGorm()
        }
    }

    List<DataModel> importDatabases(Properties loadedProperties, CatalogueUser catalogueUser) {
        AbstractDatabaseImporter databaseImporter = applicationContext.getBean(AbstractDatabaseImporter)
        logger.info('Importing Databases using {} (v{})', databaseImporter.class.simpleName, databaseImporter.version)

        ImporterService importerService = applicationContext.getBean(ImporterService)

        DatabaseImportParameters databaseImportParameters = importerService.createNewImporterPluginParameters(databaseImporter)
        databaseImportParameters.populateFromProperties(loadedProperties)

        Folder randomFolder = new Folder(label: 'random', createdBy: catalogueUser)
        randomFolder.id = UUID.randomUUID()
        databaseImportParameters.setFolderId(randomFolder.id)

        Errors errors = importerService.validateParameters(databaseImportParameters, databaseImporter.importerPluginParametersClass)

        if (errors.hasErrors()) {
            outputErrors(errors, applicationContext.getBean(MessageSource))
            return []
        }

        List<DataModel> dataModels = importerService.importDataModels(catalogueUser, databaseImporter, databaseImportParameters)

        dataModels.each {dataModel ->
            dataModel.folder = randomFolder
            dataModel.validate()
        }

        if (dataModels.any {it.hasErrors()}) {
            dataModels.findAll {it.hasErrors()}.each {dataModel ->
                outputErrors(dataModel.errors, applicationContext.getBean(MessageSource))
            }
            return []
        }

        dataModels
    }

    @Deprecated
    void sendModelToMetadataCatalogue(Properties loadedProperties, CatalogueUser catalogueUser, DataModel dataModel) {
        logger.info('Sending DataModel to Metadata Catalogue server')

        String failReason = null
        // Ensure session is maintained throughout
        CookieHandler.setDefault(new CookieManager(null, CookiePolicy.ACCEPT_ALL))

        String host = loadedProperties.getProperty('export.server.url')
        host = host.endsWith('/') ? host - '/' : host
        host = host.endsWith('api') ? host : "${host}/api"

        if (host.startsWith('https')) enableSslConnection()

        logger.info('Using Metadata Catalogue server: {}', host)

        logger.info('Logging into server as "{}"', loadedProperties.getProperty('server.username'))
        Object loggedIn = post(host, '/authentication/login', '{' +
                                                              "\"username\" : \"${loadedProperties.getProperty('server.username')}\"," +
                                                              "\"password\" : \"${loadedProperties.getProperty('server.password')}\"" +
                                                              '}')
        def result = null
        if (loggedIn) {
            logger.debug('Logged in, now exporting DataModel')

            // We need to do this to ensure we use the correct version of importer
            logger.info('Getting list of importers in server')
            List<Map> importersList = get(host, '/public/plugins/dataModelImporters') as List

            if (importersList) {
                Map jsonImporterInfo = importersList.find {it.name == 'JsonImporterService'}

                logger.debug('Getting Folder for DataModel')
                String folderPath = loadedProperties.getProperty('export.folder.path')
                if (folderPath) {

                    Object folderJson = get(host, "/folders/${URLEncoder.encode(folderPath, 'UTF-8')}")

                    if (folderJson) {
                        logger.debug('Exporting DataModel to JSON')
                        JsonExporterService jsonExporterService = applicationContext.getBean(JsonExporterService)
                        ByteArrayOutputStream outputStream = jsonExporterService.exportDataModel(catalogueUser, dataModel)

                        logger.info('Using JSON importer {}.{} (v{})', jsonImporterInfo.namespace, jsonImporterInfo.name, jsonImporterInfo.version)
                        DataModelFileImporterPluginParameters importerPluginParameters = new DataModelFileImporterPluginParameters(
                            importAsNewDocumentationVersion: true,
                            finalised: loadedProperties.getProperty('export.dataModel.finalised') ?: true,
                            dataModelName: loadedProperties.getProperty('export.dataModel.name') ?: dataModel.label,
                            folderId: UuidDomain.toUuid(folderJson.id),
                            importFile: new FileParameter(dataModel.label, MimeType.JSON.name, outputStream.toByteArray())
                        )
                        result = post(host, "/dataModels/import/${jsonImporterInfo.namespace}/${jsonImporterInfo.name}/${jsonImporterInfo.version}",
                                      importerPluginParameters)

                    } else failReason = "No folder could be found matching path '${folderPath}'"
                } else failReason = 'Property export.folder.path was not supplied'
            } else failReason = 'No importers could be retrieved'

            logger.info('Logging out')
            get(host, '/authentication/logout')

        } else failReason = 'Could not log in'

        if (result) {
            logger.info("Successfully exported to remote server")
        } else {
            logger.error("Could not export to remote server: {}", failReason)
        }
    }

    void sendModelsToMetadataCatalogue(Properties loadedProperties, CatalogueUser catalogueUser, List<DataModel> dataModels) {
        logger.info('Sending DataModel to Metadata Catalogue server')

        String failReason = null
        // Ensure session is maintained throughout
        CookieHandler.setDefault(new CookieManager(null, CookiePolicy.ACCEPT_ALL))

        String host = loadedProperties.getProperty('export.server.url')
        host = host.endsWith('/') ? host - '/' : host
        host = host.endsWith('api') ? host : "${host}/api"

        if (host.startsWith('https')) enableSslConnection()

        logger.info('Using Metadata Catalogue server: {}', host)

        logger.info('Logging into server as "{}"', loadedProperties.getProperty('server.username'))
        Object loggedIn = post(host, '/authentication/login', '{' +
                                                              "\"username\" : \"${loadedProperties.getProperty('server.username')}\"," +
                                                              "\"password\" : \"${loadedProperties.getProperty('server.password')}\"" +
                                                              '}')
        def result = null
        if (loggedIn) {
            logger.debug('Logged in, now exporting DataModel')

            // We need to do this to ensure we use the correct version of importer
            logger.info('Getting list of importers in server')
            List<Map> importersList = get(host, '/public/plugins/dataModelImporters') as List

            if (importersList) {
                Map jsonImporterInfo = importersList.find {it.name == 'JsonImporterService'}

                logger.debug('Getting Folder for DataModel')
                String folderPath = loadedProperties.getProperty('export.folder.path')
                if (folderPath) {

                    Object folderJson = get(host, "/folders/${URLEncoder.encode(folderPath, 'UTF-8')}")

                    if (folderJson) {
                        logger.debug('Exporting DataModels to JSON')
                        JsonExporterService jsonExporterService = applicationContext.getBean(JsonExporterService)

                        dataModels.each {dataModel ->
                            logger.debug('Exporting DataModel {} to JSON', dataModel.label)
                            ByteArrayOutputStream outputStream = jsonExporterService.exportDataModel(catalogueUser, dataModel)

                            logger.
                                info('Using JSON importer {}.{} (v{})', jsonImporterInfo.namespace, jsonImporterInfo.name, jsonImporterInfo.version)
                            DataModelFileImporterPluginParameters importerPluginParameters = new DataModelFileImporterPluginParameters(
                                importAsNewDocumentationVersion: true,
                                finalised: loadedProperties.getProperty('export.dataModel.finalised') ?: true,
                                dataModelName: loadedProperties.getProperty('export.dataModel.name') ?: dataModel.label,
                                folderId: UuidDomain.toUuid(folderJson.id),
                                importFile: new FileParameter(dataModel.label, MimeType.JSON.name, outputStream.toByteArray())
                            )
                            result =
                                post(host, "/dataModels/import/${jsonImporterInfo.namespace}/${jsonImporterInfo.name}/${jsonImporterInfo.version}",
                                     importerPluginParameters)
                        }
                    } else failReason = "No folder could be found matching path '${folderPath}'"
                } else failReason = 'Property export.folder.path was not supplied'
            } else failReason = 'No importers could be retrieved'

            logger.info('Logging out')
            get(host, '/authentication/logout')

        } else failReason = 'Could not log in'

        if (result) {
            logger.info("Successfully exported to remote server")
        } else {
            logger.error("Could not export to remote server: {}", failReason)
        }
    }

    void enableSslConnection() {
        def sc = SSLContext.getInstance('SSL')
        def trustAll = [getAcceptedIssuers: {}, checkClientTrusted: {a, b ->}, checkServerTrusted: {a, b ->}]
        sc.init(null, [trustAll as X509TrustManager] as TrustManager[], new SecureRandom())
        HttpsURLConnection.defaultSSLSocketFactory = sc.socketFactory
    }

    Object post(String host, String path, String message) {
        post(host, path, message.bytes)
    }

    Object post(String host, String path, byte[] bytes) {
        post("${host}${path}", bytes)
    }

    Object post(String host, String path, ImporterPluginParameters importerPluginParameters) {
        JsonViewTemplateEngine templateEngine = applicationContext.getBean(JsonViewTemplateEngine)
        def template = templateEngine.resolveTemplate(getDomainTemplateUri(importerPluginParameters))
        def writable = template.make(getRenderModel(importerPluginParameters))
        def sw = new StringWriter()
        writable.writeTo(sw)
        post host, path, sw.toString().bytes
    }

    Object post(String url, byte[] bytes) {
        connect(openJsonConnection(url).with {
            setDoOutput(true)
            outputStream.write(bytes)
            it
        } as HttpURLConnection)
    }

    Object get(String host, String path) {
        get("${host}${path}")
    }

    Object get(String url) {
        connect(openJsonConnection(url))
    }

    private HttpURLConnection openJsonConnection(String url) {
        new URL(url).openConnection().with {
            setRequestProperty('Accept', 'application/json')
            setRequestProperty('Content-Type', 'application/json')
            it
        } as HttpURLConnection
    }

    private Object connect(HttpURLConnection connection) {

        logger.debug('Performing {} to {}', connection.requestMethod, connection.getURL())

        HttpStatus response = HttpStatus.valueOf(connection.responseCode)

        if (response.is2xxSuccessful()) {
            String body = connection.inputStream.text
            logger.trace('Success Response:\n{}', prettyPrint(body))
            try {
                return jsonSlurper.parseText(body)
            } catch (JsonException ignored) {
                return body
            }

        }
        logger.error('Could not {} to Metadata Catalogue server at [{}]. Response: {} {}. Message: {}', connection.requestMethod,
                     connection.getURL(), response.value(),
                     response.reasonPhrase, prettyPrint(connection.errorStream?.text))
        null
    }

    private String getDomainTemplateUri(domain) {
        String domainName = domain.class.simpleName
        "/${domainName.uncapitalize()}/_${domainName.uncapitalize()}.gson"
    }

    private Map getRenderModel(domain) {
        Map<String, Object> map = [:]
        map.put("${domain.class.simpleName.uncapitalize()}".toString(), domain)
        map
    }

    private String prettyPrint(String jsonBody) {
        try {
            new JsonBuilder(jsonSlurper.parseText(jsonBody)).toPrettyString()
        } catch (Exception ignored) {
            jsonBody
        }
    }

    private CatalogueUser setupGorm() {
        logger.info('Starting Grails Application to handle GORM')
        gormProperties.each {k, v -> System.setProperty(k, v as String)}

        applicationContext = GrailsApp.run(Application)

        HibernateDatastore hibernateDatastore = applicationContext.getBean(HibernateDatastore)

        TransactionSynchronizationManager.bindResource(hibernateDatastore.getSessionFactory(),
                                                       new SessionHolder(hibernateDatastore.openSession()))

        transactionManager = applicationContext.getBean(PlatformTransactionManager)

        new CatalogueUser().with {
            setEmailAddress('databaseImporter@metadatacatalogue.com')
            setUserRole(UserRole.ADMINISTRATOR)
            setFirstName('Database')
            setLastName('Importer')
            setOrganisation('Oxford BRC Informatics')
            setJobTitle('Worker')
            it
        }

    }

    private void shutdownGorm() throws IOException {
        logger.debug('Shutting down Grails Application')
        if (applicationContext != null) GrailsApp.exit(applicationContext)
    }

    private void outputErrors(Errors errors, MessageSource messageSource) {
        logger.error 'Errors validating domain: {}', errors.objectName
        errors.allErrors.each {error ->

            String msg = messageSource ? messageSource.getMessage(error, Locale.default) :
                         "${error.defaultMessage} :: ${Arrays.asList(error.arguments)}"

            if (error instanceof FieldError) msg += " :: [${error.field}]"
            logger.error msg
        }
    }
}
