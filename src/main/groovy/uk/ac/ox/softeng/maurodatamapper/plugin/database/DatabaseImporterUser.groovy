package uk.ac.ox.softeng.maurodatamapper.plugin.database

import uk.ac.ox.softeng.maurodatamapper.security.User

import groovy.transform.CompileStatic

@Singleton
@CompileStatic
class DatabaseImporterUser implements User {

    String emailAddress = 'databaseImporter@maurodatamapper.com'
    String firstName = 'Database Importer'
    String lastName = 'User'
    String tempPassword = ''

    @Override
    UUID getId() {
        UUID.randomUUID()
    }

    @Override
    String getDomainType() {
        DatabaseImporterUser
    }
}
