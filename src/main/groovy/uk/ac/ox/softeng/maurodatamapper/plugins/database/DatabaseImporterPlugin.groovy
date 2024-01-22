package uk.ac.ox.softeng.maurodatamapper.plugins.database

import uk.ac.ox.softeng.maurodatamapper.provider.plugin.AbstractMauroDataMapperPlugin

import jakarta.inject.Singleton

@Singleton
class DatabaseImporterPlugin extends AbstractMauroDataMapperPlugin {
    @Override
    String getName() {
        'Database Importer Plugin (Generic)'
    }

    Closure doWithSpring() {
        {->
            databaseEnumerationTypeProfileProviderService DatabaseEnumerationTypeProfileProviderService
        }
    }
}
