package uk.ac.ox.softeng.maurodatamapper.plugin.database

import uk.ac.ox.softeng.maurodatamapper.util.Utils

import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.Option
import org.apache.commons.cli.OptionGroup
import org.apache.commons.cli.Options
import org.slf4j.LoggerFactory

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.core.util.StatusPrinter
import groovy.transform.CompileStatic

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

@CompileStatic
trait RemoteDatabaseDataModelImporterProviderService {

    private static Options options = null

    private static Options getOptions() {
        if (options) return options

        Collection<Option> optionDefinitions = [
                Option.builder('c').with {
                    longOpt 'config'
                    desc 'Config file defining the import configuration'
                    argName 'FILE'
                    hasArg().required().build()
                },
                Option.builder('v').longOpt('version').build(),
                Option.builder('h').longOpt('help').build()
        ]

        final OptionGroup mainOptions = new OptionGroup()
        optionDefinitions.each { Option option -> mainOptions.addOption option }

        optionDefinitions = [
                Option.builder('u').with {
                    longOpt 'username'
                    desc 'Username for the Mauro Data Mapper (required)'
                    argName 'USERNAME'
                    hasArg().build()
                },
                Option.builder('p').with {
                    longOpt 'password'
                    desc 'Password for the Mauro Data Mapper (required)'
                    argName 'PASSWORD'
                    hasArg().build()
                },
                Option.builder('w').with {
                    longOpt 'databasePassword'
                    desc 'Password for the database to import (required)'
                    argName 'DATABASE_PASSWORD'
                    hasArg().build()
                }
        ]

        options = new Options()
        options.addOptionGroup mainOptions
        optionDefinitions.each { Option option -> options.addOption option }
        options
    }

    private static void startService(CommandLine commandLine) {
        final Path path = Paths.get(commandLine.getOptionValue('c'))
        println "Starting Remote Database Import service\n${getVersionInfo()}\nConfig file: ${path.toAbsolutePath()}\n"
        Utils.outputRuntimeArgs(getClass())
        new RemoteDatabaseImporterService().performImportAndExport(
                new Properties().with { Properties properties ->
                    load Files.newInputStream(path)
                    setProperty 'server.username', commandLine.getOptionValue('u')
                    setProperty 'server.password', commandLine.getOptionValue('p')
                    setProperty 'import.database.password', commandLine.getOptionValue('w')
                    properties
                })
    }

    private static String getVersionInfo() {
        new StringBuilder().with {
            append "remote-database-importer version: ${RemoteDatabaseDataModelImporterProviderService.package.specificationVersion}\n"
            append "Java version: ${System.getProperty('java.version')}\n"
            toString()
        }
    }

    private static void printHelp() {
        new HelpFormatter().printHelp(
                120,
                'remote-database-importer -c <FILE> -u <USERNAME> -p <PASSWORD> -w <DATABASE_PASSWORD>',
                'Import database to the Mauro Data Mapper\nConnect to a database, import to a DataModel and push to the Mauro server\n\n',
                getOptions(),
                "\n${getVersionInfo()}\nPlease report issues at https://metadatacatalogue.myjetbrains.com\n",
                false)
    }

    static void main(String[] args) {
        // Assume Slf4j is bound to Logback in the current environment
        final LoggerContext loggerContext = LoggerFactory.getILoggerFactory() as LoggerContext
        StatusPrinter.print loggerContext // Print Logback's internal status

        // Parse command line arguments
        final CommandLine commandLine = new DefaultParser().parse(getOptions(), args)
        if ('cpu'.any { String option -> commandLine.hasOption option }) {
            startService commandLine
        } else if (commandLine.hasOption('v')) {
            println getVersionInfo()
        } else {
            printHelp()
        }
    }
}
