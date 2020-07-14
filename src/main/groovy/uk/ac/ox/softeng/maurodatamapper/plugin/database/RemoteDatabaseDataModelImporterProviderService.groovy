package uk.ac.ox.softeng.maurodatamapper.plugin.database

import uk.ac.ox.softeng.maurodatamapper.util.Utils

import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.CommandLineParser
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.Option
import org.apache.commons.cli.OptionGroup
import org.apache.commons.cli.Options

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.core.util.StatusPrinter
import org.slf4j.LoggerFactory

import groovy.transform.CompileStatic

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

@CompileStatic
trait RemoteDatabaseDataModelImporterProviderService {

    private static final CommandLineParser parser = new DefaultParser()

    private static Options defineOptions() {
        Collection<Option> optionsList = [
            Option.builder('c').longOpt('config')
                .desc('The config file defining the import config')
                .argName('FILE').hasArg().required().build(),
            Option.builder('h').longOpt('help').build(),
            Option.builder('v').longOpt('version').build()
        ]

        OptionGroup mainGroup = new OptionGroup()
        optionsList.each { Option option -> mainGroup.addOption option }

        optionsList = [
            Option.builder('u').longOpt('username')
                .desc('Username for Metadata Catalogue (Required)')
                .argName('USERNAME').hasArg().build(),
            Option.builder('p').longOpt('password')
                .desc('Password for Metadata Catalogue (Required)')
                .argName('PASSWORD').hasArg().build(),
            Option.builder('w').longOpt('databasePassword')
                .desc('Password for Database (Required)')
                .argName('DATABASE_PASSWORD').hasArg().build()
        ]

        Options options = new Options()
        options.addOptionGroup(mainGroup)
        optionsList.each { Option option -> options.addOption option }
        options
    }

    private static String version() {
        "remote-database-importer " +
        "  Version: \"${RemoteDatabaseDataModelImporterProviderService.getPackage().getSpecificationVersion()}\"\n" +
        "  Java Version: \"${System.getProperty('java.version')}\""
    }

    private static void help() {
        new HelpFormatter().printHelp(
            120,
            'remote-database-importer -c <FILE> -u <USERNAME> -p <PASSWORD> -w <DATABASE_PASSWORD>',
            'Export database to Metadata Catalogue\nConnect to a database, export to DataModel and push to Metadata Catalogue server\n\n',
            defineOptions(),
            "\n${version()}\n\nPlease report issues at https://metadatacatalogue.myjetbrains.com\n",
            false)
    }

    @SuppressWarnings('Println')
    static void main(String[] args) {

        // assume SLF4J is bound to logback in the current environment
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory()
        // print logback's internal status
        StatusPrinter.print(lc)

        // parse the command line arguments
        CommandLine line = parser.parse(defineOptions(), args)
        if (line.hasOption('h')) help()
        else if (line.hasOption('v')) println(version())
        else if (line.hasOption('c') && line.hasOption('u') && line.hasOption('p')) {

            Path path = Paths.get(line.getOptionValue('c'))

            println('Starting Remote Database Import service\n' +
                    "${version()}\n" +
                    "  Config File: ${path.toAbsolutePath().toString()}\n")

            RemoteDatabaseImportAndExporter remoteDatabaseImportAndExporter = new RemoteDatabaseImportAndExporter()

            Utils.outputRuntimeArgs(getClass())

            remoteDatabaseImportAndExporter.performImportAndExport(new Properties().with {
                load(Files.newInputStream(path))
                setProperty('server.username', line.getOptionValue('u'))
                setProperty('server.password', line.getOptionValue('p'))
                setProperty('import.database.password', line.getOptionValue('w'))
                it
            })

        } else help()
    }
}
