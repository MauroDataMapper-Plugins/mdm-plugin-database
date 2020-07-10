package uk.ac.ox.softeng.maurodatamapper.plugin.database

import org.slf4j.LoggerFactory

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.core.util.StatusPrinter
import groovyjarjarcommonscli.CommandLine
import groovyjarjarcommonscli.CommandLineParser
import groovyjarjarcommonscli.DefaultParser
import groovyjarjarcommonscli.HelpFormatter
import groovyjarjarcommonscli.Option
import groovyjarjarcommonscli.OptionGroup
import groovyjarjarcommonscli.Options

import java.nio.file.Path
import java.nio.file.Paths

/**
 * @since 15/03/2018
 */
@CompileStatic
trait RemoteDatabaseImporter {

    private static final CommandLineParser parser = new DefaultParser()

    private static Options defineOptions() {

        Options options = new Options()
        OptionGroup mainGroup = new OptionGroup()
        mainGroup.addOption(
            Option.builder('c').longOpt('config')
                .argName('FILE')
                .hasArg().required()
                .desc('The config file defining the import config')
                .build())
        mainGroup.addOption(Option.builder('h').longOpt('help').build())
        mainGroup.addOption(Option.builder('v').longOpt('version').build())
        options.addOptionGroup(mainGroup)

        options.addOption(Option.builder('u').longOpt('username')
                              .desc('Username for Metadata Catalogue (Required)')
                              .argName('USERNAME').hasArg()
                              .build())
        options.addOption(Option.builder('p').longOpt('password')
                              .desc('Password for Metadata Catalogue (Required)')
                              .argName('PASSWORD').hasArg()
                              .build())
        options.addOption(Option.builder('w').longOpt('databasePassword')
                              .desc('Password for Database (Required)')
                              .argName('DATABASE_PASSWORD').hasArg()
                              .build())
        options
    }

    private static void help() {
        HelpFormatter formatter = new HelpFormatter()

        String header = 'Export database to Metadata Catalogue.\n' +
                        'Connect to a database, export to DataModel and push to Metadata Catalogue server\n\n'
        String footer = "\n${version()}\n\nPlease report issues at https://metadatacatalogue.myjetbrains.com\n"

        formatter.printHelp(120,
                            'remote-database-importer -c <FILE> -u <USERNAME> -p <PASSWORD> -w <DATABASE_PASSWORD>',
                            header, defineOptions(), footer, false)
    }

    private static String fullVersion() {
        "remote-database-importer ${version()}"
    }

    private static String version() {
        "  Version: \"${RemoteDatabaseImporter.getPackage().getSpecificationVersion()}\"\n" +
        "  Java Version: \"${System.getProperty('java.version')}\""
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
        else if (line.hasOption('v')) println(fullVersion())
        else if (line.hasOption('c') && line.hasOption('u') && line.hasOption('p')) {

            Path path = Paths.get(line.getOptionValue('c'))

            println('Starting Remote Database Import service\n' +
                    "${version()}\n" +
                    "  Config File: ${path.toAbsolutePath().toString()}\n")

            RemoteDatabaseImportAndExporter remoteDatabaseImportAndExporter = new RemoteDatabaseImportAndExporter()

            remoteDatabaseImportAndExporter.outputRuntimeArgs()

            remoteDatabaseImportAndExporter.performImportAndExport(new Properties().with {
                load(path.newInputStream())
                setProperty('server.username', line.getOptionValue('u'))
                setProperty('server.password', line.getOptionValue('p'))
                setProperty('import.database.password', line.getOptionValue('w'))
                it
            })

        } else help()
    }
}
