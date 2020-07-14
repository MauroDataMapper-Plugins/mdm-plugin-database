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

    private static void startService(CommandLine commandLine) {
        Path path = Paths.get(commandLine.getOptionValue('c'))
        println "Starting Remote Database Import service\n${getVersionInfo()}\nConfig file: ${path.toAbsolutePath()}\n"
        Utils.outputRuntimeArgs(getClass())
        new RemoteDatabaseImportAndExporter().performImportAndExport(
            new Properties().with { Properties properties ->
                load Files.newInputStream(path)
                setProperty 'server.username', commandLine.getOptionValue('u')
                setProperty 'server.password', commandLine.getOptionValue('p')
                setProperty 'import.database.password', commandLine.getOptionValue('w')
                properties
            })
    }

    private static String getVersionInfo() {
        new StringBuilder().with { StringBuilder stringBuilder ->
            append 'remote-database-importer\n'
            append "  Version: \"${RemoteDatabaseDataModelImporterProviderService.getPackage().getSpecificationVersion()}\"\n"
            append "  Java version: \"${System.getProperty('java.version')}\""
            stringBuilder.toString()
        }
    }

    private static void printHelp() {
        new HelpFormatter().printHelp(
            120,
            'remote-database-importer -c <FILE> -u <USERNAME> -p <PASSWORD> -w <DATABASE_PASSWORD>',
            'Export database to Metadata Catalogue\nConnect to a database, export to DataModel and push to Metadata Catalogue server\n\n',
            defineOptions(),
            "\n${getVersionInfo()}\n\nPlease report issues at https://metadatacatalogue.myjetbrains.com\n",
            false)
    }

    static void main(String[] args) {
        // Assume Slf4j is bound to Logback in the current environment
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory()
        StatusPrinter.print loggerContext // Print Logback's internal status

        // Parse command line arguments
        CommandLine commandLine = new DefaultParser().parse(defineOptions(), args)
        if ('cpu'.any { String option -> commandLine.hasOption option }) {
            startService commandLine
        } else if (commandLine.hasOption('v')) {
            println getVersionInfo()
        } else {
            printHelp()
        }
    }
}
