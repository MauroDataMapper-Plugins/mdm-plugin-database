import ch.qos.logback.classic.filter.ThresholdFilter
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.filter.Filter
import ch.qos.logback.core.spi.FilterReply
import grails.util.BuildSettings
import grails.util.Environment
import org.springframework.boot.logging.logback.ColorConverter
import org.springframework.boot.logging.logback.WhitespaceThrowableProxyConverter

import java.nio.charset.Charset

conversionRule 'clr', ColorConverter
conversionRule 'wex', WhitespaceThrowableProxyConverter

// Message
String ansiPattern = '%clr(%d{ISO8601}){faint} ' + // Date
                     '%clr([%10.10thread]){faint} ' + // Thread
                     '%clr(%-5level) ' + // Log level
                     '%clr(%-40.40logger{39}){cyan} %clr(:){faint} ' + // Logger
                     '%m%n%wex'
// Message

String nonAnsiPattern = '%d{ISO8601} [%10.10thread] %-5level %-40.40logger{39} : %msg%n'

String cIProp = System.getProperty('mc.ciMode')
boolean ciEnv = cIProp ? cIProp.toBoolean() : false

def baseDir = Environment.current in [Environment.CUSTOM, Environment.PRODUCTION] ? BuildSettings.BASE_DIR.canonicalFile :
              BuildSettings.TARGET_DIR.canonicalFile
def clazz = Environment.current in [Environment.CUSTOM, Environment.PRODUCTION] ? RollingFileAppender : FileAppender

File logDir = new File(baseDir, 'logs').canonicalFile
String logFilename = System.getProperty('mc.logFileName') ?: Environment.current == Environment.PRODUCTION ? baseDir.name : baseDir.parentFile.name


// See http://logback.qos.ch/manual/groovy.html for details on configuration
appender('STDOUT', ConsoleAppender) {
    encoder(PatternLayoutEncoder) {
        charset = Charset.forName('UTF-8')
        pattern = ciEnv || Environment.current == Environment.TEST ? nonAnsiPattern : ansiPattern
    }

    if (Environment.current == Environment.TEST) {
        filter(ThresholdFilter) {
            level = INFO
        }
    } else {
        filter(ThresholdFilter) {
            level = INFO
        }
    }
    filter HibernateDeprecationFilter
    filter ConsoleFilter
}

appender("FILE", clazz) {
    file = "${logDir}/${logFilename}.log"
    append = false

    encoder(PatternLayoutEncoder) {
        pattern = nonAnsiPattern
    }
    filter HibernateDeprecationFilter

    if (clazz == RollingFileAppender) {
        rollingPolicy(TimeBasedRollingPolicy) {
            maxHistory = 90
            fileNamePattern = "${logDir}/${logFilename}.%d{yyyy-MM-dd}.log"
        }
    }
    filter(ThresholdFilter) {
        level = TRACE
    }
}
root(INFO, ['STDOUT', 'FILE'])

if (Environment.current != Environment.PRODUCTION) {

    logger('ox.softeng', DEBUG)
    logger('db.migration', DEBUG)

    // logger('org.hibernate.SQL', DEBUG)
    // logger 'org.hibernate.type', TRACE
    logger('org.grails.spring.beans.factory.OptimizedAutowireCapableBeanFactory', ERROR)
    logger('org.springframework.context.support.PostProcessorRegistrationDelegate', WARN)
    logger('org.hibernate.cache.ehcache.AbstractEhcacheRegionFactory', ERROR)
    logger 'org.hibernate.tool.schema.internal.ExceptionHandlerLoggedImpl', ERROR
    logger 'org.hibernate.engine.jdbc.spi.SqlExceptionHelper', ERROR

    logger 'org.springframework.mock.web.MockServletContext', ERROR

}

class HibernateDeprecationFilter extends Filter<ILoggingEvent> {

    @Override
    FilterReply decide(ILoggingEvent event) {
        event.message ==~ /HHH90000022.*/ ? FilterReply.DENY : FilterReply.NEUTRAL
    }
}

class ConsoleFilter extends Filter<ILoggingEvent> {
    @Override
    FilterReply decide(ILoggingEvent event) {
        if (event.loggerName.startsWith('org.springframework') ||
            event.loggerName.startsWith('org.hibernate') ||
            event.loggerName.startsWith('org.apache')
        ) {
            return event.level.isGreaterOrEqual(WARN) ? FilterReply.NEUTRAL : FilterReply.DENY
        }
        FilterReply.NEUTRAL
    }
}
