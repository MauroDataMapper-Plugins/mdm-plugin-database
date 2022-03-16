package uk.ac.ox.softeng.maurodatamapper.plugins.database.summarymetadata

import uk.ac.ox.softeng.maurodatamapper.test.MdmSpecification

import groovy.util.logging.Slf4j
import org.grails.testing.GrailsUnitTest

import static org.junit.Assert.assertEquals

/**
 * @since 15/03/2022
 */
@Slf4j
class IntegerIntervalHelperSpec extends MdmSpecification implements GrailsUnitTest {

    IntegerIntervalHelper iih

    def cleanup() {
        iih = null
    }

    void 'simple case'() {
        when:
        //Simple interval
        iih = new IntegerIntervalHelper(1, 500)

        then:
        checkIntervals(50, '0 - 50', '500 - 550')
    }

    void 'Negative minimum left of boundary'() {
        when:
        //Negative minimum left of boundary
        iih = new IntegerIntervalHelper(-30000001, 19999999)

        then:
        checkIntervals(5000000, '-35000000 - -30000000', '15000000 - 20000000')
    }

    void 'Negative minimum on boundary'() {
        when:
        //Negative minimum on boundary
        iih = new IntegerIntervalHelper(-30000000, 19999999)

        then:
        checkIntervals(5000000, '-30000000 - -25000000', '15000000 - 20000000')
    }

    void 'Negative minimum right of boundary'() {
        when:
        //Negative minimum right of boundary
        iih = new IntegerIntervalHelper(-29999999, 19999999)

        then:
        checkIntervals(5000000, '-30000000 - -25000000', '15000000 - 20000000')
    }

    void 'Negative max, left of boundary'() {
        when:
        //Negative max, left of boundary
        iih = new IntegerIntervalHelper(-5100, -1001)

        then:
        checkIntervals(500, '-5500 - -5000', '-1500 - -1000')
    }

    void 'Negative max, onboundary'() {
        when:
        //Negative max, onboundary
        iih = new IntegerIntervalHelper(-5100, -1000)

        then:
        checkIntervals(500, '-5500 - -5000', '-1000 - -500')
    }

    void 'Negative max, right of boundary'() {
        when:
        //Negative max, right of boundary
        iih = new IntegerIntervalHelper(-5100, -999)

        then:
        checkIntervals(500, '-5500 - -5000', '-1000 - -500')
    }

    void 'Zero interval'() {
        when:
        //Zero interval
        iih = new IntegerIntervalHelper(83, 83)

        then:
        checkIntervals(1, '83 - 84', '83 - 84')
    }

    void 'Positive min and max, both left of boundary'() {
        when:
        //Positive min and max, both left of boundary
        iih = new IntegerIntervalHelper(999, 5999)

        then:
        checkIntervals(500, '500 - 1000', '5500 - 6000')
    }

    void 'Positive min and max, both on boundary'() {
        when:
        //Positive min and max, both on boundary
        iih = new IntegerIntervalHelper(1000, 6000)

        then:
        checkIntervals(500, '1000 - 1500', '6000 - 6500')
    }

    void 'Positive min and max, both right of boundary'() {
        when:
        //Positive min and max, both right of boundary
        iih = new IntegerIntervalHelper(1001, 6001)

        then:
        checkIntervals(500, '1000 - 1500', '6000 - 6500')
    }

    void 'Beyond defined intervals'() {
        when:
        //Beyond defined intervals
        iih = new IntegerIntervalHelper(123, 558000000)

        then:
        checkIntervals(100000000, '0 - 100000000', '500000000 - 600000000')
    }

    private void checkIntervals(int length, String firstKey, String lastKey) {
        log.warn('{}', iih)
        assertEquals "Interval length", length, iih.intervalLength
        assertEquals "First key", firstKey, iih.orderedKeys.first()
        assertEquals "Last key", lastKey, iih.orderedKeys.last()
    }
}
