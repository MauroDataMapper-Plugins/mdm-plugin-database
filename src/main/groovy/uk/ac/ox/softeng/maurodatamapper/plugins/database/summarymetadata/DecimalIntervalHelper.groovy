/*
 * Copyright 2020-2022 University of Oxford and Health and Social Care Information Centre, also known as NHS Digital
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package uk.ac.ox.softeng.maurodatamapper.plugins.database.summarymetadata

import grails.util.Pair


class DecimalIntervalHelper extends AbstractIntervalHelper<BigDecimal> {

    DecimalIntervalHelper(BigDecimal minValue, BigDecimal maxValue) {
        super(minValue, maxValue)
    }

    void calculateInterval() {
        difference = maxValue - minValue

        if (difference == 0) {
            intervalLength = 1.0
        } else if (0 < difference && difference <= 0.1 ) {
            intervalLength = 0.02
        } else if (0.1 < difference && difference <= 1 ) {
            intervalLength = 0.2
        } else if (1 < difference && difference <= 5 ) {
            intervalLength = 1
        } else if (5 < difference && difference <= 10 ) {
            intervalLength = 2
        } else if (10 < difference && difference <= 20 ) {
            intervalLength = 5
        } else if (20 < difference && difference <= 100 ) {
            intervalLength = 10
        } else if (100 < difference && difference <= 200 ) {
            intervalLength = 20
        } else if (200 < difference && difference <= 1000 ) {
            intervalLength = 100
        } else if (1000 < difference && difference <= 2000 ) {
            intervalLength = 200
        } else if (2000 < difference && difference <= 10000 ) {
            intervalLength = 1000
        } else if (10000 < difference && difference <= 20000 ) {
            intervalLength = 2000
        } else if (20000 < difference && difference <= 100000 ) {
            intervalLength = 10000
        } else if (100000 < difference && difference <= 200000 ) {
            intervalLength = 20000
        } else if (200000 < difference && difference <= 1000000 ) {
            intervalLength = 100000
        } else if (1000000 < difference && difference <= 2000000 ) {
            intervalLength = 200000
        } else if (2000000 < difference && difference <= 10000000 ) {
            intervalLength = 1000000
        } else if (10000000 < difference && difference <= 20000000 ) {
            intervalLength = 2000000
        } else if (20000000 < difference && difference <= 100000000 ) {
            intervalLength = 10000000
        }  else {
            Double base = Math.log10((Double) ((maxValue - minValue)  / 10))
            intervalLength = (BigDecimal) Math.pow(10, Math.ceil(base))
        }

        BigDecimal[] minValueDivideAndRemainder = minValue.divideAndRemainder(intervalLength)
        firstIntervalStart = minValueDivideAndRemainder[0] * intervalLength
        //For negative minima which do not align with an interval start, shift the interval one step to the left
        if (Math.abs(minValueDivideAndRemainder[1]) > 0 && minValue < 0) {
            firstIntervalStart = firstIntervalStart - intervalLength
        }

        BigDecimal[] maxValueDivideAndRemainder = maxValue.divideAndRemainder(intervalLength)
        lastIntervalStart = maxValueDivideAndRemainder[0] * intervalLength
        if (Math.abs(maxValueDivideAndRemainder[1]) > 0 && maxValue < 0) {
            lastIntervalStart = lastIntervalStart - intervalLength
        }
    }

    void calculateIntervalStarts() {
        intervalStarts = []
        BigDecimal currNum = firstIntervalStart
        while(currNum <= lastIntervalStart) {
            intervalStarts.add(currNum)
            currNum += intervalLength
        }
    }

    void calculateIntervals() {
        intervals = new LinkedHashMap()
        intervalStarts.each { start ->
            BigDecimal finish = start + intervalLength
            String label = "" + start + labelSeparator + finish
            intervals[label] = (new Pair(start, finish))
        }
    }
}

