/*
 * Copyright 2020-2021 University of Oxford and Health and Social Care Information Centre, also known as NHS Digital
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

class IntegerIntervalHelper {


    Integer minNum, maxNum
    Integer difference

    String labelSeparator = " - "

    // These determine the interval length

    Integer intervalLength

    Integer firstIntervalStart

    Integer lastIntervalStart

    List<Integer> intervalStarts

    LinkedHashMap<String, Pair<Integer, Integer>> intervals


    IntegerIntervalHelper(Integer minNum, Integer maxNum) {
        this.minNum = minNum
        this.maxNum = maxNum
        difference = maxNum - minNum

        //System.err.println(differencePeriod)
        calculateInterval()
        // calculateIntervalStart()
        calculateIntervalStarts()
        calculateIntervals()
    }

    void calculateInterval() {

        if (1 < difference && difference <= 5 ) {
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
        } else intervalLength = maxNum - minNum / 10

        firstIntervalStart = minNum.intdiv(intervalLength) * intervalLength
    }

    void calculateIntervalStarts() {
        intervalStarts = []
        Integer currNum = firstIntervalStart
        while(currNum < maxNum) {
            intervalStarts.add(currNum)
            currNum += intervalLength
            lastIntervalStart = currNum
        }
    }

    void calculateIntervals() {
        intervals = new LinkedHashMap()
        intervalStarts.each { start ->

            Integer finish = start + intervalLength
            String label = "" + start + labelSeparator + finish
            intervals[label] = (new Pair(start, finish))
        }

    }
}

