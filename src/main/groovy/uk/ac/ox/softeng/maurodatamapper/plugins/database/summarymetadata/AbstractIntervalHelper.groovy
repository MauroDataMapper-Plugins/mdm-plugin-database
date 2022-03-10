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
import groovy.transform.CompileStatic

@CompileStatic
abstract class AbstractIntervalHelper <S extends Object> {

    S minValue, maxValue
    S difference
    S intervalLength
    S firstIntervalStart
    S lastIntervalStart
    List<S> intervalStarts
    Map<String, Pair<S, S>> intervals

    AbstractIntervalHelper(minValue, S maxValue) {
        this.minValue = minValue
        this.maxValue = maxValue
        initialise()
    }

    void initialise(){
        calculateInterval()
        calculateIntervalStarts()
        calculateIntervals()
    }

    String getLabelSeparator() {
        ' - '
    }

    abstract void calculateInterval()

    abstract void calculateIntervalStarts()

    abstract void calculateIntervals()

    String toString(){
        "${getClass().simpleName}:\n  ${intervals.collect {it.key}.join('\n  ')}"
    }
}

