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

import java.time.DayOfWeek
import java.time.LocalDateTime
import java.time.Duration
import java.time.LocalTime
import java.time.Period
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.temporal.TemporalAdjusters


class DateIntervalHelper  extends AbstractIntervalHelper<LocalDateTime> {

    Duration differenceDuration
    Period differencePeriod

    DateTimeFormatter getDateDateTimeFormatter() {
        DateTimeFormatter.ofPattern("dd/MM/yyyy")
    }

    DateTimeFormatter getDateTimeDateTimeFormatter() {
        DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm")
    }

    DateTimeFormatter getMonthDateTimeFormatter() {
        DateTimeFormatter.ofPattern("MMM yyyy")
    }

    // These determine the interval length
    int intervalLengthSize
    ChronoUnit intervalLengthDimension

    DateIntervalHelper(LocalDateTime minValue, LocalDateTime maxValue) {
        super(minValue, maxValue)
    }

    void calculateInterval() {
        differenceDuration = Duration.between(minValue, maxValue)
        differencePeriod = Period.between(minValue.toLocalDate(), maxValue.toLocalDate())

        long diffYears = ChronoUnit.YEARS.between(minValue, maxValue)
        long diffMonths = ChronoUnit.MONTHS.between(minValue, maxValue)
        long diffWeeks = ChronoUnit.WEEKS.between(minValue, maxValue)
        long diffDays = ChronoUnit.DAYS.between(minValue, maxValue)
        long diffHours = ChronoUnit.HOURS.between(minValue, maxValue)
        long diffMinutes = ChronoUnit.MINUTES.between(minValue, maxValue)

        if(diffYears > 40) {
            intervalLengthSize = 1
            intervalLengthDimension = ChronoUnit.DECADES

            firstIntervalStart = minValue.with(TemporalAdjusters.firstDayOfYear())
            while(firstIntervalStart.getYear() % 10 != 0) {
                firstIntervalStart = firstIntervalStart.minusDays(1)
                firstIntervalStart = firstIntervalStart.with(TemporalAdjusters.firstDayOfYear())
            }
            firstIntervalStart = LocalDateTime.of(firstIntervalStart.toLocalDate(),LocalTime.MIDNIGHT)
        }
        else if(diffYears > 2 && diffYears <= 40) {
            intervalLengthSize = 1
            intervalLengthDimension = ChronoUnit.YEARS

            firstIntervalStart = minValue.with(TemporalAdjusters.firstDayOfYear())
            firstIntervalStart = LocalDateTime.of(firstIntervalStart.toLocalDate(),LocalTime.MIDNIGHT)

        }
        else if(diffMonths > 10 && diffYears <= 2 ) {
            intervalLengthSize = 2
            intervalLengthDimension = ChronoUnit.MONTHS

            firstIntervalStart = minValue.with(TemporalAdjusters.firstDayOfMonth())
            while(firstIntervalStart.getMonthValue() % 2 != 0) {
                firstIntervalStart = firstIntervalStart.minusDays(1)
                firstIntervalStart = firstIntervalStart.with(TemporalAdjusters.firstDayOfMonth())
            }
            firstIntervalStart = LocalDateTime.of(firstIntervalStart.toLocalDate(),LocalTime.MIDNIGHT)
        }
        else if(diffMonths > 5 && diffMonths <= 10 ) {
            intervalLengthSize = 1
            intervalLengthDimension = ChronoUnit.MONTHS

            firstIntervalStart = minValue.with(TemporalAdjusters.firstDayOfMonth())
            firstIntervalStart = LocalDateTime.of(firstIntervalStart.toLocalDate(),LocalTime.MIDNIGHT)
        }
        else if(diffMonths > 3 && diffMonths <= 5 ) {
            intervalLengthSize = 2
            intervalLengthDimension = ChronoUnit.WEEKS

            firstIntervalStart = minValue.with(DayOfWeek.MONDAY)
            firstIntervalStart = LocalDateTime.of(firstIntervalStart.toLocalDate(),LocalTime.MIDNIGHT)
        }
        else if(diffMonths > 0 && diffMonths <= 3 ) {
            intervalLengthSize = 1
            intervalLengthDimension = ChronoUnit.WEEKS

            firstIntervalStart = minValue.with(DayOfWeek.MONDAY)
            firstIntervalStart = LocalDateTime.of(firstIntervalStart.toLocalDate(),LocalTime.MIDNIGHT)
        }
        else if(differencePeriod.getDays() > 15 && diffDays <= 35 ) {
            intervalLengthSize = 2
            intervalLengthDimension = ChronoUnit.DAYS

            firstIntervalStart = LocalDateTime.of(minValue.toLocalDate(), LocalTime.MIDNIGHT)
            firstIntervalStart = LocalDateTime.of(firstIntervalStart.toLocalDate(),LocalTime.MIDNIGHT)
        }
        else if(diffDays > 1 && diffDays <= 15 ) {
            intervalLengthSize = 1
            intervalLengthDimension = ChronoUnit.DAYS

            firstIntervalStart = LocalDateTime.of(minValue.toLocalDate(), LocalTime.MIDNIGHT)
        }
        else if(diffHours > 5 && diffHours <= 30 ) {
            intervalLengthSize = 1
            intervalLengthDimension = ChronoUnit.HOURS

            firstIntervalStart = minValue.withMinute(0)
        }
        else {
            intervalLengthSize = 1
            intervalLengthDimension = ChronoUnit.MINUTES

            firstIntervalStart = minValue.withSecond(0)
        }
    }

    void calculateIntervalStarts() {
        intervalStarts = []
        LocalDateTime currDateTime = firstIntervalStart
        while(currDateTime <= maxValue) {
            intervalStarts.add(currDateTime)
            currDateTime = currDateTime.plus(intervalLengthSize, intervalLengthDimension)
        }
    }

    void calculateIntervals() {
        intervals = new TreeMap()
        intervalStarts.each { start ->

            LocalDateTime finish = start.plus(intervalLengthSize, intervalLengthDimension)
            String label = ""
            if(intervalLengthSize == 1 && intervalLengthDimension == ChronoUnit.DECADES) {
                label = "" + start.getYear() + labelSeparator + finish.getYear()
            }
            else if(intervalLengthSize == 1 && intervalLengthDimension == ChronoUnit.YEARS) {
                label = "" + start.getYear()
            }
            else if(intervalLengthSize == 2 && intervalLengthDimension == ChronoUnit.MONTHS) {
                label = "" + start.format(monthDateTimeFormatter) + labelSeparator + finish.format(monthDateTimeFormatter)
            }
            else if(intervalLengthSize == 1 && intervalLengthDimension == ChronoUnit.MONTHS) {
                label = "" + start.format(monthDateTimeFormatter)
            }
            else if(intervalLengthDimension == ChronoUnit.WEEKS || intervalLengthDimension == ChronoUnit.DAYS) {
                label = "" + start.format(dateDateTimeFormatter) + labelSeparator + finish.format(dateDateTimeFormatter)
            }
            else {
                label = "" + start.format(dateTimeDateTimeFormatter) + labelSeparator + finish.format(dateTimeDateTimeFormatter)
            }

            intervals[label] = (new Pair(start, finish))
        }
    }
}

