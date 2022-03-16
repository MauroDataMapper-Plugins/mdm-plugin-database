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
package uk.ac.ox.softeng.maurodatamapper.plugins.database.calculation

import uk.ac.ox.softeng.maurodatamapper.datamodel.item.datatype.DataType
import uk.ac.ox.softeng.maurodatamapper.plugins.database.DatabaseDataModelImporterProviderServiceParameters
import uk.ac.ox.softeng.maurodatamapper.plugins.database.summarymetadata.AbstractIntervalHelper
import uk.ac.ox.softeng.maurodatamapper.plugins.database.summarymetadata.DateIntervalHelper
import uk.ac.ox.softeng.maurodatamapper.plugins.database.summarymetadata.DecimalIntervalHelper
import uk.ac.ox.softeng.maurodatamapper.plugins.database.summarymetadata.LongIntervalHelper

import grails.util.Pair

import java.time.OffsetDateTime
import java.util.regex.Pattern

/**
 * @since 08/03/2022
 */
class CalculationStrategy {

    static final Integer DEFAULT_MAX_ENUMERATIONS = 20

    boolean detectEnumerations
    Integer maxEnumerations
    List<Pattern> ignorePatternsForEnumerations
    boolean computeSummaryMetadata
    List<Pattern> ignorePatternsForSummaryMetadata
    BucketHandling dateBucketHandling

    OffsetDateTime calculationDateTime

    CalculationStrategy(DatabaseDataModelImporterProviderServiceParameters parameters) {
        this.detectEnumerations = parameters.detectEnumerations
        this.maxEnumerations = parameters.maxEnumerations
        this.ignorePatternsForEnumerations =
            parameters.ignoreColumnsForEnumerations ? parameters.
                ignoreColumnsForEnumerations.split(',').collect {Pattern.compile(it)} : Collections.emptyList() as List<Pattern>
        this.computeSummaryMetadata = parameters.calculateSummaryMetadata
        this.ignorePatternsForSummaryMetadata =
            parameters.ignoreColumnsForSummaryMetadata ?
            parameters.ignoreColumnsForSummaryMetadata.split(',').collect {Pattern.compile(it)} : Collections.emptyList() as List<Pattern>
        this.dateBucketHandling = BucketHandling.from(parameters.mergeOrRemoveDateBuckets)
        if (dateBucketHandling == BucketHandling.MERGE & parameters.shouldMergeRelativelySmallDateBuckets()){
            dateBucketHandling = BucketHandling.MERGE_RELATIVE_SMALL
        }
            calculationDateTime = OffsetDateTime.now()
    }

    boolean shouldDetectEnumerations(String columnLabel, DataType dataType, Long rowCount) {
        detectEnumerations &&
        isColumnPossibleEnumeration(dataType) &&
        !ignorePatternsForEnumerations.any {columnLabel.matches(it)} &&
        (rowCount == -1 || rowCount > maxEnumerations) // If the row count is less than maxEnum then all values will be enumerations which is not accurate
    }

    boolean shouldComputeSummaryData(String columnLabel, DataType dataType) {
        computeSummaryMetadata &&
        isColumnForDateOrNumericSummary(dataType) &&
        !ignorePatternsForSummaryMetadata.any {columnLabel.matches(it)}
    }

    boolean isEnumerationType(int distinctCount) {
        distinctCount > 0 && distinctCount <= (maxEnumerations ?: DEFAULT_MAX_ENUMERATIONS)
    }

    /**
     * Does the dataType represent a column that should be checked as a possible enumeration?
     * Subclasses can override and use database specific types e.g char/varchar or
     * character/character varying
     * @param dataType
     * @return boolean
     */
    boolean isColumnPossibleEnumeration(DataType dataType) {
        false
    }

    /**
     * Does the dataType represent a column that should be summarised as a date?
     * Subclasses can override and use database specific types.
     * @param dataType
     * @return boolean
     */
    boolean isColumnForDateSummary(DataType dataType) {
        false
    }

    /**
     * Does the dataType represent a column that should be summarised as a decimal?
     * Subclasses can override and use database specific types.
     * @param dataType
     * @return boolean
     */
    boolean isColumnForDecimalSummary(DataType dataType) {
        false
    }

    /**
     * Does the dataType represent a column that should be summarised as an integer?
     * Subclasses can override and use database specific types.
     * @param dataType
     * @return boolean
     */
    boolean isColumnForIntegerSummary(DataType dataType) {
        false
    }

    /**
     * Does the dataType represent a column that should be summarised as a long?
     * Subclasses can override and use database specific types.
     * @param dataType
     * @return boolean
     */
    boolean isColumnForLongSummary(DataType dataType) {
        false
    }

    boolean isColumnForDateOrNumericSummary(DataType dataType) {
        isColumnForDateSummary(dataType) || isColumnForDecimalSummary(dataType) || isColumnForIntegerSummary(dataType) || isColumnForLongSummary(dataType)
    }

    AbstractIntervalHelper getIntervalHelper(DataType dataType, Pair minMax) {
        if (isColumnForLongSummary(dataType)) {
            return new LongIntervalHelper((Long) minMax.aValue, (Long) minMax.bValue)
        } else if (isColumnForIntegerSummary(dataType)) {
            return new LongIntervalHelper((Long) minMax.aValue, (Long) minMax.bValue)
        } else if (isColumnForDateSummary(dataType)) {
            return new DateIntervalHelper(((Date) minMax.aValue).toLocalDateTime(), ((Date) minMax.bValue).toLocalDateTime())
        } else if (isColumnForDecimalSummary(dataType)) {
            return new DecimalIntervalHelper((BigDecimal) minMax.aValue, (BigDecimal) minMax.bValue)
        }
        null
    }

    static enum BucketHandling {
        MERGE,
        REMOVE,
        MERGE_RELATIVE_SMALL

        static BucketHandling from(String val) {
            val ? valueOf(val.toUpperCase()) ?: MERGE_RELATIVE_SMALL : MERGE_RELATIVE_SMALL
        }
    }
}
