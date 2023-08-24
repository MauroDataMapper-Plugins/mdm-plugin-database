/*
 * Copyright 2020-2023 University of Oxford and NHS England
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
import uk.ac.ox.softeng.maurodatamapper.datamodel.item.datatype.EnumerationType
import uk.ac.ox.softeng.maurodatamapper.datamodel.summarymetadata.AbstractIntervalHelper
import uk.ac.ox.softeng.maurodatamapper.datamodel.summarymetadata.DateIntervalHelper
import uk.ac.ox.softeng.maurodatamapper.datamodel.summarymetadata.DecimalIntervalHelper
import uk.ac.ox.softeng.maurodatamapper.datamodel.summarymetadata.LongIntervalHelper
import uk.ac.ox.softeng.maurodatamapper.plugins.database.DatabaseDataModelImporterProviderServiceParameters

import grails.util.Pair

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.util.regex.Pattern

/**
 * @since 08/03/2022
 */
class CalculationStrategy {

    static final Integer DEFAULT_MAX_ENUMERATIONS = 20
    static final Integer DEFAULT_MIN_SUMMARY_VALUE = 10

    boolean detectEnumerations
    Integer maxEnumerations
    List<Pattern> includeColumnPatternsForEnumerations
    Integer minSummaryValue
    List<Pattern> ignorePatternsForEnumerations
    boolean computeSummaryMetadata
    List<Pattern> ignorePatternsForSummaryMetadata
    BucketHandling dateBucketHandling
    Boolean rowCountGteMaxEnumerations
    Boolean rowCountGteMinSummaryValue

    OffsetDateTime calculationDateTime

    CalculationStrategy(DatabaseDataModelImporterProviderServiceParameters parameters) {
        this.detectEnumerations = parameters.detectEnumerations
        this.includeColumnPatternsForEnumerations =
            parameters.includeColumnsForEnumerations ? parameters.
                includeColumnsForEnumerations.split(',').collect {Pattern.compile(it)} : Collections.emptyList() as List<Pattern>
        this.maxEnumerations = parameters.maxEnumerations ?: DEFAULT_MAX_ENUMERATIONS
        this.minSummaryValue = parameters.summaryMetadataMinimumValue ?: DEFAULT_MIN_SUMMARY_VALUE
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
        (rowCountGteMaxEnumerations || (rowCountGteMaxEnumerations == null && rowCount > maxEnumerations) || includeColumnPatternsForEnumerations.any {columnLabel.matches(it)})  // If the row count is less than maxEnum then all values will be enumerations which is not accurate
    }

    boolean shouldComputeSummaryData(String columnLabel, DataType dataType, Long rowCount) {
        computeSummaryMetadata &&
        isColumnForDateOrNumericSummary(dataType) &&
        !ignorePatternsForSummaryMetadata.any {columnLabel.matches(it)}
    }

    boolean isEnumerationType(String columnLabel, int distinctCount) {
        distinctCount > 0 && (distinctCount <= (maxEnumerations ?: DEFAULT_MAX_ENUMERATIONS) || isColumnAlwaysEnumeration(columnLabel))
    }

    boolean isColumnAlwaysEnumeration(String columnLabel) {
        includeColumnPatternsForEnumerations.any {columnLabel.matches(it)}
    }

    boolean requiresRowCountGteMaxEnumerations() {
        this.detectEnumerations ||
        (isColumnForDateOrNumericSummary(dataType) || dataType instanceof EnumerationType) &&
        !ignorePatternsForSummaryMetadata.any {columnLabel.matches(it)} &&
        (rowCountGteMinSummaryValue || (rowCountGteMinSummaryValue == null && rowCount > minSummaryValue))
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
            return new DateIntervalHelper(getLocalDateTime(minMax.aValue), getLocalDateTime(minMax.bValue))
        } else if (isColumnForDecimalSummary(dataType)) {
            return new DecimalIntervalHelper((BigDecimal) minMax.aValue, (BigDecimal) minMax.bValue)
        }
        null
    }

    static LocalDateTime getLocalDateTime(Object value) {
        if (value instanceof LocalDateTime) return value
        if (value instanceof OffsetDateTime) return value.toLocalDateTime()
        if (value instanceof LocalDate) return value.atStartOfDay()
        if (value instanceof Date) return value.toLocalDateTime()
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
