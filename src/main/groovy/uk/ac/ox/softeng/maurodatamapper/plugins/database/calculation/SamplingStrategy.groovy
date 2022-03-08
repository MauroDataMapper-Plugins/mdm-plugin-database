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

import uk.ac.ox.softeng.maurodatamapper.plugins.database.DatabaseDataModelWithSamplingImporterProviderServiceParameters

class SamplingStrategy {

    static final Integer DEFAULT_SAMPLE_THRESHOLD = 0
    static final BigDecimal DEFAULT_SAMPLE_PERCENTAGE = 1
    static final boolean DEFAULT_USE_SAMPLING = false

    String schema
    String table
    Integer smThreshold
    BigDecimal smPercentage
    Integer evThreshold
    BigDecimal evPercentage
    boolean smUseSampling
    boolean evUseSampling
    Long approxCount
    String tableType

    SamplingStrategy(String schema, String table) {
        this.schema = schema
        this.table = table
        this.approxCount = -1
        this.tableType = ''
    }

    SamplingStrategy(String schema, String table, DatabaseDataModelWithSamplingImporterProviderServiceParameters samplingImporterProviderServiceParameters) {
        this(schema, table)
        this.smThreshold = samplingImporterProviderServiceParameters.summaryMetadataSampleThreshold ?: DEFAULT_SAMPLE_THRESHOLD
        this.smPercentage = samplingImporterProviderServiceParameters.summaryMetadataSamplePercent ?: DEFAULT_SAMPLE_PERCENTAGE
        this.smUseSampling = samplingImporterProviderServiceParameters.summaryMetadataUseSampling == null ? DEFAULT_USE_SAMPLING :
                             samplingImporterProviderServiceParameters.summaryMetadataUseSampling
        this.evThreshold = samplingImporterProviderServiceParameters.enumerationValueSampleThreshold ?: DEFAULT_SAMPLE_THRESHOLD
        this.evPercentage = samplingImporterProviderServiceParameters.enumerationValueSamplePercent ?: DEFAULT_SAMPLE_PERCENTAGE
        this.evUseSampling = samplingImporterProviderServiceParameters.enumerationValueUseSampling == null ? DEFAULT_USE_SAMPLING :
                             samplingImporterProviderServiceParameters.enumerationValueUseSampling

    }

    boolean dataExists() {
        approxCount > 0 || approxCount == -1
    }

    /**
     * Does this SamplingStrategy need to know the table type (BASE TABLE or VIEW) in order to decide if sampling is possible?
     * @return
     */
    boolean requiresTableType() {
        true
    }

    /**
     * Does this SamplingStrategy need to know the approx count of the table for any of its checks.
     * Default is that
     * 1. if you can sample then we need it
     * 2. if the threshold is greater than 0 then we could be sampling
     * 3. if sampling is enabled then we need it
     *
     * @return
     */
    boolean requiresApproxCount() {
        (smThreshold > 0 || evThreshold > 0) && (smUseSampling || evUseSampling)
    }

    /**
     * By default, no sampling. Subclasses should override.
     * @return
     */
    boolean canSampleTypeType() {
        false
    }

    boolean canComputeSummaryMetadata() {
        smThreshold == 0 || approxCount < smThreshold || (canSampleTypeType() && smUseSampling)
    }

    boolean canDetectEnumerationValues() {
        evThreshold == 0 || approxCount < evThreshold || (canSampleTypeType() && evUseSampling)
    }

    /**
     * Only use sampling if sampling is enabled, both threshold and percentage are > 0, the number
     * of rows exceeds the (non-zero) threshold for sampling, and we are looking at a table (not a view)
     * @return
     */
    boolean useSamplingForSummaryMetadata() {
        this.canSampleTypeType() && this.smThreshold > 0 && this.smPercentage > 0 && this.approxCount > this.smThreshold
    }

    boolean useSamplingForEnumerationValues() {
        this.canSampleTypeType() && this.smThreshold > 0 && this.smPercentage > 0 && this.approxCount > this.smThreshold
    }

    boolean useSamplingFor(Type type) {
        switch (type) {
            case Type.SUMMARY_METADATA:
                return useSamplingForSummaryMetadata()
            case Type.ENUMERATION_VALUES:
                return useSamplingForEnumerationValues()
        }
    }

    /**
     * Return a sampling clause. Subclasses should override wth vendor specific sampling clauses
     * @return
     */
    String samplingClause(Type type) {
        ''
    }

    Integer scaleFactor() {
        if (this.useSamplingForSummaryMetadata()) {
            return 100 / this.smPercentage
        } else {
            return 1
        }
    }

    String toString() {
        StringBuilder sb = new StringBuilder('Sampling Strategy\nIn General:')
        if (evUseSampling) {
            sb.append('\n  Allow sampling for EVs; Using sampling for row count over ').append(evThreshold).append(' rows & sample ').append(evPercentage)
                .append('% of the data')
        } else {
            sb.append('\n  Do not allow sampling for EVs')
        }
        if (smUseSampling) {
            sb.append('\n  Allow sampling for SM; Using sampling for row count over ').append(smThreshold).append(' rows & sample ').append(smPercentage)
                .append('% of the data')
        } else {
            sb.append('\n  Do not allow sampling for SM')
        }

        sb.append('\nFor ').append(tableType.toLowerCase()).append(' ').append(schema).append('.').append(table)
        if (approxCount == -1) sb.append(' with an uncalculated row count')
        else sb.append(' with a rowcount of ').append(approxCount).append(':')

        if (canDetectEnumerationValues()) {
            sb.append('\n  Can detect EVs ')
            if (useSamplingForEnumerationValues()) sb.append('using sampling')
            else sb.append('without sampling')
        } else sb.append('\n  Cannot detect EVs')


        if (canComputeSummaryMetadata()) {
            sb.append('\n  Can compute SM ')
            if (useSamplingForSummaryMetadata()) sb.append('using sampling')
            else sb.append('without sampling')
        } else sb.append('\n  Cannot compute SM')


        sb.toString()
    }

    static enum Type {
        SUMMARY_METADATA,
        ENUMERATION_VALUES
    }
}
