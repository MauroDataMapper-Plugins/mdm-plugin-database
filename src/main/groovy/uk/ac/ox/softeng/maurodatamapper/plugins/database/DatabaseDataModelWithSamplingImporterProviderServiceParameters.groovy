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
package uk.ac.ox.softeng.maurodatamapper.plugins.database

import uk.ac.ox.softeng.maurodatamapper.core.provider.importer.parameter.config.ImportGroupConfig
import uk.ac.ox.softeng.maurodatamapper.core.provider.importer.parameter.config.ImportParameterConfig

import javax.sql.DataSource

// @CompileStatic
abstract class DatabaseDataModelWithSamplingImporterProviderServiceParameters<K extends DataSource> extends DatabaseDataModelImporterProviderServiceParameters {

    @ImportParameterConfig(
        displayName = 'Use Sampling, if required, for Summary Metadata',
        description = [
            'Use sampling to determine Summary Metadata if the row count of the table/view is above the threshold.',
            'If this is false then any tables/views with a row count above the threshold will have no summary metadata detection performed.',
        'Default is true.'],
        order = 5,
        optional = true,
        group = @ImportGroupConfig(
            name = 'Summary Metadata Computation',
            order = 6
        )
    )
    Boolean summaryMetadataUseSampling

    @ImportParameterConfig(
        displayName = 'Sample Threshold for Summary Metadata',
        description = [
            'If the approximate number of rows in a table or view exceeds this threshold, then use sampling when computing summary metadata.',
            'A value of 0, which is the default, means that sampling will not be considered and summary metadata computation will occur over the whole table/view.',
            'Sampling is done using vendor specific SQL.'],
        order = 6,
        optional = true,
        group = @ImportGroupConfig(
            name = 'Summary Metadata Computation',
            order = 6
        )
    )
    Integer summaryMetadataSampleThreshold

    @ImportParameterConfig(
        displayName = 'Sample Percentage for Summary Metadata',
        description = [
            'If sampling is used, the percentage of rows to use as a sample.',
            'If the sampling threshold is > 0 but no value is supplied for Sample Percentage, a default value of 1% will be used.'
        ],
        order = 7,
        optional = true,
        group = @ImportGroupConfig(
            name = 'Summary Metadata Computation',
            order = 6
        )
    )
    BigDecimal summaryMetadataSamplePercent

    @ImportParameterConfig(
        displayName = 'Use Sampling, if required, for Enumeration Values',
        description = [
            'Use sampling to determine Enumeration Values if the row count of the table/view is above the threshold.',
            'If this is false then any tables/views with a row count above the threshold will have no enumeration value detection performed.',
            'Default is true.'],
        order = 5,
        optional = true,
        group = @ImportGroupConfig(
            name = 'Enumeration Values Detection',
            order = 5
        )
    )
    Boolean enumerationValueUseSampling

    @ImportParameterConfig(
        displayName = 'Sample Threshold for Enumeration Values',
        description = [
            'If the approximate number of rows in a table or view exceeds this threshold, then use sampling when detecting enumeration values.',
            'A value of 0, which is the default, means that sampling will not be considered and enumeration values detection will occur over the whole table/view.',
            'Sampling is done using vendor specific SQL.'],
        order = 6,
        optional = true,
        group = @ImportGroupConfig(
            name = 'Enumeration Values Detection',
            order = 5
        )
    )
    Integer enumerationValueSampleThreshold

    @ImportParameterConfig(
        displayName = 'Sample Percentage for Enumeration Values',
        description = [
            'If sampling is used, the percentage of rows to use as a sample.',
            'If the sampling threshold is > 0 but no value is supplied for Sample Percentage, a default value of 1% will be used.'
        ],
        order = 7,
        optional = true,
        group = @ImportGroupConfig(
            name = 'Enumeration Values Detection',
            order = 5
        )
    )
    BigDecimal enumerationValueSamplePercent
}
