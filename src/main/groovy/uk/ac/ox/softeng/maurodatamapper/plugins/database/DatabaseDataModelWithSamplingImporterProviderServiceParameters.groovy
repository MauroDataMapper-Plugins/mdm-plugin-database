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
package uk.ac.ox.softeng.maurodatamapper.plugins.database

import uk.ac.ox.softeng.maurodatamapper.core.provider.importer.parameter.config.ImportGroupConfig
import uk.ac.ox.softeng.maurodatamapper.core.provider.importer.parameter.config.ImportParameterConfig

import javax.sql.DataSource

// @CompileStatic
abstract class DatabaseDataModelWithSamplingImporterProviderServiceParameters<K extends DataSource> extends DatabaseDataModelImporterProviderServiceParameters {

    @ImportParameterConfig(
        displayName = 'Sample Threshold',
        description = [
            'If the approximate number of rows in a table or view exceeds this threshold, then use sampling when detecting enumerations',
            'and computing summary metadata. A value of 0, which is the default, means that sampling will not be used.',
            'Sampling is done using vendor specific SQL.'],
        order = 4,
        optional = true,
        group = @ImportGroupConfig(
            name = 'Sampling',
            order = 6
        )
    )
    Integer sampleThreshold = 0

    @ImportParameterConfig(
        displayName = 'Sample Percentage',
        description = [
            'If sampling, the percentage of rows to use as a sample. If the sampling threshold is > 0 but no',
            'value is supplied for Sample Percentage, a default value of 1% will be used.'
        ],
        order = 5,
        optional = true,
        group = @ImportGroupConfig(
            name = 'Sampling',
            order = 6
        )
    )
    BigDecimal samplePercent = 1
}
