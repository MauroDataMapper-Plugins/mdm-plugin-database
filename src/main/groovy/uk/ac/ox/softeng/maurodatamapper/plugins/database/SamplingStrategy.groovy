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

class SamplingStrategy {

    boolean enabled
    Integer threshold
    BigDecimal percentage
    Long approxCount
    String tableType

    SamplingStrategy() {
        this.enabled = false
    }

    SamplingStrategy(Integer threshold, BigDecimal percentage) {
        this.enabled = true
        this.threshold = threshold
        this.percentage = percentage
        this.approxCount = 0
        this.tableType = ""
    }

    /**
     * Sampling does not work on Views.
     * @return true if tableType is BASE TABLE
     */
    public boolean canSample() {
        this.tableType == 'BASE TABLE'
    }

    /**
     * Only use sampling if sampling is enabled, both threshold and percentage are > 0, the number
     * of rows exceeds the (non-zero) threshold for sampling, and we are looking at a table (not a view)
     * @return
     */
    public boolean useSampling() {
       this.enabled && this.threshold > 0 && this.percentage > 0 && this.approxCount > this.threshold && this.canSample()
    }

    public Integer scaleFactor() {
        if (this.useSampling()) {
            return 100 / this.percentage
        } else {
            return 1
        }
    }
}
