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
    Integer approxCount

    SamplingStrategy() {
        this.enabled = false
    }

    SamplingStrategy(Integer threshold, BigDecimal percentage) {
        this.enabled = true
        this.threshold = threshold
        this.percentage = percentage
        this.approxCount = 0
    }

    /**
     * Only use sampling if sampling is enabled, both threshold and percentage are > 0, and the number
     * of rows exceeds the (non-zero) threshold for sampling.
     * @return
     */
    public boolean useSampling() {
       this.enabled && this.threshold > 0 && this.percentage > 0 && this.approxCount > this.threshold
    }

    public Integer scaleFactor() {
        if (this.useSampling()) {
            return 100 / this.percentage
        } else {
            return 1
        }
    }
}
