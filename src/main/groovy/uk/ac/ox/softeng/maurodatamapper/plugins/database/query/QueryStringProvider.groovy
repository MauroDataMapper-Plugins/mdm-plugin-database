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
package uk.ac.ox.softeng.maurodatamapper.plugins.database.query

import uk.ac.ox.softeng.maurodatamapper.datamodel.item.datatype.DataType
import uk.ac.ox.softeng.maurodatamapper.datamodel.summarymetadata.AbstractIntervalHelper
import uk.ac.ox.softeng.maurodatamapper.plugins.database.calculation.CalculationStrategy
import uk.ac.ox.softeng.maurodatamapper.plugins.database.calculation.SamplingStrategy

/**
 * @since 11/03/2022
 */
abstract class QueryStringProvider {

    /**
     * Must return a String which will be queryable by schema name,
     * and return a row with the following elements:
     *  * table_name
     *  * index_name
     *  * unique_index (boolean)
     *  * primary_index (boolean)
     *  * clustered (boolean)
     *  * column_names
     * @return Query string for index information
     */
    abstract String getIndexInformationQueryString()

    /**
     * Must return a String which will be queryable by schema name,
     * and return a row with the following elements:
     *  * constraint_name
     *  * table_name
     *  * column_name
     *  * reference_table_name
     *  * reference_column_name
     * @return Query string for foreign key information
     */
    abstract String getForeignKeyInformationQueryString()

    abstract String getDatabaseStructureQueryString()

    /**
     * Must return a String which will be queryable by table name
     * and column name, and return rows with the following elements:
     *  * interval_start
     *  * interval_end
     *  * interval_count i.e count of values in the range interval_start to interval_end
     *
     *  interval_start is inclusive. interval_end is exclusive. interval_count is the
     *  count of values in the interval. Rows must be ordered by interval_start ascending.
     *
     *  Subclasses must implement this method using vendor specific SQL as necessary.
     *
     * @return Query string for count by interval
     */
    abstract String columnRangeDistributionQueryString(SamplingStrategy samplingStrategy, DataType dataType,
                                                       AbstractIntervalHelper intervalHelper,
                                                       String columnName, String tableName, String schemaName)


    /**
     * Escape an identifier. Subclasses can override and using vendor specific syntax.
     * @param identifier
     * @return The escaped identifier
     */
    String escapeIdentifier(String identifier) {
        identifier
    }

    /**
     * Must return a String which will be queryable by schema name,
     * and return a row with the following elements:
     *  * table_name
     *  * check_clause (the constraint information)
     * @return Query string for standard constraint information
     */
    String standardConstraintInformationQueryString() {
        '''
            SELECT
              tc.table_name,
              cc.check_clause
            FROM information_schema.table_constraints tc
              INNER JOIN information_schema.check_constraints cc ON tc.constraint_name = cc.constraint_name
            WHERE tc.constraint_schema = ?;
            '''.stripIndent()
    }

    /**
     * Must return a String which will be queryable by schema name,
     * and return a row with the following elements:
     *  * constraint_name
     *  * table_name
     *  * constraint_type (primary_key or unique)
     *  * column_name
     *  * ordinal_position
     * @return Query string for primary key and unique constraint information
     */
    String primaryKeyAndUniqueConstraintInformationQueryString() {
        '''
            SELECT
              tc.constraint_name,
              tc.table_name,
              tc.constraint_type,
              kcu.column_name,
              kcu.ordinal_position
            FROM
              information_schema.table_constraints AS tc
              LEFT JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name
            WHERE
              tc.constraint_schema = ?
              AND constraint_type NOT IN ('FOREIGN KEY', 'CHECK');
            '''.stripIndent()
    }

    /**
     * Must return a String which will be queryable by column name, table name
     * and optionally schema name, and return a row with the following columns:
     *  * count
     * and preferably apply a sampling strategy.
     *
     * The base method returns a query that does not use any sampling; subclasses which
     * support sampling should override with vendor specific SQL.
     *
     * @return Query string for count of distinct values in a column
     */
    @Deprecated
    String countDistinctColumnValuesQueryString(SamplingStrategy samplingStrategy, String columnName, String tableName, String schemaName = null) {
        String schemaIdentifier = schemaName ? "${escapeIdentifier(schemaName)}." : ""
        "SELECT COUNT(DISTINCT(${escapeIdentifier(columnName)})) AS count FROM ${schemaIdentifier}${escapeIdentifier(tableName)}" +
        samplingStrategy.samplingClause(SamplingStrategy.Type.ENUMERATION_VALUES) +
        "WHERE ${escapeIdentifier(columnName)} <> ''"
    }

    /**
     * Must return a String which will be queryable by column name, table name
     * and optionally schema name, and return rows with the following columns:
     *  * distinct_value
     *
     * and preferably apply a sampling strategy.
     *
     * The base method returns a query that does not use any sampling; subclasses which
     * support sampling should override with vendor specific SQL.
     *
     * Optimisation can also occur by vendor specific SQL limiting the return to the max allowed EVs + 1,
     * this will mean the system gets the first (e.g 21) entries. If the max allowed values is 20 then the size of 21 will stop it from being an ET
     * it will also be faster as its only checking for 21 distinct values then returning a result.
     *
     * @return Query string for distinct values in a column
     */
    String distinctColumnValuesQueryString(CalculationStrategy calculationStrategy, SamplingStrategy samplingStrategy, String columnName, String tableName,
                                           String schemaName = null) {
        String schemaIdentifier = schemaName ? "${escapeIdentifier(schemaName)}." : ""
        "SELECT DISTINCT(${escapeIdentifier(columnName)}) AS distinct_value FROM ${schemaIdentifier}${escapeIdentifier(tableName)}" +
        samplingStrategy.samplingClause(SamplingStrategy.Type.ENUMERATION_VALUES) +
        "WHERE ${escapeIdentifier(columnName)} <> ''"
    }

    /**
     * Must return a List of Strings, each of which will be queryable by table name
     * and optionally schema name, and return rows with the following elements:
     *  * approx_count
     *
     * The base implementation returns a single COUNT(*) query, which is vendor neutral,
     * returns an exact row count, and is likely to be slow on large tables. Subclasses should
     * override this method and push to the front of the list alternative faster implementations
     * for an approximate count, using vendor specific queries as appropriate. Some such
     * queries may return null values (for example if statistics have not been calculated
     * on the relevant table); if a query does return a null count, the next query
     * in the list is executed.
     *
     * @return Query string for approximate count of rows in a table
     */
    List<String> approxCountQueryString(String tableName, String schemaName = null) {
        String schemaIdentifier = schemaName ? "${escapeIdentifier(schemaName)}." : ""
        [
            "SELECT COUNT(*) AS approx_count FROM ${schemaIdentifier}${escapeIdentifier(tableName)}".toString()
        ]
    }

    /**
     * Must return a String which will be queryable by table name, schema name, and model name,
     * and return one row with the following elements:
     *  * table_type
     *
     *
     * @return Query string for table type
     */
    String tableTypeQueryString() {
        "SELECT TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = ? AND TABLE_SCHEMA = ? AND TABLE_NAME = ?"
    }

    /**
     * Must return a String which will be queryable by column name, table name
     * and optionally schema name, and return rows with the following elements:
     *  * min_value
     *  * max_value
     *
     * and use an appropriate sampling strategy.
     *
     * The base method does not use sampling, and should be overridden by subclasses where possible.
     * @return Query string for distinct values in a column
     */
    String minMaxColumnValuesQueryString(SamplingStrategy samplingStrategy, String columnName, String tableName, String schemaName = null) {
        String schemaIdentifier = schemaName ? "${escapeIdentifier(schemaName)}." : ""
        """SELECT MIN(${escapeIdentifier(columnName)}) AS min_value, 
MAX(${escapeIdentifier(columnName)}) AS max_value 
FROM ${schemaIdentifier}${escapeIdentifier(tableName)} ${samplingStrategy.samplingClause(SamplingStrategy.Type.SUMMARY_METADATA)}
WHERE ${escapeIdentifier(columnName)} IS NOT NULL""".stripIndent()
    }

    /**
     * Must return a String which will be queryable by table name
     * and column name, and return rows with the following elements:
     *  * enumeration_value
     *  * enumeration_count
     *
     *  Subclasses must implement this method using vendor specific SQL as necessary.
     *
     * @return Query string for count grouped by enumeration value
     */
    String enumerationValueDistributionQueryString(SamplingStrategy samplingStrategy,
                                                   String columnName,
                                                   String tableName,
                                                   String schemaName) {

        """SELECT 
  ${escapeIdentifier(schemaName)}.${escapeIdentifier(tableName)}.${escapeIdentifier(columnName)} AS enumeration_value,
  ${samplingStrategy.scaleFactor()} * COUNT(*) AS enumeration_count
FROM ${escapeIdentifier(schemaName)}.${escapeIdentifier(tableName)} ${samplingStrategy.samplingClause(SamplingStrategy.Type.SUMMARY_METADATA)}
GROUP BY ${escapeIdentifier(schemaName)}.${escapeIdentifier(tableName)}.${escapeIdentifier(columnName)}
ORDER BY ${escapeIdentifier(schemaName)}.${escapeIdentifier(tableName)}.${escapeIdentifier(columnName)}""".stripIndent()
    }
}
