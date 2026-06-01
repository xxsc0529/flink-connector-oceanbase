/*
 * Copyright 2024 OceanBase.
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
 */
package com.oceanbase.connector.flink.dialect;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

class MySQLDuplicateKeyUpdateClauseBuilderTest {

    @Test
    void buildVersionCompareClause() {
        String clause =
                MySQLDuplicateKeyUpdateClauseBuilder.buildVersionCompare(
                        Arrays.asList("id", "data", "version"),
                        Collections.singletonList("id"),
                        f -> "`" + f + "`",
                        "version");
        Assertions.assertEquals(
                "`data`=IF(VALUES(`version`)>`version`,VALUES(`data`),`data`), "
                        + "`version`=IF(VALUES(`version`)>`version`,VALUES(`version`),`version`)",
                clause);
    }

    @Test
    void versionColumnMustExistInFieldNames() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        MySQLDuplicateKeyUpdateClauseBuilder.buildVersionCompare(
                                Arrays.asList("id", "data"),
                                Collections.singletonList("id"),
                                f -> "`" + f + "`",
                                "version"));
    }
}
