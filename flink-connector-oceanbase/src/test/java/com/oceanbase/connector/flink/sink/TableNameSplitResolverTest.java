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

package com.oceanbase.connector.flink.sink;

import com.oceanbase.connector.flink.TableNameSplitAffix;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TableNameSplitResolverTest {

    private static final String BASE = "user_info";

    @Test
    void nullSplitWritesToBaseTable() {
        assertEquals(
                BASE,
                TableNameSplitResolver.resolvePhysicalTableName(
                        null, BASE, TableNameSplitAffix.SUFFIX, "_"));
    }

    @Test
    void emptySplitWritesToBaseTable() {
        assertEquals(
                BASE,
                TableNameSplitResolver.resolvePhysicalTableName(
                        "", BASE, TableNameSplitAffix.SUFFIX, "_"));
    }

    @Test
    void blankSplitWritesToBaseTable() {
        assertEquals(
                BASE,
                TableNameSplitResolver.resolvePhysicalTableName(
                        "   ", BASE, TableNameSplitAffix.SUFFIX, "_"));
    }

    @Test
    void suffixAffixJoinsBaseAndTrimmedValue() {
        assertEquals(
                "user_info_cn",
                TableNameSplitResolver.resolvePhysicalTableName(
                        " cn ", BASE, TableNameSplitAffix.SUFFIX, "_"));
    }

    @Test
    void prefixAffixJoinsValueAndBase() {
        assertEquals(
                "cn_user_info",
                TableNameSplitResolver.resolvePhysicalTableName(
                        "cn", BASE, TableNameSplitAffix.PREFIX, "_"));
    }
}
