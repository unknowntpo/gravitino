package com.datastrato.gravitino.authorization.ranger;

import org.apache.ranger.plugin.util.SearchFilter;

public class RangerRef {
    // In the Ranger 2.4.0 security-admin/src/main/java/org/apache/ranger/service/RangerServiceDefService.java:L43
    public static final String IMPLICIT_CONDITION_EXPRESSION_NAME = "_expression";

    // In the Ranger 2.4.0 security-admin/src/main/java/org/apache/ranger/common/RangerSearchUtil.java:L159
    public static final String SEARCH_FILTER_DATABASE = SearchFilter.RESOURCE_PREFIX + "database";
    public static final String SEARCH_FILTER_TABLE = SearchFilter.RESOURCE_PREFIX + "table";
    public static final String SEARCH_FILTER_COLUMN = SearchFilter.RESOURCE_PREFIX + "colunm";
}
