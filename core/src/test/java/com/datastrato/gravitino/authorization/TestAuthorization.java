package com.datastrato.gravitino.authorization;

import java.util.Map;

public class TestAuthorization extends BaseAuthorization<TestAuthorization> {

    public TestAuthorization() {
    }

    @Override
    public String shortName() {
        return "test";
    }

    @Override
    protected AuthorizationOperations newOps(Map<String, String> config) {
        return new TestAuthorizationOperations(config);
    }
}
