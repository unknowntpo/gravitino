package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.authorization.AuthorizationOperations;

import java.io.IOException;
import java.util.Map;

public class TestAuthorizationOperations implements AuthorizationOperations {
    public TestAuthorizationOperations(Map<String, String> config) {

    }

    @Override
    public void initialize(Map<String, String> config) throws RuntimeException {
    }

    @Override
    public void close() throws IOException {

    }
}
