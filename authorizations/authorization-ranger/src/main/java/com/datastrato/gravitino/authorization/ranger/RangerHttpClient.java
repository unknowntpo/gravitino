package com.datastrato.gravitino.authorization.ranger;

import com.google.common.collect.Lists;
import com.sendgrid.Client;
import com.sendgrid.Method;
import com.sendgrid.Request;
import com.sendgrid.Response;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.ranger.RangerClient;
import org.apache.ranger.plugin.util.RangerRESTClient;

import java.io.IOException;
import java.util.Base64;
import java.util.List;

public class RangerHttpClient extends RangerClient {
//    private final String rangerUrl;
//    private final String username;
//    private final String password;
//
//    Client client = new Client();

    private static String rangerUrl = "http://localhost:6080";
    private static final String username = "admin";
    // Apache Ranger Password should be minimum 8 characters with min one alphabet and one numeric.
    private static final String password = "rangerR0cks!";
    /* for kerberos authentication:
    authType = "kerberos"
    username = principal
    password = path of the keytab file */
    private static final String authType = "simple";

    public static final RangerClient.API DELETE_POLICY_DELTAS2;

    static {
        // /service/public/v2/api/policies/%s/for-resource?serviceName=%s
        DELETE_POLICY_DELTAS2 = new API("/service/public/v2/api/servicedef", "GET", javax.ws.rs.core.Response.Status.OK);
    }

    public RangerHttpClient() {
        super(rangerUrl, authType, username, password, null);
    }

//    public List<String> getPoliciesForResource(String serviceDefName, String serviceName) throws IOException {
//        RangerRESTClient restClient = new RangerRESTClient();
//
//
//        Request request = new Request();
//        request.setBaseUri("localhost:6080");
////        request.addHeader("Authorization", "Bearer " + System.getenv("SENDGRID_API_KEY"));
//        request.setMethod(Method.GET);
//
//        // Set Basic Authentication
//        String credentials = String.format("%s:%s", username, password);
//        String encodedCredentials = new String(Base64.getEncoder().encode(credentials.getBytes()));
//        request.addHeader("Authorization", "Basic " + encodedCredentials);
//
//        String endpoint = String.format("/service/public/v2/api/policies/%s/for-resource?serviceName=%s", serviceDefName, serviceName);
//        request.setEndpoint(endpoint);
//        request.addQueryParam("limit", "100");
//        request.addQueryParam("offset", "0");
//
//        Response response = client.api(request);
//        System.out.println(response.getStatusCode());
//        System.out.println(response.getBody());
//        System.out.println(response.getHeaders());
//
//
////        CloseableHttpClient httpClient = HttpClients.createDefault();
////        HttpGet request = new HttpGet(url);
////
//
////        HttpResponse response = httpClient.execute(request);
////        int statusCode = response.getStatusLine().getStatusCode();
////
////        if (statusCode == 200) {
////            HttpEntity entity = response.getEntity();
////            String jsonResponse = EntityUtils.toString(entity);
//////            ObjectMapper mapper = new ObjectMapper();
//////            List<RangerPolicy> policies = mapper.readValue(jsonResponse,
//////                    mapper.getTypeFactory().constructCollectionType(List.class, RangerPolicy.class));
//////            return policies;
////        } else {
////            throw new RuntimeException("Failed to get policies: " + statusCode);
////        }
//        return Lists.asList("a", new String[]{"b", "c"});
//    }
}
