package io.fission;

import com.google.gson.Gson;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.springframework.boot.autoconfigure.gson.GsonAutoConfiguration;
import org.springframework.http.*;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.asynchttpclient.Dsl.asyncHttpClient;

import io.fission.Function;
import io.fission.Context;

public class AsynchronousBenchmark implements Function {
    private static AsyncHttpClient asyncHttpClient = asyncHttpClient();

    private static final int INVOKE_DEFAULT = 1;
    private static final int N_DEFAULT = 2;

    private static final String URL = "http://router.fission/async-benchmark";

    private AtomicInteger counter;
    private AtomicInteger numInvoked;

    @Override
    public ResponseEntity<?> call(RequestEntity req, Context context) {
        System.out.println("Function invoked!");
        long start_time = System.nanoTime();

        int INVOKE = INVOKE_DEFAULT;

        // This is always true. Just trying to get the function to run atm.
        if (INVOKE > 0) {
            return ResponseEntity.ok("TESTING 123, I REPEAT, TESTING 1-2-3...");
        }

        HashMap data = (HashMap) req.getBody();
        Gson gson = new Gson();

        counter = new AtomicInteger(0);
        numInvoked = new AtomicInteger(0);
        ArrayList<Future<Response>> responses = new ArrayList<Future<Response>>();

        // Check URL for query parameters, such as the N value or the INVOKE value.
        Map<String, String> query_pairs = extractQueryPairs(req.getUrl());

        // Extract the INVOKE parameter if it was included in the query.
        if (query_pairs.containsKey("INVOKE")) {
            INVOKE = Integer.parseInt(query_pairs.get("INVOKE"));

            System.out.println("Assigned num to invoke value from query pairs: " + INVOKE);
        }

        Executor executor = Executors.newFixedThreadPool(4);

        // Base case. HTTP GET request from Fission CLI.
        if (data == null || req.getMethod() == HttpMethod.GET) {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            // Initial N value. N gets decremented for each layer.
            int N = N_DEFAULT;

            if (query_pairs.containsKey("N")) {
                N = Integer.parseInt(query_pairs.get("N"));

                System.out.println("Assigned initial N value from query pairs: " + N);
            }

            Map<String, String> payloadValues = new HashMap<String, String>();
            payloadValues.put("N", Integer.toString(N - 1));
            payloadValues.put("INVOKE", Integer.toString(INVOKE));

            numInvoked.incrementAndGet();

            String payloadAsJson = gson.toJson(payloadValues);

            for (int i = 0; i < INVOKE; i++) {
                final ListenableFuture<Response> whenResponse = asyncHttpClient.preparePost(URL)
                        .addHeader("content-type", "application/json")
                        .setBody(payloadAsJson)
                        .execute();

                responses.add(whenResponse);

                whenResponse.addListener(() -> {
                    counter.incrementAndGet();
                    try {
                        Response response = whenResponse.get();

                        String body = response.getResponseBody();
                        numInvoked.addAndGet(Integer.parseInt(body));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                }, executor);
            }

            // Sleep until we've collected all of the results.
            while (counter.get() < INVOKE) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            return ResponseEntity.ok(numInvoked.get() + "");

        }
        else {
            String N_s = (String)data.get("N");
            int N = Integer.parseInt(N_s);

            String invoke_s = (String)data.get("INVOKE");
            INVOKE = Integer.parseInt(invoke_s);

            if (N <= 0) {
                return ResponseEntity.ok(0 + "");
            } else {
                int next_n = N - 1;

                Map<String, String> payloadValues = new HashMap<String, String>();
                payloadValues.put("N", Integer.toString(next_n));
                payloadValues.put("INVOKE", Integer.toString(INVOKE));
                String payloadAsJson = gson.toJson(payloadValues);

                for (int i = 0; i < INVOKE; i++) {
                    final ListenableFuture<Response> whenResponse = asyncHttpClient.preparePost(URL)
                            .addHeader("content-type", "application/json")
                            .setBody(payloadAsJson)
                            .execute();

                    responses.add(whenResponse);

                    whenResponse.addListener(() -> {
                        counter.incrementAndGet();
                        try {
                            Response response = whenResponse.get();

                            String body = response.getResponseBody();
                            numInvoked.addAndGet(Integer.parseInt(body));
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (ExecutionException e) {
                            e.printStackTrace();
                        }
                    }, executor);
                }

                // Sleep until we've collected all of the results.
                while (counter.get() < INVOKE) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                return ResponseEntity.ok(numInvoked.get() + "");
            }
        }
    }

    private Map<String, String> extractQueryPairs(URI url) {
        Map<String, String> query_pairs = new LinkedHashMap<String, String>();
        System.out.println("Request URL = " + url.toString());
        String query = url.getQuery();

        // Decode the query.
        if (query != null) {
            String[] pairs = query.split("&");
            for (String pair : pairs) {
                int idx = pair.indexOf("=");
                try {
                    query_pairs.put(URLDecoder.decode(pair.substring(0, idx), "UTF-8"), URLDecoder.decode(pair.substring(idx + 1), "UTF-8"));
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
            }
        }

        return query_pairs;
    }
}
