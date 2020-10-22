package io.fission;

import org.springframework.http.*;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import io.fission.Function;
import io.fission.Context;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class Microbenchmark implements Function {

    private static final String URL = "http://router.fission/synch-benchmark-post";

    @Override
    public ResponseEntity<?> call(RequestEntity req, Context context) {
        long start_time = System.nanoTime();
        HashMap data = (HashMap) req.getBody();

        // This is the INVOKE parameter. This specifies how many functions to invoke.
        int num_to_invoke = 1;

        // Check URL for query parameters, such as the N value or the INVOKE value.
        Map<String, String> query_pairs = new LinkedHashMap<String, String>();
        URI url = req.getUrl();
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

        // Extract the INVOKE parameter if it was included in the query.
        if (query_pairs.containsKey("INVOKE")) {
            num_to_invoke = Integer.parseInt(query_pairs.get("INVOKE"));

            System.out.println("Assigned num to invoke value from query pairs: " + num_to_invoke);
        }

        // Base case. HTTP GET request from Fission CLI.
        if (data == null) {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            // Initial N value. N gets decremented for each layer.
            int N = 2;

            if (query_pairs.containsKey("N")) {
                N = Integer.parseInt(query_pairs.get("N"));

                System.out.println("Assigned initial N value from query pairs: " + num_to_invoke);
            }

            // We're gonna pass these values to the next functions via this map embedded in the HTTP request (as JSON).
            HashMap<String, String> map = new HashMap<String, String>();
            map.put("N", (N - 1) + "");
            map.put("INVOKE", num_to_invoke + "");

            // We're computing the total number of functions that have been invoked.
            int total = 1 + num_to_invoke; // We do 1 + num_to_invoke bc the 1 captures the initial invoke.

            // Issue HTTP POST requests to invoke additional functions.
            // We obtain the response so we know they've finished.
            // This is a SYNCHRONOUS process.
            for (int i = 0; i < num_to_invoke; i++) {
                HttpEntity<MultiValueMap<String, String>> request = new HttpEntity(map, headers);

                RestTemplate restTemplate = new RestTemplate();
                ResponseEntity<String> response = restTemplate.postForEntity(URL, request , String.class);

                String body = response.getBody();

                total += Integer.parseInt(body);
            }

            // int total = Integer.parseInt(body) + num_to_invoke;

            long end_time = System.nanoTime();

            long time_elapsed_ms = (end_time - start_time) / 1000000;

            // Return the number of tasks invoked by downstream task + number invoked.
            return ResponseEntity.ok("Total number of tasks invoked: " + total + ". Time elapsed: " + time_elapsed_ms + " ms.");
        }
        else {
            // Since we're using MultiValueMap, the entries are lists...
            // ArrayList<String> lst = (ArrayList<String>)data.get("N");
            // String N_s = lst.get(0);
            String N_s = (String)data.get("N");
            int N = Integer.parseInt(N_s);

            String invoke_s = (String)data.get("INVOKE");
            num_to_invoke = Integer.parseInt(invoke_s);

            if (N <= 0 ) {
                // We aren't invoking anything so return 0.
                return ResponseEntity.ok(0 + "");
            }
            else {
                int next_n = N - 1;

                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);

                HashMap<String, String> map= new HashMap<String, String>();
                map.put("N", next_n + "");
                map.put("INVOKE", num_to_invoke + "");

                int total = num_to_invoke;

                for (int i = 0; i < num_to_invoke; i++) {
                    HttpEntity<MultiValueMap<String, String>> request = new HttpEntity(map, headers);

                    RestTemplate restTemplate = new RestTemplate();
                    ResponseEntity<String> response = restTemplate.postForEntity(URL, request , String.class);

                    String body = response.getBody();

                    total += Integer.parseInt(body);
                }
                // int total = Integer.parseInt(body) + num_to_invoke;

                // Return the number of tasks invoked by downstream task + number invoked.
                return ResponseEntity.ok(total + "");
            }
        }
    }
}
