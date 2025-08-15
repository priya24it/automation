![image](https://user-images.githubusercontent.com/62958714/200053896-16e60bbf-0e26-4198-b607-a1fdde9e4921.png)




python3 mini_load_tester.py --url "https://httpbin.org/post" --method POST --dynamic --concurrency 20  --requests 100 --timeout 10 --csv results.csv

| Feature                           | What It Means                                                                   |
| --------------------------------- | ------------------------------------------------------------------------------- |
| **Concurrency** (`--concurrency`) | How many requests are in-flight at the same time.                               |
| **Requests count** (`--requests`) | Total number of requests to send during the test.                               |
| **Dynamic payload** (`--dynamic`) | Generates a unique JSON body for every request.                                 |
| **Content-Type** auto-setting     | If `--dynamic` is used, defaults to `application/json`.                         |
| **Percentiles**                   | Calculates p50, p90, p95, p99 latency.                                          |
| **CSV logging**                   | Saves index, status code, latency, response size, and errors for every request. |
| **Warmup phase**                  | Optional warmup requests that don’t count toward the main results.              |


How it Works Internally

Parse command-line arguments
Reads your options (URL, method, concurrency, request count, headers, etc.).

Prepare payloads

If --dynamic is off → it uses the same static payload (--data or --data-file) for all requests.

If --dynamic is on → calls make_payload(index) for every request.

That function builds JSON with fields like request_id, guid, timestamp, user, metrics, features, etc., using random values and the request index.

Create a thread pool
Uses ThreadPoolExecutor to run requests in parallel.
Each batch of threads waits on a barrier so they start almost simultaneously (simulating “X users hit at the same time”).

Send HTTP requests
Uses urllib.request (built into Python) to send the request and measure the time from before sending to after reading the full response body.

Record results
For each request, stores:

Index number

HTTP status

Latency in ms

Size of the response body

Any error message

Write CSV log
Saves all per-request results to your chosen --csv file.

Print a summary
Shows:

Total requests sent

Concurrency

Wall-clock time

Throughput (req/sec)

Success rate


What happens here:

Sends 30 total POST requests, 10 at a time in parallel.

Each request has different JSON content.

Logs every request in results.csv.

Prints performance summary.
Latency percentiles: p50, p90, p95, p99, plus min/max




