import asyncio
import statistics
import time

import httpx


URL = "http://127.0.0.1:8000/chat"

CONCURRENT_REQUESTS = 20




async def send_request(
    client: httpx.AsyncClient,
    request_id: int,
):
    start = time.perf_counter()
    first_token_time = None
    previous_token_time = None
    inter_token_latencies = []
    token_count = 0

    try:
        async with client.stream(
            "POST",
            URL,
            params={
                "prompt": f"Explain request {request_id}"
            },
        ) as response:

            if response.status_code == 503:
                return {
                    "status": "rejected",
                    "latency": None,
                    "ttft": None,
                    "itl": [],
                    "tokens": 0,
                }

            response.raise_for_status()

            async for line in response.aiter_lines():
                if not line.startswith("data: "):
                    continue

                now = time.perf_counter()
                token_count += 1

                if first_token_time is None:
                    first_token_time = now
                else:
                    inter_token_latencies.append(
                        now - previous_token_time
                    )

                previous_token_time = now

        end = time.perf_counter()

        return {
            "status": "success",
            "latency": end - start,
            "ttft": (
                first_token_time - start
                if first_token_time is not None
                else None
            ),
            "itl": inter_token_latencies,
            "tokens": token_count,
        }

    except Exception as exc:
        print(f"Request {request_id} failed: {exc}")

        return {
            "status": "error",
            "latency": None,
            "ttft": None,
            "itl": [],
            "tokens": 0,
        }




async def main():

    print(
        f"Starting load test with "
        f"{CONCURRENT_REQUESTS} concurrent requests..."
    )

    start = time.perf_counter()

    async with httpx.AsyncClient(timeout=None) as client:
        # Just a list comprehension tasks =  [ whatever u wanna insert inside this list(task = send_request()) for i in range()] bcs of these brackets it works as tasks.append(task) or tasks = []
        tasks = [
            send_request(client, request_id)
            for request_id in range(CONCURRENT_REQUESTS)
        ]
        #.gather works on both tasks/couroutines so it's fine if we don't create_task() explicitly
        results = await asyncio.gather(*tasks)

    total_time = time.perf_counter() - start

    #Again list comprehension list = [ what u wanna insert() for loop conditional statements to be checked in the first statement]
    #for latency, suceess in results: if sucess = True : latencies.append(latency)
    
    
    successful = [
        result for result in results
        if result["status"] == "success"
    ]

    rejected = sum(
        result["status"] == "rejected"
        for result in results
    )

    errors = sum(
        result["status"] == "error"
        for result in results
    )

    latencies = [
        result["latency"] for result in successful
    ]

    ttfts = [
        result["ttft"] for result in successful
        if result["ttft"] is not None
    ]

    all_itls = [
        itl
        for result in successful
        for itl in result["itl"]
    ]

    total_tokens = sum(
        result["tokens"] for result in successful
    )

    def percentile(values, p):
        values = sorted(values)
        if not values:
            return 0.0

        index = min(int(len(values) * p), len(values) - 1)
        return values[index]

    print()
    print("========== LOAD TEST RESULTS ==========")
    print(f"Total requests:      {CONCURRENT_REQUESTS}")
    print(f"Successful requests: {len(successful)}")
    print(f"Rejected (503):      {rejected}")
    print(f"Unexpected errors:   {errors}")
    print(f"Total duration:      {total_time:.2f} sec")

    if successful:
        print(
            f"Request throughput:  "
            f"{len(successful) / total_time:.2f} req/sec"
        )
        print(
            f"Token throughput:    "
            f"{total_tokens / total_time:.2f} tokens/sec"
        )
        print(
            f"Average latency:     "
            f"{statistics.mean(latencies):.3f} sec"
        )
        print(
            f"p95 latency:         "
            f"{percentile(latencies, 0.95):.3f} sec"
        )

    if ttfts:
        print(
            f"Average TTFT:        "
            f"{statistics.mean(ttfts) * 1000:.1f} ms"
        )
        print(
            f"p95 TTFT:             "
            f"{percentile(ttfts, 0.95) * 1000:.1f} ms"
        )

    if all_itls:
        print(
            f"Average ITL:         "
            f"{statistics.mean(all_itls) * 1000:.1f} ms"
        )
        print(
            f"p95 ITL:              "
            f"{percentile(all_itls, 0.95) * 1000:.1f} ms"
        )



if __name__ == "__main__":
    asyncio.run(main())