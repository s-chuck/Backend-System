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

    try:
        response = await client.post(
            URL,
            params={
                "prompt": f"Explain request {request_id}"
            },
        )

        response.raise_for_status()

        # Consume the complete streaming response.
        await response.aread()

        latency = time.perf_counter() - start

        return latency, True

    except Exception as exc:
        print(f"Request {request_id} failed: {exc}")

        return None, False


async def main():

    print(
        f"Starting load test with "
        f"{CONCURRENT_REQUESTS} concurrent requests..."
    )

    start = time.perf_counter()

    async with httpx.AsyncClient(timeout=None) as client:

        tasks = [
            send_request(client, request_id)
            for request_id in range(CONCURRENT_REQUESTS)
        ]

        results = await asyncio.gather(*tasks)

    total_time = time.perf_counter() - start

    latencies = [
        latency
        for latency, success in results
        if success
    ]

    successful = len(latencies)

    failed = CONCURRENT_REQUESTS - successful

    print()
    print("========== LOAD TEST RESULTS ==========")

    print(f"Total requests:      {CONCURRENT_REQUESTS}")
    print(f"Successful requests: {successful}")
    print(f"Failed requests:     {failed}")
    print(f"Total duration:      {total_time:.2f} sec")

    if successful:

        throughput = successful / total_time

        print(f"Throughput:          {throughput:.2f} req/sec")
        print(
            f"Average latency:     "
            f"{statistics.mean(latencies):.2f} sec"
        )

        sorted_latencies = sorted(latencies)

        def percentile(values, percentile):
            index = int(len(values) * percentile)
            index = min(index, len(values) - 1)
            return values[index]

        print(
            f"p50 latency:         "
            f"{percentile(sorted_latencies, 0.50):.2f} sec"
        )

        print(
            f"p95 latency:         "
            f"{percentile(sorted_latencies, 0.95):.2f} sec"
        )

        print(
            f"p99 latency:         "
            f"{percentile(sorted_latencies, 0.99):.2f} sec"
        )


if __name__ == "__main__":
    asyncio.run(main())