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

        if response.status_code == 503:
            return None, "rejected"

        response.raise_for_status()
        await response.aread()

        latency = time.perf_counter() - start
        return latency, "success"

    except Exception as exc:
        print(f"Request {request_id} failed unexpectedly: {exc}")
        return None, "error"



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
    
    successful_latencies = [
        latency
        for latency, status in results
        if status == "success"
    ]

    rejected = sum(
        status == "rejected"
        for _, status in results
    )

    errors = sum(
        status == "error"
        for _, status in results
    )

    successful = len(successful_latencies)

    print()
    print("========== LOAD TEST RESULTS ==========")
    print(f"Total requests:      {CONCURRENT_REQUESTS}")
    print(f"Successful requests: {successful}")
    print(f"Rejected (503):      {rejected}")
    print(f"Unexpected errors:   {errors}")
    print(f"Total duration:      {total_time:.2f} sec")

    if successful:
        throughput = successful / total_time
        print(f"Throughput:          {throughput:.2f} req/sec")
        print(
            f"Average latency:     "
            f"{statistics.mean(successful_latencies):.2f} sec"
        )

        sorted_latencies = sorted(successful_latencies)

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