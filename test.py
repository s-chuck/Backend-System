import asyncio


async def handle_request(request_id, semaphore):
    print(f"Request {request_id}: waiting")
    async with semaphore:
        # active_request += 1
        try:
            print(f"Request {request_id}: acquired permit")
            # print(f"Currently active {active_request}")
            await asyncio.sleep(2)
        finally:
            print(f"Request {request_id}: finished")
        


async def main():
    semaphore = asyncio.Semaphore(2)

    tasks = [
        # asyncio.create_task(handle_request(i, semaphore))
        handle_request(i,semaphore)
        for i in range(1, 6)
    ]

    await asyncio.gather(*tasks)


asyncio.run(main())