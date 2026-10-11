
import asyncio

from simulator.continuous_batch import (
    ContinuousBatchScheduler,
)


async def main():
    print("TEST STARTED", flush=True)

    scheduler = ContinuousBatchScheduler(
        max_batch_size=2,
        max_queue_size=10,
        prefill_seconds_per_word=0.005,
        decode_seconds_per_step=0.1,
    )

    await scheduler.start()

    try:
        print("SCHEDULER STARTED", flush=True)

        requests = [
            ("short prompt", 3),
            ("another short prompt", 6),
            ("third prompt waiting for a slot", 2),
        ]

        sequences = []

        for prompt, output_length in requests:
            sequence = await scheduler.submit(
                prompt=prompt,
                max_new_tokens=output_length,
            )
            sequences.append(sequence)

        print("ALL REQUESTS SUBMITTED", flush=True)

        async def consume(sequence):
            loop = asyncio.get_running_loop()
            start_time = loop.time()
            tokens = []

            async for token in scheduler.stream(sequence):
                tokens.append(token)

            elapsed = loop.time() - start_time

            print(
                f"Request {sequence.request_id}: "
                f"{len(tokens)} tokens -> {tokens}",
                flush=True,
            )

            print(
                f"Request {sequence.request_id}: "
                f"elapsed={elapsed:.2f}s",
                flush=True,
            )

        await asyncio.gather(
            *(consume(sequence) for sequence in sequences)
        )

        print("ALL REQUESTS COMPLETED", flush=True)

    finally:
        await scheduler.stop()


if __name__ == "__main__":
    asyncio.run(main())
