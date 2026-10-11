import asyncio


class InferenceWorker:
    def __init__(
        self,
        worker_id: str,
        max_concurrent_requests: int = 2,
        max_queue_size: int = 2,
    ):
        self.worker_id = worker_id
        self.max_concurrent_requests = max_concurrent_requests
        self.max_queue_size = max_queue_size
        self.total_queue_wait_seconds = 0.0
        self.total_generation_seconds = 0.0
        self.completed_generations = 0
        self.total_prefill_seconds = 0.0
        self.total_decode_seconds = 0.0

        self.active_requests = 0
        self.inflight_requests = 0

        self.semaphore = asyncio.Semaphore(
            max_concurrent_requests
        )

    @property
    def available_capacity(self) -> int:
        return max(
            0,
            self.max_concurrent_requests
            + self.max_queue_size
            - self.inflight_requests,
        )

    async def generate(
        self,
        prompt: str,
        max_new_tokens: int = 10,
    ):
        loop = asyncio.get_running_loop()
        queue_start = loop.time()

        async with self.semaphore:
            generation_start = loop.time()
            queue_wait = generation_start - queue_start

            self.total_queue_wait_seconds += queue_wait
            self.active_requests += 1

            prefill_seconds = 0.0
            decode_seconds = 0.0

            try:
                # Approximate prompt size using word count.
                # This is a simulation, not a real tokenizer.
                estimated_prompt_tokens = max(
                    1, len(prompt.split())
                )

                # Simulate prompt processing.
                prefill_delay = min(
                    estimated_prompt_tokens * 0.005,
                    1.0,
                )

                prefill_start = loop.time()
                await asyncio.sleep(prefill_delay)
                prefill_seconds = loop.time() - prefill_start

                # Simulate autoregressive token generation.
                for i in range(max_new_tokens):
                    await asyncio.sleep(0.1)
                    decode_seconds += 0.1
                    yield f" token-{i + 1}"

            finally:
                total_generation = loop.time() - generation_start

                self.total_prefill_seconds += prefill_seconds
                self.total_decode_seconds += decode_seconds
                self.total_generation_seconds += total_generation
                self.completed_generations += 1
                self.active_requests -= 1
