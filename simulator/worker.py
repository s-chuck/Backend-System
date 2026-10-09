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

    async def generate(self, prompt: str):
        async with self.semaphore:
            self.active_requests += 1
            print(
                f"Worker {self.worker_id}: "
                f"active={self.active_requests}, "
                f"inflight={self.inflight_requests}"
            )
            try:
                tokens = [
                    "This",
                    " is",
                    " a",
                    " simulated",
                    " LLM",
                    " response",
                    " generated",
                    " by",
                    f" worker-{self.worker_id}",
                    ".",
                ]

                for token in tokens:
                    await asyncio.sleep(0.1)
                    yield token

            finally:
                self.active_requests -= 1
