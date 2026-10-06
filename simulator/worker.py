import asyncio


class InferenceWorker:

    def __init__(self, worker_id: str, max_concurrent_requests: int = 2):
        self.worker_id = worker_id
        self.max_concurrent_requests = max_concurrent_requests
        self.active_requests = 0

    @property
    def available_capacity(self) -> int:
        return self.max_concurrent_requests - self.active_requests

    async def generate(self, prompt: str):

        self.active_requests += 1

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