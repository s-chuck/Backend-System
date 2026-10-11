
import asyncio
import logging
from dataclasses import dataclass, field


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

logger = logging.getLogger(__name__)


@dataclass
class Sequence:
    request_id: int
    prompt: str
    max_new_tokens: int
    output: asyncio.Queue = field(
        default_factory=asyncio.Queue
    )
    tokens_generated: int = 0
    ready_at: float = 0.0
    cancelled: bool = False


class ContinuousBatchScheduler:
    def __init__(
        self,
        max_batch_size: int = 4,
        max_queue_size: int = 20,
        prefill_seconds_per_word: float = 0.005,
        decode_seconds_per_step: float = 0.1,
    ):
        self.max_batch_size = max_batch_size
        self.max_queue_size = max_queue_size
        self.prefill_seconds_per_word = (
            prefill_seconds_per_word
        )
        self.decode_seconds_per_step = (
            decode_seconds_per_step
        )

        self.pending = asyncio.Queue(
            maxsize=max_queue_size
        )
        self.active = []
        self.next_request_id = 1
        self.task = None
        self.running = False

    def log_state(self, event, sequence):
        logger.info(
            "event=%s request=%s active=%s/%s "
            "queue=%s/%s tokens=%s",
            event,
            sequence.request_id,
            len(self.active),
            self.max_batch_size,
            self.pending.qsize(),
            self.max_queue_size,
            sequence.tokens_generated,
        )

    async def start(self):
        if self.running:
            return

        self.running = True
        self.task = asyncio.create_task(self._run())
        logger.info("SCHEDULER_STARTED")

    async def stop(self):
        if not self.running:
            return

        self.running = False

        if self.task is not None:
            self.task.cancel()

            try:
                await self.task
            except asyncio.CancelledError:
                pass

            self.task = None

        logger.info("SCHEDULER_STOPPED")

    async def submit(
        self,
        prompt: str,
        max_new_tokens: int = 10,
    ):
        if not self.running:
            raise RuntimeError(
                "Scheduler is not running"
            )

        if max_new_tokens < 1:
            raise ValueError(
                "max_new_tokens must be at least 1"
            )

        sequence = Sequence(
            request_id=self.next_request_id,
            prompt=prompt,
            max_new_tokens=max_new_tokens,
        )
        self.next_request_id += 1

        try:
            self.pending.put_nowait(sequence)
        except asyncio.QueueFull:
            logger.warning(
                "event=QUEUE_FULL request=%s queue=%s/%s",
                sequence.request_id,
                self.pending.qsize(),
                self.max_queue_size,
            )
            raise RuntimeError(
                "Scheduler queue is full"
            )

        self.log_state("QUEUED", sequence)
        return sequence

    async def stream(self, sequence: Sequence):
        completed = False

        try:
            while True:
                token = await sequence.output.get()

                if token is None:
                    completed = True
                    break

                yield token

        finally:
            if not completed:
                sequence.cancelled = True
                self.log_state("CANCELLED", sequence)

    async def _run(self):
        loop = asyncio.get_running_loop()

        try:
            while self.running:
                # Admit requests while batch slots are available.
                while (
                    len(self.active) < self.max_batch_size
                    and not self.pending.empty()
                ):
                    sequence = self.pending.get_nowait()

                    if sequence.cancelled:
                        continue

                    prompt_words = max(
                        1, len(sequence.prompt.split())
                    )

                    prefill_delay = min(
                        prompt_words
                        * self.prefill_seconds_per_word,
                        1.0,
                    )

                    sequence.ready_at = (
                        loop.time() + prefill_delay
                    )

                    self.active.append(sequence)
                    self.log_state("ADMITTED", sequence)

                # Advance every active sequence that is ready.
                for sequence in list(self.active):
                    if sequence.cancelled:
                        self.active.remove(sequence)
                        continue

                    if loop.time() < sequence.ready_at:
                        continue

                    # Finish and release the slot after the
                    # final token has been emitted.
                    if (
                        sequence.tokens_generated
                        >= sequence.max_new_tokens
                    ):
                        sequence.output.put_nowait(None)
                        self.active.remove(sequence)
                        self.log_state("COMPLETED", sequence)
                        continue

                    sequence.tokens_generated += 1

                    token = (
                        f" token-{sequence.tokens_generated}"
                    )

                    sequence.output.put_nowait(token)
                    self.log_state("TOKEN", sequence)

                await asyncio.sleep(
                    self.decode_seconds_per_step
                )

        except asyncio.CancelledError:
            # Notify active consumers during shutdown.
            for sequence in self.active:
                sequence.output.put_nowait(None)

            self.active.clear()

            # Notify queued consumers and clear the queue.
            while not self.pending.empty():
                sequence = self.pending.get_nowait()
                sequence.output.put_nowait(None)

            raise
