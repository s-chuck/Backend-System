from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse

from simulator.worker import InferenceWorker
from simulator.continuous_batch import ContinuousBatchScheduler
import time
from contextlib import asynccontextmanager


batch_scheduler = ContinuousBatchScheduler(
    max_batch_size=4,
    max_queue_size=20,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Runs once when this application process starts.
    await batch_scheduler.start()

    try:
        yield
    finally:
        # Runs when the application shuts down.
        await batch_scheduler.stop()


app = FastAPI(
    title="LLM Serving Platform",
    lifespan=lifespan,
)



metrics = {
    "accepted_requests": 0,
    "completed_requests": 0,
    "rejected_requests": 0,
    "failed_requests": 0,
    "finished_requests": 0,
    "total_response_time_seconds": 0.0,
}




workers = [
    InferenceWorker(worker_id="1"),
    InferenceWorker(worker_id="2"),
    InferenceWorker(worker_id="3"),
]


current_worker = 0


def get_worker() -> InferenceWorker:
    eligible_workers = [
        worker
        for worker in workers
        if worker.available_capacity > 0
    ]

    if not eligible_workers:
        metrics["rejected_requests"] += 1
        raise HTTPException(
            status_code=503,
            detail="All inference workers are busy. Try again later.",
            headers={"Retry-After": "1"},
        )

    worker = min(
        eligible_workers,
        key=lambda w: w.inflight_requests,
    )

    worker.inflight_requests += 1
    metrics["accepted_requests"] += 1
    return worker



# @app.post("/chat")
# async def chat(prompt: str):

#     worker = get_worker()

#     async def generate():

#         async for token in worker.generate(prompt):
#             yield f"data: {token}\n\n"

#     return StreamingResponse(
#         generate(),
#         media_type="text/event-stream",
#     )


@app.post("/chat")
async def chat(
    prompt: str,
    max_new_tokens: int =  Query(default=10, ge=1),
):
    worker = get_worker()
    start_time = time.perf_counter()

    async def generate():
        completed = False

        try:
            async for token in worker.generate(
                prompt,
                max_new_tokens=max_new_tokens,
            ):
                yield f"data: {token}\n\n"

            completed = True

        except Exception:
            metrics["failed_requests"] += 1
            raise

        finally:
            worker.inflight_requests -= 1
            metrics["finished_requests"] += 1
            metrics["total_response_time_seconds"] += (
                time.perf_counter() - start_time
            )

            if completed:
                metrics["completed_requests"] += 1

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
    )


@app.get("/metrics")
async def get_metrics():
    finished = metrics["finished_requests"]

    average_response_time = (
        metrics["total_response_time_seconds"] / finished
        if finished
        else 0.0
    )

    return {
        **metrics,
        "average_response_time_seconds": round(
            average_response_time, 3
        ),
        "workers": [
            {
                "worker_id": worker.worker_id,
                "active_requests": worker.active_requests,
                "inflight_requests": worker.inflight_requests,
                "available_capacity": worker.available_capacity,
                "completed_generations": worker.completed_generations,
                "total_queue_wait_seconds": round(
                    worker.total_queue_wait_seconds, 3
                ),
                "total_generation_seconds": round(
                    worker.total_generation_seconds, 3
                ),
            }
            for worker in workers
        ],
    }


@app.post("/chat/batched")
async def chat_batched(
    prompt: str,
    max_new_tokens: int = 10,
):
    try:
        sequence = await batch_scheduler.submit(
            prompt=prompt,
            max_new_tokens=max_new_tokens,
        )
    except RuntimeError as exc:
        raise HTTPException(
            status_code=503,
            detail=str(exc),
            headers={"Retry-After": "1"},
        ) from exc

    async def generate():
        async for token in batch_scheduler.stream(sequence):
            yield f"data: {token}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
    )

