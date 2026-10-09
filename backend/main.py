from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse

from simulator.worker import InferenceWorker


app = FastAPI(title="LLM Serving Platform")


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
async def chat(prompt: str):
    worker = get_worker()

    async def generate():
        try:
            async for token in worker.generate(prompt):
                yield f"data: {token}\n\n"
        finally:
            worker.inflight_requests -= 1

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
    )
