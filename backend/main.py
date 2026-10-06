from fastapi import FastAPI
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
    global current_worker

    worker = workers[current_worker]

    current_worker = (current_worker + 1) % len(workers)

    return worker


@app.post("/chat")
async def chat(prompt: str):

    worker = get_worker()

    async def generate():

        async for token in worker.generate(prompt):
            yield f"data: {token}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
    )