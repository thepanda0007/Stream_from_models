from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pathlib import Path
import asyncio
import random
import json

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["POST", "GET"],
    allow_headers=["*"],
)

ANSWERS_FILE = Path("answers.json")
ANSWERS_DATA = []

class PromptRequest(BaseModel):
    prompt: str

def sse(model: str, text: str) -> str:
    return f"data: {json.dumps({'model': model, 'text': text})}\n\n"

@app.on_event("startup")
async def load_data():
    global ANSWERS_DATA
    ANSWERS_DATA = json.loads(ANSWERS_FILE.read_text())

@app.get("/")
async def root():
    return {"status": "ok"}

@app.post("/stream")
async def stream_both(req: PromptRequest):
    entry = next(
        (item for item in ANSWERS_DATA if item["prompt"].strip().lower() == req.prompt.strip().lower()),
        None
    )
    if entry is None:
        raise HTTPException(status_code=404, detail=f"No answer found for prompt: '{req.prompt}'")

    # Capture these BEFORE entering the generator
    model_a_name = entry["model_a"]["name"]
    model_a_text = entry["model_a"]["text"]
    model_b_name = entry["model_b"]["name"]
    model_b_text = entry["model_b"]["text"]

    async def generate():
        queue: asyncio.Queue[str | None] = asyncio.Queue()

        async def pump(model_name: str, text: str):
            for word in text.split():
                await queue.put(sse(model_name, word + " "))
                await asyncio.sleep(random.uniform(0.03, 0.08))
            await queue.put(None)

        # Create tasks INSIDE the coroutine, not inside async generator
        loop = asyncio.get_event_loop()
        loop.create_task(pump(model_a_name, model_a_text))
        loop.create_task(pump(model_b_name, model_b_text))

        done = 0
        while done < 2:
            item = await queue.get()
            if item is None:
                done += 1
            else:
                yield item

        yield f"data: {json.dumps({'model': 'both', 'done': True})}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )