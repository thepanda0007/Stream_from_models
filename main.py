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

class PromptRequest(BaseModel):
    prompt: str

def sse(model: str, text: str) -> str:
    return f"data: {json.dumps({'model': model, 'text': text})}\n\n"

@app.get("/")
async def root():
    return {"status": "ok"}

@app.post("/stream")
async def stream_both(req: PromptRequest):
    if not ANSWERS_FILE.exists():
        raise HTTPException(status_code=500, detail="answers.json not found")
    try:
        data = json.loads(ANSWERS_FILE.read_text())
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="answers.json is invalid JSON")

    # Find matching entry by prompt (case-insensitive)
    entry = next(
        (item for item in data if item["prompt"].strip().lower() == req.prompt.strip().lower()),
        None
    )
    if entry is None:
        raise HTTPException(status_code=404, detail=f"No answer found for prompt: '{req.prompt}'")

    async def generate():
        queue: asyncio.Queue[str | None] = asyncio.Queue()

        async def pump(model_name: str, text: str):
            for word in text.split():
                await queue.put(sse(model_name, word + " "))
                await asyncio.sleep(random.uniform(0.03, 0.08))
            await queue.put(None)

        async with asyncio.TaskGroup() as tg:
            tg.create_task(pump(entry["model_a"]["name"], entry["model_a"]["text"]))
            tg.create_task(pump(entry["model_b"]["name"], entry["model_b"]["text"]))

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