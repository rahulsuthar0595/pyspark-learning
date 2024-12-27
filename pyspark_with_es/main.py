import asyncio
import json
import threading
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Form, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from kafka import KafkaProducer

# Environment variables for Kafka connection
KAFKA_BROKER_URL = "localhost:9092"
KAFKA_TOPIC_NAME = "mytopic"
time.sleep(5)
# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER_URL],
    value_serializer=lambda x: json.dumps(x).encode('ascii')
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(kafka_consumer(settings.KAFKA_TOPIC_NAME, settings.KAFKA_GROUP_NAME))
    yield


# Initialize FastAPI app
app = FastAPI()

# Middleware to handle CORS (if needed)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust origins as per your requirements
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Jinja2 templates for rendering HTML
templates = Jinja2Templates(directory="templates")

# Static files for serving CSS/JS
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def read_root(request: Request):
    """
    Render the homepage.
    """
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/order")
async def create_order(
        request: Request
):
    """
    Handle order submission.
    """
    form_data = await request.form()
    main_dish = form_data.get("MainDish")
    appetizer = form_data.get("Appetizer")
    beverage = form_data.get("Beverage")
    # Ensure something has been ordered
    if not (main_dish or appetizer or beverage):
        response = RedirectResponse(url="/", status_code=status.HTTP_302_FOUND)
        response.set_cookie(key="message", value="Please Order Something")
        return response

    # Prepare data for Kafka producer
    data = {
        "main_dish": main_dish,
        "appetizer": appetizer,
        "beverage": beverage,
    }

    # Push to Kafka producer
    producer.send(KAFKA_TOPIC_NAME, data)

    # Redirect to homepage with success message
    response = RedirectResponse(url="/", status_code=status.HTTP_302_FOUND)
    response.set_cookie(key="message", value="Ordered!")
    return response
