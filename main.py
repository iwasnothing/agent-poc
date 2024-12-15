import asyncio
import time
import uuid
from typing import Dict
import os
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket, status, BackgroundTasks, Security
from fastapi.security import APIKeyHeader
import logging
import declare_constants
import dotenv
from agent import DataCrew
from hashing import hash_query,decode_answer
from concurrent.futures import ThreadPoolExecutor, Future
from functools import partial
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import pathlib
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
from typing import Dict
import jwt
import json
from user_authentication import authenticate_user, create_token
from user import User
import duckdb

dotenv.load_dotenv()
logging.basicConfig(level=declare_constants.get_log_level())
logger = logging.getLogger(__name__)
TABLE_NAMES = declare_constants.GET_TABLE_NAMES()

app = FastAPI()

# Mount static files directory
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def index(request: Request):
    return FileResponse("static/index.html")
# Constants
API_KEY = os.getenv("API_KEY")
MAX_CONNECTIONS = 500
HEARTBEAT_INTERVAL = 30  # seconds
MAX_MESSAGE_SIZE = 1024 * 1024  # 1 MB
RATE_LIMIT = 120  # requests
RATE_LIMIT_WINDOW = 60  # seconds

# Store active connections
active_connections: Dict[str, WebSocket] = {}

# API Key authorization
api_key_header = APIKeyHeader(name="X-API-Key")

# Simple in-memory rate limiter
rate_limit_store = {}

thread_pool = ThreadPoolExecutor(max_workers=10)  # Adjust number as needed
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM")



def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        # Log the received token for debugging
        logger.debug(f"Received token: {token}")


        token_bytes = token.encode('utf-8')  # Convert token to bytes if it's a string

        payload = jwt.decode(token_bytes, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if username is None:
            raise HTTPException(status_code=401, detail="Invalid authentication credentials")
        return User(username=username)
    except jwt.PyJWTError as e:
        logger.error(f"JWT error: {e}")
        raise HTTPException(status_code=401, detail="Invalid authentication credentials")

@app.post("/login")
async def login(request: Request):
    form_data = await request.form()
    username = form_data.get("username")
    password = form_data.get("password")
    logger.debug(f"Login request received for user: {username}")
    user = authenticate_user(username, password)
    if not user:
        raise HTTPException(status_code=401, detail="Incorrect username or password")
    token = create_token(user.username)
    return {"access_token": token, "token_type": "bearer"}

async def get_api_key(api_key: str = Security(api_key_header)):
    if api_key != API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API Key"
        )
    return api_key

@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    client_ip = request.client.host
    current_time = time.time()
    
    if client_ip in rate_limit_store:
        last_request_time, request_count = rate_limit_store[client_ip]
        if current_time - last_request_time < RATE_LIMIT_WINDOW:
            if request_count >= RATE_LIMIT:
                raise HTTPException(status_code=429, detail="Rate limit exceeded")
            rate_limit_store[client_ip] = (last_request_time, request_count + 1)
        else:
            rate_limit_store[client_ip] = (current_time, 1)
    else:
        rate_limit_store[client_ip] = (current_time, 1)
    
    response = await call_next(request)
    return response

@app.websocket("/ws")  # Annotation for the WebSocket endpoint
async def websocket_endpoint(websocket: WebSocket, token: str):
    logger.debug(f"WebSocket connection received")
    await websocket.accept()
    session_id = str(uuid.uuid4())
    dc = DataCrew(session_id)
    active_connections[session_id] = websocket
    logger.info(f"New connection established: {session_id}")
    if len(active_connections) >= MAX_CONNECTIONS:
        await websocket.close(code=1008)  # Connection limit reached
        return
    try:
        token_user = get_current_user(token)
        logger.debug(f"Token user: {token_user}")
        #await websocket.send_text(f"Welcome")
        data = await websocket.receive_text()
        logger.debug(f"Data received: {data}")
        auth_data = json.loads(data)    
        username = auth_data.get("username")
        password = auth_data.get("password")
        ws_user = authenticate_user(username, password)
        if ws_user != token_user:
            logger.error(f"Authentication error")
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return   
        await websocket.send_text(f"You can start asking questions now")
        while True:
            data = await websocket.receive_text()
            if len(data) > MAX_MESSAGE_SIZE:
                await websocket.close(code=1009)  # Message too big
                break
            logger.debug(f"Received message: {data}")
            hashed_query = hash_query(data)
            logger.debug(f"Hashed query: {hashed_query}")
            
            dc.set_user_query(hashed_query)
            dc.create_agents()

            await websocket.send_text("Generating Action Plan...<br>")
            result = await dc.run_planning_task()
            logger.debug(f"Planning Message generated: {result}")
            logger.debug(f"Next task: {result.next_task}")
            if result.next_task != "User Clarification":
                actions = result.actions
                decoded_actions = [decode_answer(action) for action in actions]
                for index,action in enumerate(decoded_actions):
                    await websocket.send_text(f"{index+1}. {action} <br>")
            else:
                await websocket.send_text(result.message)
                await websocket.send_text("Please clarify the question <br>")
                continue
            await websocket.send_text("Please confirm to execute the action plan (Y/N) <br>")
            logger.debug("Starting data retrieval...")
            result = await dc.run_data_retrieving_task()
            logger.debug(f"Data Retrieving Message generated: {result}")
                    
            confirm = await websocket.receive_text()
            if confirm.upper().strip() == "Y" or confirm.upper().strip() == "YES":
                await websocket.send_text("Starting data retrieval... <br>")
                await websocket.send_text(f"Data Retrieving Message generated: <br>")
                await websocket.send_text(dc.visualization_html)

                while True:
                    last_word = ""
                    async for chunk in dc.run_data_analyzing_task():
                        if len(chunk) > 0:
                            if chunk[-1] == " ":
                                last_word = ""
                                decoded_result = decode_answer(chunk)
                            else:
                                tokenized_chunk = chunk.split()
                                if len(tokenized_chunk) > 0:    
                                    suffix = tokenized_chunk[-1]
                                    prefix_chunk = " ".join(tokenized_chunk[:-1])
                                    prefix_chunk = " " + last_word + prefix_chunk
                                    last_word = suffix
                                    logger.debug(f"Last word: {last_word} + {prefix_chunk}")
                                    decoded_result = decode_answer(prefix_chunk)
                            logger.debug(f"Data Analyzing Message generated: {decoded_result}")
                            await websocket.send_text(decoded_result + " ")
                    await websocket.send_text(" " + last_word + "<br>")
                    await websocket.send_text("Any further questions? <br>")
                    data = await websocket.receive_text()
                    if len(data) > MAX_MESSAGE_SIZE:
                        await websocket.close(code=1009)  # Message too big
                        break
                    logger.debug(f"Received message: {data}")
                    hashed_query = hash_query(data)
                    logger.debug(f"Hashed query: {hashed_query}")
                        
                    dc.set_user_query(hashed_query)

            else:
                await websocket.send_text("Action plan not executed")
    except Exception as e:
        logger.error(f"Error in session {session_id}: {e}")
        logger.exception("Full traceback:")
        del active_connections[session_id]
        await websocket.close()
    finally:
        del active_connections[session_id]
        await websocket.close()

@app.post("/search_mg")
async def search_mg(request: Request,token: str = Depends(api_key_header)):
    token_user = get_current_user(token)
    if token_user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API Key"
        )
    data = await request.json()
    mg_id = data.get("mg_id")
    con = duckdb.connect(os.getenv("DB_PATH"),read_only=True)
    result = con.sql(f"select mg_id from {TABLE_NAMES['MG_HASH_MAP']} where mg_id like '{mg_id}%'").to_df()
    return result['mg_id'].tolist()



if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, workers=4)
