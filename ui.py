import gradio as gr
import websockets
import asyncio
import json
import dotenv
import os
from asyncio.exceptions import CancelledError
import logging

dotenv.load_dotenv()
log_level = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(level=log_level)
logger = logging.getLogger(__name__)

API_KEY = os.getenv("API_KEY")
WS_URL = os.getenv("WS_URL")

# Global websocket connection
ws_connection = None

async def get_websocket():
    global ws_connection
    if ws_connection is None:
        logger.info(f"Creating new websocket connection to: {WS_URL}")
        ws_connection = await websockets.connect(
            WS_URL,
            additional_headers={"X-API-Key": API_KEY}
        )
    return ws_connection

async def bot(history):
    try:
        websocket = await get_websocket()
        for message in history:
            if message["role"] == "user":
                logger.info(f"Sending user message: {message['content']}")
                await websocket.send(message["content"])
                response = await websocket.recv()
                while response != "Final Message generated":
                    history.append({"role": "assistant", "content": response})
                    logger.info(f"Bot response: {response}")
                    response = await websocket.recv()
        return history
    except CancelledError:
        return history
    except websockets.exceptions.ConnectionClosed:
        # If connection is closed, set global connection to None and retry once
        global ws_connection
        ws_connection = None
        return await bot(history)
    except Exception as e:
        return history + [{"role": "assistant", "content": f"Error: {str(e)}"}]

def update_chart(message):
    # Logic to update the chart based on chat input
    return f"""
    <html>
    <body>
        <h2>Chart Display</h2>
        <p>Chart updated based on: {message}</p>
        <!-- Insert your chart HTML/JavaScript here -->
    </body>
    </html>
    """

with gr.Blocks() as demo:
    gr.Markdown("# Chat Interface with Chart Display")
    
    with gr.Row():
        with gr.Column(scale=1):
            chart_display = gr.HTML("<h2>Chart Display</h2><p>Chat to update the chart</p>")
        
        with gr.Column(scale=1):
            chatbot = gr.Chatbot(type="messages")
            msg = gr.Textbox(label="Type your message here")
            clear = gr.Button("Clear")

    def user(user_message, history):
        return "", history + [{"role": "user", "content": user_message}]

    # Add cleanup function to close websocket connection
    def cleanup():
        global ws_connection
        if ws_connection:
            asyncio.create_task(ws_connection.close())
        return None

    msg.submit(user, [msg, chatbot], [msg, chatbot], queue=False).then(
        bot, chatbot, chatbot
    ).then(
        update_chart, msg, chart_display
    )

    clear.click(cleanup, None, chatbot, queue=False)

demo.launch()
