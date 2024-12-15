from pydantic_ai import Agent, RunContext, Tool
from pydantic_ai.messages import ToolReturn
from pydantic import BaseModel
from typing import List, Dict
import dotenv
import os
from pydantic_ai.models.openai import OpenAIModel
from openai import AsyncAzureOpenAI
import duckdb
import tools
import pandas as pd
import asyncio
import logging
import os
from tools import ContextData
import visualization

dotenv.load_dotenv()
logging_level = os.getenv("LOGGING_LEVEL")
logging.basicConfig(level=logging_level)
logger = logging.getLogger(__name__)

class ActionPlan(BaseModel):
    actions: List[str]
    message: str
    next_task: str


class DataCrew:
    def __init__(self, session_id: str):
        self.user_query = ""
        self.session_id = session_id
        self.db_path = os.getenv("DB_PATH")
        self.con = duckdb.connect(self.db_path,read_only=True)
        self.api_key = os.getenv("GROQ_API_KEY")
        #self.model = GroqModel('llama-3.1-70b-versatile', api_key=self.api_key)
        self.client = AsyncAzureOpenAI(
            azure_endpoint=os.getenv("AZURE_API_ENDPOINT"),
            azure_deployment="gpt-4o-agent",
            api_version=os.getenv("AZURE_API_VERSION"),
            api_key=os.getenv("AZURE_AI_KEY")
        )
        self.model = OpenAIModel('gpt-4o', openai_client=self.client)
        self.action_plan = None
        self.message_history = None
        self.context_data = None
        self.visualization_html = ""


    def set_user_query(self, query: str):
        self.user_query = query

    def retrieve_supplier(self,ctx: RunContext[duckdb.DuckDBPyConnection], id: str) -> str:
        """Retrieve the supplier of the id"""
        return str(tools.get_supplier_of_mg(ctx.deps, id))

    def retrieve_buyer(self,ctx: RunContext[duckdb.DuckDBPyConnection], id: str) -> str:
        """Retrieve the buyer of the id"""
        return str(tools.get_buyer_of_mg(ctx.deps, id))

    def create_agents(self):
        self.planner_agent = Agent(
            model=self.model,
            result_type=ActionPlan,
            system_prompt=(
                'If the question is about querying data about buyer and supplier, please generate an action plan'
                'for the data retrieval agent and the data analyzing agent to answer the question: '
                'Fill in the entity in the query into the action plan.'
                '1. Retrieve the data for the [entity in the query]'
                '2. Analyze the data'
                '3. Generate a concise answer to the question'
                'set the next_task to the data_retrieval_agent to execute.'
                'If the question is not about querying data about buyer and supplier,'
                'but answer with the following message only: "I am sorry, I cannot answer this question, please rephrase the question."'
                'Please do not generate any actin plan,'
                'but set the next_task to "User Clarification".'
            )
        )
        self.data_retriever_agent = Agent(
            model=self.model,
            deps_type=duckdb.DuckDBPyConnection,
            result_type=ContextData,
            tools=[Tool(self.retrieve_supplier,takes_ctx=True), Tool(self.retrieve_buyer, takes_ctx=True)],
            retries=3
        )
        self.data_analyzer_agent = Agent(
            model=self.model,
            system_prompt=(
                'Please analyze the data and generate a concise answer to the question base on the provided data'
            ),
        )
    async def run_planning_task(self):
        result = await self.planner_agent.run(f"please generate an action plan to answer the question: {self.user_query}")
        self.action_plan = result.data
        return result.data
        
    async def run_data_retrieving_task(self):
        action_plan = self.action_plan.actions
        logger.debug(f"Action plan: {action_plan}")
        action_plan_str = "\n".join(action_plan)
        while True:
            result = await self.data_retriever_agent.run(
                f"""
                Use the provided tools to retrieve the data in order to answer the question: {self.user_query} 
                according to the action plan: {action_plan_str}
                """
                , deps=self.con)
            for msg in result.all_messages():
                logger.info(f"Data retrieving task message: {msg}")
                if isinstance(msg, ToolReturn):
                    logger.info(f"Data retrieving task result: {result.data}")
                    self.context_data = result.data
                    self.visualization_html = self.visualize_data(result.data)
                    return result.data 
            await asyncio.sleep(1)

    def visualize_data(self, result: ContextData):
        graph_div_html = """
<div class="iframe-container">
"""
        output_html = ""
        logger.debug(f"Visualizing data: {result.source}")
        df = pd.DataFrame(result.data)
        if result.source == "get_supplier_of_mg":
            image_path = f"static/{self.session_id}_supplier_bar_chart.png"
            graph_path = f"static/{self.session_id}_supplier_graph.html"
            visualization.create_bar_chart(df, 'supplier', image_path)
            g = visualization.create_supplier_graph(df)
            visualization.visualize_graph(g,graph_path)
            output_html += f"<div class='iframe-container'><img src='static/{self.session_id}_supplier_bar_chart.png' alt='Supplier Bar Chart' style='width:200px;height:200px;'></div>"
            output_html += f"<div class='iframe-container'><iframe src='static/{self.session_id}_supplier_graph.html' style='width:150px;height:150px;'></iframe></div>"
        elif result.source == "get_buyer_of_mg":
            image_path = f"static/{self.session_id}_buyer_bar_chart.png"
            graph_path = f"static/{self.session_id}_buyer_graph.html"
            visualization.create_bar_chart(df, 'buyer', image_path)
            g = visualization.create_buyer_graph(df)
            visualization.visualize_graph(g,graph_path)
            output_html += f"<div class='iframe-container'><img src='static/{self.session_id}_buyer_bar_chart.png' alt='Buyer Bar Chart' style='width:300px;height:300px;'></div>"
            output_html += f"<div class='iframe-container'><iframe src='static/{self.session_id}_buyer_graph.html' width='100%' height='150px' style='width:300px;height:300px;'></iframe></div>"
        logger.debug(f"Visualization HTML: {output_html}")
        return output_html
    
    async def run_data_analyzing_task(self):   
        context_data = self.context_data
        logger.debug(f"Context data: {context_data.data}")
        async with self.data_analyzer_agent.run_stream(f"analyze the context data {str(context_data.data)} to answer the question: {self.user_query}", message_history=self.message_history) as result:
            async for chunk in result.stream_text(delta=True):
                logger.info(f"Data analyzing task result: {chunk}")
                yield chunk
            self.message_history = result.all_messages()
    
    @classmethod
    async def test_run(cls):
        dc = cls("123")
        dc.set_user_query("What is the supplier of the mg 6af0bc20b990f9bac650be7604f110e5893490e16217c6aca9392096725b53af ?")
        dc.create_agents()
        result = await dc.run_planning_task()
        print(result.data)
        result = await dc.run_data_retrieving_task(result.data)
        print(result.data)
        result = await dc.run_data_analyzing_task(result.data)
        print(result.data)


