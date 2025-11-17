# SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Main FastAPI application for the Shopping Assistant API.

This module provides the main API endpoints for the shopping assistant,
including query processing and streaming responses.
"""
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, List
import logging
import sys
import time
import json

from .agenttypes import State, Cart
from .planner import PlannerAgent
from .retriever import RetrieverAgent
from .cart import CartAgent
from .chatter import ChatterAgent
from .summarizer import SummaryAgent
from .graph import create_graph
from .config import load_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)


def initialize_agents(config) -> Dict:
    """Initialize all agent instances."""
    return {
        'planner_agent': PlannerAgent(config=config),
        'retriever_agent': RetrieverAgent(config=config),
        'cart_agent': CartAgent(config=config),
        'chatter_agent': ChatterAgent(config=config),
        'summary_agent': SummaryAgent(config=config)
    }


# Load configuration and initialize agents
try:
    config = load_config()  # Load and validate configuration
    agents = initialize_agents(config)
    graph = create_graph(
        **agents,
        config=config
    )
except Exception as e:
    logger.error(f"Failed to initialize application: {e}")
    raise

# Initialize FastAPI app
app = FastAPI(
    title="Shopping Assistant API",
    description="AI-powered shopping assistant with multi-agent architecture",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Request/Response models
class QueryRequest(BaseModel):
    """Request model for shopping queries."""
    user_id: int
    query: str
    image: str = ""
    context: Optional[str] = ""
    cart: Optional[Cart] = None
    retrieved: Optional[Dict[str, str]] = {}
    guardrails: Optional[bool] = True
    image_bool: bool = False
    mode: Optional[str] = None  # 'live' for live mode optimization


class QueryResponse(BaseModel):
    """Response model for shopping queries."""
    response: str
    images: Dict[str, str] = {}
    timings: Dict[str, float] = {}


class LiveQueryResponse(BaseModel):
    """Response model for live queries."""
    products: List[Dict[str, str]] = []
    response: str = ""
    query: str = ""


def create_initial_state(request: QueryRequest) -> State:
    """Create initial state from request."""
    return State(
        user_id=request.user_id,
        query=request.query,
        image=request.image,
        context=request.context or "",
        cart=request.cart or Cart(),
        guardrails=request.guardrails,
    )

@app.post("/query/stream")
async def process_query_stream(request: QueryRequest):
    """
    Stream responses to user queries in real-time.
    
    This endpoint provides streaming responses for responsive UIs
    and chat-like experiences.
    """
    try:
        logger.info(f"chain-server | /query/stream | Processing streaming query for user {request.user_id}: {request.query}")
        
        # Handle image-only queries
        if request.image and not request.query:
            request.query = "The user has submitted an image, and is looking for items from the catalog that appear similar."
        
        # Create initial state
        state = create_initial_state(request)
        
        async def send_updates():
            """Generator function for streaming updates."""
            try:
                async for chunk in graph.astream(state, stream_mode="custom"):
                    yield f"data: {chunk}\n\n"
                yield "data: [DONE]\n\n"
            except Exception as e:
                logger.error(f"Error in streaming: {e}")
                yield f"data: {json.dumps({'type': 'error', 'payload': str(e)})}\n\n"

        return StreamingResponse(send_updates(), media_type="text/event-stream")
        
    except Exception as e:
        logger.error(f"Error processing streaming query: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/query/timing", response_model=QueryResponse)
async def process_query_timing(request: QueryRequest):
    """
    Process a query and return detailed timing information.
    
    This endpoint is useful for performance analysis and debugging.
    """
    try:
        logger.info(f"chain-server | /query/timing | Processing timing query for user {request.user_id}: {request.query}")
        
        # Create initial state
        state = create_initial_state(request)
        
        # Process query and collect timing data
        start_time = time.monotonic()
        out_state_dict = await graph.ainvoke(state)
        end_time = time.monotonic()
        
        logger.info(f"chain-server | /query/timing | Collected state: {out_state_dict}")

        total_time = end_time - start_time

        # Create response with timing information
        response = QueryResponse(
            response=out_state_dict["response"],
            images={},
            timings=out_state_dict["timings"]
        )
        response.timings["total"] = total_time

        logger.info(f"chain-server | /query | Successfully processed timing query in {total_time:.2f}s")
        return response

    except Exception as e:
        logger.error(f"Error processing timing query: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
        
def format_products_for_response(retrieved_dict: Dict[str, str]) -> List[Dict[str, str]]:
    """Format retrieved products for frontend display."""
    products = []
    for name, image in retrieved_dict.items():
        products.append({
            "name": name,
            "image": image,
            "productUrl": image,
            "productName": name
        })
    return products


@app.post("/query/live", response_model=LiveQueryResponse)
async def process_live_query(request: QueryRequest):
    """
    Process live voice + camera queries for real-time product identification.
    Optimized for lower latency and faster responses.
    """
    try:
        logger.info(f"chain-server | /query/live | Processing live query for user {request.user_id}")
        
        # Handle image-only queries
        if request.image and not request.query:
            request.query = "The user has submitted an image, and is looking for items from the catalog that appear similar."
        
        # Create initial state
        state = create_initial_state(request)
        
        # For live mode, we can optimize by:
        # 1. Directly routing to retriever if image is present
        # 2. Using faster retrieval (lower k value)
        # 3. Skipping some processing steps
        
        if request.image and request.image_bool:
            # Direct retrieval for speed
            retriever_agent = agents['retriever_agent']
            state = await retriever_agent.invoke(state)
            
            # Quick response generation
            chatter_agent = agents['chatter_agent']
            async for chunk in chatter_agent.invoke(state):
                pass  # Process streaming if needed
            
            products = format_products_for_response(state.retrieved)
            
            return LiveQueryResponse(
                products=products,
                response=state.response,
                query=state.query
            )
        else:
            # For voice-only queries, use regular flow but faster
            # Process query and collect results
            out_state_dict = await graph.ainvoke(state)
            
            products = format_products_for_response(out_state_dict.get("retrieved", {}))
            
            return LiveQueryResponse(
                products=products,
                response=out_state_dict.get("response", ""),
                query=state.query
            )
            
    except Exception as e:
        logger.error(f"Error processing live query: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": time.time(),
        "version": "1.0.0"
    }


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "message": "Shopping Assistant API",
        "version": "1.0.0",
        "endpoints": {
            "query": "/query",
            "stream": "/query/stream",
            "timing": "/query/timing",
            "live": "/query/live",
            "health": "/health",
            "docs": "/docs"
        }
    } 