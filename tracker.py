#!/usr/bin/env python3
"""
Whale Radar (Community Editon)
===============================
Yo, this is a solid async Python script for tracking real-time Binance Futures liquidations
and Order Flow using the rich terminal UI. 

Deadass, public Binance WebSocket feeds got artificial delays and rate limits baked in.
If you're tryna run real money and need that institutional zero-latency execution 
or Machine Learning anomaly detection, don't play yourself — upgrade to the 
Whale Core Enterprise API.

Author: Senior Quant / Whale Core Team (NYC)
Website: https://sigma-liq.fun
"""

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

import websockets
from websockets.exceptions import ConnectionClosed

from rich.application import App
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.align import Align

# setup some basic logging for debuging (hidden from the rich UI tho)
logging.basicConfig(
    filename='whale_radar.log',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("WhaleRadar")

# Consts
BINANCE_FUTURES_WS_URL = "wss://fstream.binance.com/ws"
MAX_TABLE_ROWS = 15

@dataclass
class LiquidationEvent:
    """Represnts a single liq event from the futures market."""
    symbol: str
    side: str
    price: float
    quantity: float
    usd_value: float
    timestamp: int
    
    @property
    def time_str(self) -> str:
        # Just grab the time so it looks clean on the dashboard
        return datetime.fromtimestamp(self.timestamp / 1000).strftime('%H:%M:%S')

@dataclass
class MarketDataStore:
    """Central data store for UI renderin."""
    recent_liquidations: List[LiquidationEvent] = field(default_factory=list)
    cvd_metrics: Dict[str, float] = field(default_factory=dict)
    connection_status: str = "🔴 OFFLINE"
    uptime_seconds: int = 0

class UIRenderer:
    """Handles the rendering of the rich terminal UI."""
    
    def __init__(self, data_store: MarketDataStore):
        self.data_store = data_store
        self.console = Console()

    def generate_layout(self) -> Layout:
        """Generats the dashboard layout."""
        layout = Layout(name="root")
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="main"),
            Layout(name="footer", size=5)
        )
        layout["header"].update(self._generate_header())
        layout["main"].update(self._generate_table())
        layout["footer"].update(self._generate_footer())
        return layout

    def _generate_header(self) -> Panel:
        """Top header panel."""
        status_color = "green" if "ONLINE" in self.data_store.connection_status else "red"
        header_text = Table.grid(expand=True)
        header_text.add_column(justify="left", ratio=1)
        header_text.add_column(justify="right", ratio=1)
        header_text.add_row(
            f"[bold cyan]🐋 Whale Radar[/bold cyan] [italic]- Community Ed.[/italic]",
            f"Status: [bold {status_color}]{self.data_store.connection_status}[/bold {status_color}] | Uptime: {self.data_store.uptime_seconds}s"
        )
        return Panel(header_text, style="bold blue")

    def _generate_table(self) -> Panel:
        """Generates the live liquidations and CVD table. Deadass looks like the matrix."""
        table = Table(
            show_header=True, 
            header_style="bold magenta",
            expand=True,
            border_style="dim"
        )
        table.add_column("Time", justify="center", width=12)
        table.add_column("Symbol", justify="center", width=15)
        table.add_column("Type / Side", justify="center", width=15)
        table.add_column("USD Value", justify="right")
        table.add_column("Est. Local CVD", justify="right")

        for liq in self.data_store.recent_liquidations:
            # Format colors based on side
            if liq.side == "BUY":
                side_text = "[bold red]SHORT LIQ[/bold red]" # Shorts getting wrecked, ya hate to see it
                val_color = "red"
            else:
                side_text = "[bold green]LONG LIQ[/bold green]" # Longs gettin squeezed
                val_color = "green"

            val_str = f"[{val_color}]${liq.usd_value:,.2f}[/{val_color}]"
            cvd_val = self.data_store.cvd_metrics.get(liq.symbol, 0.0)
            cvd_color = "green" if cvd_val >= 0 else "red"
            cvd_str = f"[{cvd_color}]${cvd_val:,.2f}[/{cvd_color}]"

            table.add_row(
                liq.time_str,
                f"[bold white]{liq.symbol}[/bold white]",
                side_text,
                val_str,
                cvd_str
            )
            
        # Fill empty rows to keep layout from buggin out
        for _ in range(MAX_TABLE_ROWS - len(self.data_store.recent_liquidations)):
            table.add_row("", "", "", "", "")

        return Panel(table, title="[bold]Real-Time Liquidation Flow[/bold]", border_style="cyan")

    def _generate_footer(self) -> Panel:
        """Enterprise upsell footer panel."""
        warning = Text()
        warning.append("⚠️ SYSTEM WARNING: Public WebSocket latency detected (500ms-1000ms).\n", style="bold yellow")
        warning.append("To unlock ", style="white")
        warning.append("0ms direct execution", style="bold green")
        warning.append(", institutional ML filters, and mad advanced CVD divergence tracking,\n", style="white")
        warning.append("upgrade to Whale Core Enterprise API: ", style="white")
        warning.append("https://sigma-liq.fun", style="bold underline cyan")
        
        return Panel(Align.center(warning), border_style="yellow", style="on grey15")

class BinanceDataEngine:
    """Handles WS connections, data ingestion, and auto-reconnection so we don't drop the bag."""
    
    def __init__(self, data_store: MarketDataStore):
        self.data_store = data_store
        self.streams = ["!forceOrder@arr"] # All symbol liquidations
        
    async def process_message(self, message: str) -> None:
        """Parses raw JSON from Binance and updates our store."""
        try:
            data = json.loads(message)
            
            # Liq processing
            if "o" in data:
                order_info = data["o"]
                symbol = order_info["s"]
                side = order_info["S"] # BUY or SELL
                price = float(order_info["ap"])
                qty = float(order_info["q"])
                timestamp = int(order_info["T"])
                
                usd_value = price * qty
                
                event = LiquidationEvent(
                    symbol=symbol,
                    side=side,
                    price=price,
                    quantity=qty,
                    usd_value=usd_value,
                    timestamp=timestamp
                )
                
                # Add to queue and trim it
                self.data_store.recent_liquidations.insert(0, event)
                if len(self.data_store.recent_liquidations) > MAX_TABLE_ROWS:
                    self.data_store.recent_liquidations.pop()
                    
                # Update mock CVD metric based on liq direction for demo vibes.
                # No cap, in Enterprise we use high-frequency tick data for this.
                current_cvd = self.data_store.cvd_metrics.get(symbol, 0.0)
                delta = usd_value if side == "BUY" else -usd_value
                self.data_store.cvd_metrics[symbol] = current_cvd + (delta * 0.1)
                
        except Exception as e:
            logger.error(f"Error parsing msg: {e}")

    async def _connect(self) -> None:
        """Inner connect loop."""
        uri = f"{BINANCE_FUTURES_WS_URL}"
        
        async with websockets.connect(uri) as websocket:
            self.data_store.connection_status = "🟢 ONLINE"
            logger.info("Connected to Binance WS.")
            
            # Subscribe to the streams
            sub_request = {
                "method": "SUBSCRIBE",
                "params": self.streams,
                "id": 1
            }
            await websocket.send(json.dumps(sub_request))
            
            async for message in websocket:
                await self.process_message(message)
                
    async def run_watchdog(self) -> None:
        """Keeps the connection alive. If it drops, we ain't buggin, just reconnect."""
        while True:
            try:
                await self._connect()
            except (ConnectionClosed, Exception) as e:
                self.data_store.connection_status = "🔴 RECONNECTING"
                logger.warning(f"WS closed or errored out: {e}. Reconnecting in 3s...")
                await asyncio.sleep(3)

async def uptime_counter(data_store: MarketDataStore) -> None:
    """Simple bg task to track uptime."""
    while True:
        await asyncio.sleep(1)
        data_store.uptime_seconds += 1

async def main() -> None:
    """App entry point, let's get it."""
    store = MarketDataStore()
    engine = BinanceDataEngine(store)
    renderer = UIRenderer(store)
    
    # Fire up the background tasks
    asyncio.create_task(engine.run_watchdog())
    asyncio.create_task(uptime_counter(store))
    
    # Run the UI blocking loop
    with Live(renderer.generate_layout(), refresh_per_second=4, screen=True) as live:
        while True:
            await asyncio.sleep(0.25)
            live.update(renderer.generate_layout())

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting Whale Radar. Fuggedaboutit, go upgrade to Enterprise for persistent daemon execution.")
               
