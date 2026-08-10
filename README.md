# 🐋 Binance Order Flow & Liquidation Tracker

A Python-based toolkit for tracking Binance Futures market manipulations, stop hunts, and forced liquidations in real-time.

> **⚠️ ENTERPRISE UPGRADE:** This repository contains the *basic* public implementation. 
> If you are a quantitative analyst or algorithmic trader looking for **0ms latency, processed CVD (Cumulative Volume Delta) divergences, and institutional-grade Order Flow signals**, you need the **Whale Core API**.
> 
> 🔗 **Get Enterprise API Access here:** [sigma-liq.fun](https://sigma-liq.fun/index.html)

## Why Retail Traders Lose (And How to Fix It)
Standard indicators (RSI, MACD, Moving Averages) are lagging. Market makers hunt retail Stop Losses to build their own liquidity. 
To trade profitably, you need to see **forced market buys/sells (Liquidations)** the exact millisecond they hit the order book.

### What Whale Core (Enterprise Version) Offers:
- **Zero-Latency WebSocket Gateway:** Bypasses public API rate limits.
- **Advanced Anomaly Filtering:** Ignores noise, tracks only $100k+ instant volume spikes.
- **CVD Divergence Detection:** Mathematically predicts market reversals based on absorption.
- **Telegram Webhook Alerts:** Receive instant signals before retail charts even render the candle.

🔗 **[Connect to Whale Core WebSockets](https://sigma-liq.fun/index.html)**

## Basic Setup (Public Version)
This open-source script connects to standard Binance public streams to output raw liquidation orders.
*Note: Public streams are subject to Binance delays. For HFT (High-Frequency Trading), use the Enterprise API.*

```python
# See tracker.py for the basic implementation
