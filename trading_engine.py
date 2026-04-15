"""
Trading Engine v3 - Valuation-Aware Allocation.

Signal flow:
1. Teknisk analys (45%) + Makro (55%) → combined_score
2. Valuation Engine overlay → modifierar score baserat på 3-lagers beslut:
   - REGIME (makro) → BUY/HOLD/SELL
   - VALUATION (fundamenta) → billigt/fair/dyrt
   - LEADING INDICATORS (ETF-proxies) → BULLISH/NEUTRAL/BEARISH
3. make_trade_decision() → STRONG_BUY/BUY/HOLD/SELL/STRONG_SELL + position_size_modifier

Nyckellogik:
- Billigt + Regime SELL → HOLD (inte SELL — här hittar man bottnar)
- Dyrt + Regime BUY → HOLD (jaga inte uppgång)
- Alla tre aligned → STRONG BUY/SELL med 1.5x position

TR Hard Cap: Signaler för TR blockeras om allokering >= 20%.
"""

from datetime import datetime, timedelta
from config import (
    STOCK_UNIVERSE, MARKET_ALLOCATION, SIGNAL_THRESHOLDS,
    SIGNAL_WEIGHTS, USE_SCANNER, RISK_MANAGEMENT,
    TURKEY_CONFIG, ALLOCATION_RULES, ALLOCATION_BY_SIZE,
    VALUATION_CONFIG, MULTI_STRATEGY_CONFIG, QUALITY_FILTERS,
    DYNAMIC_THRESHOLDS, SENTIMENT_CONFIG, EDGE_SIGNAL_CONFIG,
)
from market_hours import is_market_open, get_all_market_status, get_open_markets
from watchlist import (
    Watchlist, TrailingStopManager, CooldownManager, DrawdownTracker,
    AllocationChecker, BlockingLog, RISK_PARAMS, calculate_kelly_position_size,
)
from stock_scanner import StockScanner
from strategies import StrategyManager
from sentiment import SentimentScanner


class TradingEngine:
    """Agent som genererar och exekverar handelssignaler."""

    def __init__(self, data_fetcher, technical_analyzer, macro_analyzer, portfolio_manager):
        self.data = data_fetcher
        self.ta = technical_analyzer
        self.macro = macro_analyzer
        self.pm = portfolio_manager
        self.last_signals = []
        self.last_analysis = {}
        self.last_macro = {}
        self.last_valuation = {}
        self.last_run = None
        self.last_execution_result = {}

        # Risk-moduler
        self.watchlist = Watchlist()
        self.trailing_stops = TrailingStopManager()
        self.cooldown = CooldownManager()
        self.drawdown = DrawdownTracker()

        # Valuation engine
        from analyzers import ValuationAnalyzer
        self.valuation = ValuationAnalyzer()

        # Stock scanner
        self.scanner = StockScanner()

        # Multi-strategi
        self.strategy_manager = StrategyManager()

        # Sentiment
        self.sentiment = SentimentScanner()

        # Edge signal system (trav-modellen)
        self.edge_fetcher = None
        self.edge_analyzer = None
        self.last_edge_signals = {}
        edge_enabled = EDGE_SIGNAL_CONFIG.get("enabled", False)
        if edge_enabled:
            try:
                from edge_data_fetcher import EdgeDataFetcher
                from edge_analyzer import EdgeAnalyzer
                self.edge_fetcher = EdgeDataFetcher(EDGE_SIGNAL_CONFIG)
                self.edge_analyzer = EdgeAnalyzer(EDGE_SIGNAL_CONFIG)
                print("[ENGINE] Edge signals (trav-modellen) initialiserad")
            except Exception as e:
                print(f"[ENGINE] Edge signals kunde ej initialiseras: {e}")

        # Blockeringslog
        self.blocking_log = BlockingLog()

    def get_active_universe(self):
        """Hämta aktivt aktieuniversum."""
        if USE_SCANNER and self.scanner.filtered_universe:
            total = sum(len(v) for v in self.scanner.filtered_universe.values())
            if total > 0:
                universe = self.scanner.get_stock_universe()
                print(f"[ENGINE] Använder scanner-universum: {total} aktier")
                return universe

        print(f"[ENGINE] Använder fallback STOCK_UNIVERSE")
        return STOCK_UNIVERSE

    def run_scan_if_needed(self):
        """Kör börsscan om cache är gammal."""
        if USE_SCANNER and self.scanner.needs_refresh():
            print("[ENGINE] Scanner-cache utgången — kör ny scan...")
            return self.scanner.run_scan()
        return None

    def run_full_analysis(self):
        """
        Kör full analys av alla marknader.
        Signal flow: Teknisk 45% + Makro 55% → Valuation Override.
        """
        print(f"[ENGINE] Startar full analys {datetime.now().strftime('%H:%M:%S')}...")

        # 1. Fetch macro data
        macro_data = self.data.fetch_macro_data()
        fx_rates = self.data.fetch_fx_rates()

        # 2. Run macro analysis
        macro_result = self.macro.analyze(macro_data, fx_rates)
        self.last_macro = {
            "data": macro_data,
            "analysis": macro_result,
            "fx": fx_rates,
        }

        # 2b. Leading indicators + Gold + Turkey (EN GÅNG per cykel)
        leading_analysis = None
        gold_valuation = None
        turkey_valuation = None
        val_enabled = VALUATION_CONFIG.get("enabled", True)

        if val_enabled:
            try:
                etf_data = self.data.fetch_leading_indicator_etfs()
                leading_analysis = self.valuation.fund_fetcher.build_leading_signals(
                    etf_data, macro_data
                )
                print(f"[ENGINE] Leading indicators: {leading_analysis.get('direction', '?')} (score: {leading_analysis.get('score', 0)})")
            except Exception as e:
                print(f"[ENGINE] Leading signals fel: {e}")

            try:
                gold_hist = self.data.fetch_gold_history()
                gold_valuation = self.valuation.analyze_gold(macro_data, gold_hist)
                print(f"[ENGINE] Guld: {gold_valuation.get('verdict', '?')} (score: {gold_valuation.get('score', 0)})")
            except Exception as e:
                print(f"[ENGINE] Guld valuation fel: {e}")

            try:
                turkey_valuation = self.valuation.analyze_turkey(macro_data, fx_rates)
                print(f"[ENGINE] Turkiet: {turkey_valuation.get('decision', '?')} (score: {turkey_valuation.get('score', 0)})")
            except Exception as e:
                print(f"[ENGINE] Turkiet valuation fel: {e}")

        self.last_valuation = {
            "gold": gold_valuation,
            "turkey": turkey_valuation,
            "leading": leading_analysis,
            "timestamp": datetime.now().isoformat(),
        }

        # 2c. Edge signals (trav-modellen) — for SE stocks only
        edge_signals = {}
        edge_enabled = EDGE_SIGNAL_CONFIG.get("enabled", False) and self.edge_fetcher and self.edge_analyzer

        if edge_enabled:
            try:
                universe_preview = self.get_active_universe()
                se_stocks = universe_preview.get("SE", {}).get("stocks", {})
                se_tickers = list(se_stocks.keys())
                if se_tickers:
                    edge_raw = self.edge_fetcher.get_all_edge_data(se_tickers)
                    edge_signals = self.edge_analyzer.analyze_all(se_tickers, edge_raw)

                    # Mata in aggregerad insider-data i leading signals
                    if val_enabled:
                        try:
                            agg_insider = self.edge_fetcher.get_aggregate_insider_stats()
                            if agg_insider.get("buy_sell_ratio", 1.0) != 1.0:
                                etf_data = self.data.fetch_leading_indicator_etfs()
                                leading_analysis = self.valuation.fund_fetcher.build_leading_signals(
                                    etf_data, macro_data, insider_data=agg_insider
                                )
                                self.last_valuation["leading"] = leading_analysis
                        except Exception:
                            pass

                    n_edge = sum(1 for e in edge_signals.values()
                                 if e.signal_type.value != "NO_EDGE")
                    print(f"[ENGINE] Edge signals: {n_edge} aktier med edge (av {len(se_tickers)})")
            except Exception as e:
                print(f"[ENGINE] Edge signal fel: {e}")

        self.last_edge_signals = edge_signals

        # 3. Analyze each stock
        universe = self.get_active_universe()
        all_signals = []
        stock_analyses = {}

        tech_weight = SIGNAL_WEIGHTS.get("technical", 0.45)
        macro_weight = SIGNAL_WEIGHTS.get("macro", 0.55)
        val_blend = VALUATION_CONFIG.get("score_blend", 0.6)
        val_min_confidence = VALUATION_CONFIG.get("min_confidence_override", 0.4)
        val_action_map = VALUATION_CONFIG.get("action_score_map", {})
        multi_strat_enabled = MULTI_STRATEGY_CONFIG.get("enabled", False)

        # Dynamiska trösklar baserat på regim
        regime_str = macro_result.get("regime", "NEUTRAL")
        buy_threshold, sell_threshold = self._get_dynamic_thresholds(regime_str)
        strong_buy_threshold = buy_threshold * 2
        strong_sell_threshold = sell_threshold * 2

        # TR hard cap check: räkna nuvarande TR-signaler för att blockera vid gränsen
        tr_signal_count = 0
        tr_hard_cap = RISK_MANAGEMENT.get("turkey_hard_cap", 0.20)

        for market, market_data in universe.items():
            all_tickers = {**market_data["stocks"], **market_data.get("etfs", {})}

            for ticker, name in all_tickers.items():
                try:
                    hist = self.data.fetch_stock_data(ticker, period="6mo")
                    if hist is None or len(hist) < 50:
                        continue

                    ta_result = self.ta.analyze(hist)
                    if ta_result is None:
                        continue

                    # ── QUALITY GATE (dubbel-check) ───────────────
                    passes_quality, quality_reason = self._quality_gate(ticker, market, ta_result)
                    if not passes_quality:
                        self.blocking_log.log_block(
                            ticker, name, market, "N/A", 0, quality_reason, "quality_filter"
                        )
                        continue

                    # ── MULTI-STRATEGI eller original technical_score ──
                    strategy_scores = None
                    if multi_strat_enabled:
                        # Hämta sentiment-score (0 om ej tillgänglig)
                        sent_score = self.sentiment.get_sentiment_score(ticker)

                        # Hämta fundamental data för earnings-strategi
                        fund_data_for_strat = None
                        try:
                            fund_data_for_strat = self.data.fetch_fundamental_data(ticker)
                        except Exception:
                            pass

                        multi_result = self.strategy_manager.compute_multi_strategy_score(
                            ta_result, fund_data_for_strat, sent_score
                        )

                        if multi_result:
                            tech_score = multi_result["combined_score"]
                            strategy_scores = multi_result["strategy_scores"]
                        else:
                            tech_score = ta_result["technical_score"]
                    else:
                        tech_score = ta_result["technical_score"]

                    # Macro components
                    macro_bias = macro_result["market_bias"].get(market, 0)
                    risk_appetite = macro_result["risk_appetite"]

                    # Combined macro score [-1, 1]
                    macro_combined = macro_bias * 0.6 + risk_appetite * 0.4
                    macro_combined = max(-1, min(1, macro_combined))

                    # Combined score: teknik * tech_weight + makro * macro_weight
                    combined_score = (
                        tech_score * tech_weight +
                        macro_combined * macro_weight
                    )
                    combined_score = max(-1, min(1, combined_score))

                    # ── VALUATION OVERLAY ──────────────────────────
                    valuation_decision = None
                    position_size_modifier = 1.0
                    val_reasoning = None

                    if val_enabled and abs(combined_score) >= buy_threshold:
                        try:
                            fund_data = self.data.fetch_fundamental_data(ticker)
                            valuation_decision = self.valuation.analyze_equity(
                                ticker=ticker,
                                market=market,
                                fund_data=fund_data,
                                technical_score=tech_score,
                                macro_result=macro_result,
                                leading_data=leading_analysis,
                            )

                            if valuation_decision and valuation_decision.confidence >= val_min_confidence:
                                val_score_adj = val_action_map.get(valuation_decision.action, 0)
                                combined_score = combined_score * (1 - val_blend) + val_score_adj * val_blend
                                combined_score = max(-1, min(1, combined_score))
                                position_size_modifier = valuation_decision.position_size_modifier
                                val_reasoning = valuation_decision.reasoning
                        except Exception as e:
                            pass  # Fallback: behåll original score

                    # ── EDGE SIGNAL OVERLAY (trav-modellen, SE only) ──
                    edge_signal = None
                    if edge_enabled and market == "SE" and ticker in edge_signals:
                        edge_sig = edge_signals[ticker]
                        edge_min_conf = EDGE_SIGNAL_CONFIG.get("min_confidence", 0.3)
                        if edge_sig.confidence >= edge_min_conf:
                            edge_blend = EDGE_SIGNAL_CONFIG.get("score_blend", 0.25)
                            edge_adj = EDGE_SIGNAL_CONFIG.get("signal_score_adjustments", {}).get(
                                edge_sig.signal_type.value, 0
                            )
                            combined_score = combined_score * (1 - edge_blend) + (combined_score + edge_adj) * edge_blend
                            combined_score = max(-1, min(1, combined_score))
                            edge_signal = edge_sig

                    # Determine signal (med dynamiska trösklar)
                    if combined_score >= strong_buy_threshold:
                        signal = "STARK KÖP"
                        signal_color = "#00e676"
                    elif combined_score >= buy_threshold:
                        signal = "KÖP"
                        signal_color = "#69f0ae"
                    elif combined_score <= strong_sell_threshold:
                        signal = "STARK SÄLJ"
                        signal_color = "#ff1744"
                    elif combined_score <= sell_threshold:
                        signal = "SÄLJ"
                        signal_color = "#ff5252"
                    else:
                        signal = "HÅLL"
                        signal_color = "#ffd740"

                    # ── TR HARD CAP ENFORCEMENT vid signalnivå ─────
                    if market == "TR" and signal in ["KÖP", "STARK KÖP"]:
                        tr_signal_count += 1
                        # Räkna nuvarande TR-positioner i alla portföljer
                        tr_pos_count = sum(
                            1 for p in self.pm.portfolios.values()
                            for t, pos in p.positions.items()
                            if pos.get("market") == "TR"
                        )
                        # Blockera om vi redan har 6+ TR-positioner eller fler TR-signaler
                        if (tr_pos_count + tr_signal_count) * 0.03 > tr_hard_cap:
                            self.blocking_log.log_block(
                                ticker, name, market, signal, combined_score,
                                f"TR hard cap ({tr_hard_cap*100:.0f}%) nådd — {tr_pos_count} positioner",
                                "turkey_hardcap"
                            )
                            signal = "HÅLL"
                            signal_color = "#ffd740"
                            combined_score = 0.0
                            val_reasoning = f"TR hard cap ({tr_hard_cap*100:.0f}%) blockerad"

                    # ── Logga "nära"-block (score > 70% av threshold men under) ──
                    if signal == "HÅLL" and combined_score > 0 and combined_score > buy_threshold * 0.7:
                        self.blocking_log.log_block(
                            ticker, name, market, "NÄRA KÖP", combined_score,
                            f"Score {combined_score:.3f} under tröskel {buy_threshold:.2f}",
                            "score_threshold"
                        )

                    analysis = {
                        "ticker": ticker,
                        "name": name,
                        "market": market,
                        "currency": market_data["currency"],
                        "price": ta_result["price"],
                        "technical_score": round(tech_score, 3),
                        "macro_bias": macro_bias,
                        "macro_combined": round(macro_combined, 3),
                        "combined_score": round(combined_score, 3),
                        "signal": signal,
                        "signal_color": signal_color,
                        "rsi": ta_result.get("rsi", 50),
                        "macd_hist": ta_result.get("macd_hist", 0),
                        "momentum_20d": ta_result.get("momentum_20d", 0),
                        "volume_ratio": ta_result.get("volume_ratio", 1),
                        "atr_pct": ta_result.get("atr_pct", 0),
                        "sma20": ta_result.get("sma20"),
                        "sma50": ta_result.get("sma50"),
                        "sma200": ta_result.get("sma200"),
                        "ema200": ta_result.get("ema200"),
                        "above_ema200": ta_result.get("above_ema200", True),
                        "pct_above_ema200": ta_result.get("pct_above_ema200"),
                        "bb_upper": ta_result.get("bb_upper"),
                        "bb_lower": ta_result.get("bb_lower"),
                        "atr_stop_price": ta_result.get("atr_stop_price"),
                        "avg_daily_volume": ta_result.get("avg_daily_volume", 0),
                        "adx": ta_result.get("adx", 0),
                        "52w_high": ta_result.get("52w_high", 0),
                        "52w_low": ta_result.get("52w_low", 0),
                        # Multi-strategi scores
                        "strategy_scores": strategy_scores,
                        # Valuation fields
                        "valuation_action": valuation_decision.action if valuation_decision else None,
                        "valuation_confidence": round(valuation_decision.confidence, 2) if valuation_decision else None,
                        "valuation_reasoning": val_reasoning,
                        "valuation_regime": valuation_decision.regime_says if valuation_decision else None,
                        "valuation_says": valuation_decision.valuation_says if valuation_decision else None,
                        "valuation_leading": valuation_decision.leading_says if valuation_decision else None,
                        "position_size_modifier": round(position_size_modifier, 2),
                        "pe_ratio": getattr(valuation_decision, '_fund_data', {}).get("pe_ratio") if valuation_decision else None,
                        "pb_ratio": getattr(valuation_decision, '_fund_data', {}).get("pb_ratio") if valuation_decision else None,
                        "fcf_yield": getattr(valuation_decision, '_fund_data', {}).get("fcf_yield") if valuation_decision else None,
                        # Edge signal fields (trav-modellen)
                        "edge_score": edge_signal.edge_score if edge_signal else None,
                        "edge_signal_type": edge_signal.signal_type.value if edge_signal else None,
                        "edge_confidence": round(edge_signal.confidence, 2) if edge_signal else None,
                        "edge_reasoning": edge_signal.reasoning if edge_signal else None,
                        "edge_insider_score": edge_signal.insider_score if edge_signal else None,
                        "edge_short_score": edge_signal.short_score if edge_signal else None,
                        "edge_retail_score": edge_signal.retail_score if edge_signal else None,
                        "edge_divergence": edge_signal.divergence_magnitude if edge_signal else None,
                        "short_pct": edge_signal.short_pct if edge_signal else None,
                        "avanza_owners": edge_signal.owner_count if edge_signal else None,
                    }

                    stock_analyses[ticker] = analysis

                    if signal in ["KÖP", "STARK KÖP", "SÄLJ", "STARK SÄLJ"]:
                        reasons = []
                        if ta_result.get("rsi", 50) < 30:
                            reasons.append(f"RSI oversold ({ta_result.get('rsi', 50):.0f})")
                        elif ta_result.get("rsi", 50) > 70:
                            reasons.append(f"RSI overbought ({ta_result.get('rsi', 50):.0f})")
                        else:
                            reasons.append(f"RSI {ta_result.get('rsi', 50):.0f}")

                        macd_s = ta_result.get("macd_score", 0)
                        if macd_s > 0.3:
                            reasons.append(f"MACD bullish ({macd_s:.2f})")
                        elif macd_s < -0.3:
                            reasons.append(f"MACD bearish ({macd_s:.2f})")

                        if ta_result.get("above_ema200"):
                            pct_ema = ta_result.get("pct_above_ema200", 0)
                            reasons.append(f"Ovanför EMA200 (+{pct_ema:.1f}%)" if pct_ema else "Ovanför EMA200")
                        else:
                            reasons.append("Under EMA200")

                        vol_r = ta_result.get("volume_ratio", 1)
                        if vol_r >= 2.0:
                            reasons.append(f"Hög volym ({vol_r:.1f}x)")

                        if macro_bias > 0.1:
                            reasons.append(f"Positiv makro ({market})")
                        elif macro_bias < -0.1:
                            reasons.append(f"Negativ makro ({market})")

                        # Strategy scores i reason-text
                        if strategy_scores:
                            top_strats = sorted(strategy_scores.items(), key=lambda x: abs(x[1]), reverse=True)[:2]
                            for sname, sscore in top_strats:
                                if abs(sscore) > 0.1:
                                    direction = "+" if sscore > 0 else ""
                                    reasons.append(f"{sname}: {direction}{sscore:.2f}")

                        # Valuation reason
                        if val_reasoning:
                            reasons.append(val_reasoning)
                        elif valuation_decision:
                            v_says = valuation_decision.valuation_says
                            if v_says and v_says != "FAIR_VALUE":
                                reasons.append(f"Värdering: {v_says}")

                        # Edge signal reason (trav-modellen)
                        if edge_signal and edge_signal.signal_type.value != "NO_EDGE":
                            edge_label = {
                                "SMART_MONEY_BUY": "Smart money köper",
                                "SMART_MONEY_SELL": "Smart money säljer",
                                "RETAIL_FOMO": "Retail FOMO",
                                "CONSENSUS_BULLISH": "Alla hausse",
                                "CONSENSUS_BEARISH": "Alla baisse",
                            }.get(edge_signal.signal_type.value, "")
                            if edge_label:
                                reasons.append(f"Edge: {edge_label} ({edge_signal.edge_score:+.2f})")

                        all_signals.append({
                            "ticker": ticker,
                            "name": name,
                            "market": market,
                            "currency": market_data["currency"],
                            "signal": signal,
                            "signal_color": signal_color,
                            "combined_score": round(combined_score, 3),
                            "price": ta_result["price"],
                            "reasons": reasons,
                            "reason_text": " | ".join(reasons) if reasons else signal,
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                            "above_ema200": ta_result.get("above_ema200", True),
                            "valuation_action": valuation_decision.action if valuation_decision else None,
                            "position_size_modifier": round(position_size_modifier, 2),
                            "strategy_scores": strategy_scores,
                        })

                except Exception as e:
                    print(f"[ENGINE] Fel vid analys av {ticker}: {e}")
                    continue

        all_signals.sort(key=lambda x: abs(x["combined_score"]), reverse=True)

        self.last_signals = all_signals
        self.last_analysis = stock_analyses
        self.last_run = datetime.now().isoformat()

        # Uppdatera watchlist
        self.watchlist.update_from_signals(all_signals, stock_analyses)

        # Stats
        val_count = sum(1 for s in all_signals if s.get("valuation_action"))
        edge_count = sum(1 for s in edge_signals.values()
                         if s.signal_type.value != "NO_EDGE") if edge_signals else 0
        print(f"[ENGINE] Analys klar. {len(all_signals)} signaler ({val_count} med valuation, {edge_count} med edge).")
        return all_signals

    def execute_signals(self):
        """
        Exekvera signaler på alla portföljer.
        Ny logik:
        0. Kolla drawdown-paus (stoppa vid -12%)
        1. Trailing stop-loss
        2. Hård stop-loss / take-profit
        3. Sälj-signaler
        4. Watchlist-köp med EMA200-filter + Turkiet-regler
        5. Drawdown/förlustgräns-uppdatering
        6. Allokeringskontroll
        """
        if not self.last_signals and not any(p.positions for p in self.pm.portfolios.values()):
            return {"executed": [], "skipped": [], "market_status": get_all_market_status()}

        open_markets = get_open_markets()
        market_status = get_all_market_status()

        if not open_markets:
            print("[ENGINE] ⏸ Ingen börs öppen just nu — inga trades exekveras")
            skipped_all = [
                {
                    "ticker": s["ticker"],
                    "reason": f"Börsen {s['market']} är stängd ({market_status[s['market']]['reason']})"
                }
                for s in self.last_signals
                if s["signal"] in ["KÖP", "STARK KÖP", "SÄLJ", "STARK SÄLJ"]
            ]
            return {"executed": [], "skipped": skipped_all, "market_status": market_status}

        print(f"[ENGINE] Öppna marknader: {', '.join(open_markets)}")

        fx_rates = self.data.fetch_fx_rates()
        executed = []
        skipped = []
        allocation_status = {}

        for pid, portfolio in self.pm.portfolios.items():
            # ── 0. DRAWDOWN CHECK ───────────────────────────────
            current_prices = {}
            all_tickers = (
                list(portfolio.positions.keys()) +
                [s["ticker"] for s in self.last_signals] +
                list(self.watchlist.entries.keys())
            )
            for ticker in set(all_tickers):
                price = self.data.fetch_current_price(ticker)
                if price:
                    current_prices[ticker] = price

            total_value = portfolio.get_total_value(current_prices, fx_rates)
            dd_status = self.drawdown.update(pid, total_value, portfolio.initial_cash)

            if dd_status["trading_paused"]:
                print(f"[ENGINE] ⛔ Portfölj {pid} PAUSAD: {dd_status['pause_reason']}")
                skipped.append({
                    "ticker": f"ALLA ({pid})",
                    "reason": f"Trading pausad: {dd_status['pause_reason']}"
                })
                # Uppdatera dagligt värde ändå
                portfolio.update_daily_value(current_prices, fx_rates)
                continue

            # ── 1. TRAILING STOP-LOSS ───────────────────────────
            trailing_signals = self.trailing_stops.update_and_check(
                portfolio.positions, current_prices
            )
            for sig in trailing_signals:
                ticker = sig["ticker"]
                pos = portfolio.positions.get(ticker)
                if not pos:
                    continue

                pos_market = pos.get("market", "")
                if pos_market not in open_markets:
                    skipped.append({
                        "ticker": ticker,
                        "reason": f"Trailing stop väntar — {pos_market} stängd"
                    })
                    continue

                fx_rate = self._get_fx_rate(pos.get("currency", "SEK"), fx_rates)
                success, result = portfolio.sell(
                    ticker, price=sig["price"], fx_rate=fx_rate,
                    reason=sig["reason"]
                )
                if success:
                    self.trailing_stops.remove(ticker)
                    self.cooldown.record_trade(ticker, "SÄLJ")
                    executed.append({
                        "portfolio": pid,
                        "action": "SÄLJ (TRAILING STOP)",
                        "ticker": ticker,
                        "result": result,
                    })

            # ── 2. HÅRD STOP-LOSS / TAKE-PROFIT ─────────────────
            sl_tp_signals = portfolio.check_stop_loss_take_profit(current_prices)
            for sig in sl_tp_signals:
                ticker = sig["ticker"]
                pos = portfolio.positions.get(ticker)
                if not pos:
                    continue

                pos_market = pos.get("market", "")
                if pos_market not in open_markets:
                    skipped.append({
                        "ticker": ticker,
                        "reason": f"SL/TP väntar — {pos_market} stängd"
                    })
                    continue

                fx_rate = self._get_fx_rate(pos.get("currency", "SEK"), fx_rates)
                success, result = portfolio.sell(
                    ticker, price=sig["price"], fx_rate=fx_rate,
                    reason=sig["reason"]
                )
                if success:
                    self.trailing_stops.remove(ticker)
                    self.cooldown.record_trade(ticker, "SÄLJ")
                    executed.append({
                        "portfolio": pid,
                        "action": "SÄLJ (SL/TP)",
                        "ticker": ticker,
                        "result": result,
                    })

            # ── 3. SÄLJ-SIGNALER ────────────────────────────────
            sell_signals = [s for s in self.last_signals
                          if s["signal"] in ["SÄLJ", "STARK SÄLJ"]
                          and s["ticker"] in portfolio.positions]

            for sig in sell_signals:
                if sig["market"] not in open_markets:
                    skipped.append({"ticker": sig["ticker"], "reason": f"Börsen {sig['market']} stängd"})
                    continue

                can, reason = self.cooldown.can_trade(sig["ticker"], "SÄLJ")
                if not can:
                    skipped.append({"ticker": sig["ticker"], "reason": reason})
                    continue

                ticker = sig["ticker"]
                pos = portfolio.positions.get(ticker)
                if pos:
                    price = current_prices.get(ticker, sig["price"])
                    fx_rate = self._get_fx_rate(sig["currency"], fx_rates)
                    success, result = portfolio.sell(
                        ticker, price=price, fx_rate=fx_rate,
                        reason=sig["reason_text"]
                    )
                    if success:
                        self.trailing_stops.remove(ticker)
                        self.cooldown.record_trade(ticker, "SÄLJ")
                        executed.append({
                            "portfolio": pid,
                            "action": sig["signal"],
                            "ticker": ticker,
                            "result": result,
                        })

            # ── 4. KÖP VIA WATCHLIST (med nya filter) ───────────
            ready_to_buy = self.watchlist.check_targets(current_prices)

            for sig in ready_to_buy:
                ticker = sig["ticker"]
                market = sig["market"]

                # Redan i portföljen?
                if ticker in portfolio.positions:
                    continue

                # Marknaden öppen?
                if market not in open_markets:
                    skipped.append({"ticker": ticker, "reason": f"Börsen {market} stängd"})
                    continue

                # Cooldown
                can, reason = self.cooldown.can_trade(ticker, "KÖP")
                if not can:
                    skipped.append({"ticker": ticker, "reason": reason})
                    self.blocking_log.log_block(
                        ticker, sig.get("name", ticker), market, "KÖP",
                        sig.get("combined_score", 0), reason, "cooldown"
                    )
                    continue

                # ★ NYTT: EMA200-filter — köp BARA ovanför EMA200
                if not sig.get("above_ema200", True):
                    skipped.append({"ticker": ticker, "reason": "Under EMA200 — avvaktar"})
                    self.blocking_log.log_block(
                        ticker, sig.get("name", ticker), market, "KÖP",
                        sig.get("combined_score", 0), "Under EMA200 — avvaktar", "ema200_below"
                    )
                    continue

                # ★ NYTT: Likviditetskrav (100k snittvolym)
                avg_vol = sig.get("avg_daily_volume", 0) or 0
                min_vol = RISK_MANAGEMENT.get("min_avg_volume", 100_000)
                if avg_vol > 0 and avg_vol < min_vol:
                    skipped.append({"ticker": ticker, "reason": f"Låg likviditet ({avg_vol:.0f} < {min_vol})"})
                    self.blocking_log.log_block(
                        ticker, sig.get("name", ticker), market, "KÖP",
                        sig.get("combined_score", 0), f"Låg likviditet ({avg_vol:.0f} < {min_vol})", "low_liquidity"
                    )
                    continue

                # Cash-reserv check
                total_value = portfolio.get_total_value(current_prices, fx_rates)
                min_cash = total_value * RISK_PARAMS["min_cash_reserve_pct"]
                if portfolio.cash <= min_cash:
                    skipped.append({"ticker": ticker, "reason": f"Cash-reserv ({RISK_PARAMS['min_cash_reserve_pct']*100:.0f}%) skyddat"})
                    self.blocking_log.log_block(
                        ticker, sig.get("name", ticker), market, "KÖP",
                        sig.get("combined_score", 0), f"Cash-reserv skyddat", "cash_reserve"
                    )
                    continue

                # ★ NYTT: Turkiet-specifika regler
                if market == "TR":
                    # Max 3% per turkisk aktie
                    max_position_pct = RISK_PARAMS["turkey_position_max"]

                    # Turkiet hard cap check
                    tr_value = sum(
                        current_prices.get(t, p["avg_price"]) * p["shares"] *
                        self._get_fx_rate(p.get("currency", "SEK"), fx_rates)
                        for t, p in portfolio.positions.items()
                        if p.get("market") == "TR"
                    )
                    if total_value > 0 and (tr_value / total_value) >= RISK_PARAMS["turkey_hard_cap"]:
                        skipped.append({"ticker": ticker, "reason": f"Turkiet hard cap ({RISK_PARAMS['turkey_hard_cap']*100:.0f}%) nådd"})
                        self.blocking_log.log_block(
                            ticker, sig.get("name", ticker), market, "KÖP",
                            sig.get("combined_score", 0),
                            f"Turkiet hard cap ({RISK_PARAMS['turkey_hard_cap']*100:.0f}%) nådd",
                            "turkey_hardcap"
                        )
                        continue

                    # Preferred tickers (exportfokus)
                    if TURKEY_CONFIG.get("buy_conditions", {}).get("require_export_focus", False):
                        if ticker not in TURKEY_CONFIG.get("preferred_tickers", []):
                            skipped.append({"ticker": ticker, "reason": "Ej i Turkiet preferred list (exportfokus)"})
                            continue
                else:
                    max_position_pct = portfolio.max_position_pct

                # Beräkna positionsstorlek (Kelly-inspirerad)
                price = sig["current_price"]
                if price is None or price <= 0:
                    continue

                # Kelly position size
                closed_trades = [t for t in portfolio.trade_history if t["action"] == "SÄLJ"]
                if len(closed_trades) >= 5:
                    wins = [t for t in closed_trades if t.get("pnl_pct", 0) > 0]
                    win_rate = len(wins) / len(closed_trades) if closed_trades else 0.55
                    avg_win = sum(t.get("pnl_pct", 0) / 100 for t in wins) / len(wins) if wins else 0.08
                    losses = [t for t in closed_trades if t.get("pnl_pct", 0) <= 0]
                    avg_loss = abs(sum(t.get("pnl_pct", 0) / 100 for t in losses) / len(losses)) if losses else 0.05
                else:
                    win_rate, avg_win, avg_loss = 0.55, 0.08, 0.05

                kelly_size = calculate_kelly_position_size(
                    total_value, win_rate, avg_win, avg_loss,
                    RISK_PARAMS["max_risk_per_trade"],
                    RISK_PARAMS["kelly_fraction"],
                )

                # Valuation position size modifier
                stock_analysis = self.last_analysis.get(ticker, {})
                size_mod = stock_analysis.get("position_size_modifier", 1.0)
                size_mod = max(
                    VALUATION_CONFIG.get("min_size_modifier", 0.0),
                    min(VALUATION_CONFIG.get("max_size_modifier", 1.5), size_mod)
                )
                kelly_size *= size_mod

                # Begränsa till max position %
                max_position_value = total_value * min(max_position_pct, portfolio.max_position_pct)
                position_value_sek = min(kelly_size, max_position_value)

                # Begränsa till tillgänglig cash minus reserv
                available_cash = portfolio.cash - min_cash
                position_value_sek = min(position_value_sek, available_cash * 0.5)

                if position_value_sek < 5000:
                    skipped.append({"ticker": ticker, "reason": "Otillräckligt kapital"})
                    continue

                # Marknadsallokering
                targets = ALLOCATION_BY_SIZE.get(pid, ALLOCATION_BY_SIZE.get("medium", {}))
                target_alloc = targets.get(market, MARKET_ALLOCATION.get(market, 0.33))
                max_alloc = ALLOCATION_RULES.get(market, {}).get("max", target_alloc * 1.5)

                current_market_value = sum(
                    current_prices.get(t, p["avg_price"]) * p["shares"] *
                    self._get_fx_rate(p.get("currency", "SEK"), fx_rates)
                    for t, p in portfolio.positions.items()
                    if p.get("market") == market
                )
                current_market_pct = current_market_value / total_value if total_value > 0 else 0

                if current_market_pct >= max_alloc:
                    skipped.append({
                        "ticker": ticker,
                        "reason": f"Marknadsallokering {market} full ({current_market_pct*100:.0f}% >= max {max_alloc*100:.0f}%)"
                    })
                    self.blocking_log.log_block(
                        ticker, sig.get("name", ticker), market, "KÖP",
                        sig.get("combined_score", 0),
                        f"Allokering {market} full ({current_market_pct*100:.0f}% >= max {max_alloc*100:.0f}%)",
                        "allocation_full"
                    )
                    continue

                # Beräkna antal aktier
                fx_rate = self._get_fx_rate(sig["currency"], fx_rates)
                position_value_local = position_value_sek / fx_rate if fx_rate > 0 else position_value_sek
                shares = int(position_value_local / price)

                if shares < 1:
                    skipped.append({"ticker": ticker, "reason": "Priset för högt"})
                    continue

                success, result = portfolio.buy(
                    ticker, shares, price, sig["currency"],
                    market, sig["name"], fx_rate,
                    reason=f"Watchlist target nådd: {sig.get('reason_text', sig['signal'])}"
                )
                if success:
                    self.cooldown.record_trade(ticker, "KÖP")
                    self.watchlist.remove(ticker)
                    executed.append({
                        "portfolio": pid,
                        "action": f"KÖP (WATCHLIST)",
                        "ticker": ticker,
                        "result": result,
                    })
                else:
                    skipped.append({"ticker": ticker, "reason": result})

            # ── 5. UPPDATERA DAGLIGT VÄRDE + DRAWDOWN ───────────
            portfolio.update_daily_value(current_prices, fx_rates)

            # Allokering check
            alloc = AllocationChecker.check_allocation(
                pid, portfolio.positions, current_prices, fx_rates, total_value
            )
            allocation_status[pid] = alloc

            if alloc["needs_rebalance"]:
                for action in alloc.get("rebalance_actions", []):
                    print(f"[ENGINE] ⚖ {pid}: {action}")

        result = {
            "executed": executed,
            "skipped": skipped,
            "market_status": market_status,
            "allocation": allocation_status,
        }
        self.last_execution_result = result
        return result

    def _get_fx_rate(self, currency, fx_rates):
        """Hämta FX-kurs till SEK."""
        if currency == "USD":
            return fx_rates.get("USDSEK", 10.5)
        elif currency == "TRY":
            usdtry = fx_rates.get("USDTRY", 36.0)
            usdsek = fx_rates.get("USDSEK", 10.5)
            return usdsek / usdtry
        return 1.0

    def _get_dynamic_thresholds(self, regime_str):
        """
        Returnera (buy_threshold, sell_threshold) baserat på nuvarande regim.
        Om inga trades i 3 dagar under RISK_ON/NEUTRAL → sänk buy med 10%.
        """
        if not DYNAMIC_THRESHOLDS.get("enabled", False):
            return SIGNAL_THRESHOLDS["buy"], SIGNAL_THRESHOLDS["sell"]

        # Mappa regime-sträng till vår nyckel
        mapping = DYNAMIC_THRESHOLDS.get("regime_mapping", {})
        regime_key = mapping.get(regime_str, "NEUTRAL")

        regimes = DYNAMIC_THRESHOLDS.get("regimes", {})
        thresholds = regimes.get(regime_key, regimes.get("NEUTRAL", {"buy": 0.35, "sell": -0.20}))

        buy_threshold = thresholds["buy"]
        sell_threshold = thresholds["sell"]

        # Auto-lower: om inga trades i N dagar, sänk buy-tröskel
        auto_lower_regimes = DYNAMIC_THRESHOLDS.get("auto_lower_regimes", ["RISK_ON", "NEUTRAL"])
        if regime_key in auto_lower_regimes:
            no_trades_days = DYNAMIC_THRESHOLDS.get("auto_lower_no_trades_days", 3)
            auto_lower_pct = DYNAMIC_THRESHOLDS.get("auto_lower_pct", 0.10)

            # Kolla senaste trades från alla portföljer
            recent_trades = []
            for pid, portfolio in self.pm.portfolios.items():
                for trade in portfolio.trade_history[-20:]:
                    recent_trades.append(trade)

            if recent_trades:
                latest = max(
                    (datetime.fromisoformat(t["timestamp"]) for t in recent_trades if "timestamp" in t),
                    default=None
                )
                if latest:
                    days_since = (datetime.now() - latest).days
                    if days_since >= no_trades_days:
                        buy_threshold *= (1 - auto_lower_pct)

        return buy_threshold, sell_threshold

    def _quality_gate(self, ticker, market, ta_result):
        """
        Kvalitetsgate: dubbelkolla att aktien passerar quality filters.
        Returnerar (passes, reason) — scanner-cache kan vara gammal.
        """
        quality = QUALITY_FILTERS.get(market, {})
        min_volume = quality.get("min_avg_volume", 0)
        avg_vol = ta_result.get("avg_daily_volume", 0) or 0

        # Volymcheck
        if min_volume > 0 and avg_vol > 0 and avg_vol < min_volume:
            return False, f"Volym {avg_vol:.0f} < {min_volume} (kvalitetsfilter)"

        # Turkey hard filter
        if market == "TR" and TURKEY_CONFIG.get("hard_filter", False):
            approved = set(TURKEY_CONFIG.get("preferred_tickers", []))
            if ticker not in approved:
                return False, f"Ej i BIST 30-listan (Turkey hard filter)"

        return True, "OK"

    def get_dashboard_data(self):
        """Samla all data för dashboard."""
        fx_rates = self.data.fetch_fx_rates()

        current_prices = {}
        for pid, portfolio in self.pm.portfolios.items():
            for ticker in portfolio.positions:
                if ticker not in current_prices:
                    price = self.data.fetch_current_price(ticker)
                    if price:
                        current_prices[ticker] = price

        for ticker in self.last_analysis:
            if ticker not in current_prices:
                current_prices[ticker] = self.last_analysis[ticker].get("price", 0)

        # Trailing stop status
        trailing_status = {}
        for pid, portfolio in self.pm.portfolios.items():
            trailing_status[pid] = self.trailing_stops.get_status(
                portfolio.positions, current_prices
            )

        # Allocation status
        allocation_status = {}
        for pid, portfolio in self.pm.portfolios.items():
            total_value = portfolio.get_total_value(current_prices, fx_rates)
            allocation_status[pid] = AllocationChecker.check_allocation(
                pid, portfolio.positions, current_prices, fx_rates, total_value
            )

        # Dynamic thresholds
        regime_str = self.last_macro.get("analysis", {}).get("regime", "NEUTRAL")
        dyn_buy, dyn_sell = self._get_dynamic_thresholds(regime_str)

        return {
            "timestamp": datetime.now().isoformat(),
            "last_analysis_run": self.last_run,
            "portfolios": self.pm.get_all_summaries(current_prices, fx_rates),
            "positions": {
                pid: p.get_positions_detail(current_prices, fx_rates)
                for pid, p in self.pm.portfolios.items()
            },
            "signals": self.last_signals[:50],
            "stock_analyses": self.last_analysis,
            "macro": self.last_macro,
            "fx_rates": fx_rates,
            "market_status": get_all_market_status(),
            # Risk data
            "watchlist": self.watchlist.get_summary(),
            "trailing_stops": trailing_status,
            "cooldown": self.cooldown.get_status(),
            "drawdown": self.drawdown.get_status(),
            "allocation": allocation_status,
            "risk_params": {
                "min_cash_reserve": f"{RISK_PARAMS['min_cash_reserve_pct']*100:.0f}%",
                "max_trades_per_day": RISK_PARAMS["max_trades_per_day"],
                "cooldown_hours": RISK_PARAMS["cooldown_hours"],
                "trailing_activation": f"+{RISK_PARAMS['trailing_stop_activation_pct']*100:.0f}%",
                "trailing_distance": f"{RISK_PARAMS['trailing_stop_distance_pct']*100:.0f}%",
                "max_loss_per_day": f"-{RISK_PARAMS['max_loss_per_day']*100:.0f}%",
                "max_loss_per_week": f"-{RISK_PARAMS['max_loss_per_week']*100:.0f}%",
                "max_loss_per_month": f"-{RISK_PARAMS['max_loss_per_month']*100:.0f}%",
                "max_drawdown": f"-{RISK_PARAMS['max_drawdown_pause']*100:.0f}%",
                "turkey_max_per_stock": f"{RISK_PARAMS['turkey_position_max']*100:.0f}%",
                "turkey_hard_cap": f"{RISK_PARAMS['turkey_hard_cap']*100:.0f}%",
                "ema200_filter": "Aktiv — köp bara ovanför EMA200",
                "kelly_sizing": f"Half-Kelly (fraction: {RISK_PARAMS['kelly_fraction']})",
                "valuation_engine": "Aktiv" if VALUATION_CONFIG.get("enabled") else "Av",
                "multi_strategy": "Aktiv" if MULTI_STRATEGY_CONFIG.get("enabled") else "Av",
                "dynamic_thresholds": "Aktiv" if DYNAMIC_THRESHOLDS.get("enabled") else "Av",
                "sentiment": "Aktiv" if SENTIMENT_CONFIG.get("enabled") else "Av (PRAW ej konfigurerad)",
                "edge_signals": "Aktiv — Trav-modellen (Insider x Blankning x Avanza)" if EDGE_SIGNAL_CONFIG.get("enabled") else "Av",
            },
            # Dynamic thresholds
            "effective_thresholds": {
                "regime": regime_str,
                "buy": round(dyn_buy, 3),
                "sell": round(dyn_sell, 3),
                "strong_buy": round(dyn_buy * 2, 3),
                "strong_sell": round(dyn_sell * 2, 3),
            },
            # Blocking log
            "blocking_log": self.blocking_log.get_summary(),
            # Valuation data
            "valuation": self.last_valuation,
            "scanner": self.scanner.get_scan_summary(),
            # Sentiment
            "sentiment": self.sentiment.get_summary(),
        }
