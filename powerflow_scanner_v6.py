"""
PowerFlow Scanner v6 (Intraday Trigger Edition)
==============================================
미국 장중 30분 단위 실시간 스캐너 — Polygon.io + GitHub Actions

v5 대비 핵심 개선
------------------
✅ Premarket high / low 인식 (04:00~09:30 ET)
✅ Opening Range Breakout (ORB) 트리거 추가
✅ Session VWAP 및 VWAP reclaim 트리거 추가
✅ Relative Volume Since Open (누적 세션 RVOL) 추가
✅ Breakout 이후 첫 눌림(first pullback) 트리거 추가
✅ 부분 일봉(current day partial) 배제 → MTF 컨텍스트 왜곡 감소
✅ 구조적 트리거 + 활동성 + 과열 필터를 함께 사용하여 정확도 개선
"""
from __future__ import annotations

import asyncio
import html
import json
import logging
import math
import os
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import aiohttp
import numpy as np
import pandas as pd
import requests


# ═══════════════════════════════════════════════════════════
#  로거
# ═══════════════════════════════════════════════════════════
LOGGER = logging.getLogger("pf_v6")


def setup_logging(level: int = logging.INFO) -> None:
    if LOGGER.handlers:
        return
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


# ═══════════════════════════════════════════════════════════
#  시그널 모드
# ═══════════════════════════════════════════════════════════
MODE_META: Dict[str, Dict] = {
    "BOTH":         {"icon": "⚡", "desc": "반전+연속 복합", "priority": 1},
    "STRONG_REV":   {"icon": "🔄", "desc": "강한 매수 반전", "priority": 2},
    "STRONG_CONT":  {"icon": "🚀", "desc": "강한 추세 지속", "priority": 3},
    "REVERSAL":     {"icon": "↩️", "desc": "매수세 반전",   "priority": 4},
    "CONTINUATION": {"icon": "📈", "desc": "추세 지속",      "priority": 5},
    "SQUEEZE_POP":  {"icon": "💥", "desc": "압축 해제 폭발", "priority": 6},
}

MODE_SCORE_LABEL = {
    "BOTH":         "★★★",
    "STRONG_REV":   "★★☆",
    "STRONG_CONT":  "★★☆",
    "REVERSAL":     "★☆☆",
    "CONTINUATION": "★☆☆",
    "SQUEEZE_POP":  "◈◈◈",
}


# ═══════════════════════════════════════════════════════════
#  설정 Dataclasses
# ═══════════════════════════════════════════════════════════
@dataclass(frozen=True)
class DataConfig:
    # ── 30분봉 (주 시그널 타임프레임) ──
    intraday_timespan: str = "minute"
    intraday_multiplier: int = 30
    intraday_lookback_days: int = 30

    # ── 일봉 (MTF 트렌드 컨텍스트) ──
    daily_lookback_days: int = 220

    # ── 공통 ──
    adjusted: bool = True
    benchmark_symbol: str = "QQQ"
    market_symbol: str = "SPY"
    min_intraday_bars: int = 110
    min_daily_bars: int = 60

    # ── Polygon API ──
    polygon_base_url: str = "https://api.polygon.io"
    max_concurrent: int = 50
    request_timeout_sec: float = 30.0
    max_retries: int = 3
    retry_backoff_sec: float = 1.5


@dataclass(frozen=True)
class SignalConfig:
    # ── CLV ──
    clv_pre_window: int = 3
    clv_post_window: int = 2

    # ── REVERSAL ──
    strong_rev_pre_max: float = -0.20
    strong_rev_post_min: float = 0.38
    rev_pre_max: float = 0.05
    rev_post_min: float = 0.22

    # ── CONTINUATION ──
    cont_post_min: float = 0.30
    cont_clv_min: float = 0.25
    strong_cont_post_min: float = 0.48
    strong_cont_clv_min: float = 0.40

    # ── MF Z-score ──
    mf_window: int = 20
    mf_z_strong: float = 2.2
    mf_z_normal: float = 1.4
    mf_z_soft: float = 0.8

    # ── 절대 거래량 스파이크 ──
    vol_window: int = 20
    vol_strong: float = 2.0
    vol_normal: float = 1.45
    vol_soft: float = 1.10

    # ── 슬롯 RVOL (같은 시간대 단일 봉) ──
    slot_rvol_window: int = 10
    slot_rvol_strong: float = 2.0
    slot_rvol_normal: float = 1.4
    slot_rvol_soft: float = 1.1

    # ── Session RVOL (개장 이후 누적 거래량) ──
    session_rvol_window: int = 12
    session_rvol_strong: float = 2.0
    session_rvol_normal: float = 1.35
    session_rvol_soft: float = 1.10

    # ── 추세 ──
    ema_fast: int = 9
    ema_slow: int = 21
    ema_slope_bars: int = 2

    # ── 변동성 ──
    atr_window: int = 14
    bb_window: int = 20
    bb_std: float = 2.0
    kc_window: int = 20
    kc_atr_mult: float = 1.5
    nr7_window: int = 7
    atr_expand_th: float = 1.15

    # ── 돌파 레벨 (30m) ──
    intraday_hh_window: int = 13
    breakout_buf: float = 0.0
    box_window: int = 20

    # ── ORB / VWAP / Pullback ──
    opening_range_bars: int = 1           # 30m 1봉 = 첫 30분 OR
    vwap_reclaim_tolerance: float = 0.001
    pullback_lookback: int = 3            # 돌파 후 최대 몇 봉 내 첫 눌림 볼지
    pullback_touch_tolerance: float = 0.004
    max_vwap_extension: float = 0.035     # VWAP에서 3.5% 이상 이격 시 과열 판정
    max_breakout_extension: float = 0.04  # 돌파 레벨 위 4% 초과 시 추격 억제

    # ── 상대강도 ──
    rs_short: int = 5
    rs_long: int = 20

    # ── 캔들 품질 ──
    max_wick_ratio: float = 0.45
    min_body_ratio: float = 0.30
    min_close_high: float = 0.50

    # ── 유동성 ──
    dollar_vol_min_m: float = 5.0
    min_price: float = 5.0

    # ── 모드별 PASS_SCORE ──
    score_both: float = 9.0
    score_strong_rev: float = 7.8
    score_strong_cont: float = 7.4
    score_reversal: float = 6.8
    score_continuation: float = 6.4
    score_squeeze_pop: float = 6.0

    # ── 일봉 MTF ──
    daily_ema_fast: int = 20
    daily_ema_slow: int = 50
    require_daily_uptrend: bool = True
    require_structural_trigger: bool = True
    require_above_vwap_for_breakout: bool = True


@dataclass(frozen=True)
class ScanConfig:
    top_n: int = 20
    scan_last_n_bars: int = 2


@dataclass(frozen=True)
class ScannerConfig:
    data: DataConfig = field(default_factory=DataConfig)
    signal: SignalConfig = field(default_factory=SignalConfig)
    scan: ScanConfig = field(default_factory=ScanConfig)


# ═══════════════════════════════════════════════════════════
#  티커 / 섹터 매핑
# ═══════════════════════════════════════════════════════════
SECTOR_ETF_MAP: Dict[str, str] = {
    "TECH": "XLK", "SEMIS": "SMH", "SOFTWARE": "IGV",
    "COMM": "XLC", "CONSUMER": "XLY", "FINANCIALS": "XLF",
    "INDUSTRIALS": "XLI", "DEFENSE": "ITA",
    "HEALTHCARE": "XLV", "BIOTECH": "XBI", "ENERGY": "XLE",
}

DEFAULT_TICKER_SECTOR: Dict[str, str] = {
    "MSFT": "SOFTWARE", "AAPL": "TECH",   "META": "COMM",
    "AMZN": "CONSUMER", "GOOGL": "COMM",  "NVDA": "SEMIS",
    "AMD":  "SEMIS",    "AVGO": "SEMIS",  "ARM":  "SEMIS",
    "QCOM": "SEMIS",    "MU":   "SEMIS",  "MRVL": "SEMIS",
    "ASML": "SEMIS",    "AMAT": "SEMIS",  "LRCX": "SEMIS",
    "CRWD": "SOFTWARE", "PANW": "SOFTWARE","ZS":  "SOFTWARE",
    "DDOG": "SOFTWARE", "SNOW": "SOFTWARE","ORCL":"SOFTWARE",
    "ADBE": "SOFTWARE", "INTU": "SOFTWARE","NOW": "SOFTWARE",
    "CRM":  "SOFTWARE", "NFLX": "COMM",   "UBER":"TECH",
    "DASH": "TECH",     "ABNB": "TECH",   "SPOT":"COMM",
    "V":    "FINANCIALS","MA":  "FINANCIALS","PYPL":"FINANCIALS",
    "COIN": "FINANCIALS","SQ":  "FINANCIALS",
    "LMT":  "DEFENSE",  "NOC": "DEFENSE", "RTX": "DEFENSE",
    "AXON": "INDUSTRIALS","PLTR":"TECH",
    "ISRG": "HEALTHCARE","DXCM":"HEALTHCARE","TMO":"HEALTHCARE",
    "ILMN": "BIOTECH",  "UNH": "HEALTHCARE",
}

REQUIRED_COLS = ["Open", "High", "Low", "Close", "Volume"]
DEFAULT_TICKERS_FILE = Path("tickers.txt")
DEFAULT_RESULTS_DIR = Path("results")
SENT_LOG_FILE = Path("sent_signals.json")

SESSION_TZ = "America/New_York"
SESSION_OPEN = (9, 30)
SESSION_CLOSE = (16, 0)
PREMARKET_OPEN = (4, 0)
POSTMARKET_CLOSE = (20, 0)


# ═══════════════════════════════════════════════════════════
#  Polygon.io 비동기 클라이언트
# ═══════════════════════════════════════════════════════════
class PolygonClient:
    def __init__(self, api_key: str, cfg: DataConfig):
        if not api_key:
            raise ValueError("POLYGON_API_KEY 환경변수가 설정되어 있지 않습니다.")
        self.api_key = api_key
        self.cfg = cfg
        self.semaphore = asyncio.Semaphore(cfg.max_concurrent)
        self._call_count = 0

    @property
    def call_count(self) -> int:
        return self._call_count

    async def _fetch_aggs(
        self,
        session: aiohttp.ClientSession,
        ticker: str,
        timespan: str,
        multiplier: int,
        start_date: str,
        end_date: str,
    ) -> Optional[List[Dict]]:
        url = (
            f"{self.cfg.polygon_base_url}/v2/aggs/ticker/{ticker}"
            f"/range/{multiplier}/{timespan}/{start_date}/{end_date}"
        )
        params = {
            "adjusted": "true" if self.cfg.adjusted else "false",
            "sort": "asc",
            "limit": 50000,
            "apiKey": self.api_key,
        }

        for attempt in range(1, self.cfg.max_retries + 1):
            try:
                async with self.semaphore:
                    self._call_count += 1
                    async with session.get(
                        url,
                        params=params,
                        timeout=aiohttp.ClientTimeout(total=self.cfg.request_timeout_sec),
                    ) as resp:
                        if resp.status == 429 or resp.status >= 500:
                            text = (await resp.text())[:200]
                            LOGGER.warning(
                                "[%s %dm] HTTP %d 재시도 %d/%d: %s",
                                ticker, multiplier, resp.status, attempt,
                                self.cfg.max_retries, text,
                            )
                            raise aiohttp.ClientResponseError(
                                resp.request_info, resp.history, status=resp.status
                            )
                        if resp.status != 200:
                            text = (await resp.text())[:200]
                            LOGGER.warning("[%s %dm] HTTP %d: %s",
                                           ticker, multiplier, resp.status, text)
                            return None
                        payload = await resp.json()
                        return payload.get("results") or []
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt >= self.cfg.max_retries:
                    LOGGER.warning("[%s %dm] 최종 실패: %s", ticker, multiplier, exc)
                    return None
                await asyncio.sleep(self.cfg.retry_backoff_sec * (2 ** (attempt - 1)))
        return None

    async def fetch_intraday(
        self,
        session: aiohttp.ClientSession,
        ticker: str,
        start_date: str,
        end_date: str,
    ) -> Tuple[str, pd.DataFrame]:
        results = await self._fetch_aggs(
            session, ticker,
            self.cfg.intraday_timespan, self.cfg.intraday_multiplier,
            start_date, end_date,
        )
        return ticker, _polygon_to_df(results, intraday=True)

    async def fetch_daily(
        self,
        session: aiohttp.ClientSession,
        ticker: str,
        start_date: str,
        end_date: str,
    ) -> Tuple[str, pd.DataFrame]:
        results = await self._fetch_aggs(
            session, ticker, "day", 1, start_date, end_date,
        )
        return ticker, _polygon_to_df(results, intraday=False)

    async def fetch_all(
        self,
        tickers: Sequence[str],
        intraday_start: str,
        intraday_end: str,
        daily_start: str,
        daily_end: str,
    ) -> Tuple[Dict[str, pd.DataFrame], Dict[str, pd.DataFrame]]:
        connector = aiohttp.TCPConnector(limit=self.cfg.max_concurrent)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = []
            for t in tickers:
                tasks.append(self.fetch_intraday(session, t, intraday_start, intraday_end))
                tasks.append(self.fetch_daily(session, t, daily_start, daily_end))
            results = await asyncio.gather(*tasks, return_exceptions=True)

        intraday_map: Dict[str, pd.DataFrame] = {}
        daily_map: Dict[str, pd.DataFrame] = {}
        for i, item in enumerate(results):
            if isinstance(item, Exception):
                LOGGER.warning("fetch exception: %s", item)
                continue
            ticker, df = item
            if i % 2 == 0:
                intraday_map[ticker] = df
            else:
                daily_map[ticker] = df
        return intraday_map, daily_map


def _polygon_to_df(results: Optional[List[Dict]], intraday: bool) -> pd.DataFrame:
    if not results:
        return pd.DataFrame(columns=REQUIRED_COLS)
    df = pd.DataFrame(results)
    rename = {"o": "Open", "h": "High", "l": "Low", "c": "Close", "v": "Volume"}
    if not all(k in df.columns for k in rename):
        return pd.DataFrame(columns=REQUIRED_COLS)
    df = df.rename(columns=rename)

    ts_utc = pd.to_datetime(df["t"], unit="ms", utc=True)
    if intraday:
        df["date"] = ts_utc.dt.tz_convert(SESSION_TZ)
    else:
        df["date"] = ts_utc.dt.tz_convert(None).dt.normalize()

    df = df.set_index("date")[REQUIRED_COLS]
    df = df.sort_index().loc[~df.index.duplicated(keep="last")]
    df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=REQUIRED_COLS)
    for c in REQUIRED_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.dropna(subset=REQUIRED_COLS).astype(float)


def _filter_extended_bars(df: pd.DataFrame) -> pd.DataFrame:
    """프리/정규/애프터 포함 04:00~20:00 ET 봉만 유지"""
    if df.empty or df.index.tz is None:
        return df
    hr = df.index.hour
    mn = df.index.minute
    mask = (
        ((hr > PREMARKET_OPEN[0]) | ((hr == PREMARKET_OPEN[0]) & (mn >= PREMARKET_OPEN[1]))) &
        ((hr < POSTMARKET_CLOSE[0]) | ((hr == POSTMARKET_CLOSE[0]) & (mn == 0)))
    )
    return df.loc[mask]


def _filter_session_bars(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or df.index.tz is None:
        return df
    hr = df.index.hour
    mn = df.index.minute
    mask = (
        ((hr == 9) & (mn >= 30)) |
        ((hr > 9) & (hr < 16)) |
        ((hr == 16) & (mn == 0))
    )
    return df.loc[mask]


def _filter_premarket_bars(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or df.index.tz is None:
        return df
    hr = df.index.hour
    mn = df.index.minute
    mask = (
        ((hr > PREMARKET_OPEN[0]) | ((hr == PREMARKET_OPEN[0]) & (mn >= PREMARKET_OPEN[1]))) &
        ((hr < 9) | ((hr == 9) & (mn < 30)))
    )
    return df.loc[mask]


def _filter_completed_bars(df: pd.DataFrame, multiplier: int) -> pd.DataFrame:
    if df.empty or df.index.tz is None:
        return df
    now_et = pd.Timestamp.now(tz=SESSION_TZ)
    bar_end = df.index + pd.Timedelta(minutes=multiplier)
    return df.loc[bar_end <= now_et]


def fetch_all_data(
    symbols: Sequence[str],
    cfg: DataConfig,
    api_key: str,
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, pd.DataFrame], Dict[str, pd.DataFrame], int]:
    clean = list(dict.fromkeys(s.strip().upper() for s in symbols if s.strip()))
    if not clean:
        return {}, {}, {}, 0

    end = pd.Timestamp.utcnow().normalize() + pd.Timedelta(days=1)
    intra_start = (end - pd.Timedelta(days=cfg.intraday_lookback_days)).strftime("%Y-%m-%d")
    daily_start = (end - pd.Timedelta(days=cfg.daily_lookback_days)).strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")

    LOGGER.info(
        "Polygon 수집: %d심볼 | intra %s~%s (30m raw) | daily %s~%s | 동시 %d",
        len(clean), intra_start, end_str, daily_start, end_str, cfg.max_concurrent,
    )

    t0 = time.perf_counter()
    client = PolygonClient(api_key=api_key, cfg=cfg)
    raw_intra_map, daily_map = asyncio.run(
        client.fetch_all(clean, intra_start, end_str, daily_start, end_str)
    )
    elapsed = time.perf_counter() - t0

    session_map: Dict[str, pd.DataFrame] = {}
    for t, df in list(raw_intra_map.items()):
        if df.empty:
            session_map[t] = df
            continue
        df2 = _filter_completed_bars(df, cfg.intraday_multiplier)
        df2 = _filter_extended_bars(df2)
        raw_intra_map[t] = df2
        session_map[t] = _filter_session_bars(df2)

    intra_filled = sum(1 for df in session_map.values() if not df.empty)
    daily_filled = sum(1 for df in daily_map.values() if not df.empty)
    LOGGER.info(
        "수집 완료: session %d/%d · daily %d/%d · API %d회 · %.2f초",
        intra_filled, len(clean), daily_filled, len(clean), client.call_count, elapsed,
    )
    return raw_intra_map, session_map, daily_map, client.call_count


# ═══════════════════════════════════════════════════════════
#  기술적 지표
# ═══════════════════════════════════════════════════════════
def _ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False, min_periods=max(2, span // 2)).mean()


def _atr(df: pd.DataFrame, window: int) -> pd.Series:
    pc = df["Close"].shift(1)
    tr = pd.concat([
        (df["High"] - df["Low"]),
        (df["High"] - pc).abs(),
        (df["Low"] - pc).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(window, min_periods=max(3, window // 2)).mean()


def _zscore(s: pd.Series, window: int) -> pd.Series:
    mn = s.rolling(window, min_periods=max(5, window // 2)).mean().shift(1)
    sd = s.rolling(window, min_periods=max(5, window // 2)).std(ddof=0).shift(1)
    return (s - mn) / sd.replace(0.0, np.nan)


def _clv(df: pd.DataFrame) -> pd.Series:
    rng = (df["High"] - df["Low"]).replace(0, np.nan)
    return (((df["Close"] - df["Low"]) - (df["High"] - df["Close"])) / rng
            ).clip(-1.0, 1.0).fillna(0.0)


def _rs(close: pd.Series, bench: pd.Series, window: int) -> pd.Series:
    return (close / close.shift(window) - 1.0) - (bench / bench.shift(window) - 1.0)


def _session_keys(index: pd.DatetimeIndex) -> pd.Series:
    return pd.Series(index.tz_convert(SESSION_TZ).strftime("%Y-%m-%d"), index=index)


def _slot_rvol(df: pd.DataFrame, window_days: int) -> pd.Series:
    if df.empty or df.index.tz is None:
        return pd.Series(np.nan, index=df.index)
    slot_key = df.index.strftime("%H:%M")
    tmp = df[["Volume"]].copy()
    tmp["SLOT"] = slot_key
    slot_avg = (tmp.groupby("SLOT")["Volume"]
                  .transform(lambda s: s.shift(1).rolling(window_days, min_periods=3).mean()))
    return df["Volume"] / slot_avg.replace(0, np.nan)


def _session_rvol_since_open(df: pd.DataFrame, window_days: int) -> pd.Series:
    if df.empty:
        return pd.Series(np.nan, index=df.index)
    tmp = df[["Volume"]].copy()
    tmp["SESSION_DATE"] = _session_keys(df.index)
    tmp["REG_BAR_IDX"] = tmp.groupby("SESSION_DATE").cumcount() + 1
    tmp["CUM_VOL"] = tmp.groupby("SESSION_DATE")["Volume"].cumsum()
    avg_cum = (tmp.groupby("REG_BAR_IDX")["CUM_VOL"]
                 .transform(lambda s: s.shift(1).rolling(window_days, min_periods=3).mean()))
    return tmp["CUM_VOL"] / avg_cum.replace(0, np.nan)


def _event_cross_above(series: pd.Series, level: pd.Series) -> pd.Series:
    prev_s = series.shift(1)
    prev_l = level.shift(1).fillna(level)
    same_day = _session_keys(series.index).eq(_session_keys(series.index).shift(1))
    return same_day & prev_s.lt(prev_l) & series.ge(level)


def _compute_premarket_stats(raw_df: pd.DataFrame) -> pd.DataFrame:
    pm = _filter_premarket_bars(raw_df)
    if pm.empty:
        return pd.DataFrame(columns=["PM_HIGH", "PM_LOW", "PM_VOL"])
    tmp = pm.copy()
    tmp["SESSION_DATE"] = _session_keys(tmp.index)
    stats = tmp.groupby("SESSION_DATE").agg(
        PM_HIGH=("High", "max"),
        PM_LOW=("Low", "min"),
        PM_VOL=("Volume", "sum"),
    )
    return stats.sort_index()


def _trim_partial_daily(daily_df: pd.DataFrame) -> pd.DataFrame:
    if daily_df.empty:
        return daily_df
    today_et = pd.Timestamp.now(tz=SESSION_TZ).date()
    out = daily_df.copy()
    while not out.empty and pd.Timestamp(out.index[-1]).date() >= today_et:
        out = out.iloc[:-1]
    return out


# ═══════════════════════════════════════════════════════════
#  일봉 MTF 컨텍스트
# ═══════════════════════════════════════════════════════════
def compute_daily_context(daily_df: pd.DataFrame, cfg: SignalConfig) -> Dict:
    daily = _trim_partial_daily(daily_df)
    if daily.empty or len(daily) < cfg.daily_ema_slow:
        return {
            "daily_uptrend": False,
            "daily_last_close": np.nan,
            "daily_ema_stack": False,
            "daily_20h_pct": np.nan,
            "daily_ctx_bars": len(daily),
        }

    ema_f = _ema(daily["Close"], cfg.daily_ema_fast)
    ema_s = _ema(daily["Close"], cfg.daily_ema_slow)

    last_c = float(daily["Close"].iloc[-1])
    last_ef = float(ema_f.iloc[-1])
    last_es = float(ema_s.iloc[-1])
    slope_ok = float(ema_f.diff(3).iloc[-1]) > 0 if len(ema_f) > 3 else False
    stack = last_c >= last_ef >= last_es

    high_20 = float(daily["High"].rolling(20, min_periods=10).max().iloc[-1]) \
        if len(daily) >= 20 else np.nan
    pct_from_hh = (last_c / high_20 - 1.0) if pd.notna(high_20) and high_20 > 0 else np.nan

    return {
        "daily_uptrend": bool(stack and slope_ok),
        "daily_last_close": last_c,
        "daily_ema_stack": bool(stack),
        "daily_20h_pct": pct_from_hh,
        "daily_ctx_bars": len(daily),
    }


# ═══════════════════════════════════════════════════════════
#  Intraday 피처 엔지니어링
# ═══════════════════════════════════════════════════════════
def add_intraday_features(
    session_df: pd.DataFrame,
    raw_df: pd.DataFrame,
    bench_close: pd.Series,
    market_close: pd.Series,
    sector_close: Optional[pd.Series],
    cfg: SignalConfig,
) -> pd.DataFrame:
    out = session_df.copy()
    out["SESSION_DATE"] = _session_keys(out.index)
    out["REG_BAR_IDX"] = out.groupby("SESSION_DATE").cumcount() + 1

    # CLV
    out["CLV"] = _clv(out)
    out["PRE_CLV"] = (
        out["CLV"].shift(cfg.clv_post_window)
        .rolling(cfg.clv_pre_window, min_periods=2).mean()
    )
    out["POST_CLV"] = out["CLV"].rolling(cfg.clv_post_window, min_periods=1).mean()

    # EMA / 추세
    out["EMA_FAST"] = _ema(out["Close"], cfg.ema_fast)
    out["EMA_SLOW"] = _ema(out["Close"], cfg.ema_slow)
    out["EMA_SLOPE"] = out["EMA_FAST"].diff(cfg.ema_slope_bars)
    out["INTRA_UPTREND"] = (
        (out["Close"] >= out["EMA_FAST"]) &
        (out["EMA_FAST"] >= out["EMA_SLOW"]) &
        (out["EMA_SLOPE"] > 0)
    )

    # VWAP (세션 누적)
    tp = (out["High"] + out["Low"] + out["Close"]) / 3.0
    tpv = tp * out["Volume"]
    out["VWAP"] = tpv.groupby(out["SESSION_DATE"]).cumsum() / \
        out["Volume"].groupby(out["SESSION_DATE"]).cumsum().replace(0, np.nan)
    out["ABOVE_VWAP"] = out["Close"] >= out["VWAP"]
    out["DIST_VWAP"] = (out["Close"] / out["VWAP"]) - 1.0

    prev_close = out["Close"].shift(1)
    prev_vwap = out["VWAP"].shift(1)
    same_day = out["SESSION_DATE"].eq(out["SESSION_DATE"].shift(1))
    out["VWAP_RECLAIM"] = (
        same_day &
        prev_close.lt(prev_vwap * (1 - cfg.vwap_reclaim_tolerance)) &
        out["Close"].gt(out["VWAP"] * (1 + cfg.vwap_reclaim_tolerance)) &
        out["Low"].le(out["VWAP"] * (1 + cfg.vwap_reclaim_tolerance)) &
        (out["CLV"] >= max(0.0, cfg.cont_clv_min - 0.05))
    )

    # Money Flow / Volume
    out["DOLLAR_VOL"] = out["Close"] * out["Volume"]
    out["SIGNED_FLOW"] = out["CLV"] * out["DOLLAR_VOL"]
    out["MF_Z"] = _zscore(out["SIGNED_FLOW"], cfg.mf_window)

    vol_ma = out["Volume"].rolling(
        cfg.vol_window,
        min_periods=max(5, cfg.vol_window // 2),
    ).mean().shift(1)
    out["VOL_SPIKE"] = out["Volume"] / vol_ma.replace(0, np.nan)
    out["RVOL_SLOT"] = _slot_rvol(out, cfg.slot_rvol_window)
    out["SESSION_RVOL"] = _session_rvol_since_open(out, cfg.session_rvol_window)

    # ATR / Range / Squeeze
    out["ATR"] = _atr(out, cfg.atr_window)
    out["RANGE"] = out["High"] - out["Low"]
    out["ATR_EXPAND"] = out["RANGE"] / out["ATR"].replace(0, np.nan)

    basis = out["Close"].rolling(
        cfg.bb_window, min_periods=max(5, cfg.bb_window // 2)
    ).mean()
    dev = out["Close"].rolling(
        cfg.bb_window, min_periods=max(5, cfg.bb_window // 2)
    ).std(ddof=0)
    out["BB_UP"] = basis + cfg.bb_std * dev
    out["BB_LO"] = basis - cfg.bb_std * dev
    kc_mid = _ema(out["Close"], cfg.kc_window)
    kc_atr = _atr(out, cfg.kc_window)
    out["KC_UP"] = kc_mid + cfg.kc_atr_mult * kc_atr
    out["KC_LO"] = kc_mid - cfg.kc_atr_mult * kc_atr
    out["IN_SQUEEZE"] = (out["BB_UP"] <= out["KC_UP"]) & (out["BB_LO"] >= out["KC_LO"])
    prev_in_sq = out["IN_SQUEEZE"].shift(1).eq(True)
    curr_in_sq = out["IN_SQUEEZE"].eq(True)
    out["SQ_RELEASE"] = prev_in_sq & (~curr_in_sq)

    nr_rank = out["RANGE"].rolling(cfg.nr7_window, min_periods=cfg.nr7_window).apply(
        lambda x: 1.0 if x.iloc[-1] == x.min() else 0.0, raw=False
    )
    out["NR7"] = nr_rank.fillna(0.0).astype(bool)
    out["INSIDE_BAR"] = (
        (out["High"] <= out["High"].shift(1)) &
        (out["Low"] >= out["Low"].shift(1))
    )
    prev_nr7 = out["NR7"].shift(1).eq(True)
    prev_inside = out["INSIDE_BAR"].shift(1).eq(True)
    out["NR7_EXPAND"] = (
        (prev_nr7 | prev_inside) &
        (out["ATR_EXPAND"] >= cfg.atr_expand_th)
    )

    # 최근 고가 / 박스 상단
    out["HIGH_N"] = out["High"].shift(1).rolling(
        cfg.intraday_hh_window,
        min_periods=max(3, cfg.intraday_hh_window // 2),
    ).max()
    out["BOX_TOP"] = out["Close"].shift(1).rolling(
        cfg.box_window,
        min_periods=max(5, cfg.box_window // 2),
    ).max()

    out["BREAKOUT_N"] = _event_cross_above(
        out["Close"], out["HIGH_N"] * (1 + cfg.breakout_buf)
    )
    out["BOX_BREAK"] = _event_cross_above(out["Close"], out["BOX_TOP"])

    # Opening Range (첫 30분)
    or_stats = out.groupby("SESSION_DATE").agg(
        OR_HIGH=("High", lambda s: s.iloc[:cfg.opening_range_bars].max()),
        OR_LOW=("Low", lambda s: s.iloc[:cfg.opening_range_bars].min()),
        OR_CLOSE=("Close", lambda s: s.iloc[cfg.opening_range_bars - 1]
                  if len(s) >= cfg.opening_range_bars else s.iloc[-1]),
    )
    out["OR_HIGH"] = out["SESSION_DATE"].map(or_stats["OR_HIGH"].to_dict())
    out["OR_LOW"] = out["SESSION_DATE"].map(or_stats["OR_LOW"].to_dict())
    out["ABOVE_OR"] = out["Close"] >= out["OR_HIGH"] * (1 + cfg.breakout_buf)
    out["ORB_BREAK"] = (
        (out["REG_BAR_IDX"] > cfg.opening_range_bars) &
        _event_cross_above(out["Close"], out["OR_HIGH"] * (1 + cfg.breakout_buf))
    )

    # Premarket high / low
    pm_stats = _compute_premarket_stats(raw_df)
    if not pm_stats.empty:
        out["PM_HIGH"] = out["SESSION_DATE"].map(pm_stats["PM_HIGH"].to_dict())
        out["PM_LOW"] = out["SESSION_DATE"].map(pm_stats["PM_LOW"].to_dict())
        out["PM_VOL"] = out["SESSION_DATE"].map(pm_stats["PM_VOL"].to_dict())
        out["HAS_PM"] = out["PM_HIGH"].notna()
        out["ABOVE_PMH"] = out["Close"] >= out["PM_HIGH"] * (1 + cfg.breakout_buf)
        out["PMH_BREAK"] = (
            out["HAS_PM"] &
            _event_cross_above(out["Close"], out["PM_HIGH"] * (1 + cfg.breakout_buf))
        )
    else:
        out["PM_HIGH"] = np.nan
        out["PM_LOW"] = np.nan
        out["PM_VOL"] = np.nan
        out["HAS_PM"] = False
        out["ABOVE_PMH"] = False
        out["PMH_BREAK"] = False

    # 캔들 품질
    rng_safe = out["RANGE"].replace(0, np.nan)
    out["REAL_BODY"] = (out["Close"] - out["Open"]).abs()
    out["UPPER_WICK"] = out["High"] - out[["Open", "Close"]].max(axis=1)
    out["WICK_RATIO"] = out["UPPER_WICK"] / rng_safe
    out["BODY_RATIO"] = out["REAL_BODY"] / rng_safe
    out["CLOSE_HIGH"] = (out["Close"] - out["Low"]) / rng_safe

    # 유동성
    out["LIQ_OK"] = (
        (out["DOLLAR_VOL"] >= cfg.dollar_vol_min_m * 1_000_000) &
        (out["Close"] >= cfg.min_price)
    )

    # RS (멀티 기준)
    bench = bench_close.reindex(out.index).ffill()
    mkt = market_close.reindex(out.index).ffill()
    out["RS_QQQ_5"] = _rs(out["Close"], bench, cfg.rs_short)
    out["RS_QQQ_20"] = _rs(out["Close"], bench, cfg.rs_long)
    out["RS_SPY_20"] = _rs(out["Close"], mkt, cfg.rs_long)

    sess_open = out.groupby("SESSION_DATE")["Open"].transform("first")
    bench_open = bench.groupby(out["SESSION_DATE"]).transform("first")
    mkt_open = mkt.groupby(out["SESSION_DATE"]).transform("first")
    out["RS_OPEN_QQQ"] = (out["Close"] / sess_open - 1.0) - (bench / bench_open - 1.0)
    out["RS_OPEN_SPY"] = (out["Close"] / sess_open - 1.0) - (mkt / mkt_open - 1.0)

    if sector_close is not None and not sector_close.empty:
        sec = sector_close.reindex(out.index).ffill()
        sec_open = sec.groupby(out["SESSION_DATE"]).transform("first")
        out["RS_SEC_5"] = _rs(out["Close"], sec, cfg.rs_short)
        out["RS_SEC_20"] = _rs(out["Close"], sec, cfg.rs_long)
        out["RS_OPEN_SEC"] = (out["Close"] / sess_open - 1.0) - (sec / sec_open - 1.0)
    else:
        out["RS_SEC_5"] = np.nan
        out["RS_SEC_20"] = np.nan
        out["RS_OPEN_SEC"] = np.nan

    # Breakout 이후 첫 눌림
    out["BREAKOUT_EVENT"] = out[["BREAKOUT_N", "BOX_BREAK", "ORB_BREAK", "PMH_BREAK"]].any(axis=1)
    recent_break = (
        out.groupby("SESSION_DATE")["BREAKOUT_EVENT"]
           .transform(lambda s: s.shift(1).rolling(cfg.pullback_lookback, min_periods=1).max())
           .fillna(0.0).astype(bool)
    )

    tol = cfg.pullback_touch_tolerance
    touch_vwap = out["Low"].le(out["VWAP"] * (1 + tol)) & out["Close"].ge(out["VWAP"])
    touch_ema = out["Low"].le(out["EMA_FAST"] * (1 + tol)) & out["Close"].ge(out["EMA_FAST"])
    touch_or = out["OR_HIGH"].notna() & out["Low"].le(out["OR_HIGH"] * (1 + tol)) & \
        out["Close"].ge(out["OR_HIGH"] * (1 - tol))
    touch_pmh = out["PM_HIGH"].notna() & out["Low"].le(out["PM_HIGH"] * (1 + tol)) & \
        out["Close"].ge(out["PM_HIGH"] * (1 - tol))

    pullback_candidate = (
        recent_break &
        (~out["BREAKOUT_EVENT"]) &
        (touch_vwap | touch_ema | touch_or | touch_pmh) &
        (out["CLOSE_HIGH"] >= max(0.45, cfg.min_close_high - 0.05)) &
        (out["CLV"] >= 0) &
        out["DIST_VWAP"].le(cfg.max_vwap_extension)
    )
    prev_pullback = pullback_candidate.groupby(out["SESSION_DATE"]).shift(1).eq(True)
    out["FIRST_PULLBACK"] = pullback_candidate & (~prev_pullback)

    # 확장/과열 필터용 레벨
    breakout_ref = pd.concat([
        out["HIGH_N"],
        out["BOX_TOP"],
        out["OR_HIGH"],
        out["PM_HIGH"],
    ], axis=1).max(axis=1, skipna=True)
    out["BREAKOUT_REF"] = breakout_ref
    out["DIST_BREAKOUT"] = np.where(
        breakout_ref.gt(0),
        (out["Close"] / breakout_ref) - 1.0,
        np.nan,
    )

    # 구조적 트리거 종합
    out["ACTIONABLE_TRIGGER"] = out[[
        "ORB_BREAK", "PMH_BREAK", "VWAP_RECLAIM",
        "FIRST_PULLBACK", "BREAKOUT_N", "BOX_BREAK",
        "SQ_RELEASE", "NR7_EXPAND"
    ]].any(axis=1)

    return out


# ═══════════════════════════════════════════════════════════
#  모드 분류
# ═══════════════════════════════════════════════════════════
def _classify_mode(row: pd.Series, cfg: SignalConfig) -> Tuple[str, float]:
    pre = float(row.get("PRE_CLV", 0) or 0)
    post = float(row.get("POST_CLV", 0) or 0)
    clv = float(row.get("CLV", 0) or 0)
    sq_rel = bool(row.get("SQ_RELEASE", False))
    nr7_exp = bool(row.get("NR7_EXPAND", False))
    in_uptrend = bool(row.get("INTRA_UPTREND", False))
    vwap_reclaim = bool(row.get("VWAP_RECLAIM", False))
    first_pullback = bool(row.get("FIRST_PULLBACK", False))
    orb_break = bool(row.get("ORB_BREAK", False))
    pmh_break = bool(row.get("PMH_BREAK", False))
    above_vwap = bool(row.get("ABOVE_VWAP", False))

    is_strong_rev = (pre <= cfg.strong_rev_pre_max) and (post >= cfg.strong_rev_post_min)
    is_rev = (pre <= cfg.rev_pre_max) and (post >= cfg.rev_post_min)
    is_strong_cont = (post >= cfg.strong_cont_post_min) and (clv >= cfg.strong_cont_clv_min)
    is_cont = (post >= cfg.cont_post_min) and (clv >= cfg.cont_clv_min) and in_uptrend
    is_squeeze_pop = (sq_rel or nr7_exp)

    if (is_strong_rev or is_rev) and (is_strong_cont or is_cont):
        return "BOTH", cfg.score_both
    if is_strong_rev:
        return "STRONG_REV", cfg.score_strong_rev
    if is_strong_cont:
        return "STRONG_CONT", cfg.score_strong_cont
    if is_rev:
        return "REVERSAL", cfg.score_reversal
    if is_cont:
        return "CONTINUATION", cfg.score_continuation

    # v6 추가: 구조적 트리거가 있으면 모드 기준을 약간 완화
    if vwap_reclaim and (post >= max(0.12, cfg.rev_post_min - 0.10)):
        return "REVERSAL", cfg.score_reversal
    if first_pullback and in_uptrend and above_vwap and (clv >= 0.0):
        return "CONTINUATION", cfg.score_continuation
    if (orb_break or pmh_break) and in_uptrend and above_vwap and (clv >= max(0.15, cfg.cont_clv_min - 0.10)):
        return "CONTINUATION", cfg.score_continuation

    if is_squeeze_pop:
        return "SQUEEZE_POP", cfg.score_squeeze_pop
    return "NONE", 9999.0


# ═══════════════════════════════════════════════════════════
#  점수 산출
# ═══════════════════════════════════════════════════════════
def _compute_score(row: pd.Series, daily_ctx: Dict, cfg: SignalConfig) -> float:
    def f(k: str, default=0.0):
        v = row.get(k, default)
        return float(v) if pd.notna(v) else default

    score = 0.0
    mf_z = f("MF_Z")
    vol_sp = f("VOL_SPIKE")
    rvol_slot = f("RVOL_SLOT")
    rvol_session = f("SESSION_RVOL")
    post_clv = f("POST_CLV")
    rs_q20 = f("RS_QQQ_20")
    rs_s20 = f("RS_SEC_20")
    rs_open_q = f("RS_OPEN_QQQ")
    rs_open_s = f("RS_OPEN_SEC")
    dist_vwap = f("DIST_VWAP")
    dist_break = f("DIST_BREAKOUT")

    # 활동성
    if mf_z >= cfg.mf_z_strong:    score += 2.2
    elif mf_z >= cfg.mf_z_normal:  score += 1.2
    elif mf_z >= cfg.mf_z_soft:    score += 0.5

    if vol_sp >= cfg.vol_strong:   score += 1.5
    elif vol_sp >= cfg.vol_normal: score += 0.9
    elif vol_sp >= cfg.vol_soft:   score += 0.3

    if rvol_slot >= cfg.slot_rvol_strong:   score += 1.7
    elif rvol_slot >= cfg.slot_rvol_normal: score += 0.9
    elif rvol_slot >= cfg.slot_rvol_soft:   score += 0.3

    if rvol_session >= cfg.session_rvol_strong:   score += 1.8
    elif rvol_session >= cfg.session_rvol_normal: score += 1.0
    elif rvol_session >= cfg.session_rvol_soft:   score += 0.3

    # 추세 / 컨텍스트
    if bool(row.get("INTRA_UPTREND", False)):   score += 0.9
    if bool(row.get("ABOVE_VWAP", False)):      score += 0.6
    if daily_ctx.get("daily_uptrend", False):   score += 1.2
    if daily_ctx.get("daily_ema_stack", False): score += 0.4

    # 상대강도
    if rs_q20 > 0:     score += 0.7
    if rs_s20 > 0:     score += 0.5
    if f("RS_QQQ_5") > 0: score += 0.3
    if rs_open_q > 0:  score += 0.5
    if rs_open_s > 0:  score += 0.3

    # 구조적 트리거
    if bool(row.get("VWAP_RECLAIM", False)): score += 1.2
    if bool(row.get("ORB_BREAK", False)):    score += 1.3
    if bool(row.get("PMH_BREAK", False)):    score += 1.1
    if bool(row.get("FIRST_PULLBACK", False)): score += 1.4
    if bool(row.get("BREAKOUT_N", False)):   score += 0.5
    if bool(row.get("BOX_BREAK", False)):    score += 0.5

    # 변동성 이벤트
    if bool(row.get("SQ_RELEASE", False)):   score += 1.3
    if bool(row.get("NR7_EXPAND", False)):   score += 0.8
    if bool(row.get("IN_SQUEEZE", False)):   score += 0.2

    # 캔들 품질
    wick_ok = f("WICK_RATIO", 1.0) <= cfg.max_wick_ratio
    body_ok = f("BODY_RATIO") >= cfg.min_body_ratio
    close_ok = f("CLOSE_HIGH") >= cfg.min_close_high
    if wick_ok and body_ok and close_ok:
        score += 1.0
    elif wick_ok and body_ok:
        score += 0.4

    # 유동성
    if bool(row.get("LIQ_OK", False)):
        score += 0.4

    # 연속 부스터
    score += max(0.0, post_clv) * 1.8
    score += min(max(0.0, mf_z), 5.0) * 0.4
    score += math.log1p(max(0.0, vol_sp)) * 0.8
    score += math.log1p(max(0.0, rvol_slot)) * 0.6
    score += math.log1p(max(0.0, rvol_session)) * 0.7
    score += min(max(0.0, rs_q20), 0.05) * 30.0
    score += min(max(0.0, rs_open_q), 0.05) * 18.0

    # 과열/추격 페널티
    if dist_vwap > cfg.max_vwap_extension:
        score -= 1.6
    elif dist_vwap > cfg.max_vwap_extension * 0.8:
        score -= 0.6

    if pd.notna(dist_break):
        if dist_break > cfg.max_breakout_extension:
            score -= 1.2
        elif dist_break > cfg.max_breakout_extension * 0.75:
            score -= 0.4

    return round(score, 4)


def _passes_hard_gate(row: pd.Series, mode: str, daily_ctx: Dict, cfg: SignalConfig) -> bool:
    mf_z = float(row.get("MF_Z", 0) or 0)
    vol_sp = float(row.get("VOL_SPIKE", 0) or 0)
    rvol_slot = float(row.get("RVOL_SLOT", 0) or 0)
    rvol_session = float(row.get("SESSION_RVOL", 0) or 0)
    liq = bool(row.get("LIQ_OK", False))
    above_vwap = bool(row.get("ABOVE_VWAP", False))
    structural = bool(row.get("ACTIONABLE_TRIGGER", False))
    is_pullback = bool(row.get("FIRST_PULLBACK", False))
    is_breakoutish = any(bool(row.get(k, False)) for k in [
        "BREAKOUT_N", "BOX_BREAK", "ORB_BREAK", "PMH_BREAK"
    ])

    if not liq:
        return False

    if cfg.require_daily_uptrend and not daily_ctx.get("daily_ema_stack", True):
        return False

    if cfg.require_structural_trigger and not structural:
        return False

    if cfg.require_above_vwap_for_breakout and is_breakoutish and not above_vwap:
        return False

    dist_vwap = row.get("DIST_VWAP", np.nan)
    if pd.notna(dist_vwap) and dist_vwap > cfg.max_vwap_extension and not is_pullback:
        return False

    dist_break = row.get("DIST_BREAKOUT", np.nan)
    if pd.notna(dist_break) and dist_break > cfg.max_breakout_extension and not is_pullback:
        return False

    if mode == "SQUEEZE_POP":
        hits = sum([
            mf_z >= cfg.mf_z_soft,
            rvol_slot >= cfg.slot_rvol_soft,
            rvol_session >= cfg.session_rvol_soft,
        ])
        return hits >= 1

    if is_pullback:
        hits = sum([
            above_vwap,
            mf_z >= cfg.mf_z_soft,
            rvol_slot >= cfg.slot_rvol_soft,
            rvol_session >= cfg.session_rvol_soft,
        ])
        return hits >= 2

    hits = sum([
        mf_z >= cfg.mf_z_normal,
        vol_sp >= cfg.vol_normal,
        rvol_slot >= cfg.slot_rvol_normal,
        rvol_session >= cfg.session_rvol_normal,
    ])
    return hits >= 2


def _setup_tags(row: pd.Series) -> List[str]:
    tags = []
    if bool(row.get("ORB_BREAK", False)):      tags.append("ORB")
    if bool(row.get("VWAP_RECLAIM", False)):   tags.append("VWAP")
    if bool(row.get("PMH_BREAK", False)):      tags.append("PMH")
    if bool(row.get("FIRST_PULLBACK", False)): tags.append("1stPB")
    if bool(row.get("BREAKOUT_N", False)):     tags.append("HH")
    if bool(row.get("BOX_BREAK", False)):      tags.append("BOX")
    if bool(row.get("SQ_RELEASE", False)):     tags.append("SQ")
    if bool(row.get("NR7_EXPAND", False)):     tags.append("NR7")
    return tags


# ═══════════════════════════════════════════════════════════
#  단일 티커 스캔
# ═══════════════════════════════════════════════════════════
def scan_one_ticker(
    ticker: str,
    raw_df: pd.DataFrame,
    session_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    bench_close: pd.Series,
    market_close: pd.Series,
    sector_close: Optional[pd.Series],
    cfg_s: SignalConfig,
    cfg_d: DataConfig,
    scan_last_n_bars: int,
) -> Optional[Dict]:
    if session_df.empty or len(session_df) < cfg_d.min_intraday_bars:
        return None

    daily_ctx = compute_daily_context(daily_df, cfg_s)
    feat = add_intraday_features(session_df, raw_df, bench_close, market_close, sector_close, cfg_s)

    scan_rows = feat.tail(scan_last_n_bars)
    if scan_rows.empty:
        return None

    best = None
    for ts, row in scan_rows.iterrows():
        mode, min_score = _classify_mode(row, cfg_s)
        if mode == "NONE":
            continue
        if not _passes_hard_gate(row, mode, daily_ctx, cfg_s):
            continue
        score = _compute_score(row, daily_ctx, cfg_s)
        if score < min_score:
            continue
        if best is None or score > best["pass_score"]:
            best = {"ts": ts, "row": row, "mode": mode, "pass_score": score}

    if best is None:
        return None

    ts = best["ts"]
    row = best["row"]
    mode = best["mode"]
    score = best["pass_score"]

    def _f(k, d=np.nan):
        v = row.get(k, d)
        return float(v) if pd.notna(v) else d

    bar_et = pd.Timestamp(ts)
    if bar_et.tzinfo is None:
        bar_et = bar_et.tz_localize(SESSION_TZ)
    bar_et_str = bar_et.strftime("%Y-%m-%d %H:%M")

    tags = _setup_tags(row)
    trigger_str = ", ".join(tags) if tags else "—"

    return {
        "ticker": ticker,
        "bar_et": bar_et_str,
        "scan_key": f"{ticker}|{bar_et_str}",
        "mode": mode,
        "setup_tags": trigger_str,
        "pass_score": round(score, 3),
        "close": round(_f("Close"), 4),
        "pre_clv": round(_f("PRE_CLV", 0), 4),
        "post_clv": round(_f("POST_CLV", 0), 4),
        "mf_z": round(_f("MF_Z", 0), 3),
        "vol_spike": round(_f("VOL_SPIKE", 0), 3),
        "rvol_slot": round(_f("RVOL_SLOT", 0), 3),
        "session_rvol": round(_f("SESSION_RVOL", 0), 3),
        "vwap": round(_f("VWAP", np.nan), 4),
        "dist_vwap_pct": round(_f("DIST_VWAP", 0) * 100.0, 2),
        "atr_expand": round(_f("ATR_EXPAND", 0), 3),
        "rs_qqq_20": round(_f("RS_QQQ_20", 0), 4),
        "rs_open_qqq": round(_f("RS_OPEN_QQQ", 0), 4),
        "rs_sec_20": round(_f("RS_SEC_20", 0), 4),
        "wick_ratio": round(_f("WICK_RATIO", 0), 3),
        "body_ratio": round(_f("BODY_RATIO", 0), 3),
        "daily_uptrend": bool(daily_ctx.get("daily_uptrend", False)),
        "daily_ema_stack": bool(daily_ctx.get("daily_ema_stack", False)),
        "intra_uptrend": bool(row.get("INTRA_UPTREND", False)),
        "above_vwap": bool(row.get("ABOVE_VWAP", False)),
        "breakout": bool(row.get("BREAKOUT_N", False) or row.get("BOX_BREAK", False)),
        "orb_break": bool(row.get("ORB_BREAK", False)),
        "vwap_reclaim": bool(row.get("VWAP_RECLAIM", False)),
        "pmh_break": bool(row.get("PMH_BREAK", False)),
        "first_pullback": bool(row.get("FIRST_PULLBACK", False)),
        "squeeze_release": bool(row.get("SQ_RELEASE", False)),
    }


# ═══════════════════════════════════════════════════════════
#  전체 유니버스 스캔
# ═══════════════════════════════════════════════════════════
def scan_universe(
    tickers: Sequence[str],
    ticker_sector_map: Dict[str, str],
    config: ScannerConfig,
    api_key: str,
) -> Tuple[pd.DataFrame, int]:
    setup_logging()
    cfg_d = config.data
    cfg_s = config.signal

    sector_etfs = sorted({SECTOR_ETF_MAP[s] for s in ticker_sector_map.values()
                          if s in SECTOR_ETF_MAP})
    all_syms = list(dict.fromkeys([
        *tickers, cfg_d.benchmark_symbol, cfg_d.market_symbol, *sector_etfs
    ]))
    LOGGER.info("스캔 유니버스: %d 심볼 (티커 %d + 보조 %d)",
                len(all_syms), len(tickers), len(all_syms) - len(tickers))

    raw_map, session_map, daily_map, api_calls = fetch_all_data(all_syms, cfg_d, api_key)

    bench_intra = session_map.get(cfg_d.benchmark_symbol, pd.DataFrame())
    mkt_intra = session_map.get(cfg_d.market_symbol, pd.DataFrame())
    if bench_intra.empty or mkt_intra.empty:
        raise RuntimeError(
            f"벤치마크 30m 데이터 수신 실패: "
            f"{cfg_d.benchmark_symbol}={len(bench_intra)}, "
            f"{cfg_d.market_symbol}={len(mkt_intra)}"
        )
    bench_close = bench_intra["Close"]
    market_close = mkt_intra["Close"]

    results: List[Dict] = []
    dedup_tickers = list(dict.fromkeys(
        str(t).strip().upper() for t in tickers if str(t).strip()
    ))
    for ticker in dedup_tickers:
        raw_df = raw_map.get(ticker, pd.DataFrame())
        session_df = session_map.get(ticker, pd.DataFrame())
        daily_df = daily_map.get(ticker, pd.DataFrame())
        sector = ticker_sector_map.get(ticker)
        etf = SECTOR_ETF_MAP.get(sector) if sector else None
        sec_close = session_map.get(etf, pd.DataFrame()).get("Close") if etf else None
        try:
            rec = scan_one_ticker(
                ticker, raw_df, session_df, daily_df,
                bench_close, market_close, sec_close,
                cfg_s, cfg_d,
                config.scan.scan_last_n_bars,
            )
            if rec is not None:
                rec["sector"] = sector or "UNKNOWN"
                results.append(rec)
        except Exception as exc:
            LOGGER.warning("시그널 계산 실패 [%s]: %s", ticker, exc)

    out = pd.DataFrame(results)
    if out.empty:
        return out, api_calls

    sort_cols = ["pass_score", "session_rvol", "rvol_slot", "mf_z", "rs_open_qqq"]
    out = (out.sort_values(sort_cols, ascending=False)
              .head(config.scan.top_n)
              .reset_index(drop=True))
    return out, api_calls


# ═══════════════════════════════════════════════════════════
#  봉 단위 중복 알림 방지
# ═══════════════════════════════════════════════════════════
def _today_et() -> str:
    return pd.Timestamp.now(tz=SESSION_TZ).strftime("%Y-%m-%d")


def load_sent_log(path: Path = SENT_LOG_FILE) -> Dict:
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
        except Exception:
            pass
    return {"sent_keys": [], "runs": {}}


def save_sent_log(log: Dict, path: Path = SENT_LOG_FILE) -> None:
    cutoff = (pd.Timestamp.now(tz=SESSION_TZ) - pd.Timedelta(days=3)).strftime("%Y-%m-%d")
    sent = log.get("sent_keys", [])
    trimmed_sent = [k for k in sent if k.split("|")[1][:10] >= cutoff]
    log["sent_keys"] = trimmed_sent
    runs = log.get("runs", {})
    log["runs"] = {k: v for k, v in runs.items() if k >= cutoff}
    path.write_text(json.dumps(log, indent=2, ensure_ascii=False), encoding="utf-8")


def filter_new_signals(df: pd.DataFrame, log: Dict) -> pd.DataFrame:
    if df.empty:
        return df
    sent = set(log.get("sent_keys", []))
    mask = ~df["scan_key"].isin(sent)
    return df.loc[mask].reset_index(drop=True)


def mark_sent(log: Dict, df: pd.DataFrame, api_calls: int) -> None:
    today = _today_et()
    sent = set(log.get("sent_keys", []))
    sent |= set(df["scan_key"].tolist())
    log["sent_keys"] = sorted(sent)
    runs = log.setdefault("runs", {})
    day_runs = runs.setdefault(today, [])
    day_runs.append({
        "run_time_et": pd.Timestamp.now(tz=SESSION_TZ).strftime("%Y-%m-%d %H:%M"),
        "new_signals": len(df),
        "api_calls": api_calls,
    })


# ═══════════════════════════════════════════════════════════
#  미국 장 시간 게이트
# ═══════════════════════════════════════════════════════════
def in_market_hours(buffer_min: int = 5) -> bool:
    now_et = pd.Timestamp.now(tz=SESSION_TZ)
    if now_et.weekday() >= 5:
        return False
    start = now_et.normalize() + pd.Timedelta(
        hours=SESSION_OPEN[0],
        minutes=SESSION_OPEN[1] - buffer_min,
    )
    end = now_et.normalize() + pd.Timedelta(
        hours=SESSION_CLOSE[0],
        minutes=SESSION_CLOSE[1] + buffer_min,
    )
    return start <= now_et <= end


# ═══════════════════════════════════════════════════════════
#  텔레그램 알림봇
# ═══════════════════════════════════════════════════════════
def _mode_display(mode: str) -> str:
    meta = MODE_META.get(mode, {})
    return f"{meta.get('icon', '')} {meta.get('desc', mode)} {MODE_SCORE_LABEL.get(mode, '')}"


def format_header(df: pd.DataFrame, api_calls: int) -> str:
    now_et = pd.Timestamp.now(tz=SESSION_TZ).strftime("%Y-%m-%d %H:%M ET")
    mode_counts = df["mode"].value_counts().to_dict()
    mode_str = " · ".join(
        f"{MODE_META.get(m, {}).get('icon', m)} {m}:{cnt}"
        for m, cnt in mode_counts.items()
    )
    setup_counter: Dict[str, int] = {}
    for tags in df.get("setup_tags", []):
        for tag in [t.strip() for t in str(tags).split(",") if t.strip() and t.strip() != "—"]:
            setup_counter[tag] = setup_counter.get(tag, 0) + 1
    setup_str = " · ".join(f"{k}:{v}" for k, v in sorted(setup_counter.items())) or "setup:0"
    return (
        f"🔔 <b>PowerFlow Intraday v6 (Trigger)</b>\n"
        f"⏰ {html.escape(now_et)}\n"
        f"📊 신규 시그널 {len(df)}건  ·  API {api_calls}회\n"
        f"{mode_str}\n"
        f"🧩 {html.escape(setup_str)}\n"
        f"{'─' * 30}"
    )


def format_signal(row: dict, rank: int) -> str:
    mode_str = _mode_display(row.get("mode", ""))
    ticker = html.escape(str(row["ticker"]))
    sector = html.escape(str(row.get("sector", "")))
    daily_up = "✅" if row.get("daily_ema_stack") else "⚠️"
    intra_up = "✅" if row.get("intra_uptrend") else "—"

    extras = []
    if row.get("above_vwap"):       extras.append("🟢 AboveVWAP")
    if row.get("orb_break"):        extras.append("🟠 ORB")
    if row.get("vwap_reclaim"):     extras.append("🔵 VWAP reclaim")
    if row.get("pmh_break"):        extras.append("🟣 PMH")
    if row.get("first_pullback"):   extras.append("🟤 1stPB")
    if row.get("squeeze_release"):  extras.append("💥 SQ")
    if row.get("breakout"):         extras.append("🔹 HH/BOX")
    extras_str = " | ".join(extras) if extras else "—"

    return (
        f"<b>#{rank} {ticker}</b>  <i>{sector}</i>\n"
        f"  {mode_str}\n"
        f"  📅 Bar: {row.get('bar_et','')}  |  💰 ${row.get('close',0):.2f}\n"
        f"  🎯 Score: <b>{row.get('pass_score',0):.2f}</b>  |  🧩 {html.escape(str(row.get('setup_tags','—')))}\n"
        f"  📈 MF_Z {row.get('mf_z',0):.2f}  |  VOL {row.get('vol_spike',0):.2f}×  |  RVOL슬롯 {row.get('rvol_slot',0):.2f}×\n"
        f"  ⏱️ RVOL시가후 {row.get('session_rvol',0):.2f}×  |  VWAP {row.get('vwap',0):.2f}  |  이격 {row.get('dist_vwap_pct',0):+.2f}%\n"
        f"  CLV: pre {row.get('pre_clv',0):+.3f} → post {row.get('post_clv',0):+.3f}\n"
        f"  🧭 일봉 {daily_up}  |  30m 추세 {intra_up}  |  {extras_str}\n"
        f"  📊 RS(QQQ 20봉) {row.get('rs_qqq_20',0):+.4f}  |  RS(시가대비) {row.get('rs_open_qqq',0):+.4f}"
    )


def send_telegram(text: str) -> bool:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        missing = []
        if not token:
            missing.append("TELEGRAM_BOT_TOKEN")
        if not chat_id:
            missing.append("TELEGRAM_CHAT_ID")
        LOGGER.warning("텔레그램 환경변수 없음 [%s] → 전송 생략", ", ".join(missing))
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        resp = requests.post(url, json={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=20)
        resp.raise_for_status()
        LOGGER.info("텔레그램 전송 성공 (chat_id=%s, %d chars)", chat_id, len(text))
        return True
    except requests.RequestException as exc:
        body = ""
        resp_obj = getattr(exc, "response", None)
        if resp_obj is not None:
            try:
                body = f" | status={resp_obj.status_code} body={resp_obj.text[:300]}"
            except Exception:
                pass
        LOGGER.warning("텔레그램 전송 실패: %s%s", exc, body)
        return False


def send_scan_results(df: pd.DataFrame, api_calls: int, announce_empty: bool = False) -> None:
    if df.empty:
        if announce_empty:
            send_telegram(
                f"🔕 <b>PowerFlow Intraday v6</b>\n"
                f"⏰ {pd.Timestamp.now(tz=SESSION_TZ).strftime('%Y-%m-%d %H:%M ET')}\n"
                f"신규 시그널 없음  ·  API {api_calls}회"
            )
        return
    send_telegram(format_header(df, api_calls))
    for rank, (_, row) in enumerate(df.iterrows(), start=1):
        try:
            send_telegram(format_signal(row.to_dict(), rank))
        except Exception as exc:
            LOGGER.warning("종목 전송 실패 [%s]: %s", row.get("ticker"), exc)


# ═══════════════════════════════════════════════════════════
#  결과 저장
# ═══════════════════════════════════════════════════════════
def save_results(df: pd.DataFrame, config: ScannerConfig, api_calls: int, results_dir: Path) -> None:
    results_dir.mkdir(parents=True, exist_ok=True)
    today = _today_et()

    daily_csv = results_dir / f"scan_{today}.csv"
    if not df.empty:
        if daily_csv.exists():
            df.to_csv(daily_csv, mode="a", index=False, header=False, encoding="utf-8-sig")
        else:
            df.to_csv(daily_csv, index=False, encoding="utf-8-sig")

    lines = [f"===== PowerFlow Intraday v6 (Trigger)  "
             f"{pd.Timestamp.now(tz=SESSION_TZ).strftime('%Y-%m-%d %H:%M ET')} ====="]
    if df.empty:
        lines.append("신규 시그널 없음")
    else:
        cols = [
            "ticker", "bar_et", "mode", "setup_tags", "sector", "close",
            "pass_score", "mf_z", "rvol_slot", "session_rvol", "rs_open_qqq"
        ]
        show = df[[c for c in cols if c in df.columns]]
        lines.append(show.to_string(index=False))
    (results_dir / "latest_scan.txt").write_text("\n".join(lines), encoding="utf-8")

    meta = {
        "run_time_et": pd.Timestamp.now(tz=SESSION_TZ).strftime("%Y-%m-%d %H:%M"),
        "generated_utc": pd.Timestamp.utcnow().isoformat(),
        "data_source": "polygon.io",
        "timeframe": f"{config.data.intraday_multiplier}{config.data.intraday_timespan[0]}",
        "signal_count": len(df),
        "api_calls_used": api_calls,
        "config": {
            "data": asdict(config.data),
            "signal": asdict(config.signal),
            "scan": asdict(config.scan),
        },
    }
    (results_dir / "run_meta.json").write_text(
        json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8"
    )


# ═══════════════════════════════════════════════════════════
#  티커 로더
# ═══════════════════════════════════════════════════════════
def load_tickers(path: Path) -> Tuple[List[str], Dict[str, str]]:
    if not path.exists():
        return list(DEFAULT_TICKER_SECTOR.keys()), dict(DEFAULT_TICKER_SECTOR)
    tickers: List[str] = []
    sector_map: Dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        sym = parts[0].upper()
        sector = parts[1].upper() if len(parts) >= 2 else DEFAULT_TICKER_SECTOR.get(sym, "UNKNOWN")
        if sym not in sector_map:
            tickers.append(sym)
            sector_map[sym] = sector
    if not tickers:
        return list(DEFAULT_TICKER_SECTOR.keys()), dict(DEFAULT_TICKER_SECTOR)
    return tickers, sector_map


# ═══════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════
def main() -> None:
    setup_logging()

    api_key = os.getenv("POLYGON_API_KEY", "").strip()
    if not api_key:
        LOGGER.error("POLYGON_API_KEY 환경변수가 없습니다.")
        raise SystemExit(1)

    force_run = os.getenv("FORCE_RUN", "false").lower() == "true"
    announce_empty = os.getenv("ANNOUNCE_EMPTY", "false").lower() == "true"
    top_n = int(os.getenv("TOP_N", "20"))
    max_concurrent = int(os.getenv("MAX_CONCURRENT", "50"))
    scan_last_n = int(os.getenv("SCAN_LAST_N_BARS", "2"))
    results_dir = Path(os.getenv("RESULTS_DIR", "results"))

    LOGGER.info(
        "설정 | FORCE_RUN=%s | ANNOUNCE_EMPTY=%s | TG_TOKEN=%s | TG_CHAT=%s",
        force_run,
        announce_empty,
        "set" if os.getenv("TELEGRAM_BOT_TOKEN", "").strip() else "MISSING",
        "set" if os.getenv("TELEGRAM_CHAT_ID", "").strip() else "MISSING",
    )

    if not force_run and not in_market_hours(buffer_min=5):
        now_et = pd.Timestamp.now(tz=SESSION_TZ).strftime("%Y-%m-%d %H:%M %Z")
        LOGGER.info("정규장 시간이 아님 → 스캔 스킵 (%s)", now_et)
        if announce_empty:
            send_telegram(
                f"🕒 <b>PowerFlow Intraday v6</b>\n"
                f"⏰ {html.escape(now_et)}\n"
                f"정규장 시간이 아님 → 스캔 스킵\n"
                f"<i>(테스트: 텔레그램 연결 OK)</i>"
            )
        return

    tickers, ticker_sector_map = load_tickers(DEFAULT_TICKERS_FILE)
    LOGGER.info("스캔 티커 %d개 | 동시요청 %d | 최근 %d봉 검사",
                len(tickers), max_concurrent, scan_last_n)

    config = ScannerConfig(
        data=DataConfig(max_concurrent=max_concurrent),
        signal=SignalConfig(),
        scan=ScanConfig(top_n=top_n, scan_last_n_bars=scan_last_n),
    )

    df, api_calls = scan_universe(tickers, ticker_sector_map, config, api_key)

    if df.empty:
        LOGGER.info("시그널 없음")
    else:
        LOGGER.info(
            "\n%s",
            df[[
                "ticker", "bar_et", "mode", "setup_tags", "pass_score",
                "session_rvol", "rvol_slot", "mf_z", "rs_open_qqq"
            ]].to_string(index=False)
        )

    save_results(df, config, api_calls, results_dir)

    sent_log = load_sent_log()
    new_df = filter_new_signals(df, sent_log)
    LOGGER.info("신규 시그널: %d건 (전체 %d건)", len(new_df), len(df))

    send_scan_results(new_df, api_calls, announce_empty=announce_empty)

    mark_sent(sent_log, new_df, api_calls)
    save_sent_log(sent_log)
    LOGGER.info("완료 | Polygon API %d회", api_calls)


if __name__ == "__main__":
    main()
