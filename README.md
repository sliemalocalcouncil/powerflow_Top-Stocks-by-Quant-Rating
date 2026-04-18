
# PowerFlow Intraday Scanner v6 (Trigger Edition)

v5를 장중 **실행형 트리거 스캐너** 쪽으로 더 밀어붙인 버전입니다.  
기존의 30분봉 + 일봉 MTF 구조는 유지하되, 아래 요소를 새로 반영했습니다.

## 핵심 개선점

| 항목 | v5 | v6 |
|---|---|---|
| Opening Range Breakout | 없음 | **추가** |
| Session VWAP / VWAP reclaim | 없음 | **추가** |
| Premarket High Break | 없음 | **추가** |
| First Pullback after Breakout | 없음 | **추가** |
| Relative Volume Since Open | 없음 | **추가** |
| 부분 일봉(current day partial) 배제 | 없음 | **추가** |
| 구조적 트리거 필수화 | 약함 | **강화** |
| 과열/추격 필터 | 약함 | **강화** |

## 이 버전이 정확도를 높이는 방식

v6는 단순히 “장중에 강하다”를 찾는 대신, 아래 3가지를 동시에 보도록 바꿨습니다.

1. **구조적 트리거**
   - ORB
   - VWAP reclaim
   - premarket high break
   - breakout 후 first pullback
   - HH / BOX / squeeze release

2. **활동성 확인**
   - MF_Z
   - 절대 거래량 spike
   - 슬롯 RVOL
   - **시가 이후 누적 RVOL**

3. **품질 / 추격 억제**
   - VWAP 위 유지
   - 일봉 EMA stack
   - upper wick / body / close-high 품질
   - VWAP / breakout level 대비 과열 이격 제한

즉 v5보다 **“실행 가능한 구조가 있는 강한 종목”** 쪽으로 더 좁혀집니다.

## 주요 트리거 설명

### 1) ORB
첫 30분 high 돌파를 체크합니다.  
첫 30분 range가 형성된 뒤 그 위로 실제 종가 기준 돌파가 나올 때만 이벤트로 인식합니다.

### 2) VWAP reclaim
직전 봉이 VWAP 아래였고 현재 봉이 VWAP를 다시 회복하면서,  
low가 VWAP 근처를 찍고 CLV가 개선된 경우만 잡습니다.

### 3) Premarket High Break
04:00~09:30 ET 프리마켓 high를 계산한 뒤,  
정규장 봉이 그 위를 실제로 돌파하는지를 체크합니다.

### 4) First Pullback
HH/BOX/ORB/PMH 돌파가 먼저 나온 뒤,  
다음 1~3개 봉 안에서 VWAP / EMA9 / OR high / PM high 근처 첫 눌림 후 재지지되는 경우를 잡습니다.

### 5) Relative Volume Since Open
같은 시간대 단일 봉 거래량만 보는 대신,  
개장 이후 지금까지 누적 거래량이 과거 N일 같은 시점 평균 대비 얼마나 강한지 봅니다.

## 실행 스케줄 (한국시간)

워크플로우는 v5와 동일하게 **미국 정규장 동안 30분마다** 실행됩니다.

| 미국 장 시각 (ET) | UTC | 한국시간 (KST) — EDT | 한국시간 (KST) — EST |
|:---:|:---:|:---:|:---:|
| 09:30 | 13:30 / 14:30 | **22:30** | **23:30** |
| 10:00 | 14:00 / 15:00 | 23:00 | 00:00 (+1) |
| 10:30 | 14:30 / 15:30 | 23:30 | 00:30 (+1) |
| ... | ... | ... | ... |
| 15:30 | 19:30 / 20:30 | 04:30 (+1) | 05:30 (+1) |
| 16:00 | 20:00 / 21:00 | **05:00 (+1)** | **06:00 (+1)** |

## 수동 실행 옵션

Actions 탭 → **PowerFlow Intraday Scanner v6 (Trigger)** → Run workflow

| 옵션 | 기본값 | 설명 |
|---|:---:|---|
| `force_run` | false | 장시간 체크 무시 |
| `announce_empty` | false | 신호 0건일 때도 요약 알림 |
| `top_n` | 20 | 상위 N개만 알림 |
| `scan_last_n_bars` | 2 | 최근 몇 개 완료봉 재검사 |

## 파일 구조

```text
.
├── powerflow_scanner_v6.py
├── requirements.txt
├── tickers.txt
├── README.md
├── .github/workflows/
│   └── powerflow_intraday.yml
├── sent_signals.json           # 자동 생성
└── results/
    ├── scan_YYYY-MM-DD.csv
    ├── latest_scan.txt
    └── run_meta.json
```

## 로컬 테스트

```bash
export POLYGON_API_KEY="your_key"
export TELEGRAM_BOT_TOKEN="your_bot_token"
export TELEGRAM_CHAT_ID="your_chat_id"
export FORCE_RUN=true
export ANNOUNCE_EMPTY=true

pip install -r requirements.txt
python powerflow_scanner_v6.py
```

## 튜닝 포인트

코드 상단 `SignalConfig`에서 조절하면 됩니다.

- 신호가 너무 많다
  - `score_*` 상향
  - `session_rvol_normal`, `slot_rvol_normal`, `mf_z_normal` 상향
  - `max_vwap_extension` 하향

- 신호가 너무 적다
  - 위 값을 반대로 완화
  - `require_structural_trigger=False` 로 테스트

- 눌림 진입 신호를 더 넓히고 싶다
  - `pullback_lookback` 증가
  - `pullback_touch_tolerance` 소폭 상향

## 주의사항

- 이 버전도 **30분봉 완료 후** 평가합니다. 초단타 tick scanner는 아닙니다.
- Premarket high는 Polygon 30분봉 extended-hours 데이터 품질에 의존합니다.
- 현재 미국 휴장일 캘린더는 별도 반영하지 않았습니다.
- GitHub Actions cron은 수 분 지연될 수 있으므로 `scan_last_n_bars=2` 기본값을 유지하는 편이 안전합니다.
