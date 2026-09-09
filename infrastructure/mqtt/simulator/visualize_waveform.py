"""
NILM 전력 데이터 시뮬레이터 파형 시각화 도구 (Waveform Visualizer)

시뮬레이터(simulator.py)가 생성하는 다변량 교류 전력 데이터(유효전력, 무효전력, 역률, 전류, 전압)와
가전 기동 시의 돌입전류(Inrush), 인덕션 듀티 사이클, 냉장고 기저부하 파형을
인터랙티브 웹 차트로 시각화하여 브라우저에서 바로 확인할 수 있도록 렌더링합니다.

사용법:
    python visualize_waveform.py               # 데모 시나리오 (6대 가전 기동 파형 300초 시뮬레이션)
    python visualize_waveform.py --seconds 600 # 600초(10분) 시뮬레이션
    python visualize_waveform.py --random      # 순수 확률 기반 랜덤 시뮬레이션
"""

import os
import sys
import csv
import json
import math
import random
import argparse
import webbrowser
from datetime import datetime, timezone

# simulator 모듈 import 경로 설정
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.append(CURRENT_DIR)

import simulator


def export_to_csv(records: list[dict], filepath: str) -> None:
    """시뮬레이션 생성 데이터를 Excel 호환 CSV (UTF-8-BOM)로 내보내기"""
    with open(filepath, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "t_sec", "active_power", "reactive_power", "apparent_power",
            "power_factor", "voltage", "current", "active_devices"
        ])
        for r in records:
            dev_str = ", ".join(r["devices"]) if r["devices"] else "대기(기저부하)"
            writer.writerow([
                r["t"], r["active_power"], r["reactive_power"], r["apparent_power"],
                r["power_factor"], r["voltage"], r["current"], dev_str
            ])



def generate_demo_timeline(total_seconds: int = 300) -> list[dict]:
    """
    AI 모델(TCN) 299초 윈도우 검증을 위한 6대 가전 대표 기동 시나리오 파형 생성
    - t=15s: 전기포트 (1,650W 저항성 단일 블록)
    - t=70s: 전자레인지 (950W 마그네트론 돌입 피크)
    - t=130s: 인덕션 조리 세션 (1,450W 가열 ↔ 서모스탯 휴지 듀티 사이클 반복)
    - t=230s: 진공청소기 (820W, 기동 시 1.50배 강한 모터 돌입 피크 및 역률 저하)
    """
    # 상태 초기화
    house = "H001"
    simulator.house_states[house] = {
        "voltage": 220.0,
        "base_nominal_w": 50.0,
        "base_current_w": 50.0,
        "fridge_active": True,
        "fridge_remaining_sec": 9999,
        "fridge_nominal_w": 85.0,
        "fridge_pf": 0.78,
    }
    for dev in simulator.DEVICE_PROFILES:
        simulator.device_states[house][dev] = {
            "state": "OFF",
            "session_remaining": 0,
            "inrush_remaining": 0,
            "nominal_w": 0.0,
            "nominal_pf": 0.0,
            "is_heating": False,
            "duty_remaining": 0
        }

    records = []

    for t in range(total_seconds):
        # 데모 시나리오 스케줄링 (원하는 시점에 가전 강제 기동)
        if t == 15:
            d = simulator.device_states[house]["kettle"]
            d["state"] = "STARTING"
            d["session_remaining"] = 90
            d["inrush_remaining"] = 1
            d["nominal_w"] = 1650.0
            d["nominal_pf"] = 0.99

        elif t == 65:
            d = simulator.device_states[house]["microwave"]
            d["state"] = "STARTING"
            d["session_remaining"] = 50
            d["inrush_remaining"] = 2
            d["nominal_w"] = 950.0
            d["nominal_pf"] = 0.91

        elif t == 130:
            d = simulator.device_states[house]["induction"]
            d["state"] = "STARTING"
            d["session_remaining"] = 80
            d["inrush_remaining"] = 2
            d["nominal_w"] = 1450.0
            d["nominal_pf"] = 0.93
            d["is_heating"] = True
            d["duty_remaining"] = 25

        elif t == 230:
            d = simulator.device_states[house]["vacuum_cleaner"]
            d["state"] = "STARTING"
            d["session_remaining"] = 55
            d["inrush_remaining"] = 2
            d["nominal_w"] = 820.0
            d["nominal_pf"] = 0.80

        # 지표 계산
        m = simulator.calculate_main_panel_metrics(house)
        records.append({
            "t": t,
            "active_power": m["active_power"],
            "reactive_power": m["reactive_power"],
            "apparent_power": m["apparent_power"],
            "power_factor": m["power_factor"],
            "voltage": m["voltage"],
            "current": m["current"],
            "devices": list(m["active_devices"])
        })

    return records


def generate_random_timeline(total_seconds: int = 300) -> list[dict]:
    """순수 확률 기반 시뮬레이터 파형 생성"""
    house = "H001"
    records = []
    for t in range(total_seconds):
        m = simulator.calculate_main_panel_metrics(house)
        records.append({
            "t": t,
            "active_power": m["active_power"],
            "reactive_power": m["reactive_power"],
            "apparent_power": m["apparent_power"],
            "power_factor": m["power_factor"],
            "voltage": m["voltage"],
            "current": m["current"],
            "devices": list(m["active_devices"])
        })
    return records


def build_html_report(records: list[dict], mode_name: str) -> str:
    """인터랙티브 파형 시각화 HTML 대시보드 생성"""
    times = [r["t"] for r in records]
    active_powers = [r["active_power"] for r in records]
    reactive_powers = [r["reactive_power"] for r in records]
    apparent_powers = [r["apparent_power"] for r in records]
    power_factors = [r["power_factor"] for r in records]
    currents = [r["current"] for r in records]
    voltages = [r["voltage"] for r in records]
    active_labels = [", ".join(r["devices"]) if r["devices"] else "대기(기저부하)" for r in records]

    # 평균 소비전력 계산
    avg_p = round(sum(active_powers) / len(active_powers), 1)

    # 가전 동작 이벤트 구간 추출 (AI 정답 라벨 타임라인)
    events = []
    current_event = None
    for r in records:
        dev_str = ", ".join(r["devices"])
        if dev_str:
            if not current_event or current_event["devices"] != dev_str:
                if current_event:
                    current_event["end"] = r["t"] - 1
                    events.append(current_event)
                current_event = {
                    "start": r["t"],
                    "end": r["t"],
                    "devices": dev_str,
                    "peak_power": r["active_power"]
                }
            else:
                current_event["end"] = r["t"]
                current_event["peak_power"] = max(current_event["peak_power"], r["active_power"])
        else:
            if current_event:
                current_event["end"] = r["t"] - 1
                events.append(current_event)
                current_event = None
    if current_event:
        events.append(current_event)

    events_json = json.dumps(events, ensure_ascii=False)
    times_json = json.dumps(times)
    active_p_json = json.dumps(active_powers)
    reactive_p_json = json.dumps(reactive_powers)
    apparent_p_json = json.dumps(apparent_powers)
    pf_json = json.dumps(power_factors)
    current_json = json.dumps(currents)
    voltage_json = json.dumps(voltages)
    active_labels_json = json.dumps(active_labels, ensure_ascii=False)

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>NILM AI 4특징 파형 시각화 대시보드</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  <style>
    :root {{
      --bg-dark: #0f172a;
      --card-bg: #1e293b;
      --border: #334155;
      --text-main: #f8fafc;
      --text-muted: #94a3b8;
    }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Pretendard", sans-serif;
      background: var(--bg-dark);
      color: var(--text-main);
      padding: 24px;
      line-height: 1.5;
    }}
    .header {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 24px;
      border-bottom: 1px solid var(--border);
      padding-bottom: 16px;
    }}
    .title h1 {{
      font-size: 1.5rem;
      font-weight: 700;
      background: linear-gradient(135deg, #38bdf8, #818cf8);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
    }}
    .title p {{
      color: var(--text-muted);
      font-size: 0.88rem;
      margin-top: 4px;
    }}
    .badge {{
      display: inline-block;
      padding: 6px 14px;
      border-radius: 9999px;
      font-size: 0.8rem;
      font-weight: 600;
      background: rgba(56, 189, 248, 0.12);
      color: #38bdf8;
      border: 1px solid rgba(56, 189, 248, 0.3);
    }}
    .btn-download {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      background: linear-gradient(135deg, #0284c7, #2563eb);
      color: #ffffff;
      border: 1px solid rgba(56, 189, 248, 0.4);
      padding: 7px 16px;
      border-radius: 9999px;
      font-size: 0.82rem;
      font-weight: 600;
      cursor: pointer;
      box-shadow: 0 2px 8px rgba(2, 132, 199, 0.3);
      transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
    }}
    .btn-download:hover {{
      background: linear-gradient(135deg, #0ea5e9, #3b82f6);
      transform: translateY(-1px);
      box-shadow: 0 4px 14px rgba(14, 165, 233, 0.45);
    }}
    .grid-stats {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 16px;
      margin-bottom: 24px;
    }}
    .stat-card {{
      background: var(--card-bg);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 16px 20px;
    }}
    .stat-card .label {{
      font-size: 0.78rem;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }}
    .stat-card .value {{
      font-size: 1.6rem;
      font-weight: 700;
      margin-top: 6px;
    }}
    .chart-container {{
      background: var(--card-bg);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 20px;
      margin-bottom: 24px;
    }}
    .chart-header {{
      margin-bottom: 16px;
    }}
    .chart-title {{
      font-size: 1.05rem;
      font-weight: 600;
      color: var(--text-main);
    }}
    .chart-desc {{
      font-size: 0.82rem;
      color: var(--text-muted);
      margin-top: 3px;
    }}
    .table-container {{
      background: var(--card-bg);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 20px;
      overflow-x: auto;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 0.88rem;
    }}
    th, td {{
      padding: 12px 16px;
      text-align: left;
      border-bottom: 1px solid var(--border);
    }}
    th {{
      color: var(--text-muted);
      font-weight: 600;
      background: rgba(15, 23, 42, 0.4);
    }}
    tr:hover {{
      background: rgba(255, 255, 255, 0.02);
    }}
    .tag {{
      display: inline-block;
      padding: 3px 8px;
      border-radius: 4px;
      font-size: 0.75rem;
      font-weight: 600;
    }}
    .tag-kettle {{ background: rgba(245, 158, 11, 0.2); color: #fbbf24; border: 1px solid rgba(245, 158, 11, 0.4); }}
    .tag-microwave {{ background: rgba(168, 85, 247, 0.2); color: #c084fc; border: 1px solid rgba(168, 85, 247, 0.4); }}
    .tag-induction {{ background: rgba(244, 63, 94, 0.2); color: #fb7185; border: 1px solid rgba(244, 63, 94, 0.4); }}
    .tag-vacuum {{ background: rgba(6, 182, 212, 0.2); color: #22d3ee; border: 1px solid rgba(6, 182, 212, 0.4); }}
    .tag-default {{ background: rgba(59, 130, 246, 0.2); color: #60a5fa; border: 1px solid rgba(59, 130, 246, 0.4); }}
  </style>
</head>
<body>

  <div class="header">
    <div class="title">
      <h1>NILM AI 4특징 파형 뷰어</h1>
      <p>AI 모델(TCN/Seq2Point) 299초 입력 다변량 4피처(P, Q, PF, I) 및 가전 기동 이벤트</p>
    </div>
    <div style="display: flex; gap: 12px; align-items: center;">
      <button id="btnDownloadCsv" onclick="downloadCsv()" class="btn-download" title="현재 시뮬레이션 데이터를 CSV 파일로 다운로드합니다">
        <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">
          <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
          <polyline points="7 10 12 15 17 10"></polyline>
          <line x1="12" y1="15" x2="12" y2="3"></line>
        </svg>
        CSV 데이터 다운로드
      </button>
      <span class="badge">{mode_name} ({len(times)}초)</span>
    </div>
  </div>

  <!-- AI 관점 핵심 상태 카드 -->
  <div class="grid-stats">
    <div class="stat-card">
      <div class="label">입력 윈도우 크기</div>
      <div class="value" style="color: #38bdf8;">{len(times)} <span style="font-size: 0.95rem; font-weight: 400;">초 (1Hz)</span></div>
    </div>
    <div class="stat-card">
      <div class="label">AI 모델 입력 피처</div>
      <div class="value" style="color: #818cf8;">4 <span style="font-size: 0.95rem; font-weight: 400;">종 (P, Q, PF, I)</span></div>
    </div>
    <div class="stat-card">
      <div class="label">가전 기동 이벤트</div>
      <div class="value" style="color: #10b981;">{len(events)} <span style="font-size: 0.95rem; font-weight: 400;">건 (Ground Truth)</span></div>
    </div>
    <div class="stat-card">
      <div class="label">평균 소비전력</div>
      <div class="value" style="color: #f59e0b;">{avg_p:,.1f} <span style="font-size: 0.95rem; font-weight: 400;">W</span></div>
    </div>
  </div>

  <!-- 차트 1: 전력 파형 (AI 피처 1 & 2) -->
  <div class="chart-container">
    <div class="chart-header">
      <div class="chart-title">1. 전력 시계열 파형: 유효전력(P) & 무효전력(Q)</div>
      <div class="chart-desc">AI 피처 1 (Active Power) & 피처 2 (Reactive Power): 가전 On/Off 전력 스텝 및 모터·코일 유도성 무효 성분</div>
    </div>
    <div style="height: 320px;">
      <canvas id="chartPower"></canvas>
    </div>
  </div>

  <!-- 차트 2: 부하 특성 파형 (AI 피처 3 & 4) -->
  <div class="chart-container">
    <div class="chart-header">
      <div class="chart-title">2. 부하 특성 파형: 부하 전류(I) & 역률(PF)</div>
      <div class="chart-desc">AI 피처 3 (Power Factor) & 피처 4 (Current): 모터 가전 기동 돌입전류(Inrush) 피크 및 순수 저항체 vs 유도 모터 구분 역률</div>
    </div>
    <div style="height: 280px;">
      <canvas id="chartSecondary"></canvas>
    </div>
  </div>

  <!-- 가전 기동 이벤트 목록 -->
  <div class="table-container">
    <div style="margin-bottom: 12px; font-size: 1.05rem; font-weight: 600;">3. 가전 기동 이벤트 타임라인 (AI 모델 정답 레이블 구간)</div>
    <table>
      <thead>
        <tr>
          <th>시작 (t)</th>
          <th>종료 (t)</th>
          <th>지속시간</th>
          <th>가동 가전</th>
          <th>피크 전력 (W)</th>
          <th>AI 물리 파형 특성</th>
        </tr>
      </thead>
      <tbody id="eventTableBody">
      </tbody>
    </table>
  </div>

  <script>
    const times = {times_json};
    const activeP = {active_p_json};
    const reactiveP = {reactive_p_json};
    const apparentP = {apparent_p_json};
    const pfData = {pf_json};
    const currentData = {current_json};
    const voltageData = {voltage_json};
    const activeLabels = {active_labels_json};
    const events = {events_json};

    // 1. 메인 전력 차트 (P, Q)
    const ctxPower = document.getElementById('chartPower').getContext('2d');
    new Chart(ctxPower, {{
      type: 'line',
      data: {{
        labels: times.map(t => t + 's'),
        datasets: [
          {{
            label: '유효전력 (Active Power P, W)',
            data: activeP,
            borderColor: '#38bdf8',
            backgroundColor: 'rgba(56, 189, 248, 0.08)',
            fill: true,
            tension: 0.15,
            borderWidth: 2,
            pointRadius: 0
          }},
          {{
            label: '무효전력 (Reactive Power Q, var)',
            data: reactiveP,
            borderColor: '#f59e0b',
            borderWidth: 1.8,
            fill: false,
            tension: 0.15,
            pointRadius: 0
          }}
        ]
      }},
      options: {{
        responsive: true,
        maintainAspectRatio: false,
        interaction: {{ mode: 'index', intersect: false }},
        plugins: {{
          legend: {{ labels: {{ color: '#cbd5e1' }} }},
          tooltip: {{
            callbacks: {{
              afterTitle: function(items) {{
                const idx = items[0].dataIndex;
                return '가전 상태: ' + activeLabels[idx];
              }}
            }}
          }}
        }},
        scales: {{
          x: {{
            grid: {{ color: 'rgba(51, 65, 85, 0.4)' }},
            ticks: {{ color: '#94a3b8', maxTicksLimit: 20 }}
          }},
          y: {{
            title: {{ display: true, text: '전력 (W / var)', color: '#94a3b8' }},
            grid: {{ color: 'rgba(51, 65, 85, 0.4)' }},
            ticks: {{ color: '#94a3b8' }}
          }}
        }}
      }}
    }});

    // 2. 부하 특성 차트 (I, PF 이중 Y축)
    const ctxSec = document.getElementById('chartSecondary').getContext('2d');
    new Chart(ctxSec, {{
      type: 'line',
      data: {{
        labels: times.map(t => t + 's'),
        datasets: [
          {{
            label: '부하 전류 (Current I, A)',
            data: currentData,
            borderColor: '#f43f5e',
            borderWidth: 2,
            pointRadius: 0,
            yAxisID: 'yCurrent'
          }},
          {{
            label: '역률 (Power Factor PF)',
            data: pfData,
            borderColor: '#10b981',
            borderWidth: 1.8,
            pointRadius: 0,
            yAxisID: 'yPf'
          }}
        ]
      }},
      options: {{
        responsive: true,
        maintainAspectRatio: false,
        interaction: {{ mode: 'index', intersect: false }},
        plugins: {{
          legend: {{ labels: {{ color: '#cbd5e1' }} }}
        }},
        scales: {{
          x: {{
            grid: {{ color: 'rgba(51, 65, 85, 0.4)' }},
            ticks: {{ color: '#94a3b8', maxTicksLimit: 20 }}
          }},
          yCurrent: {{
            type: 'linear',
            position: 'left',
            title: {{ display: true, text: '부하 전류 (A)', color: '#f43f5e' }},
            grid: {{ color: 'rgba(51, 65, 85, 0.4)' }},
            ticks: {{ color: '#f43f5e' }}
          }},
          yPf: {{
            type: 'linear',
            position: 'right',
            min: 0.6,
            max: 1.05,
            title: {{ display: true, text: '역률 (PF)', color: '#10b981' }},
            grid: {{ drawOnChartArea: false }},
            ticks: {{ color: '#10b981' }}
          }}
        }}
      }}
    }});

    // 3. 이벤트 테이블 렌더링
    const tbody = document.getElementById('eventTableBody');
    if (events.length === 0) {{
      tbody.innerHTML = '<tr><td colspan="6" style="text-align:center; color:#94a3b8;">기간 내 가동된 수동 가전이 없습니다 (상시 기저부하 가동).</td></tr>';
    }} else {{
      events.forEach(ev => {{
        let tagClass = 'tag-default';
        let note = '정상 가동';
        if (ev.devices.includes('전기포트')) {{ tagClass = 'tag-kettle'; note = '순수 저항성 히터: 고역률 (PF ~0.99) 단일 블록'; }}
        else if (ev.devices.includes('전자레인지')) {{ tagClass = 'tag-microwave'; note = '마그네트론 고주파 기동 피크 (PF ~0.91)'; }}
        else if (ev.devices.includes('인덕션')) {{ tagClass = 'tag-induction'; note = '서모스탯 듀티 사이클 펄스 반복 (PF ~0.93)'; }}
        else if (ev.devices.includes('진공 청소기')) {{ tagClass = 'tag-vacuum'; note = '고속 직류모터 기동 돌입전류 피크 (1.50x) + 역률 저하 (PF ~0.80)'; }}

        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td><strong>${{ev.start}}초</strong></td>
          <td>${{ev.end}}초</td>
          <td>${{ev.end - ev.start + 1}}초 (${{((ev.end - ev.start + 1)/60).toFixed(1)}}분)</td>
          <td><span class="tag ${{tagClass}}">${{ev.devices}}</span></td>
          <td style="color:#38bdf8; font-weight:600;">${{ev.peak_power.toFixed(1)}} W</td>
          <td style="color:#94a3b8;">${{note}}</td>
        `;
        tbody.appendChild(tr);
      }});
    }}

    // CSV 파일 다운로드 기능 (Excel 한글 호환 UTF-8 BOM)
    function downloadCsv() {{
      const headers = [
        "t_sec",
        "active_power",
        "reactive_power",
        "apparent_power",
        "power_factor",
        "voltage",
        "current",
        "active_devices"
      ];

      let csvContent = "\\uFEFF" + headers.join(",") + "\\r\\n";

      for (let i = 0; i < times.length; i++) {{
        const row = [
          times[i],
          activeP[i],
          reactiveP[i],
          apparentP[i],
          pfData[i],
          voltageData[i],
          currentData[i],
          `"${{activeLabels[i].replace(/"/g, '""')}}"`
        ];
        csvContent += row.join(",") + "\\r\\n";
      }}

      const blob = new Blob([csvContent], {{ type: "text/csv;charset=utf-8;" }});
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      const timestamp = new Date().toISOString().slice(0, 19).replace(/[-:T]/g, "");
      a.href = url;
      a.download = `nilm_sim_waveform_${{times.length}}s_${{timestamp}}.csv`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    }}
  </script>
</body>
</html>
"""
    return html


def main():
    parser = argparse.ArgumentParser(description="NILM 시뮬레이터 파형 시각화 도구")
    parser.add_argument("--seconds", type=int, default=300, help="시뮬레이션 시간(초) (기본값: 300초)")
    parser.add_argument("--random", action="store_true", help="데모 시나리오 대신 순수 확률 랜덤 시뮬레이션 실행")
    parser.add_argument("--no-browser", action="store_true", help="브라우저 자동 열기 비활성화")
    parser.add_argument("--csv", nargs="?", const="waveform_data.csv", help="생성된 시뮬레이션 데이터를 CSV 파일로도 함께 저장 (기본 파일명: waveform_data.csv)")
    args = parser.parse_args()

    print(f"[1/3] 시뮬레이터 파형 데이터 생성 중... (총 {args.seconds}초)", flush=True)

    if args.random:
        mode_name = "확률 기반 랜덤 시뮬레이션"
        records = generate_random_timeline(args.seconds)
    else:
        mode_name = "AI 모델 검증용 대표 데모 시나리오 (전기포트·전자레인지·인덕션·청소기)"
        records = generate_demo_timeline(args.seconds)

    print(f"[2/3] 인터랙티브 HTML 대시보드 리포트 작성 중...", flush=True)
    html_content = build_html_report(records, mode_name)

    output_path = os.path.join(CURRENT_DIR, "waveform_viewer.html")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"[3/3] 파형 리포트 저장 완료: {output_path}")

    if args.csv:
        csv_filename = args.csv if args.csv else "waveform_data.csv"
        csv_path = os.path.abspath(csv_filename) if os.path.isabs(csv_filename) else os.path.join(CURRENT_DIR, csv_filename)
        export_to_csv(records, csv_path)
        print(f"[+] CSV 데이터 파일 저장 완료: {csv_path}")

    if not args.no_browser:
        print(" 웹 브라우저에서 파형 뷰어를 실행합니다...")
        webbrowser.open(f"file:///{output_path}")


if __name__ == "__main__":
    main()
