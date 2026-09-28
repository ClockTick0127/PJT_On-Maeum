    // 시계열 데이터 버퍼 및 Chart.js 초기화
    const historyData = {
      times: [], activeP: [], reactiveP: [], apparentS: [],
      currents: [], pfs: [], voltages: [], activeLabels: []
    };

    // ==========================================
    // 2. Chart.js 인스턴스 초기화
    // ==========================================
    const ctxMain = document.getElementById('chartMain').getContext('2d');
    const chartMain = new Chart(ctxMain, {
      type: 'line',
      data: {
        labels: [],
        datasets: [
          {
            label: '유효전력 (Active P, W)',
            data: [],
            borderColor: '#f37929',
            backgroundColor: 'rgba(243, 121, 41, 0.1)',
            borderWidth: 2.2,
            fill: true,
            tension: 0.15,
            pointRadius: 0,
            spanGaps: false
          },
          {
            label: '피상전력 (Apparent S, VA)',
            data: [],
            borderColor: '#2563eb',
            borderWidth: 1.8,
            borderDash: [4, 4],
            fill: false,
            tension: 0.15,
            pointRadius: 0,
            spanGaps: false
          },
          {
            label: '무효전력 (Reactive Q, var)',
            data: [],
            borderColor: '#78716c',
            borderWidth: 1.5,
            fill: false,
            tension: 0.15,
            pointRadius: 0,
            spanGaps: false
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: { duration: 0 },
        interaction: { mode: 'index', intersect: false },
        plugins: {
          legend: { labels: { color: '#44403c' } }
        },
        scales: {
          x: {
            grid: { color: 'rgba(214, 211, 209, 0.4)' },
            ticks: { color: '#78716c', maxTicksLimit: 15 }
          },
          y: {
            grid: { color: 'rgba(214, 211, 209, 0.4)' },
            ticks: { color: '#78716c' },
            // 데이터 범위에 맞춰 자동 스케일링 (대기전력 ~55W 구간에서도 파형이 보이도록 상한 고정 제거)
            beginAtZero: true,
            grace: '8%'
          }
        }
      }
    });

    const ctxSec = document.getElementById('chartSecondary').getContext('2d');
    const chartSecondary = new Chart(ctxSec, {
      type: 'line',
      data: {
        labels: [],
        datasets: [
          {
            label: '부하 전류 (Current I, A)',
            data: [],
            borderColor: '#be123c',
            borderWidth: 2,
            pointRadius: 0,
            yAxisID: 'yCurrent',
            spanGaps: false
          },
          {
            label: '역률 (Power Factor PF)',
            data: [],
            borderColor: '#065f46',
            borderWidth: 1.8,
            pointRadius: 0,
            yAxisID: 'yPf',
            spanGaps: false
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: { duration: 0 },
        interaction: { mode: 'index', intersect: false },
        plugins: {
          legend: { labels: { color: '#44403c' } }
        },
        scales: {
          x: {
            grid: { color: 'rgba(214, 211, 209, 0.4)' },
            ticks: { color: '#78716c', maxTicksLimit: 15 }
          },
          yCurrent: {
            type: 'linear',
            position: 'left',
            title: { display: true, text: '부하 전류 (A)', color: '#be123c' },
            grid: { color: 'rgba(214, 211, 209, 0.4)' },
            ticks: { color: '#be123c' }
          },
          yPf: {
            type: 'linear',
            position: 'right',
            min: 0.6,
            max: 1.05,
            title: { display: true, text: '역률 (PF)', color: '#065f46' },
            grid: { drawOnChartArea: false },
            ticks: { color: '#065f46' }
          }
        }
      }
    });

    // display:none 상태에서 생성된 Chart.js 캔버스는 0x0으로 고정되어 펼쳐도 그려지지 않는다.
    // 모니터링 영역이 펼쳐질 때 panels.js가 이 훅을 호출하면 두 차트의 크기를 다시 계산한다.
    function onPanelExpanded(bodyId) {
      if (bodyId !== 'monitoringBody') return;
      [chartMain, chartSecondary].forEach(chart => {
        if (chart && typeof chart.resize === 'function') {
          chart.resize();
        }
      });
    }
