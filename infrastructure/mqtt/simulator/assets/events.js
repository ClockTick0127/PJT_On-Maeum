    // 이벤트 로그 및 CSV 내보내기
    function addEventRow(sec, type, device, power, desc) {
      const tbody = document.getElementById('eventTableBody');
      const firstChild = tbody && tbody.children && tbody.children[0];
      const rowText = firstChild ? (firstChild.innerText || firstChild.textContent || '') : '';
      if (tbody && tbody.children.length === 1 && rowText.includes('시뮬레이션을 시작하면')) {
        tbody.innerHTML = '';
      }

      let badgeClass = 'status-normal';
      if (type.includes('피크') || type.includes('이상치')) badgeClass = 'status-peak';
      else if (type.includes('고장')) badgeClass = 'status-fault';
      else if (type.includes('켜짐') || type.includes('수동')) badgeClass = 'status-running';
      else if (type.includes('루틴')) badgeClass = 'status-missed';

      let displayPower = "—";
      if (typeof power === 'number') {
        displayPower = `${power.toFixed(0)} W`;
      } else if (power !== null && power !== undefined) {
        displayPower = String(power);
      }

      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td><strong>T+${String(sec).padStart(2, '0')}s</strong></td>
        <td><span class="badge-status ${badgeClass}">${type}</span></td>
        <td><strong>${device}</strong></td>
        <td style="color:#b8490d; font-weight:600;">${displayPower}</td>
        <td style="color:#44403c;">${desc}</td>
      `;
      tbody.insertBefore(tr, tbody.firstChild);
    }

    function getCsvContentForHouse(house) {
      const targetHouse = house || observedHouse;
      const hist = getOrCreateHistory(targetHouse);
      if (!hist || hist.times.length === 0) {
        return null;
      }

      const headers = ["t_time", "house", "active_power", "reactive_power", "apparent_power", "power_factor", "voltage", "current", "active_devices"];
      let csvContent = "\uFEFF" + headers.join(",") + "\r\n";

      for (let i = 0; i < hist.times.length; i++) {
        if (hist.activeP[i] === null || hist.activeP[i] === undefined) {
          continue; // 결측 행은 CSV에 기록하지 않음 (오직 실제 계측치만 포함)
        }
        const row = [
          hist.times[i],
          targetHouse,
          hist.activeP[i],
          hist.reactiveP[i],
          hist.apparentS[i],
          hist.pfs[i],
          hist.voltages[i],
          hist.currents[i],
          `"${(hist.activeLabels[i] || '').replace(/"/g, '""')}"`
        ];
        csvContent += row.join(",") + "\r\n";
      }
      return csvContent;
    }

    function downloadCsv() {
      const csvContent = getCsvContentForHouse(observedHouse);
      if (!csvContent) {
        alert(`관찰 가구(${observedHouse})의 내보낼 시뮬레이션 데이터가 없습니다.`);
        return;
      }

      const hist = getOrCreateHistory(observedHouse);
      const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      const timestamp = new Date().toISOString().slice(0, 19).replace(/[-:T]/g, "");
      a.href = url;
      a.download = `nilm_live_sim_${observedHouse}_${hist.times.length}s_${timestamp}.csv`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    }

