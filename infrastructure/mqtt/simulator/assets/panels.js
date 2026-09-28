    // ==========================================
    // 섹션 접기/펼치기 공통 로직
    // ==========================================
    // 접을 수 있는 섹션은 아래 목록에 { 본문 id, 토글 버튼 id }로 등록한다.
    // 새 섹션을 추가하려면 HTML에 섹션 헤더(.section-head)와 본문 id를 두고 이 목록에 한 줄만 더하면 된다.
    // 접힌 상태는 localStorage에 유지하되, 저장소를 못 쓰는 환경(시크릿 모드 등)에서는
    // 조회/저장 실패를 무시하고 항상 펼친 상태를 기본값으로 사용한다.
    const PANEL_STORAGE_PREFIX = 'panelCollapsed:';
    const COLLAPSIBLE_PANELS = [
      { bodyId: 'e2ePanelBody', toggleId: 'e2eBtnTogglePanel' },
      { bodyId: 'monitoringBody', toggleId: 'monitoringToggleBtn' },
    ];

    function applyPanelCollapsed(bodyId, collapsed) {
      const panel = COLLAPSIBLE_PANELS.find(p => p.bodyId === bodyId);
      if (!panel) return;

      const body = document.getElementById(bodyId);
      if (body) body.style.display = collapsed ? 'none' : '';

      // 버튼 아이콘(빈 번개/채운 번개)은 CSS가 aria-expanded를 보고 교체한다.
      const toggle = document.getElementById(panel.toggleId);
      if (toggle) {
        toggle.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
      }
    }

    function togglePanel(bodyId) {
      const body = document.getElementById(bodyId);
      if (!body) return;
      const collapsed = (body.style.display !== 'none');
      applyPanelCollapsed(bodyId, collapsed);

      // 접힌 채로 로드된 섹션 안의 차트는 0x0으로 남으므로, 펼칠 때 크기 재계산 훅을 호출한다.
      if (!collapsed && typeof onPanelExpanded === 'function') {
        onPanelExpanded(bodyId);
      }

      try {
        localStorage.setItem(PANEL_STORAGE_PREFIX + bodyId, collapsed ? '1' : '0');
      } catch (_) { }
    }

    function restorePanelStates() {
      COLLAPSIBLE_PANELS.forEach(panel => {
        let collapsed = false;
        try {
          collapsed = (localStorage.getItem(PANEL_STORAGE_PREFIX + panel.bodyId) === '1');
        } catch (_) { }
        applyPanelCollapsed(panel.bodyId, collapsed);
      });
    }

    restorePanelStates();
