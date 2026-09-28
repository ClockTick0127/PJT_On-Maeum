import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App";
import QueryProvider from "./providers/QueryProvider";
import "./styles/global.css";
import AgentationClient from "./utils/AgentationClient";

if ("serviceWorker" in navigator) {
  window.addEventListener("load", () => {
    void navigator.serviceWorker.register("/sw.js").catch((error: unknown) => {
      console.error("알림 서비스 워커를 등록하지 못했습니다.", error);
    });
  });
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <QueryProvider>
      <App />
      <AgentationClient />
    </QueryProvider>
  </StrictMode>,
);
