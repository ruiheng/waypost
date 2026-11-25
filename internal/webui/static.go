package webui

const indexHTML = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>waypost roundtable</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f4ef;
      --ink: #24211d;
      --muted: #756f66;
      --line: #d8d2c7;
      --panel: #fffdf8;
      --accent: #0f766e;
      --accent-soft: #d9f3ee;
      --warn: #9f3a2f;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      min-height: 100vh;
    }
    button, select {
      font: inherit;
    }
    .shell {
      display: grid;
      grid-template-columns: 280px minmax(0, 1fr);
      min-height: 100vh;
    }
    aside {
      border-right: 1px solid var(--line);
      padding: 22px 18px;
      background: #ebe6dc;
    }
    .brand {
      font-size: 15px;
      font-weight: 700;
      margin-bottom: 22px;
    }
    .group-list {
      display: grid;
      gap: 6px;
    }
    .load-more {
      margin-top: 10px;
      border: 1px solid var(--line);
      border-radius: 5px;
      background: var(--panel);
      color: var(--ink);
      cursor: pointer;
      padding: 8px 10px;
    }
    .group-button {
      width: 100%;
      border: 0;
      background: transparent;
      color: var(--ink);
      text-align: left;
      padding: 9px 10px;
      border-radius: 6px;
      cursor: pointer;
      overflow-wrap: anywhere;
    }
    .group-button:hover, .group-button.active {
      background: var(--panel);
    }
    main {
      display: grid;
      grid-template-rows: auto minmax(0, 1fr);
      min-width: 0;
    }
    header {
      border-bottom: 1px solid var(--line);
      padding: 18px 28px;
      background: rgba(255, 253, 248, 0.82);
      backdrop-filter: blur(16px);
      position: sticky;
      top: 0;
      z-index: 2;
    }
    .title-row {
      display: flex;
      gap: 14px;
      align-items: center;
      justify-content: space-between;
    }
    h1 {
      margin: 0;
      font-size: 22px;
      line-height: 1.2;
      overflow-wrap: anywhere;
    }
    .status {
      color: var(--muted);
      font-size: 13px;
      white-space: nowrap;
    }
    .status.live::before {
      content: "";
      display: inline-block;
      width: 7px;
      height: 7px;
      margin-right: 7px;
      border-radius: 50%;
      background: var(--accent);
      box-shadow: 0 0 0 5px var(--accent-soft);
    }
    .meta {
      color: var(--muted);
      margin-top: 6px;
      font-size: 13px;
    }
    .timeline {
      overflow-y: auto;
      padding: 28px;
    }
    .empty, .error {
      color: var(--muted);
      padding: 40px 0;
      max-width: 560px;
    }
    .error { color: var(--warn); }
    .inline-error {
      color: var(--warn);
      font-size: 13px;
      margin-top: 8px;
      overflow-wrap: anywhere;
    }
    .message {
      max-width: 920px;
      padding: 14px 0;
      animation: rise 160ms ease-out;
    }
    .message-header {
      display: grid;
      grid-template-columns: minmax(150px, 0.9fr) minmax(0, 1.6fr) minmax(148px, auto) auto;
      gap: 14px;
      align-items: start;
      padding: 12px 14px;
      background: #e9e2d7;
      border: 1px solid var(--line);
      border-bottom: 0;
      border-radius: 6px 6px 0 0;
    }
    .message-body {
      position: relative;
      padding: 16px 14px 18px;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 0 0 6px 6px;
    }
    .copy-button {
      position: absolute;
      top: 10px;
      right: 10px;
      border: 1px solid var(--line);
      border-radius: 5px;
      background: #f6f4ef;
      color: var(--muted);
      cursor: pointer;
      font-size: 12px;
      line-height: 1;
      padding: 6px 8px;
    }
    .copy-button:hover {
      color: var(--ink);
      border-color: #bfb6a8;
    }
    .header-field {
      min-width: 0;
    }
    .header-label {
      color: var(--muted);
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0;
      text-transform: uppercase;
      margin-bottom: 4px;
    }
    .header-value {
      color: var(--ink);
      font-size: 13px;
      font-weight: 650;
      overflow-wrap: anywhere;
    }
    .header-detail {
      color: var(--muted);
      font-size: 12px;
      margin-top: 5px;
      overflow-wrap: anywhere;
    }
    .body {
      white-space: pre-wrap;
      overflow-wrap: anywhere;
      line-height: 1.52;
      font-size: 15px;
      padding-right: 62px;
    }
    .read {
      white-space: nowrap;
    }
    @keyframes rise {
      from { opacity: 0; transform: translateY(6px); }
      to { opacity: 1; transform: translateY(0); }
    }
    @media (max-width: 760px) {
      .shell { grid-template-columns: 1fr; }
      aside {
        border-right: 0;
        border-bottom: 1px solid var(--line);
        padding: 14px;
      }
      .group-list {
        display: flex;
        overflow-x: auto;
      }
      .group-button {
        white-space: nowrap;
        width: auto;
      }
      header, .timeline { padding-left: 16px; padding-right: 16px; }
      .message-header {
        grid-template-columns: 1fr;
        gap: 8px;
      }
      .read { white-space: normal; }
      .title-row {
        align-items: flex-start;
        flex-direction: column;
      }
    }
  </style>
</head>
<body>
  <div class="shell">
    <aside>
      <div class="brand">waypost</div>
      <div id="groups" class="group-list"></div>
      <button id="more-groups" class="load-more" type="button" hidden>More groups</button>
      <div id="groups-error" class="inline-error" role="alert" hidden></div>
    </aside>
    <main>
      <header>
        <div class="title-row">
          <h1 id="title">Roundtable transcript</h1>
          <div id="status" class="status">Loading</div>
        </div>
        <div id="meta" class="meta"></div>
      </header>
      <section id="timeline" class="timeline">
        <div id="history-messages"></div>
        <button id="more-messages" class="load-more" type="button" hidden>More messages</button>
        <div id="messages-error" class="inline-error" role="alert" hidden></div>
        <div id="live-messages"></div>
      </section>
    </main>
  </div>
  <script>
    const defaultGroup = {{DEFAULT_GROUP}};
    const groupsEl = document.querySelector("#groups");
    const titleEl = document.querySelector("#title");
    const metaEl = document.querySelector("#meta");
    const statusEl = document.querySelector("#status");
    const timelineEl = document.querySelector("#timeline");
    const historyMessagesEl = document.querySelector("#history-messages");
    const liveMessagesEl = document.querySelector("#live-messages");
    const moreGroupsEl = document.querySelector("#more-groups");
    const moreMessagesEl = document.querySelector("#more-messages");
    const groupsErrorEl = document.querySelector("#groups-error");
    const messagesErrorEl = document.querySelector("#messages-error");
    let selectedGroup = defaultGroup || "";
    let source = null;
    const rendered = new Set();
    const groups = [];
    let groupsCursor = "";
    let groupsPageLoading = false;
    let transcriptCursor = "";
    let latestMessageID = "";
    let transcriptGeneration = 0;
    let transcriptRequestController = null;
    let moreMessagesController = null;

    function setStatus(text, live) {
      statusEl.textContent = text;
      statusEl.classList.toggle("live", Boolean(live));
    }

    function showInlineError(el, message) {
      if (!el) return;
      el.textContent = message;
      el.hidden = false;
    }

    function clearInlineError(el) {
      if (!el) return;
      el.textContent = "";
      el.hidden = true;
    }

    async function responseErrorMessage(response, fallback) {
      const text = await response.text();
      if (!text) return fallback;
      try {
        const parsed = JSON.parse(text);
        if (parsed && typeof parsed.error === "string" && parsed.error) return parsed.error;
      } catch (_) {}
      return text.trim() || fallback;
    }

    function groupURL(group, suffix) {
      return "/api/groups/" + encodeURIComponent(group) + "/" + suffix;
    }

    async function loadGroups(cursor) {
      if (groupsPageLoading) return;
      groupsPageLoading = true;
      moreGroupsEl.disabled = true;
      clearInlineError(groupsErrorEl);
      const initialLoad = !cursor;
      try {
        const response = await fetch("/api/groups" + (cursor ? "?cursor=" + encodeURIComponent(cursor) : ""));
        if (!response.ok) throw new Error(await responseErrorMessage(response, "Unable to load groups."));
        const payload = await response.json();
        groups.push(...(payload.items || []));
        groupsCursor = payload.next_cursor || "";
        moreGroupsEl.hidden = !groupsCursor;
        if (!selectedGroup && groups.length) selectedGroup = groups[0].address;
        renderGroups(groups);
        if (initialLoad) {
          if (selectedGroup) await loadTranscript(selectedGroup);
          else {
            setStatus("No groups", false);
            resetTimeline();
            showTimelineNotice("empty", "No group wayposts exist yet.");
          }
        }
      } finally {
        groupsPageLoading = false;
        moreGroupsEl.disabled = false;
      }
    }

    function renderGroups(groups) {
      groupsEl.innerHTML = "";
      for (const group of groups) {
        const button = document.createElement("button");
        button.className = "group-button" + (group.address === selectedGroup ? " active" : "");
        button.textContent = group.address;
        button.addEventListener("click", () => {
          selectedGroup = group.address;
          renderGroups(groups);
          loadTranscript(selectedGroup);
        });
        groupsEl.appendChild(button);
      }
    }

    async function loadTranscript(group) {
      const generation = ++transcriptGeneration;
      cancelTranscriptRequests();
      const controller = new AbortController();
      transcriptRequestController = controller;
      closeSource();
      rendered.clear();
      transcriptCursor = "";
      latestMessageID = "";
      titleEl.textContent = group;
      metaEl.textContent = "";
      resetTimeline();
      setStatus("Loading", false);
      try {
        const response = await fetch(groupURL(group, "transcript"), {signal: controller.signal});
        if (!isCurrentTranscript(group, generation)) return;
        if (!response.ok) {
          const detail = await responseErrorMessage(response, "Unable to load transcript.");
          if (!isCurrentTranscript(group, generation)) return;
          setStatus("Error", false);
          showTimelineNotice("error", detail);
          return;
        }
        const payload = await response.json();
        if (!isCurrentTranscript(group, generation)) return;
        const messages = payload.items || [];
        transcriptCursor = payload.next_cursor || "";
        latestMessageID = payload.latest_message_id || "";
        for (const message of messages) appendMessage(message, historyMessagesEl, false);
        if (!messages.length) showTimelineNotice("empty", "No messages yet.");
        moreMessagesEl.hidden = !transcriptCursor;
        metaEl.textContent = messages.length + " messages loaded";
        startEvents(group, latestMessageID, generation);
      } catch (error) {
        if (error.name !== "AbortError" && isCurrentTranscript(group, generation)) {
          setStatus("Error", false);
          showTimelineNotice("error", error.message || "Unable to load transcript.");
        }
      } finally {
        if (transcriptRequestController === controller) transcriptRequestController = null;
      }
    }

    async function loadMoreMessages() {
      if (!selectedGroup || !transcriptCursor || moreMessagesController) return;
      const group = selectedGroup;
      const cursor = transcriptCursor;
      const generation = transcriptGeneration;
      const controller = new AbortController();
      moreMessagesController = controller;
      moreMessagesEl.disabled = true;
      clearInlineError(messagesErrorEl);
      try {
        const response = await fetch(groupURL(group, "transcript") + "?cursor=" + encodeURIComponent(cursor), {signal: controller.signal});
        if (!isCurrentTranscript(group, generation)) return;
        if (!response.ok) throw new Error(await responseErrorMessage(response, "Unable to load more messages."));
        const payload = await response.json();
        if (!isCurrentTranscript(group, generation)) return;
        transcriptCursor = payload.next_cursor || "";
        for (const message of (payload.items || [])) appendMessage(message, historyMessagesEl, false);
        moreMessagesEl.hidden = !transcriptCursor;
        metaEl.textContent = rendered.size + " messages loaded";
      } catch (error) {
        if (error.name !== "AbortError" && isCurrentTranscript(group, generation)) {
          showInlineError(messagesErrorEl, error.message || "Unable to load more messages.");
          if (!source) {
            setStatus("Error", false);
          }
        }
      } finally {
        if (moreMessagesController === controller) {
          moreMessagesController = null;
          moreMessagesEl.disabled = false;
        }
      }
    }

    function isCurrentTranscript(group, generation) {
      return selectedGroup === group && transcriptGeneration === generation;
    }

    function cancelTranscriptRequests() {
      if (transcriptRequestController) transcriptRequestController.abort();
      if (moreMessagesController) moreMessagesController.abort();
      transcriptRequestController = null;
      moreMessagesController = null;
      moreMessagesEl.disabled = false;
    }

    function startEvents(group, after, generation) {
      closeSource();
      if (!isCurrentTranscript(group, generation)) return;
      const url = groupURL(group, "events") + "?after=" + encodeURIComponent(after || "");
      const eventSource = new EventSource(url);
      source = eventSource;
      eventSource.addEventListener("open", () => {
        if (source === eventSource && isCurrentTranscript(group, generation)) setStatus("Live", true);
      });
      eventSource.addEventListener("message", (event) => {
        if (source !== eventSource || !isCurrentTranscript(group, generation)) return;
        const message = JSON.parse(event.data);
        removeTimelineNotice();
        appendMessage(message, liveMessagesEl, true);
        metaEl.textContent = rendered.size + " messages loaded";
      });
      eventSource.addEventListener("error", (event) => {
        if (source !== eventSource || !isCurrentTranscript(group, generation)) return;
        if ((event && event.data) || eventSource.readyState === EventSource.CLOSED) {
          if (event && event.data) {
            let detail = String(event.data);
            try {
              const parsed = JSON.parse(event.data);
              if (parsed && typeof parsed.error === "string" && parsed.error) detail = parsed.error;
            } catch (_) {}
            showInlineError(messagesErrorEl, detail);
          }
          closeSource();
          setStatus("Error", false);
          return;
        }
        setStatus("Reconnecting", false);
      });
    }

    function closeSource() {
      if (source) source.close();
      source = null;
    }

    function resetTimeline() {
      historyMessagesEl.innerHTML = "";
      liveMessagesEl.innerHTML = "";
      moreMessagesEl.hidden = true;
      moreMessagesEl.disabled = false;
      clearInlineError(messagesErrorEl);
      removeTimelineNotice();
    }

    function showTimelineNotice(className, text) {
      removeTimelineNotice();
      const notice = document.createElement("div");
      notice.className = className + " timeline-notice";
      notice.textContent = text;
      historyMessagesEl.appendChild(notice);
    }

    function removeTimelineNotice() {
      const notice = timelineEl.querySelector(".timeline-notice");
      if (notice) notice.remove();
    }

    function appendMessage(message, target, scroll) {
      if (rendered.has(message.message_id)) return;
      rendered.add(message.message_id);
      const row = document.createElement("article");
      row.className = "message";
      const when = new Date(message.message_created_at);
      row.innerHTML = [
        '<div class="message-header">',
        '<div class="header-field">',
          '<div class="header-label">From</div>',
          '<div class="header-value from"></div>',
          '<div class="header-detail via"></div>',
        '</div>',
        '<div class="header-field">',
          '<div class="header-label">Subject</div>',
          '<div class="header-value subject"></div>',
        '</div>',
        '<div class="header-field">',
          '<div class="header-label">Time</div>',
          '<div class="header-value time"></div>',
        '</div>',
        '<div class="header-field read">',
          '<div class="header-label">Read</div>',
          '<div class="header-value read-value"></div>',
        '</div>',
        '</div>',
        '<div class="message-body">',
          '<button class="copy-button" type="button">Copy</button>',
          '<div class="body"></div>',
        '</div>',
      ].join("");
      row.querySelector(".from").textContent = message.display_sender || "unknown";
      const via = senderVia(message);
      row.querySelector(".via").textContent = via ? "Via: " + via : "";
      row.querySelector(".time").textContent = formatTimestamp(when, message.message_created_at);
      row.querySelector(".subject").textContent = message.subject || "(no subject)";
      row.querySelector(".body").textContent = message.body || "";
      row.querySelector(".read-value").textContent = message.read_count + "/" + message.eligible_count;
      row.querySelector(".copy-button").addEventListener("click", (event) => {
        copyBody(message.body || "", event.currentTarget);
      });
      target.appendChild(row);
      if (scroll) timelineEl.scrollTop = timelineEl.scrollHeight;
    }

    function senderVia(message) {
      if (!message.forwarded_from_address || !message.sender_address) return "";
      if (message.forwarded_from_address === message.sender_address) return "";
      return message.sender_address;
    }

    function formatTimestamp(date, fallback) {
      if (isNaN(date)) return fallback || "";
      const pad = (value) => String(value).padStart(2, "0");
      return [
        date.getFullYear(),
        "-",
        pad(date.getMonth() + 1),
        "-",
        pad(date.getDate()),
        " ",
        pad(date.getHours()),
        ":",
        pad(date.getMinutes()),
        ":",
        pad(date.getSeconds())
      ].join("");
    }

    async function copyBody(text, button) {
      try {
        if (navigator.clipboard && window.isSecureContext) {
          await navigator.clipboard.writeText(text);
        } else {
          fallbackCopy(text);
        }
        flashCopyButton(button, "Copied");
      } catch (error) {
        flashCopyButton(button, "Failed");
      }
    }

    function fallbackCopy(text) {
      const textarea = document.createElement("textarea");
      textarea.value = text;
      textarea.setAttribute("readonly", "");
      textarea.style.position = "fixed";
      textarea.style.top = "-1000px";
      document.body.appendChild(textarea);
      textarea.select();
      document.execCommand("copy");
      textarea.remove();
    }

    function flashCopyButton(button, label) {
      if (!button.dataset.defaultLabel) button.dataset.defaultLabel = button.textContent;
      if (button._copyResetTimer) window.clearTimeout(button._copyResetTimer);
      button.textContent = label;
      button._copyResetTimer = window.setTimeout(() => {
        button.textContent = button.dataset.defaultLabel;
        button._copyResetTimer = 0;
      }, 1100);
    }

    moreGroupsEl.addEventListener("click", () => loadGroups(groupsCursor).catch((error) => {
      showInlineError(groupsErrorEl, error.message || "Unable to load more groups.");
      if (!source) setStatus("Error", false);
    }));
    moreMessagesEl.addEventListener("click", () => loadMoreMessages());

    loadGroups().catch((error) => {
      setStatus("Error", false);
      resetTimeline();
      showTimelineNotice("error", error.message);
    });
  </script>
</body>
</html>`
