# cline-llm_proxy

A lightweight, zero-dependency local proxy designed to sit between the **Cline VS Code extension** and a **Cloudflare AI Gateway** endpoint. It intercepts, sanitizes, and corrects malformed GLM native function-calling tokens, converting them into standard XML formats that Cline's internal parser can successfully execute.

---

## 🚀 Key Features

* **Zero-Dependency Node.js Server:** Lightweight and efficient proxy running locally on port `7650`.
* **Full Header Pass-Through:** Seamlessly forwards all headers, including `Authorization`, `cf-aig-*`, `x-api-key`, etc.
* **SSE Stream Sanitization:** Utilizes a rolling-buffer chunk system to safely parse and repair broken tokens across stream boundaries.
* **Regex-Based XML Correction:**
    * Transforms `<tool_call>name>` → `<name>`
    * Strips redundant `<arg_value>` and `</arg_value>` tags
* **Smart Pass-Through:** Automatically detects non-SSE (Server-Sent Events) payloads and pipes them with zero transformation.
* **Robust Error Handling:** Features built-in `EADDRINUSE` handling and clean, graceful startup logs.

---

## 🛠️ Getting Started

### Prerequisites
* [Node.js](https://nodejs.org/) (v16 or higher recommended)

### Installation
1. Clone this repository or copy the core script.
2. Ensure `cline-llm_proxy.js` is in your root directory.

### Running the Proxy
Start the local proxy by running:

node cline-llm_proxy.js
