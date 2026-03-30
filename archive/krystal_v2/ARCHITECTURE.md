                                   ┌─────────────────────────────────────────────────────────────┐
                                   │                    Krystal v2.0 Architecture                  │
                                   │                    Intelligent ETL Testing                    │
                                   └─────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                        User Layer                                                          │
│                                                                                                                          │
│  ┌───────────────────┐         ┌───────────────────┐         ┌───────────────────┐                                       │
│  │   CLI Interface   │────────▶│   ConfigManager   │────────▶│  Service Config   │                                       │
│  │   (krystal_v2)    │         │   (existing)      │         │  (YAML files)     │                                       │
│  └───────────────────┘         └───────────────────┘         └───────────────────┘                                       │
│           │                                                                                                                │
│           ▼                                                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────────────────┐                                 │
│  │                              ETLTestCrew.run()                                        │                                 │
│  │                          ┌──────────────────┐                                       │                                 │
│  │                          │  self.test_id    │                                       │                                 │
│  │                          │  execute_etl()   │                                       │                                 │
│  │                          │  validate()      │                                       │                                 │
│  │                          │  generate()      │                                       │                                 │
│  │                          └──────────────────┘                                       │                                 │
│  └──────────────────────────────────────────────────────────────────────────────────────┘                                 │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
                                                             │
                                                             ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                              ETL Execution Layer                                                         │
│                                                                                                                          │
│   ┌──────────────────┐          ┌──────────────────┐          ┌──────────────────┐          ┌──────────────────┐         │
│   │  Step 1: Upload  │────────▶│ Step 2: Trigger  │────────▶│ Step 3: Poll     │────────▶│ Step 4: Download │         │
│   │                  │          │                  │          │                  │          │                  │         │
│   │  ┌────────────┐  │          │  ┌────────────┐  │          │  ┌────────────┐  │          │  ┌────────────┐  │         │
│   │  │ SFTPClient │  │          │  │ APIClient  │  │          │  │  Polling   │  │          │  │ SFTPClient │  │         │
│   │  │   Tool     │  │          │  │   Tool     │  │          │  │   Service  │  │          │  │   Tool     │  │         │
│   │  └────────────┘  │          │  └────────────┘  │          │  └────────────┘  │          │  └────────────┘  │         │
│   │        │         │          │        │         │          │        │         │          │        │         │         │
│   │        ▼         │          │        ▼         │          │        ▼         │          │        ▼         │         │
│   │  @network_retry  │          │  @network_retry  │          │  @network_retry  │          │  @network_retry  │         │
│   │  (3 attempts,    │          │  (3 attempts,    │          │  (3 attempts,    │          │  (3 attempts,    │         │
│   │   2s-10s backoff)│          │   2s-10s backoff)│          │   2s-10s backoff)│          │   2s-10s backoff)│         │
│   └────────┬─────────┘          └────────┬─────────┘          └────────┬─────────┘          └────────┬─────────┘         │
│            │                            │                            │                            │                     │
│            ▼                            ▼                            ▼                            ▼                     │
│   ┌──────────────────────────────────────────────────────────────────────────────────────────────────────┐              │
│   │                                    ETLExecutor.execute_full_etl()                                     │              │
│   │                                                                                                       │              │
│   │   Results: {                                                                                          │              │
│   │     success: True/False,                                                                              │              │
│   │     steps: {                                                                                          │              │
│   │       upload: {success, duration, size},                                                              │              │
│   │       trigger: {success, task_id, duration},                                                          │              │
│   │       poll: {success, status, attempts, duration},                                                    │              │
│   │       download: {success, duration, size}                                                             │              │
│   │     },                                                                                                │              │
│   │     total_duration: float,                                                                            │              │
│   │     result_file: path                                                                                 │              │
│   │   }                                                                                                   │              │
│   └──────────────────────────────────────────────────────────────────────────────────────────────────────┘              │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
                                                             │
                                                             ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                               Validation Layer                                                           │
│                                                                                                                          │
│   ┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐                │
│   │                              ResultValidator._validate_results()                                     │                │
│   │                                                                                                      │                │
│   │   Input:                                                                                             │                │
│   │     - expected_file (user provided)                                                                 │                │
│   │     - actual_file (from ETLExecutor)                                                                 │                │
│   │                                                                                                      │                │
│   │   Processing:                                                                                        │                │
│   │     ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐                            │                │
│   │     │ Read Files       │───▶│ Line-by-Line     │───▶│ Build Statistics │                            │                │
│   │     │                  │    │ Comparison       │    │                  │                            │                │
│   │     │ expected_lines[] │    │                  │    │ total_rows       │                            │                │
│   │     │ actual_lines[]   │    │ for i in range:  │    │ matching_rows    │                            │                │
│   │     │                  │    │   if match:      │    │ different_rows   │                            │                │
│   │     └──────────────────┘    │     matching++   │    │ similarity       │                            │                │
│   │                             │   else:          │    │ differences[]    │                            │                │
│   │                             │     different++  │    │                  │                            │                │
│   │                             │     record diff  │    └──────────────────┘                            │                │
│   │                             └──────────────────┘                                                     │                │
│   │                                                                                                      │                │
│   │   Output:                                                                                            │                │
│   │     {                                                                                                │                │
│   │       match: True/False,                                                                             │                │
│   │       statistics: {total, matching, different, similarity},                                          │                │
│   │       differences: [{row, expected, actual}, ...]                                                    │                │
│   │     }                                                                                                │                │
│   └─────────────────────────────────────────────────────────────────────────────────────────────────────┘                │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
                                                             │
                                                             ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                             Reporting Layer (CrewAI Ready)                                               │
│                                                                                                                          │
│   ┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐                │
│   │                              ReportGenerator.generate_both_formats()                                 │                │
│   │                                                                                                      │                │
│   │   ┌───────────────────────┐                          ┌───────────────────────┐                     │                │
│   │   │   Markdown Report     │                          │   HTML Report         │                     │                │
│   │   │                       │                          │                       │                     │                │
│   │   │   • Test Summary      │                          │   • Tech-Green Theme  │                     │                │
│   │   │   • ETL Steps Table   │                          │   • ETL Timeline      │                     │                │
│   │   │   • Validation Stats  │                          │   • Validation Charts │                     │                │
│   │   │   • Diff Table (MD)   │                          │   • Diff Highlight    │                     │                │
│   │   │   • LLM Analysis      │                          │   • LLM Analysis      │                     │                │
│   │   │     (placeholder)     │                          │   • Interactive       │                     │                │
│   │   │                       │                          │                       │                     │                │
│   │   │   Output: .md file    │                          │   Output: .html file  │                     │                │
│   │   └───────────────────────┘                          └───────────────────────┘                     │                │
│   │                                                                                                      │                │
│   │   Template:                                                                                          │                │
│   │   krystal_v2/templates/report_template.html (Jinja2)                                                 │                │
│   └─────────────────────────────────────────────────────────────────────────────────────────────────────┘                │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
                                                             │
                                                             ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                External Services                                                         │
│                                                                                                                          │
│   ┌────────────────────────────────┐    ┌────────────────────────────────┐    ┌────────────────────────────────┐          │
│   │     SFTP Server                │    │     API Service                │    │     OpenAI API                 │          │
│   │                                │    │                                │    │                                │          │
│   │   • Upload input files         │    │   • Receive trigger requests   │    │   • CrewAI LLM (optional)      │          │
│   │   • Store processing results   │    │   • Return task_id             │    │   • Report analysis (future)   │          │
│   │   • Download output files      │    │   • Status polling endpoint    │    │   • Intelligent insights       │          │
│   │                                │    │   • Process batch jobs         │    │                                │          │
│   │   Local: localhost:2223        │    │   Local: localhost:8000        │    │   Required for v1, optional v2 │          │
│   └────────────────────────────────┘    └────────────────────────────────┘    └────────────────────────────────┘          │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘


┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                             CrewAI Agent Architecture (Design)                                           │
│                                                                                                                          │
│   ┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐                │
│   │                                    ETLTestCrew (CrewAI Version)                                    │                │
│   │                                                                                                      │                │
│   │   Process: Sequential + Planning + Memory                                                            │                │
│   │                                                                                                      │                │
│   │        ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                                   │                │
│   │        │              │     │              │     │              │                                   │                │
│   │        │  ETL         │────▶│  Result      │────▶│  Report      │                                   │                │
│   │        │  Operator    │     │  Validator   │     │  Writer      │                                   │                │
│   │        │  Agent       │     │  Agent       │     │  Agent       │                                   │                │
│   │        │              │     │              │     │              │                                   │                │
│   │        │ Role: ETL    │     │ Role: Data   │     │ Role: Report │                                   │                │
│   │        │       Expert │     │       Auditor│     │       Author │                                   │                │
│   │        │ Goal: Execute│     │ Goal: Verify │     │ Goal: Document│                                   │                │
│   │        │  ETL Flow    │     │  Data Quality│     │  Findings    │                                   │                │
│   │        │              │     │              │     │              │                                   │                │
│   │        │ Tools:       │     │ Tools:       │     │ Tools:       │                                   │                │
│   │        │ • SFTPUpload │     │ • FileCompare│     │ • MDGenerator│                                   │                │
│   │        │ • APITrigger │     │ • DiffStats  │     │ • HTMLGen    │                                   │                │
│   │        │ • PollStatus │     │              │     │ • LLMAnalysis│                                   │                │
│   │        │ • SFTPDownload│    │              │     │              │                                   │                │
│   │        │              │     │              │     │              │                                   │                │
│   │        │ Retry: 3x    │     │              │     │              │                                   │                │
│   │        │ Backoff:     │     │              │     │              │                                   │                │
│   │        │ 2s-10s exp   │     │              │     │              │                                   │                │
│   │        └──────────────┘     └──────────────┘     └──────────────┘                                   │                │
│   │                                                                                                      │                │
│   └─────────────────────────────────────────────────────────────────────────────────────────────────────┘                │
│                                                                                                                          │
│   Note: Current implementation uses simplified direct execution for speed and stability.                                 │
│         CrewAI agents are defined but not fully orchestrated (hybrid approach).                                          │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘


Key Features:
════════════
✅ 3-Step ETL Flow: Upload → Trigger → Poll → Download
✅ 3x Automatic Retry: Exponential backoff (2s → 4s → 10s)
✅ Line-by-Line Validation: Precise comparison with detailed diffs
✅ Dual-Format Reports: Markdown + HTML with tech-green theme
✅ CrewAI Ready: Agent definitions prepared for future LLM orchestration
✅ Local E2E Testing: Full integration with Docker/Podman services

Architecture Pattern:
═════════════════════
Simplified Hybrid: Direct code execution + Agent definitions
Benefits: Fast execution, stable operation, ready for LLM enhancement
Trade-off: Less autonomous decision-making compared to full CrewAI
