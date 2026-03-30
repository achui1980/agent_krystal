"""
Krystal v2.0 - Intelligent ETL Testing Framework
基于 CrewAI 的智能 ETL 测试系统

新特性：
- 3-Agent 协作架构（ETLOperator → ResultValidator → ReportWriter）
- 3次自动重试机制
- 精确行级文件对比
- 双格式报告（Markdown + HTML 科技绿主题）
- LLM 智能差异分析

作者: Krystal Team
版本: 2.0.0-alpha
日期: 2026-02-01
"""

__version__ = "2.0.0-alpha"
__author__ = "Krystal Team"

from .crews.etl_test_crew import ETLTestCrew

__all__ = ["ETLTestCrew", "__version__"]
