"""
Report Writer Agent - 测试报告撰写专家
负责生成专业、详细的测试报告，包含LLM智能分析
"""

import os
import logging
from datetime import datetime
from typing import Dict, Any, List
from crewai import Agent
from jinja2 import Environment, FileSystemLoader


logger = logging.getLogger(__name__)


class ReportWriterAgent:
    """
    测试报告撰写专家

    职责：
    1. 生成专业的Markdown格式测试报告
    2. 生成美观的HTML格式测试报告
    3. 使用Jinja2模板渲染HTML
    4. 包含LLM智能差异分析
    5. 将报告保存到./reports_v2/目录
    """

    @staticmethod
    def create(llm=None, environment_context: str = "") -> Agent:
        """
        创建报告撰写Agent

        Args:
            llm: LLM模型
            environment_context: 环境上下文信息

        Returns:
            Agent实例
        """
        return Agent(
            role="测试报告撰写专家",
            goal="生成专业、详细的测试报告，包含LLM智能分析，支持Markdown和HTML两种格式",
            backstory=f"""你是一位资深的测试报告撰写专家，擅长生成清晰、专业的测试报告。
            
            你的专业能力：
            1. 生成Markdown格式报告：简洁、易读，适合快速查看和版本控制
            2. 生成HTML格式报告：美观、交互性强，使用Jinja2模板渲染
            3. 整合测试结果数据：测试概览、执行时间线、对比统计、差异详情
            4. 利用LLM进行智能差异分析：深入解读差异原因，提供改进建议
            5. 将报告保存到./reports_v2/目录，按时间戳命名
            
            你注重报告的可读性和专业性，确保所有关键信息一目了然。
            你的报告是团队协作和质量追溯的重要依据。
            
            {environment_context}
            """,
            verbose=True,
            allow_delegation=False,
            llm=llm,
        )

    @staticmethod
    def generate_markdown_report(
        test_id: str,
        service_name: str,
        environment: str,
        total_duration: float,
        statistics: Dict[str, Any],
        comparison_rows: List[Dict[str, Any]],
        etl_steps: List[Dict[str, Any]],
        llm_analysis: str = "",
        output_dir: str = "./reports_v2",
    ) -> Dict[str, Any]:
        """
        生成Markdown格式测试报告

        Args:
            test_id: 测试ID
            service_name: 服务名称
            environment: 环境名称
            total_duration: 总执行时长
            statistics: 对比统计信息
            comparison_rows: 行级对比结果
            etl_steps: ETL执行步骤列表
            llm_analysis: LLM智能分析内容
            output_dir: 报告输出目录

        Returns:
            报告生成结果
        """
        logger.info(f"📝 开始生成Markdown报告: {test_id}")

        try:
            # 确保输出目录存在
            os.makedirs(output_dir, exist_ok=True)

            # 计算整体通过状态
            overall_pass = statistics.get("different_rows", 0) == 0

            # 构建Markdown内容
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            markdown_content = f"""# Krystal ETL Test Report

## 测试概览

| 项目 | 值 |
|------|-----|
| 测试ID | {test_id} |
| 服务名称 | {service_name} |
| 环境 | {environment} |
| 执行时间 | {timestamp} |
| 总耗时 | {total_duration:.2f}s |
| 整体状态 | {"✅ PASSED" if overall_pass else "❌ FAILED"} |

## 对比统计

| 指标 | 数值 |
|------|-----|
| 总行数 | {statistics.get("total_rows", 0)} |
| 匹配行数 | {statistics.get("matching_rows", 0)} |
| 差异行数 | {statistics.get("different_rows", 0)} |
| 相似度 | {statistics.get("similarity", 0)}% |

## ETL执行时间线

| 步骤 | 状态 | 耗时 |
|------|------|------|
"""

            for step in etl_steps:
                status = "✅" if step.get("success", False) else "❌"
                markdown_content += f"| {step.get('name', 'Unknown')} | {status} | {step.get('duration', 0)}s |\n"

            # 添加差异详情
            diff_rows = [row for row in comparison_rows if not row.get("match", True)]
            if diff_rows:
                markdown_content += "\n## 差异详情\n\n| 行号 | 预期内容 | 实际内容 |\n|------|----------|----------|\n"
                for row in diff_rows[:50]:  # 最多显示50行差异
                    markdown_content += f"| {row.get('row_number', 0)} | `{row.get('expected', '')}` | `{row.get('actual', '')}` |\n"

                if len(diff_rows) > 50:
                    markdown_content += (
                        f"\n*... 还有 {len(diff_rows) - 50} 行差异未显示 ...*\n"
                    )

            # 添加LLM分析
            if llm_analysis:
                markdown_content += f"\n## 🤖 AI 智能分析\n\n{llm_analysis}\n"

            # 保存文件
            filename = f"report_{test_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
            filepath = os.path.join(output_dir, filename)

            with open(filepath, "w", encoding="utf-8") as f:
                f.write(markdown_content)

            logger.info(f"✅ Markdown报告已生成: {filepath}")

            return {
                "success": True,
                "filepath": filepath,
                "format": "markdown",
                "test_id": test_id,
            }

        except Exception as e:
            logger.error(f"❌ Markdown报告生成失败: {e}")
            return {
                "success": False,
                "error": str(e),
                "test_id": test_id,
            }

    @staticmethod
    def generate_html_report(
        test_id: str,
        service_name: str,
        environment: str,
        total_duration: float,
        statistics: Dict[str, Any],
        comparison_rows: List[Dict[str, Any]],
        etl_steps: List[Dict[str, Any]],
        llm_analysis: str = "",
        output_dir: str = "./reports_v2",
        template_dir: str = None,
    ) -> Dict[str, Any]:
        """
        生成HTML格式测试报告

        Args:
            test_id: 测试ID
            service_name: 服务名称
            environment: 环境名称
            total_duration: 总执行时长
            statistics: 对比统计信息
            comparison_rows: 行级对比结果
            etl_steps: ETL执行步骤列表
            llm_analysis: LLM智能分析内容
            output_dir: 报告输出目录
            template_dir: 模板文件目录

        Returns:
            报告生成结果
        """
        logger.info(f"🌐 开始生成HTML报告: {test_id}")

        try:
            # 确保输出目录存在
            os.makedirs(output_dir, exist_ok=True)

            # 计算整体通过状态
            overall_pass = statistics.get("different_rows", 0) == 0

            # 设置模板目录
            if template_dir is None:
                template_dir = os.path.join(
                    os.path.dirname(__file__), "..", "templates"
                )

            # 加载模板
            env = Environment(loader=FileSystemLoader(template_dir))
            template = env.get_template("report_template.html")

            # 渲染模板
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            html_content = template.render(
                test_id=test_id,
                service_name=service_name,
                environment=environment,
                total_duration=f"{total_duration:.2f}",
                timestamp=timestamp,
                overall_pass=overall_pass,
                total_rows=statistics.get("total_rows", 0),
                matching_rows=statistics.get("matching_rows", 0),
                different_rows=statistics.get("different_rows", 0),
                similarity=statistics.get("similarity", 0),
                etl_steps=etl_steps,
                comparison_rows=comparison_rows,
                llm_analysis=llm_analysis,
            )

            # 保存文件
            filename = (
                f"report_{test_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
            )
            filepath = os.path.join(output_dir, filename)

            with open(filepath, "w", encoding="utf-8") as f:
                f.write(html_content)

            logger.info(f"✅ HTML报告已生成: {filepath}")

            return {
                "success": True,
                "filepath": filepath,
                "format": "html",
                "test_id": test_id,
            }

        except Exception as e:
            logger.error(f"❌ HTML报告生成失败: {e}")
            return {
                "success": False,
                "error": str(e),
                "test_id": test_id,
            }

    @staticmethod
    def generate_both_reports(
        test_id: str,
        service_name: str,
        environment: str,
        total_duration: float,
        statistics: Dict[str, Any],
        comparison_rows: List[Dict[str, Any]],
        etl_steps: List[Dict[str, Any]],
        llm_analysis: str = "",
        output_dir: str = "./reports_v2",
    ) -> Dict[str, Any]:
        """
        同时生成Markdown和HTML两种格式的报告

        Args:
            test_id: 测试ID
            service_name: 服务名称
            environment: 环境名称
            total_duration: 总执行时长
            statistics: 对比统计信息
            comparison_rows: 行级对比结果
            etl_steps: ETL执行步骤列表
            llm_analysis: LLM智能分析内容
            output_dir: 报告输出目录

        Returns:
            报告生成结果
        """
        logger.info(f"📊 开始生成完整报告: {test_id}")

        results = {
            "test_id": test_id,
            "markdown": None,
            "html": None,
        }

        # 生成Markdown报告
        md_result = ReportWriterAgent.generate_markdown_report(
            test_id=test_id,
            service_name=service_name,
            environment=environment,
            total_duration=total_duration,
            statistics=statistics,
            comparison_rows=comparison_rows,
            etl_steps=etl_steps,
            llm_analysis=llm_analysis,
            output_dir=output_dir,
        )
        results["markdown"] = md_result

        # 生成HTML报告
        html_result = ReportWriterAgent.generate_html_report(
            test_id=test_id,
            service_name=service_name,
            environment=environment,
            total_duration=total_duration,
            statistics=statistics,
            comparison_rows=comparison_rows,
            etl_steps=etl_steps,
            llm_analysis=llm_analysis,
            output_dir=output_dir,
        )
        results["html"] = html_result

        success = md_result.get("success", False) and html_result.get("success", False)

        if success:
            logger.info(f"✅ 完整报告生成成功")
        else:
            logger.warning(f"⚠️ 部分报告生成失败")

        return {
            "success": success,
            "results": results,
        }
