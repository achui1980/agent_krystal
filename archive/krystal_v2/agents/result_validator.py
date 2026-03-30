"""
Result Validator Agent - 结果验证专家
负责精确对比实际结果和预期结果，标记所有差异
"""

import logging
from typing import Dict, Any, List
from crewai import Agent


logger = logging.getLogger(__name__)


class ResultValidatorAgent:
    """
    结果验证专家

    职责：
    1. 精确对比实际结果和预期结果
    2. 执行行级对比，标记所有差异
    3. 计算统计信息：总行数、匹配行数、差异行数、相似度
    4. 返回详细的对比结果，包含行号和内容
    """

    @staticmethod
    def create(llm=None, environment_context: str = "") -> Agent:
        """
        创建结果验证Agent

        Args:
            llm: LLM模型
            environment_context: 环境上下文信息

        Returns:
            Agent实例
        """
        return Agent(
            role="结果验证专家",
            goal="精确对比实际结果和预期结果，执行行级对比并标记所有差异，生成详细的统计信息和对比报告",
            backstory=f"""你是一位严谨的结果验证专家，专注于数据准确性验证和差异分析。
            
            你的专业能力：
            1. 执行行级精确对比，逐行检查实际结果与预期结果的差异
            2. 识别并标记每一行的匹配状态（匹配/不匹配）
            3. 计算详细的统计指标：
               - 总行数（total_rows）
               - 匹配行数（matching_rows）
               - 差异行数（different_rows）
               - 相似度百分比（similarity）
            4. 记录每个差异的详细信息：行号、预期内容、实际内容
            
            你做事一丝不苟，即使是最微小的差异也不会放过。
            你的对比结果是后续报告生成和质量评估的基础。
            
            {environment_context}
            """,
            verbose=True,
            allow_delegation=False,
            llm=llm,
        )

    @staticmethod
    def compare_files_line_by_line(
        expected_path: str, actual_path: str
    ) -> Dict[str, Any]:
        """
        逐行对比两个文件

        Args:
            expected_path: 预期结果文件路径
            actual_path: 实际结果文件路径

        Returns:
            详细的对比结果
        """
        logger.info(f"🔍 开始对比文件: {expected_path} vs {actual_path}")

        try:
            # 读取文件内容
            with open(expected_path, "r", encoding="utf-8") as f:
                expected_lines = f.read().strip().split("\n")
            with open(actual_path, "r", encoding="utf-8") as f:
                actual_lines = f.read().strip().split("\n")

            # 行级对比
            comparison_rows = []
            total_rows = max(len(expected_lines), len(actual_lines))
            matching_rows = 0
            different_rows = 0

            for i in range(total_rows):
                row_num = i + 1
                expected = expected_lines[i] if i < len(expected_lines) else ""
                actual = actual_lines[i] if i < len(actual_lines) else ""
                match = expected == actual

                if match:
                    matching_rows += 1
                else:
                    different_rows += 1

                comparison_rows.append(
                    {
                        "row_number": row_num,
                        "expected": expected,
                        "actual": actual,
                        "match": match,
                    }
                )

            # 计算相似度
            similarity = (matching_rows / total_rows * 100) if total_rows > 0 else 0

            result = {
                "success": True,
                "total_rows": total_rows,
                "matching_rows": matching_rows,
                "different_rows": different_rows,
                "similarity": round(similarity, 2),
                "comparison_rows": comparison_rows,
                "expected_path": expected_path,
                "actual_path": actual_path,
            }

            logger.info(
                f"✅ 对比完成: {matching_rows}/{total_rows} 行匹配 (相似度: {similarity:.1f}%)"
            )

            return result

        except Exception as e:
            logger.error(f"❌ 文件对比失败: {e}")
            return {
                "success": False,
                "error": str(e),
                "expected_path": expected_path,
                "actual_path": actual_path,
            }

    @staticmethod
    def calculate_statistics(comparison_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        计算对比统计信息

        Args:
            comparison_rows: 行级对比结果列表

        Returns:
            统计信息字典
        """
        total_rows = len(comparison_rows)
        matching_rows = sum(1 for row in comparison_rows if row.get("match", False))
        different_rows = total_rows - matching_rows
        similarity = (matching_rows / total_rows * 100) if total_rows > 0 else 0

        return {
            "total_rows": total_rows,
            "matching_rows": matching_rows,
            "different_rows": different_rows,
            "similarity": round(similarity, 2),
        }
