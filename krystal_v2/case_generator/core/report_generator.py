"""
Report Generator - 测试报告生成引擎
"""

import json
from typing import Dict, List, Any
from datetime import datetime


class ReportGenerator:
    """生成详细的测试报告"""

    def __init__(
        self,
        rules: List[Dict],
        test_cases: List[Dict],
        test_points: List[Dict],
        expected_results: List[Dict],
    ):
        self.rules = rules
        self.test_cases = test_cases
        self.test_points = test_points
        self.expected_results = expected_results

    def generate_full_report(self) -> Dict:
        """生成完整报告（JSON格式）"""
        report = {
            "generation_info": self._generate_generation_info(),
            "test_points": self._generate_test_points_section(),
            "rule_coverage": self._generate_rule_coverage_section(),
            "scenario_rule_mapping": self._generate_scenario_mapping(),
            "summary": self._generate_summary(),
        }
        return report

    def generate_markdown_report(self) -> str:
        """生成Markdown格式报告"""
        lines = []

        # 标题
        lines.append("# Krystal Case Generator 测试报告")
        lines.append("")
        lines.append(f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")

        # 生成概况
        lines.extend(self._generate_markdown_overview())
        lines.append("")

        # 测试点详细说明
        lines.extend(self._generate_markdown_test_points())
        lines.append("")

        # 规则覆盖矩阵
        lines.extend(self._generate_markdown_coverage())
        lines.append("")

        # 未覆盖规则分析
        lines.extend(self._generate_markdown_uncovered())

        return "\n".join(lines)

    def _generate_generation_info(self) -> Dict:
        """生成基本信息"""
        normal_count = sum(
            1 for tp in self.test_points if tp["scenario_type"] == "normal"
        )
        abnormal_count = sum(
            1 for tp in self.test_points if tp["scenario_type"] == "abnormal"
        )
        boundary_count = sum(
            1 for tp in self.test_points if tp["scenario_type"] == "boundary"
        )

        return {
            "total_rules": len(self.rules),
            "total_test_cases": len(self.test_cases),
            "case_breakdown": {
                "normal": normal_count,
                "abnormal": abnormal_count,
                "boundary": boundary_count,
            },
            "generation_timestamp": datetime.now().isoformat(),
        }

    def _generate_test_points_section(self) -> Dict:
        """生成测试点章节"""
        categories = {
            "normal": {"count": 0, "test_points": []},
            "abnormal": {"count": 0, "test_points": []},
            "boundary": {"count": 0, "test_points": []},
        }

        for tp in self.test_points:
            scenario_type = tp["scenario_type"]
            if scenario_type in categories:
                categories[scenario_type]["count"] += 1
                categories[scenario_type]["test_points"].append(tp)

        return {"total": len(self.test_points), "categories": categories}

    def _generate_rule_coverage_section(self) -> Dict:
        """生成规则覆盖分析"""
        # 统计规则类型
        rule_types = {}
        for rule in self.rules:
            rt = rule.get("rule_type", "unknown")
            if rt not in rule_types:
                rule_types[rt] = {"total": 0, "covered": 0, "details": []}
            rule_types[rt]["total"] += 1

        # 分析哪些规则被覆盖了
        covered_rules = set()
        rule_coverage_details = {}

        for rule in self.rules:
            target_field = rule["target_field"]
            covered_by = []

            # 检查每个测试用例是否覆盖了这个规则
            for i, expected in enumerate(self.expected_results):
                if target_field in expected and expected[target_field] not in [
                    "",
                    None,
                ]:
                    covered_by.append(i + 1)  # 行号从1开始

            if covered_by:
                covered_rules.add(target_field)
                rule_coverage_details[target_field] = {
                    "rule": rule,
                    "covered_by": covered_by,
                    "coverage_count": len(covered_by),
                }

                rt = rule.get("rule_type", "unknown")
                if rt in rule_types:
                    rule_types[rt]["covered"] += 1
                    rule_types[rt]["details"].append(
                        {
                            "field": target_field,
                            "coverage_count": len(covered_by),
                            "sample_rows": covered_by[:5],  # 最多显示5个
                        }
                    )

        # 未覆盖规则
        uncovered = [r for r in self.rules if r["target_field"] not in covered_rules]

        return {
            "summary": {
                "total_rules": len(self.rules),
                "covered_rules": len(covered_rules),
                "coverage_rate": round(len(covered_rules) / len(self.rules) * 100, 1)
                if self.rules
                else 0,
                "uncovered_count": len(uncovered),
            },
            "by_category": rule_types,
            "uncovered_rules": [
                {
                    "field": r["target_field"],
                    "rule_type": r.get("rule_type", "unknown"),
                    "reason": "无映射或无测试数据",
                }
                for r in uncovered[:20]  # 最多显示20个
            ],
        }

    def _generate_scenario_mapping(self) -> Dict:
        """生成场景与规则映射"""
        mapping = {"normal_cases": [], "abnormal_cases": [], "boundary_cases": []}

        for tp in self.test_points:
            scenario_type = tp["scenario_type"]
            key = f"{scenario_type}_cases"

            if key in mapping:
                mapping[key].append(
                    {
                        "row_id": tp["row_id"],
                        "scenario_name": tp.get("scenario_name", ""),
                        "test_focus": tp.get("test_focus", ""),
                        "rules_covered": tp.get("rules_covered", []),
                        "data_characteristics": tp.get("data_characteristics", {}),
                    }
                )

        return mapping

    def _generate_summary(self) -> Dict:
        """生成执行摘要"""
        coverage = self._generate_rule_coverage_section()["summary"]

        return {
            "test_coverage": f"{coverage['coverage_rate']}%",
            "total_test_cases": len(self.test_cases),
            "recommendations": self._generate_recommendations(),
        }

    def _generate_recommendations(self) -> List[str]:
        """生成建议"""
        recommendations = []

        # 检查产品类型覆盖
        products_covered = set()
        for tp in self.test_points:
            chars = tp.get("data_characteristics", {})
            if "Product" in chars:
                products_covered.add(chars["Product"])

        if "HAP" not in products_covered and "HV" not in products_covered:
            recommendations.append(
                "建议补充HAP/HV/RD产品类型测试数据，以验证PRODUCT_LINE条件映射的所有分支"
            )

        # 检查异常场景
        abnormal_count = sum(
            1 for tp in self.test_points if tp["scenario_type"] == "abnormal"
        )
        if abnormal_count < 5:
            recommendations.append("建议增加更多异常场景测试用例，提高容错能力覆盖率")

        coverage = self._generate_rule_coverage_section()["summary"]
        if coverage["coverage_rate"] < 50:
            recommendations.append(
                f"当前规则覆盖率较低({coverage['coverage_rate']}%)，建议完善规则映射关系"
            )

        return recommendations

    def _generate_markdown_overview(self) -> List[str]:
        """生成Markdown概览"""
        lines = []
        info = self._generate_generation_info()
        coverage = self._generate_rule_coverage_section()["summary"]

        lines.append("## 📊 生成概况")
        lines.append("")
        lines.append("| 指标 | 数值 |")
        lines.append("|------|------|")
        lines.append(f"| 总测试用例 | {info['total_test_cases']} |")
        lines.append(
            f"| 正常场景 | {info['case_breakdown']['normal']} ({info['case_breakdown']['normal'] / info['total_test_cases'] * 100:.1f}%) |"
        )
        lines.append(
            f"| 异常场景 | {info['case_breakdown']['abnormal']} ({info['case_breakdown']['abnormal'] / info['total_test_cases'] * 100:.1f}%) |"
        )
        lines.append(
            f"| 边界场景 | {info['case_breakdown']['boundary']} ({info['case_breakdown']['boundary'] / info['total_test_cases'] * 100:.1f}%) |"
        )
        lines.append(f"| 规则总数 | {info['total_rules']} |")
        lines.append(f"| 已覆盖规则 | {coverage['covered_rules']} |")
        lines.append(f"| 规则覆盖率 | **{coverage['coverage_rate']}%** |")
        lines.append("")

        # 添加建议
        recommendations = self._generate_recommendations()
        if recommendations:
            lines.append("### 💡 优化建议")
            lines.append("")
            for rec in recommendations:
                lines.append(f"- {rec}")
            lines.append("")

        return lines

    def _generate_markdown_test_points(self) -> List[str]:
        """生成Markdown测试点说明"""
        lines = []
        lines.append("## 🎯 测试点详细说明")
        lines.append("")

        # 正常场景
        normal_tps = [tp for tp in self.test_points if tp["scenario_type"] == "normal"]
        if normal_tps:
            lines.append("### 正常场景测试点")
            lines.append("")
            for tp in normal_tps:
                lines.append(f"**TP-{tp['row_id']:03d}: {tp['scenario_name']}**")
                lines.append(f"- **测试目的**: {tp.get('test_focus', 'N/A')}")
                lines.append(
                    f"- **覆盖规则**: {', '.join(tp.get('rules_covered', []))}"
                )
                lines.append(f"- **业务价值**: {tp.get('business_value', 'N/A')}")
                lines.append("")

        # 异常场景
        abnormal_tps = [
            tp for tp in self.test_points if tp["scenario_type"] == "abnormal"
        ]
        if abnormal_tps:
            lines.append("### 异常场景测试点")
            lines.append("")
            for tp in abnormal_tps:
                risk = tp.get("risk_level", "中")
                risk_emoji = "🔴" if risk == "高" else "🟡" if risk == "中" else "🟢"
                lines.append(
                    f"**TP-{tp['row_id']:03d}: {tp['scenario_name']}** {risk_emoji} {risk}风险"
                )
                lines.append(f"- **测试目的**: {tp.get('test_focus', 'N/A')}")
                lines.append(
                    f"- **覆盖规则**: {', '.join(tp.get('rules_covered', []))}"
                )
                lines.append(f"- **预期结果**: {tp.get('expected_behavior', 'N/A')}")
                lines.append("")

        # 边界场景
        boundary_tps = [
            tp for tp in self.test_points if tp["scenario_type"] == "boundary"
        ]
        if boundary_tps:
            lines.append("### 边界场景测试点")
            lines.append("")
            for tp in boundary_tps:
                lines.append(f"**TP-{tp['row_id']:03d}: {tp['scenario_name']}**")
                lines.append(f"- **测试目的**: {tp.get('test_focus', 'N/A')}")
                lines.append(f"- **业务规则**: {tp.get('business_rule', 'N/A')}")
                chars = tp.get("data_characteristics", {})
                if "modified_fields" in chars:
                    lines.append(
                        f"- **边界字段**: {', '.join(chars['modified_fields'])}"
                    )
                lines.append("")

        return lines

    def _generate_markdown_coverage(self) -> List[str]:
        """生成Markdown规则覆盖矩阵"""
        lines = []
        lines.append("## ✅ 规则覆盖矩阵")
        lines.append("")

        coverage = self._generate_rule_coverage_section()

        # 按类别展示
        for category, data in coverage["by_category"].items():
            if data["total"] > 0:
                rate = data["covered"] / data["total"] * 100
                status = "✅" if rate == 100 else "⚠️" if rate > 50 else "❌"
                lines.append(
                    f"### {category} ({data['covered']}/{data['total']}) {status}"
                )
                lines.append("")

                if data["details"]:
                    lines.append("| 目标字段 | 覆盖次数 | 示例行号 |")
                    lines.append("|----------|----------|----------|")
                    for detail in data["details"][:10]:  # 最多显示10个
                        rows_str = ", ".join(map(str, detail["sample_rows"]))
                        lines.append(
                            f"| {detail['field']} | {detail['coverage_count']} | {rows_str} |"
                        )
                    lines.append("")

        return lines

    def _generate_markdown_uncovered(self) -> List[str]:
        """生成Markdown未覆盖规则"""
        lines = []
        coverage = self._generate_rule_coverage_section()

        if coverage["uncovered_rules"]:
            lines.append("## ⚠️ 未覆盖规则分析")
            lines.append("")
            lines.append(
                f"共有 **{coverage['summary']['uncovered_count']}** 个字段未生成测试数据"
            )
            lines.append("")
            lines.append("| 字段名 | 规则类型 | 说明 |")
            lines.append("|--------|----------|------|")

            for rule in coverage["uncovered_rules"][:15]:  # 最多显示15个
                lines.append(
                    f"| {rule['field']} | {rule['rule_type']} | {rule['reason']} |"
                )

            lines.append("")

        return lines
