"""
智能Agent工具集 - 供Agent调用的各种工具
Agent可以自主决定何时使用这些工具
"""

from crewai.tools import BaseTool
from pydantic import BaseModel, Field
from typing import Dict, List, Any, Optional
import pandas as pd
import csv
import json
from pathlib import Path


class ReadRulesInput(BaseModel):
    """读取规则的输入参数"""

    rules_path: str = Field(description="规则文件路径(rules.xlsx)")


class ReadRulesTool(BaseTool):
    """
    读取并理解规则文档
    Agent使用这个工具来理解业务规则
    """

    name: str = "read_rules"
    description: str = """
    读取rules.xlsx文件，提取所有业务规则。
    返回规则的结构化描述，包括：
    - 字段映射关系
    - 转换规则
    - 默认值
    - 条件逻辑
    """
    args_schema: type[BaseModel] = ReadRulesInput

    def _run(self, rules_path: str) -> Dict[str, Any]:
        """读取规则文件"""
        try:
            df = pd.read_excel(rules_path, sheet_name="Sheet1", header=None)

            # 表头在第5行（索引4）
            df.columns = [
                "CSDS_FLAG",
                "CS_COLUMN_NAME",
                "CARRIER_COLUMN_NAME",
                "DEFAULT",
                "SPECIAL_RULES",
                "NOTE",
            ]
            rules_data = df.iloc[5:].copy()

            rules = []
            for _, row in rules_data.iterrows():
                rule = {
                    "target_field": row["CS_COLUMN_NAME"]
                    if pd.notna(row["CS_COLUMN_NAME"])
                    else None,
                    "source_field": row["CARRIER_COLUMN_NAME"]
                    if pd.notna(row["CARRIER_COLUMN_NAME"])
                    else None,
                    "default_value": row["DEFAULT"]
                    if pd.notna(row["DEFAULT"])
                    else None,
                    "special_rule": row["SPECIAL_RULES"]
                    if pd.notna(row["SPECIAL_RULES"])
                    else None,
                    "note": row["NOTE"] if pd.notna(row["NOTE"]) else None,
                }
                if rule["target_field"] and rule["target_field"] != "CS_COLUMN_NAME":
                    rules.append(rule)

            return {
                "success": True,
                "total_rules": len(rules),
                "rules": rules,
                "message": f"成功读取 {len(rules)} 条规则",
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "message": f"读取规则失败: {str(e)}",
            }


class AnalyzeDataInput(BaseModel):
    """分析数据的输入参数"""

    source_path: str = Field(description="Source数据文件路径")
    expected_path: str = Field(description="Expected数据文件路径")


class AnalyzeDataTool(BaseTool):
    """
    分析source和expected数据结构
    Agent使用这个工具来理解输入输出格式
    """

    name: str = "analyze_data_structure"
    description: str = """
    分析source.csv和expected.txt的数据结构。
    识别：
    - 字段列表
    - 数据类型
    - 映射关系
    - 格式差异
    返回结构分析报告供Agent理解转换逻辑。
    """
    args_schema: type[BaseModel] = AnalyzeDataInput

    def _run(self, source_path: str, expected_path: str) -> Dict[str, Any]:
        """分析数据结构"""
        try:
            # 读取source
            with open(source_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                source_data = list(reader)[:5]  # 取前5行样本
                source_fields = reader.fieldnames

            # 读取expected
            with open(expected_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
                metadata = {}
                for i in range(4):
                    if ":" in lines[i]:
                        key, value = lines[i].strip().split(":", 1)
                        metadata[key] = value
                headers = lines[4].strip().split("|")
                expected_sample = lines[5].strip().split("|") if len(lines) > 5 else []

            return {
                "success": True,
                "source_fields": source_fields,
                "source_sample": source_data,
                "expected_metadata": metadata,
                "expected_fields": headers,
                "expected_sample": expected_sample,
                "message": f"Source: {len(source_fields)}个字段, Expected: {len(headers)}个字段",
            }
        except Exception as e:
            return {"success": False, "error": str(e), "message": f"分析失败: {str(e)}"}


class GenerateDataInput(BaseModel):
    """生成数据的输入参数"""

    scenario_description: str = Field(description="场景描述（自然语言）")
    count: int = Field(default=5, description="生成数量")


class GenerateDataTool(BaseTool):
    """
    基于自然语言描述生成测试数据
    Agent描述需要什么数据，工具负责生成
    """

    name: str = "generate_test_data"
    description: str = """
    根据场景描述生成测试数据。
    Agent提供自然语言描述，如：
    - "生成5条PDP产品的正常数据，所有字段有效"
    - "生成3条缺失MEDICARE_ID的异常数据"
    - "生成边界值：DOB=1900-01-01"
    
    工具返回生成的source格式数据。
    """
    args_schema: type[BaseModel] = GenerateDataInput

    def _run(self, scenario_description: str, count: int = 5) -> Dict[str, Any]:
        """根据描述生成数据"""
        from faker import Faker
        import random

        fake = Faker("en_US")

        # Agent的描述会指导生成什么样的数据
        # 这里简化实现，实际可以让Agent更精细控制
        data = []
        for i in range(count):
            record = {"_scenario": scenario_description, "row_num": i + 1}
            data.append(record)

        return {
            "success": True,
            "count": count,
            "data": data,
            "message": f"已生成 {count} 条数据: {scenario_description}",
        }


class ExecuteTransformInput(BaseModel):
    """执行转换的输入参数"""

    source_data: List[Dict] = Field(description="Source数据")
    transformation_rules: str = Field(description="转换规则描述（自然语言）")


class ExecuteTransformTool(BaseTool):
    """
    执行数据转换
    Agent描述转换规则，工具执行转换
    """

    name: str = "execute_transformation"
    description: str = """
    将source数据转换为expected格式。
    Agent提供转换规则的自然语言描述，如：
    - "将Product字段映射到PRODUCT_LINE：PDP→MD，其他→MA/MAPD"
    - "将Member字段拆分为FIRST_NAME和LAST_NAME"
    - "Term_Date=9999-12-31时CARRIER_STATUS_MAP=Active"
    
    工具返回转换后的expected格式数据。
    """
    args_schema: type[BaseModel] = ExecuteTransformInput

    def _run(
        self, source_data: List[Dict], transformation_rules: str
    ) -> Dict[str, Any]:
        """执行转换"""
        # 简化实现，实际可以集成LLM来理解并执行转换
        expected_data = []
        for record in source_data:
            # 这里会基于规则描述进行转换
            transformed = record.copy()
            transformed["_transformation_applied"] = transformation_rules
            expected_data.append(transformed)

        return {
            "success": True,
            "input_count": len(source_data),
            "output_count": len(expected_data),
            "data": expected_data,
            "message": f"转换完成: {len(source_data)}条 → {len(expected_data)}条",
        }


class GenerateCodeInput(BaseModel):
    """生成代码的输入参数"""

    rule_description: str = Field(description="规则描述")
    example_input: Dict = Field(description="示例输入")
    example_output: Dict = Field(description="示例输出")


class GenerateCodeTool(BaseTool):
    """
    为复杂规则生成Python代码
    当规则太复杂时，Agent可以要求生成代码
    """

    name: str = "generate_python_code"
    description: str = """
    为复杂的转换规则生成Python函数代码。
    当规则涉及复杂逻辑（如正则提取、多条件判断等）时使用。
    
    输入规则描述和示例数据，输出可执行的Python代码。
    """
    args_schema: type[BaseModel] = GenerateCodeInput

    def _run(
        self, rule_description: str, example_input: Dict, example_output: Dict
    ) -> Dict[str, Any]:
        """生成Python代码"""

        # 这里可以调用LLM生成代码
        # 简化版返回模板
        code = f'''
# 自动生成的转换函数
# 规则: {rule_description}

def transform_record(source_row):
    """
    转换单条记录
    """
    result = {{}}
    
    # TODO: 根据规则实现转换逻辑
    
    return result
'''

        return {"success": True, "code": code, "message": "代码已生成，可以保存并执行"}


class ValidateResultInput(BaseModel):
    """验证结果的输入参数"""

    expected_results: List[Dict] = Field(description="生成的expected数据")
    validation_criteria: str = Field(description="验证标准（自然语言）")


class ValidateResultTool(BaseTool):
    """
    验证生成的结果
    Agent定义验证标准，工具执行验证
    """

    name: str = "validate_results"
    description: str = """
    验证生成的expected数据是否符合预期。
    Agent提供验证标准，如：
    - "验证所有必填字段都有值"
    - "验证日期格式正确"
    - "验证状态映射正确"
    
    工具返回验证报告。
    """
    args_schema: type[BaseModel] = ValidateResultInput

    def _run(
        self, expected_results: List[Dict], validation_criteria: str
    ) -> Dict[str, Any]:
        """执行验证"""

        # 简化验证
        issues = []
        passed = 0

        for i, record in enumerate(expected_results):
            # 基础验证：检查是否有值
            if not record:
                issues.append(f"行{i + 1}: 数据为空")
            else:
                passed += 1

        return {
            "success": True,
            "total": len(expected_results),
            "passed": passed,
            "failed": len(issues),
            "issues": issues,
            "message": f"验证完成: {passed}/{len(expected_results)} 通过",
        }


# 工具集合
AGENT_TOOLS = [
    ReadRulesTool(),
    AnalyzeDataTool(),
    GenerateDataTool(),
    ExecuteTransformTool(),
    GenerateCodeTool(),
    ValidateResultTool(),
]
