"""
数据模型模块

包含 Rule, FieldMapping, TransformationConfig 等数据类
"""

from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional


@dataclass
class Rule:
    """
    规则模型类

    对应 rules.xlsx 中的每一行
    """

    # 基础字段（从 Excel 读取）
    cs_column_name: str = ""  # 目标字段名
    carrier_column_name: str = ""  # 源字段名
    default_value: str = ""  # 默认值
    special_rules: str = ""  # 特殊规则描述
    note: str = ""  # 备注

    # 解析后的配置
    transformation_type: str = "direct"  # 转换类型
    config: Dict[str, Any] = field(default_factory=dict)  # 结构化配置

    def __post_init__(self):
        """初始化后解析规则"""
        # 如果已经从JSON加载了配置（config不为空且transformation_type已设置），则不再解析
        if self.config and self.transformation_type != "direct":
            return
        # 如果transformation_type不是默认值，也不解析
        if self.transformation_type not in ["direct", "empty"]:
            return
        self._parse_rule()

    def _parse_rule(self):
        """解析规则，确定转换类型和配置"""
        # 1. 检查是否是固定值（DEFAULT 列有值，且 CARRIER_COLUMN_NAME 为 NULL）
        if self.default_value and self.default_value not in ["NULL", "n/a", ""]:
            if not self.carrier_column_name or self.carrier_column_name in [
                "NULL",
                "n/a",
            ]:
                self.transformation_type = "fixed"
                self.config = {"value": str(self.default_value)}
                return

        # 2. 检查是否有特殊规则
        if self.special_rules and self.special_rules not in ["NULL", "n/a", ""]:
            self._parse_special_rules()
            return

        # 3. 直接映射
        if self.carrier_column_name and self.carrier_column_name not in ["NULL", "n/a"]:
            self.transformation_type = "direct"
            self.config = {"source_field": self.carrier_column_name}
            return

        # 4. 空字段
        self.transformation_type = "empty"
        self.config = {}

    def _parse_special_rules(self):
        """解析特殊规则描述"""
        rules_lower = self.special_rules.lower()

        # 条件映射: "if 'PDP' map to MD"
        if "if" in rules_lower and "map to" in rules_lower:
            self.transformation_type = "conditional_map"
            self.config = {
                "source_field": self.carrier_column_name,
                "mappings": {},  # 由外部解析填充
                "default": "",
            }
            return

        # 姓名解析: "Member format: last_name, first_name"
        if "last_name" in rules_lower and "first_name" in rules_lower:
            self.transformation_type = "name_parser"
            self.config = {
                "source_field": self.carrier_column_name,
                "format": "last_first",
                "part": self.cs_column_name.lower().replace("_name", ""),
            }
            return

        # 字符串分割: "contains A-B separated by '-'"
        if "separated by" in rules_lower or "split" in rules_lower:
            self.transformation_type = "split_extract"
            self.config = {
                "source_field": self.carrier_column_name,
                "delimiter": "-",
                "part": self.cs_column_name.lower(),
            }
            return

        # 默认：直接映射
        self.transformation_type = "direct"
        self.config = {"source_field": self.carrier_column_name}

    def __repr__(self):
        return f"Rule({self.cs_column_name} -> {self.transformation_type})"


@dataclass
class TransformationConfig:
    """
    转换配置模型

    包含所有规则和字段顺序
    """

    case_name: str = ""
    rules: List[Rule] = field(default_factory=list)
    field_order: List[str] = field(default_factory=list)
    source_fields: List[str] = field(default_factory=list)  # Source 文件需要的字段

    def get_rule(self, cs_column_name: str) -> Optional[Rule]:
        """根据目标字段名获取规则"""
        for rule in self.rules:
            if rule.cs_column_name == cs_column_name:
                return rule
        return None

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "case_name": self.case_name,
            "field_order": self.field_order,
            "source_fields": self.source_fields,
            "rules": [
                {
                    "cs_column_name": r.cs_column_name,
                    "carrier_column_name": r.carrier_column_name,
                    "default_value": r.default_value,
                    "special_rules": r.special_rules,
                    "transformation_type": r.transformation_type,
                    "config": r.config,
                }
                for r in self.rules
            ],
        }


@dataclass
class SourceRecord:
    """源数据记录"""

    data: Dict[str, str] = field(default_factory=dict)

    def get(self, field: str, default: str = "") -> str:
        return self.data.get(field, default)

    def set(self, field: str, value: str):
        self.data[field] = value


@dataclass
class ExpectedRecord:
    """预期数据记录"""

    data: Dict[str, str] = field(default_factory=dict)

    def get(self, field: str, default: str = "") -> str:
        return self.data.get(field, default)

    def set(self, field: str, value: str):
        self.data[field] = value
