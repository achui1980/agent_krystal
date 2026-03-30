"""
转换引擎

执行数据转换的核心引擎
"""

from typing import Dict, List, Any
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from models.rule import TransformationConfig, Rule
from converters.conditional_map import parse_product_line
from converters.name_parser import parse_full_name, split_cms_contract
from utils.logger import get_logger


class TransformationEngine:
    """数据转换引擎"""

    def __init__(self, config: TransformationConfig):
        self.config = config
        self.logger = get_logger()

        # 创建规则索引
        self.rule_index = {}
        for rule in config.rules:
            self.rule_index[rule.cs_column_name] = rule

    def transform(self, source_data: Dict[str, str]) -> Dict[str, str]:
        """
        转换单条源数据为预期格式

        Args:
            source_data: 源数据字典

        Returns:
            预期数据字典
        """
        result = {}

        # 按字段顺序处理
        for field_name in self.config.field_order:
            rule = self.rule_index.get(field_name)

            if rule:
                value = self._apply_rule(rule, source_data)
            else:
                # 没有规则，使用空值
                value = ""

            # 强制转换为字符串
            result[field_name] = str(value) if value is not None else ""

        return result

    def _apply_rule(self, rule: Rule, source_data: Dict[str, str]) -> str:
        """
        应用规则进行转换

        Args:
            rule: 规则对象
            source_data: 源数据

        Returns:
            转换后的值
        """
        # 1. 根据转换类型处理
        if rule.transformation_type == "fixed":
            # 固定值
            return rule.config.get("value", "")

        elif rule.transformation_type == "direct":
            # 直接映射
            source_field = rule.config.get("source_field", "")
            return source_data.get(source_field, "")

        elif rule.transformation_type == "conditional_map":
            # 条件映射
            source_field = rule.config.get("source_field", "")
            source_value = source_data.get(source_field, "")

            # 使用产品类型映射
            if rule.cs_column_name == "PRODUCT_LINE":
                return parse_product_line(source_value)

            # 通用条件映射
            mappings = rule.config.get("mappings", {})
            default = rule.config.get("default", "")
            return mappings.get(source_value, default)

        elif rule.transformation_type == "name_parser":
            # 姓名解析
            source_field = rule.config.get("source_field", "")
            name_value = source_data.get(source_field, "")
            part = rule.config.get("part", "first")

            parsed = parse_full_name(name_value)
            return parsed.get(part, "")

        elif rule.transformation_type == "split_extract":
            # 分割提取（如 CMS_CONTRACT_ID）
            source_field = rule.config.get("source_field", "")
            contract_value = source_data.get(source_field, "")

            if rule.cs_column_name == "CMS_CONTRACT_ID":
                result = split_cms_contract(contract_value)
                return result["contract"]
            elif rule.cs_column_name == "CMS_PLAN_ID":
                result = split_cms_contract(contract_value)
                return result["plan"]
            else:
                return contract_value

        elif rule.transformation_type == "empty":
            # 空字段
            return ""

        else:
            # 未知类型，尝试直接映射
            if rule.carrier_column_name:
                return source_data.get(rule.carrier_column_name, "")
            return rule.default_value or ""

    def transform_batch(
        self, source_records: List[Dict[str, str]]
    ) -> List[Dict[str, str]]:
        """
        批量转换

        Args:
            source_records: 源数据记录列表

        Returns:
            预期数据记录列表
        """
        self.logger.step(3, "执行数据转换")
        self.logger.info(f"🔄 转换 {len(source_records)} 条记录")

        results = []
        for idx, record in enumerate(source_records, 1):
            try:
                transformed = self.transform(record)
                results.append(transformed)
            except Exception as e:
                self.logger.error(f"转换记录 {idx} 失败: {e}")
                # 添加空记录保持顺序
                results.append({field: "" for field in self.config.field_order})

        self.logger.success(f"转换完成: {len(results)} 条记录")
        return results
