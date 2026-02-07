"""
Expected Calculator - 计算预期结果
"""

from typing import Dict, List


class ExpectedCalculator:
    """计算预期结果"""

    def __init__(self, rules: List[Dict]):
        self.rules = rules

    def calculate(self, source_data: List[Dict]) -> List[Dict]:
        """
        对每行source数据应用规则，生成expected

        Returns:
            List[Dict] - expected数据行
        """
        results = []

        for source_row in source_data:
            expected_row = self._calculate_row(source_row)
            results.append(expected_row)

        return results

    def _calculate_row(self, source_row: Dict) -> Dict:
        """计算单行expected"""
        expected = {}

        for rule in self.rules:
            target_field = rule["target_field"]
            handler = rule["handler"]

            try:
                value = handler(source_row)
                expected[target_field] = value if value is not None else ""
            except Exception as e:
                # 处理异常：如果转换失败，根据字段重要性决定
                if self._is_critical_field(target_field):
                    expected[target_field] = f"[ERROR: {str(e)}]"
                else:
                    expected[target_field] = ""

        return expected

    def _is_critical_field(self, field_name: str) -> bool:
        """判断是否为关键字段"""
        critical_fields = ["CARRIER_STATUS_MAP", "PRODUCT_LINE", "AGENT_NAME"]
        return field_name in critical_fields
