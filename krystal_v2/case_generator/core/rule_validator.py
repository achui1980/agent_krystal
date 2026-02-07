"""
Rule Validator - 验证规则准确性
"""

from typing import Dict, List


class RuleValidator:
    """验证规则准确性"""

    def validate(
        self, rules: List[Dict], source_sample: List[Dict], expected_sample: Dict
    ) -> Dict:
        """
        用sample数据验证规则

        Returns:
            {
                'accuracy': float,
                'matched_fields': int,
                'total_fields': int,
                'mismatches': List[Dict]
            }
        """
        if not source_sample or not expected_sample["data"]:
            return {"accuracy": 0, "matched_fields": 0, "total_fields": 0}

        expected_data = expected_sample["data"]
        mismatches = []
        total_matches = 0

        # 逐行对比
        for i, source_row in enumerate(source_sample[: len(expected_data)]):
            expected_row = expected_data[i]

            for rule in rules:
                target_field = rule["target_field"]
                handler = rule["handler"]

                predicted = handler(source_row)
                actual = expected_row.get(target_field, "")

                if str(predicted) == str(actual):
                    total_matches += 1
                else:
                    mismatches.append(
                        {
                            "row": i,
                            "field": target_field,
                            "predicted": predicted,
                            "actual": actual,
                            "rule_type": rule["rule_type"],
                        }
                    )

        total_fields = len(rules) * len(expected_data)
        accuracy = total_matches / total_fields if total_fields > 0 else 0

        return {
            "accuracy": accuracy,
            "matched_fields": total_matches,
            "total_fields": total_fields,
            "mismatches": mismatches,
        }
