"""
智能测试生成器 - 根据规格书动态生成测试用例

这个模块会：
1. 分析规格书中的转换类型
2. 为每种转换类型生成针对性测试
3. 优先测试关键业务逻辑（conditional、complex转换）
4. 中等验证严格度（核心逻辑正确即可）
"""

from typing import Dict, List, Any
import json
import textwrap


class SmartTestGenerator:
    """智能测试生成器"""

    def __init__(self, spec: Dict[str, Any]):
        """
        初始化测试生成器

        Args:
            spec: 规格书（JSON格式）
        """
        self.spec = spec
        self.source_fields = spec.get("source_fields", [])
        self.expected_fields = spec.get("expected_fields", [])
        self.field_mappings = spec.get("field_mappings", [])

    def generate_test_code(self) -> str:
        """
        生成完整的测试代码

        Returns:
            Python测试代码字符串
        """
        tests = []

        # 1. 基础测试（必选）
        tests.append(self._test_instantiation())
        tests.append(self._test_generate_normal())
        tests.append(self._test_field_completeness())

        # 2. 关键转换逻辑测试（根据规则类型动态生成）
        conditional_tests = self._generate_conditional_tests()
        tests.extend(conditional_tests[:5])  # 最多5个条件测试

        # 3. 复杂转换测试
        transform_tests = self._generate_transform_tests()
        tests.extend(transform_tests[:3])  # 最多3个复杂转换测试

        # 4. 数据匹配测试（州-城市、日期格式等）
        validation_tests = self._generate_validation_tests()
        tests.extend(validation_tests[:2])  # 最多2个验证测试

        # 编译成完整测试代码
        return self._compile_test_code(tests)

    def _test_instantiation(self) -> Dict[str, Any]:
        """测试1: 实例化测试"""
        return {
            "name": "实例化",
            "code": """
        gen = SmartDataGenerator()
        assert gen is not None, "无法实例化SmartDataGenerator"
        # 灵活检测：支持多种代码结构
        has_template_methods = hasattr(gen, 'generate_normal_cases')
        has_simple_method = hasattr(gen, 'generate')
        has_records_method = hasattr(gen, 'generate_records')
        assert has_template_methods or has_simple_method or has_records_method, \
            "缺少数据生成方法（需要generate_normal_cases、generate或generate_records）"
            """,
            "critical": True,
        }

    def _test_generate_normal(self) -> Dict[str, Any]:
        """测试2: 生成正常数据"""
        return {
            "name": "生成正常数据",
            "code": """
        # 支持多种代码结构
        if hasattr(gen, 'generate_normal_cases'):
            normal = gen.generate_normal_cases(3)
        elif hasattr(gen, 'generate'):
            normal = gen.generate(3)
        elif hasattr(gen, 'generate_records'):
            normal = gen.generate_records(3)
        else:
            raise AssertionError("找不到数据生成方法")
        assert len(normal) == 3, f"期望3条数据，实际{len(normal)}条"
        assert isinstance(normal[0], dict), "数据应该是字典类型"
        assert len(normal[0]) > 0, "数据不应为空"
            """,
            "critical": True,
        }

    def _test_field_completeness(self) -> Dict[str, Any]:
        """测试3: 字段完整性"""
        # 选择5-10个关键字段进行验证
        critical_fields = self._identify_critical_fields()
        fields_str = '", "'.join(critical_fields[:10])

        return {
            "name": "字段完整性",
            "code": f'''
        # 支持多种代码结构
        if hasattr(gen, 'generate_normal_cases'):
            normal = gen.generate_normal_cases(1)
        elif hasattr(gen, 'generate'):
            normal = gen.generate(1)
        elif hasattr(gen, 'generate_records'):
            normal = gen.generate_records(1)
        else:
            raise AssertionError("找不到数据生成方法")
        required_fields = ["{fields_str}"]
        for field in required_fields:
            assert field in normal[0], f"缺少字段: {{field}}"
            ''',
            "critical": True,
        }

    def _generate_conditional_tests(self) -> List[Dict[str, Any]]:
        """生成条件映射测试"""
        tests = []

        for mapping in self.field_mappings:
            if mapping.get("type") == "conditional":
                test = self._create_conditional_test(mapping)
                if test:
                    tests.append(test)

        return tests

    def _create_conditional_test(self, mapping: Dict) -> Dict[str, Any] | None:
        """创建单个条件映射测试"""
        target = mapping.get("target", "")
        source = mapping.get("source", "")
        conditions = mapping.get("conditions", {})

        if not target or not conditions:
            return None

        # 构建测试代码
        test_cases = []
        for condition, expected_value in list(conditions.items())[
            :3
        ]:  # 最多测试3个条件
            # 提取条件值
            if "==" in condition:
                condition_value = condition.split("==")[1].strip().strip("'\"")
            else:
                condition_value = "test_value"

            test_case = f'''
        # 测试条件: {condition}
        source_row = {{"{source}": "{condition_value}"}}
        expected_row = gen._transform_to_expected(source_row)
        actual = expected_row.get("{target}", "")
        expected = "{expected_value}"
        assert actual == expected, f"{target}: 期望'{{expected}}'，实际'{{actual}}'"
            '''
            test_cases.append(test_case)

        return {
            "name": f"{target}条件转换",
            "code": "\n".join(test_cases),
            "critical": True,
        }

    def _generate_transform_tests(self) -> List[Dict[str, Any]]:
        """生成复杂转换测试"""
        tests = []

        for mapping in self.field_mappings:
            if mapping.get("type") == "transform":
                test = self._create_transform_test(mapping)
                if test:
                    tests.append(test)

        return tests

    def _create_transform_test(self, mapping: Dict) -> Dict[str, Any] | None:
        """创建单个复杂转换测试"""
        target = mapping.get("target", "")
        source = mapping.get("source", "")
        logic = mapping.get("logic", "")

        if not target or not source:
            return None

        # 根据逻辑类型生成测试
        if "拆分" in logic or "split" in logic.lower():
            # 名字拆分测试
            code = f'''
        # 测试字段拆分: {source} -> {target}
        source_row = {{"{source}": "LAST,FIRST M"}}
        expected_row = gen._transform_to_expected(source_row)
        actual = expected_row.get("{target}", "")
        assert actual != "", f"{target}不应为空"
            '''
        else:
            # 通用转换测试
            code = f'''
        # 测试转换: {source} -> {target}
        source_row = {{"{source}": "test_value"}}
        expected_row = gen._transform_to_expected(source_row)
        assert "{target}" in expected_row, f"缺少字段{target}"
            '''

        return {"name": f"{target}转换", "code": code, "critical": False}

    def _generate_validation_tests(self) -> List[Dict[str, Any]]:
        """生成数据验证测试"""
        tests = []

        # 1. 州-城市匹配测试
        if "STATE" in self.expected_fields and "CITY" in self.expected_fields:
            tests.append(
                {
                    "name": "州-城市匹配",
                    "code": """
        # 测试州和城市是否匹配
        if hasattr(gen, 'generate_normal_cases'):
            normal = gen.generate_normal_cases(3)
        elif hasattr(gen, 'generate'):
            normal = gen.generate(3)
        elif hasattr(gen, 'generate_records'):
            normal = gen.generate_records(3)
        else:
            normal = []
        for row in normal:
            state = row.get("STATE", "")
            city = row.get("CITY", "")
            if state and city:
                # 验证州-城市格式正确
                assert len(state) == 2, f"州代码应该是2字符: {state}"
                assert len(city) > 0, f"城市不应为空"
                """,
                    "critical": False,
                }
            )

        # 2. 日期格式测试
        date_fields = [
            f for f in self.expected_fields if "DATE" in f.upper() or "DOB" in f.upper()
        ]
        if date_fields:
            date_field = date_fields[0]
            tests.append(
                {
                    "name": "日期格式验证",
                    "code": f'''
        # 测试日期格式
        if hasattr(gen, 'generate_normal_cases'):
            normal = gen.generate_normal_cases(1)
        elif hasattr(gen, 'generate'):
            normal = gen.generate(1)
        elif hasattr(gen, 'generate_records'):
            normal = gen.generate_records(1)
        else:
            normal = []
        if normal:
            date_value = normal[0].get("{date_field}", "")
            if date_value:
                # 验证日期格式（MM/DD/YYYY 或 YYYY-MM-DD）
                import re
                assert re.match(r'\\d{{2}}/\\d{{2}}/\\d{{4}}|\\d{{4}}-\\d{{2}}-\\d{{2}}', str(date_value)), \
                    f"日期格式错误: {{date_value}}"
                ''',
                    "critical": False,
                }
            )

        return tests

    def _identify_critical_fields(self) -> List[str]:
        """识别关键字段"""
        critical_fields = []

        # 1. 所有条件映射的目标字段都是关键字段
        for mapping in self.field_mappings:
            if mapping.get("type") in ["conditional", "transform"]:
                target = mapping.get("target")
                if target and target not in critical_fields:
                    critical_fields.append(target)

        # 2. 常见的关键字段
        common_critical = [
            "FIRST_NAME",
            "LAST_NAME",
            "DOB",
            "STATE",
            "CITY",
            "PRODUCT_LINE",
            "STATUS",
            "MEDICARE_ID",
            "MEMBER_NUMBER",
            "EFFECTIVE_START_DATE",
            "CARRIER_EFFECTIVE_DATE",
        ]
        for field in common_critical:
            if field in self.expected_fields and field not in critical_fields:
                critical_fields.append(field)

        # 3. 如果关键字段不足10个，添加更多expected字段
        for field in self.expected_fields:
            if len(critical_fields) >= 10:
                break
            if field not in critical_fields:
                critical_fields.append(field)

        return critical_fields

    def _compile_test_code(self, tests: List[Dict[str, Any]]) -> str:
        """编译测试代码"""
        test_code = '''
import sys
import traceback

def run_tests():
    """运行所有测试"""
    results = {
        "tests": [],
        "success": True,
        "error": None
    }
    
    try:
        # 实例化生成器
        gen = SmartDataGenerator()
        results["tests"].append({"name": "准备", "status": "PASS"})
        
'''

        # 添加所有测试
        for i, test in enumerate(tests):
            test_name = test["name"]
            test_code_snippet = test["code"]
            is_critical = test.get("critical", False)

            # Remove common leading whitespace, then add proper indentation (12 spaces)
            dedented = textwrap.dedent(test_code_snippet).strip()
            indented_snippet = "\n".join(
                ["            " + line for line in dedented.splitlines()]
            )

            test_code += f'''
        # 测试 {i + 1}: {test_name}
        try:
{indented_snippet}
            results["tests"].append({{"name": "{test_name}", "status": "PASS"}})
        except AssertionError as e:
            error_msg = str(e)
            results["tests"].append({{"name": "{test_name}", "status": "FAIL", "error": error_msg}})
            {"results['success'] = False" if is_critical else "# 非关键测试失败，继续"}
            {"results['error'] = error_msg" if is_critical else ""}
            {"results['error_type'] = 'AssertionError'" if is_critical else ""}
            {"return results" if is_critical else ""}
        except Exception as e:
            error_msg = str(e)
            results["tests"].append({{"name": "{test_name}", "status": "ERROR", "error": error_msg}})
            {"results['success'] = False" if is_critical else ""}
            {"results['error'] = error_msg" if is_critical else ""}
            {"results['error_type'] = type(e).__name__" if is_critical else ""}
            {"return results" if is_critical else ""}
'''

        # 结束测试
        test_code += """
        # 汇总结果
        passed = len([t for t in results["tests"] if t["status"] == "PASS"])
        total = len(results["tests"])
        results["summary"] = f"通过 {passed}/{total} 个测试"
        
    except Exception as e:
        results["success"] = False
        results["error"] = str(e)
        results["error_type"] = type(e).__name__
        results["traceback"] = traceback.format_exc()
        results["tests"].append({"name": "执行失败", "status": "FAIL", "error": str(e)})
    
    return results


if __name__ == "__main__":
    import json
    result = run_tests()
    print(json.dumps(result, ensure_ascii=False, indent=2))
"""

        return test_code

    def generate_test_file(self, code: str) -> str:
        """
        生成完整的测试文件内容

        Args:
            code: 被测试的SmartDataGenerator代码

        Returns:
            完整的测试文件内容
        """
        test_code = self.generate_test_code()

        # 移除被测代码中的 if __name__ == "__main__" 块，避免与测试代码冲突
        code_lines = code.split("\n")
        cleaned_code_lines = []
        skip_main_block = False

        for i, line in enumerate(code_lines):
            # 检测 if __name__ == "__main__": 行
            if line.strip().startswith("if __name__") and "__main__" in line:
                skip_main_block = True
                continue

            # 如果在 if __name__ 块中，跳过所有缩进的行
            if skip_main_block:
                # 如果行是空的或者仍然缩进，继续跳过
                if not line.strip() or line.startswith((" ", "\t")):
                    continue
                else:
                    # 遇到非缩进的行，说明 if __name__ 块结束
                    skip_main_block = False
                    cleaned_code_lines.append(line)
            else:
                cleaned_code_lines.append(line)

        cleaned_code = "\n".join(cleaned_code_lines)

        full_test_file = f"""
# 自动生成的测试文件

{cleaned_code}

# ========================================
# 测试代码
# ========================================

{test_code}
"""

        return full_test_file


if __name__ == "__main__":
    # 测试智能测试生成器
    print("测试智能测试生成器...")

    test_spec = {
        "source_fields": ["Member", "Product", "State", "City", "DOB"],
        "expected_fields": [
            "FIRST_NAME",
            "LAST_NAME",
            "PRODUCT_LINE",
            "STATE",
            "CITY",
            "DOB",
            "MEMBER_NUMBER",
            "STATUS",
            "MEDICARE_ID",
            "EFFECTIVE_START_DATE",
        ],
        "field_mappings": [
            {
                "source": "Product",
                "target": "PRODUCT_LINE",
                "type": "conditional",
                "conditions": {
                    "Product == 'PDP'": "MD",
                    "Product == 'LPPO'": "MA/MAPD",
                    "Product == 'HUM'": "MS",
                },
            },
            {
                "source": "Member",
                "target": "FIRST_NAME",
                "type": "transform",
                "logic": "拆分Member字段获取名字",
            },
        ],
    }

    generator = SmartTestGenerator(test_spec)
    test_code = generator.generate_test_code()

    print(f"\n生成测试代码长度: {len(test_code)} 字符")
    print(f"生成测试代码行数: {len(test_code.splitlines())} 行")
    print("\n前50行预览:")
    print("\n".join(test_code.splitlines()[:50]))
