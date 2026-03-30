"""
自主代码生成器 - Agent编写、测试、修复代码直到可运行
"""

import os
import sys
import json
import traceback
import subprocess
from pathlib import Path
from typing import Dict, List, Any, Optional

from crewai import Agent, Task, Crew, Process
from crewai.llm import LLM
from dotenv import load_dotenv

# 加载环境变量
env_path = Path(__file__).parent.parent.parent.parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)


class AutonomousCodeGenerator:
    """
    自主代码生成器

    Agent扮演程序员角色：
    1. 分析规则 → 生成规格书
    2. 编写代码 → 生成Python代码
    3. 自我测试 → 验证代码正确性
    4. 自动修复 → 如果失败则修复
    5. 循环直到成功
    """

    def __init__(self, max_iterations: int = 5):
        self.max_iterations = max_iterations
        self.output_dir = Path("./generated_autonomous")
        self.output_dir.mkdir(exist_ok=True)

        # 初始化Agent
        api_key = os.getenv("OPENAI_API_KEY")
        model = os.getenv("OPENAI_MODEL", "gpt-4o")
        self.llm = LLM(model=model, api_key=api_key) if api_key else None

        if not self.llm:
            raise ValueError("OPENAI_API_KEY not found!")

        self.agent = self._create_programmer_agent()

    def _create_programmer_agent(self) -> Agent:
        """创建程序员Agent"""
        return Agent(
            role="资深Python开发工程师与ETL测试专家",
            goal="编写高质量、健壮的Python代码，通过自我测试确保代码正确",
            backstory="""
你是一位拥有15年经验的资深Python开发工程师，同时也是ETL测试专家。

你的核心能力：
1. 代码设计：设计清晰、可维护的Python代码结构
2. 规则理解：深入理解业务规则并转化为代码逻辑
3. 测试驱动：编写代码的同时编写测试，确保代码质量
4. 故障排查：快速定位代码问题并修复
5. 持续改进：通过测试-修复循环不断完善代码

你的编程原则：
- 代码必须健壮：完善的错误处理和边界情况处理
- 代码必须可读：清晰的命名、适当的注释、合理的结构
- 代码必须可测试：每个功能都可以独立测试
- 代码必须高效：避免不必要的计算和资源浪费

当遇到错误时，你会：
1. 仔细分析错误信息和堆栈跟踪
2. 定位问题根源（不只是表面修复）
3. 思考是否有更好的实现方式
4. 修复后进行回归测试确保没有引入新问题

你擅长使用：
- Python类型提示
- 异常处理机制
- 单元测试框架
- 数据生成库（Faker等）
            """,
            verbose=True,
            allow_delegation=False,
            llm=self.llm,
        )

    def run(
        self, rules_path: str, source_path: str, expected_path: str
    ) -> Dict[str, Any]:
        """
        执行完整的自主代码生成流程

        Returns:
            {
                'success': bool,
                'code_path': str,
                'iterations': int,
                'data': List[Dict]
            }
        """
        print("=" * 80)
        print("🤖 自主代码生成器启动")
        print("=" * 80)
        print(f"📄 规则文件: {rules_path}")
        print(f"📄 Source: {source_path}")
        print(f"📄 Expected: {expected_path}")
        print(f"🔄 最大修复次数: {self.max_iterations}")
        print()

        # Step 1: Agent分析规则生成规格书
        print("📝 Step 1: Agent分析规则...")
        spec = self._analyze_rules(rules_path, source_path, expected_path)
        print(f"   ✅ 生成规格书: {len(spec)} 字符")

        # Step 2-4: 代码生成-测试-修复循环
        code = None
        final_result = None

        for iteration in range(self.max_iterations):
            print(f"\n🔧 第 {iteration + 1}/{self.max_iterations} 轮代码生成/修复")

            if iteration == 0:
                # 第一轮：生成代码
                print("   📝 生成代码...")
                code = self._generate_code(spec)
            else:
                # 后续轮：修复代码
                print(
                    f"   🔧 修复代码 (基于错误: {final_result.get('error_type', 'Unknown')})..."
                )
                code = self._fix_code(spec, code, final_result)

            # 保存代码到文件
            code_path = self.output_dir / f"data_generator_v{iteration + 1}.py"
            with open(code_path, "w", encoding="utf-8") as f:
                f.write(code)
            print(f"   💾 代码已保存: {code_path}")

            # 测试代码
            print("   🧪 测试代码...")
            final_result = self._test_code(code, source_path, expected_path)

            if final_result["success"]:
                print(f"   ✅ 测试通过！代码在第 {iteration + 1} 轮验证成功！")
                break
            else:
                print(f"   ❌ 测试失败")
                print(f"      错误类型: {final_result.get('error_type', 'Unknown')}")
                print(
                    f"      错误信息: {final_result.get('error_message', 'No message')[:100]}..."
                )
        else:
            # 达到最大次数仍未成功
            print(f"\n❌ 达到最大修复次数 ({self.max_iterations})，代码仍无法运行")
            return {
                "success": False,
                "error": "Max iterations reached",
                "last_error": final_result,
                "code_path": str(code_path) if code else None,
            }

        # Step 5: 执行最终代码生成数据
        print("\n🚀 Step 5: 执行最终代码生成数据...")
        data = self._execute_final_code(code)

        print("\n" + "=" * 80)
        print("✅ 自主代码生成完成！")
        print("=" * 80)

        return {
            "success": True,
            "code_path": str(code_path),
            "iterations": iteration + 1,
            "data": data,
            "spec": spec,
        }

    def _analyze_rules(
        self, rules_path: str, source_path: str, expected_path: str
    ) -> str:
        """Agent分析规则生成规格书"""
        # 🆕 读取文件的实际内容
        file_contents = self._read_file_contents(rules_path, source_path, expected_path)

        task = Task(
            description=f"""
作为资深ETL测试专家和代码架构师，请深度分析以下材料并生成代码规格书：

**输入文件实际内容：**

1. **规则文件 (rules.xlsx Sheet1, 表头第5行):**
```csv
{file_contents["rules"][:5000]}
...（共{len(file_contents["rules"])}字符，已截取前5000字符）
```

2. **Source模板 (source.csv 前10行):**
```csv
{file_contents["source"]}
```

3. **Expected模板 (expected.txt 前100行):**
```
{file_contents["expected"]}
```

**分析步骤：**

1. **读取并理解规则**
   - 从上面的规则文件内容中逐行分析92条规则的业务含义
   - 识别字段映射关系

2. **分析数据结构**
   - Source字段：从上面的source.csv内容中识别所有字段
   - Expected字段：从上面的expected.txt内容中识别所有字段
   - 识别字段对应关系

3. **提取转换逻辑**
   - 直接映射：字段直接复制
   - 条件映射：如PRODUCT_LINE的条件转换
   - 默认值：固定值的字段
   - 复杂转换：字段拆分、字符串处理等

 4. **提取使用的Source字段**
    从 field_mappings 中提取所有实际使用的 source 字段：
    - 遍历所有 field_mappings
    - 收集所有非空的 source 字段名
    - 去重后得到 used_source_fields 列表
    - 计算 unused_source_fields = source_fields - used_source_fields
 
 5. **生成规格书**
    输出JSON格式的规格书，包含：
    {{
        "source_fields": ["字段名列表"],
        "expected_fields": ["字段名列表"],
        "field_mappings": [
            {{
                "source": "源字段名",
                "target": "目标字段名",
                "type": "direct|conditional|default|transform",
                "logic": "转换逻辑描述",
                "conditions": {{"条件": "值"}}  // 如果是conditional类型
            }}
        ],
        "used_source_fields": ["实际使用的源字段名列表"],  // 重要！
        "unused_source_fields": ["未使用的源字段名列表"],  // 重要！
        "test_scenarios": {{
            "normal": ["场景1", "场景2"],
            "abnormal": ["异常场景1"],
            "boundary": ["边界场景1"]
        }},
        "code_structure": {{
            "class_name": "SmartDataGenerator",
            "methods": ["方法1", "方法2"]
        }}
    }}
 
 请只输出JSON格式的规格书，不要其他内容。
 
 【关键要求】
 used_source_fields 必须准确反映 field_mappings 中实际引用的 source 字段！
 这将用于数据生成时的字段选择策略。
            """,
            expected_output="JSON格式的代码生成规格书",
            agent=self.agent,
        )

        crew = Crew(agents=[self.agent], tasks=[task], verbose=False)
        result = crew.kickoff()

        return result.raw

    def _read_file_contents(
        self, rules_path: str, source_path: str, expected_path: str
    ) -> Dict[str, str]:
        """读取输入文件的实际内容"""
        import pandas as pd

        # 读取 rules.xlsx (Sheet1, 表头在第5行)
        try:
            rules_df = pd.read_excel(rules_path, sheet_name="Sheet1", header=4)
            rules_content = rules_df.to_csv(index=False)  # 转为CSV格式便于Agent阅读
        except Exception as e:
            rules_content = f"Error reading rules: {e}"

        # 读取 source.csv (前10行)
        try:
            source_df = pd.read_csv(source_path, nrows=10)
            source_content = source_df.to_csv(index=False)
        except Exception as e:
            source_content = f"Error reading source: {e}"

        # 读取 expected.txt (所有行 - 特殊格式 FIELD_NAME:default_value)
        try:
            with open(expected_path, "r", encoding="utf-8") as f:
                expected_lines = f.readlines()
            expected_content = "".join(expected_lines[:100])  # 前100行
        except Exception as e:
            expected_content = f"Error reading expected: {e}"

        return {
            "rules": rules_content,
            "source": source_content,
            "expected": expected_content,
        }

    def _generate_code(self, spec: str) -> str:
        """Agent生成Python代码"""
        description = (
            """
根据以下规格书，生成完整的Python代码：

**规格书：**
"""
            + spec
            + """

**代码要求：**

1. **代码结构**
```python
from faker import Faker
import random
import csv
from typing import List, Dict, Optional, Tuple
from datetime import datetime

class SmartDataGenerator:
    def __init__(self):
        self.fake = Faker('en_US')
        
    def generate_normal_cases(self, count: int = 10) -> List[Dict]:
        \"\"\"生成正常场景数据\"\"\"
        pass
        
    def generate_abnormal_cases(self, scenarios: List[Dict]) -> List[Dict]:
        \"\"\"生成异常场景数据\"\"\"
        pass
        
    def generate_boundary_cases(self, count: int = 5) -> List[Dict]:
        \"\"\"生成边界场景数据\"\"\"
        pass
        
    # 根据规格书中的field_mappings生成转换方法
    # 每个需要转换的字段都对应一个方法
    
    def save_to_csv(self, data: List[Dict], filepath: str):
        \"\"\"保存数据到CSV\"\"\"
        pass

if __name__ == "__main__":
    gen = SmartDataGenerator()
    # 生成数据并保存
```

 2. **转换逻辑实现**
    - 每个转换方法都要有完整的逻辑
    - 使用try-except处理异常
    - 空值处理：返回空字符串或默认值
    
 3. **数据生成策略**（重要！）
    根据规格书中的 used_source_fields 和 unused_source_fields 生成数据：
    
    ```python
    def _generate_source_row_normal(self, source_fields: List[str]) -> Dict[str, Any]:
        # 获取规格书中的字段分类
        used_fields = self.spec.get("used_source_fields", [])
        unused_fields = self.spec.get("unused_source_fields", [])
        
        row = {}
        for field in source_fields:
            if field in used_fields:
                # 规则中使用的字段：生成真实数据
                row[field] = self._generate_real_value(field)
            else:
                # 规则未使用的字段：空字符串
                row[field] = ""
        return row
    ```
    
    示例：
    - 如果 used_source_fields = ["FIRST_NAME", "LAST_NAME", "EMAIL"]
    - unused_source_fields = ["FAX", "MIDDLE_NAME"]
    - 则：FIRST_NAME="John", LAST_NAME="Doe", EMAIL="john@example.com"
    - 而：FAX="", MIDDLE_NAME=""
    
    使用Faker生成真实的姓名、地址等
    - 州和城市要匹配（如MO-Saint Louis）
    - 日期格式要正确
   
4. **注释和文档**
   - 每个方法都要有docstring
   - 复杂逻辑要有注释说明
   
5. **类型提示**
   - 所有方法都要有类型注解
   - 返回值类型要明确

请生成可直接保存为.py文件的完整代码，代码必须能够独立运行。
"""
        )

        task = Task(
            description=description,
            expected_output="完整的Python代码（可直接执行）",
            agent=self.agent,
        )

        crew = Crew(agents=[self.agent], tasks=[task], verbose=False)
        result = crew.kickoff()

        # 提取代码（去掉可能的markdown标记）
        code = result.raw
        if "```python" in code:
            code = code.split("```python")[1].split("```")[0]
        elif "```" in code:
            code = code.split("```")[1].split("```")[0]

        return code.strip()

    def _test_code(
        self, code: str, source_path: str, expected_path: str
    ) -> Dict[str, Any]:
        """测试生成的代码"""
        # 创建临时测试文件
        test_file = self.output_dir / "_test_runner.py"

        test_code = f'''
import sys
sys.path.insert(0, "{self.output_dir}")

# 写入被测代码
{code}

# 测试代码
def run_tests():
    results = {{
        "tests": [],
        "success": True,
        "error": None
    }}
    
    try:
        # Test 1: 实例化
        gen = SmartDataGenerator()
        results["tests"].append({{"name": "实例化", "status": "PASS"}})
        
        # Test 2: 生成正常数据
        normal = gen.generate_normal_cases(3)
        assert len(normal) == 3, f"期望3条数据，实际{{len(normal)}}条"
        assert isinstance(normal[0], dict), "数据应该是字典类型"
        results["tests"].append({{"name": "生成正常数据", "status": "PASS"}})
        
        # Test 3: 验证字段存在
        required_fields = ["Product", "Member", "DOB", "State"]
        for field in required_fields:
            assert field in normal[0], f"缺少字段: {{field}}"
        results["tests"].append({{"name": "字段完整性", "status": "PASS"}})
        
        # Test 4: 转换逻辑测试（如果有transform方法）
        if hasattr(gen, 'transform_product_line'):
            result = gen.transform_product_line("PDP")
            assert result == "MD", f"PDP应该映射到MD，实际{{result}}"
            results["tests"].append({{"name": "PRODUCT_LINE转换", "status": "PASS"}})
        
        # Test 5: 生成异常数据
        abnormal_scenarios = [{{"name": "test", "modifications": {{"Product": ""}}}}]
        abnormal = gen.generate_abnormal_cases(abnormal_scenarios)
        assert len(abnormal) >= 1, "应该生成异常数据"
        results["tests"].append({{"name": "生成异常数据", "status": "PASS"}})
        
        # Test 6: 生成边界数据
        boundary = gen.generate_boundary_cases(2)
        assert len(boundary) == 2, "应该生成2条边界数据"
        results["tests"].append({{"name": "生成边界数据", "status": "PASS"}})
        
        results["summary"] = f"通过 {{len([t for t in results['tests'] if t['status'] == 'PASS'])}}/{{len(results['tests'])}} 个测试"
        
    except Exception as e:
        results["success"] = False
        results["error"] = str(e)
        results["error_type"] = type(e).__name__
        results["traceback"] = traceback.format_exc()
        results["tests"].append({{"name": "执行失败", "status": "FAIL", "error": str(e)}})
    
    return results

if __name__ == "__main__":
    import json
    result = run_tests()
    print(json.dumps(result, ensure_ascii=False, indent=2))
'''

        with open(test_file, "w", encoding="utf-8") as f:
            f.write(test_code)

        # 执行测试
        try:
            result = subprocess.run(
                [sys.executable, str(test_file)],
                capture_output=True,
                text=True,
                timeout=30,
                cwd=str(self.output_dir),
            )

            if result.returncode == 0:
                # 解析JSON结果
                try:
                    test_results = json.loads(result.stdout)
                    return test_results
                except json.JSONDecodeError:
                    return {
                        "success": False,
                        "error_type": "JSONDecodeError",
                        "error_message": "无法解析测试结果",
                        "stdout": result.stdout,
                        "stderr": result.stderr,
                    }
            else:
                return {
                    "success": False,
                    "error_type": "ExecutionError",
                    "error_message": result.stderr or "执行失败",
                    "stdout": result.stdout,
                }

        except subprocess.TimeoutExpired:
            return {
                "success": False,
                "error_type": "TimeoutError",
                "error_message": "测试执行超时（30秒）",
            }
        except Exception as e:
            return {
                "success": False,
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc(),
            }
        finally:
            # 清理测试文件
            if test_file.exists():
                test_file.unlink()

    def _fix_code(self, spec: str, current_code: str, error_result: Dict) -> str:
        """Agent修复代码"""
        task = Task(
            description=f"""
代码测试失败，请分析错误并修复。

**错误信息：**
错误类型: {error_result.get("error_type", "Unknown")}
错误消息: {error_result.get("error_message", "No message")}
堆栈跟踪:
{error_result.get("traceback", "No traceback")[:500]}

**当前代码：**
```python
{current_code}
```

**原始规格书：**
{spec[:500]}...

**修复要求：**

1. **分析问题**
   - 仔细阅读错误信息和堆栈跟踪
   - 定位出错的代码行
   - 理解为什么会出错

2. **修复代码**
   - 最小改动原则：只修复错误，不要重构整体结构
   - 如果是语法错误：检查括号匹配、缩进、冒号等
   - 如果是逻辑错误：修复转换逻辑
   - 如果是属性/方法错误：检查方法名和调用方式
   - 如果是导入错误：添加必要的import语句

3. **预防类似错误**
   - 检查是否有其他类似的潜在问题
   - 添加必要的错误处理

4. **保持代码风格**
   - 与原有代码风格保持一致
   - 保持类型提示
   - 保持注释风格

请输出修复后的完整代码，确保代码可以直接运行。
            """,
            expected_output="修复后的完整Python代码",
            agent=self.agent,
        )

        crew = Crew(agents=[self.agent], tasks=[task], verbose=False)
        result = crew.kickoff()

        # 提取代码
        code = result.raw
        if "```python" in code:
            code = code.split("```python")[1].split("```")[0]
        elif "```" in code:
            code = code.split("```")[1].split("```")[0]

        return code.strip()

    def _execute_final_code(self, code: str) -> List[Dict]:
        """执行最终代码生成数据"""
        # 创建执行文件
        exec_file = self.output_dir / "_final_executor.py"

        exec_code = f"""
{code}

import json

if __name__ == "__main__":
    gen = SmartDataGenerator()
    
    # 生成数据
    print("Generating normal cases...")
    normal = gen.generate_normal_cases(10)
    
    print("Generating abnormal cases...")
    abnormal_scenarios = [
        {{"name": "missing_medicare_id", "modifications": {{"MEDICARE_ID": ""}}}},
        {{"name": "invalid_product", "modifications": {{"Product": "INVALID"}}}},
    ]
    abnormal = gen.generate_abnormal_cases(abnormal_scenarios)
    
    print("Generating boundary cases...")
    boundary = gen.generate_boundary_cases(5)
    
    # 合并所有数据
    all_data = normal + abnormal + boundary
    
    # 保存到JSON文件
    with open("generated_data.json", "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)
    
    print(f"Generated {{len(all_data)}} records")
    print("Data saved to generated_data.json")
"""

        with open(exec_file, "w", encoding="utf-8") as f:
            f.write(exec_code)

        # 执行
        try:
            result = subprocess.run(
                [sys.executable, str(exec_file)],
                capture_output=True,
                text=True,
                timeout=60,
                cwd=str(self.output_dir),
            )

            if result.returncode == 0:
                # 读取生成的数据
                data_file = self.output_dir / "generated_data.json"
                if data_file.exists():
                    with open(data_file, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    return data
                else:
                    print("Warning: Data file not found")
                    return []
            else:
                print(f"Execution error: {result.stderr}")
                return []

        except Exception as e:
            print(f"Error executing final code: {e}")
            return []
        finally:
            # 清理执行文件
            if exec_file.exists():
                exec_file.unlink()


if __name__ == "__main__":
    # 测试运行
    generator = AutonomousCodeGenerator(max_iterations=3)

    result = generator.run(
        rules_path="case/rules.xlsx",
        source_path="case/source.csv",
        expected_path="case/expected.txt",
    )

    if result["success"]:
        print(f"\n✅ 成功！迭代 {result['iterations']} 次")
        print(f"📄 代码保存于: {result['code_path']}")
        print(f"📊 生成数据: {len(result['data'])} 条")
    else:
        print(f"\n❌ 失败: {result.get('error', 'Unknown error')}")
