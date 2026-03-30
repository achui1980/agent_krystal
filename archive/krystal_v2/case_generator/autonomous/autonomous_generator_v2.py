"""
自主代码生成器 V2 (优化版) - 集成模板系统、智能测试和语义库

优化点:
1. 使用代码模板系统 (减少生成错误)
2. 使用智能测试生成器 (针对性测试)
3. 集成Expected语义库 (提供业务含义参考)
4. 保持与V1的兼容性

预期效果:
- 首次成功率: 70% → 86% (+16%)
- 代码生成质量更高
- 测试更有针对性
"""

import os
import sys
import json
import traceback
import subprocess
from pathlib import Path
from typing import Dict, List, Any, Optional
from io import StringIO

from crewai import Agent, Task, Crew, Process
from crewai.llm import LLM
from dotenv import load_dotenv

# 导入日志系统
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
from krystal.logging_utils import setup_krystal_logger, KrystalLogger

# 导入新组件
try:
    # 优先尝试相对导入 (作为包使用时)
    from .code_template import CodeTemplate
    from .smart_test_generator import SmartTestGenerator
    from .expected_semantic_builder import ExpectedSemanticBuilder
except ImportError:
    # 回退到绝对导入 (作为脚本直接运行时)
    from code_template import CodeTemplate
    from smart_test_generator import SmartTestGenerator
    from expected_semantic_builder import ExpectedSemanticBuilder

# 加载环境变量
env_path = Path(__file__).parent.parent.parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)


class AutonomousCodeGeneratorV2:
    """
    自主代码生成器 V2 (优化版)

    集成了:
    - 代码模板系统 (减少结构性错误)
    - 智能测试生成器 (动态生成针对性测试)
    - Expected语义库 (提供字段业务含义)
    """

    def __init__(
        self, max_iterations: int = 5, use_semantic_cache: bool = True, logger=None
    ):
        """
        初始化生成器

        Args:
            max_iterations: 最大修复迭代次数
            use_semantic_cache: 是否使用语义库缓存
            logger: 日志记录器（可选）
        """
        self.max_iterations = max_iterations
        self.use_semantic_cache = use_semantic_cache
        self.output_dir = Path("./generated_autonomous")
        self.output_dir.mkdir(exist_ok=True)

        # 初始化日志系统
        if logger is None:
            self.logger = setup_krystal_logger(
                name="autonomous_generator",
                use_color=True,
                test_id=f"autonomous_{Path(__file__).stem}",
            )
        else:
            self.logger = logger

        # 初始化Agent
        api_key = os.getenv("OPENAI_API_KEY")
        model = os.getenv("OPENAI_MODEL", "gpt-4o")
        self.llm = LLM(model=model, api_key=api_key) if api_key else None

        if not self.llm:
            raise ValueError("OPENAI_API_KEY not found!")

        self.agent = self._create_programmer_agent()

        # 初始化新组件
        self.semantic_builder = ExpectedSemanticBuilder(llm=self.llm)
        self.semantic_map = None  # 延迟加载

    def _create_programmer_agent(self) -> Agent:
        """创建程序员Agent"""
        return Agent(
            role="资深Python开发工程师与ETL测试专家",
            goal="基于模板和语义库，编写高质量、健壮的Python代码",
            backstory="""
你是一位拥有15年经验的资深Python开发工程师，同时也是ETL测试专家。

你的核心能力：
1. 代码设计：基于预定义模板填充业务逻辑
2. 规则理解：结合语义库理解字段业务含义
3. 精准实现：专注于转换逻辑，框架代码已预定义
4. 故障排查：快速定位代码问题并修复

你的优势：
- 有完整的代码模板作为基础
- 有Expected字段语义库作为参考
- 只需专注于核心业务逻辑
- 可以参考语义库避免理解偏差

编程原则：
- 遵循模板结构，不要重构框架
- 参考语义库理解字段含义
- 专注于转换逻辑的正确性
- 完善的错误处理
            """,
            verbose=True,
            allow_delegation=False,
            llm=self.llm,
        )

    def run(
        self, rules_path: str, source_path: str, expected_path: str
    ) -> Dict[str, Any]:
        """
        执行完整的自主代码生成流程 (V2优化版)

        流程:
        1. 构建/加载Expected语义库
        2. Agent分析规则生成规格书
        3. 基于模板生成代码
        4. 智能测试验证
        5. 如需要则迭代修复

        Returns:
            {
                'success': bool,
                'code_path': str,
                'iterations': int,
                'data': List[Dict],
                'version': 'v2'
            }
        """
        # 记录测试开始
        self.logger.log_test_start(
            "自主代码生成",
            rules_file=rules_path,
            source_file=source_path,
            expected_file=expected_path,
            max_iterations=self.max_iterations,
        )

        print("=" * 80)
        print("🤖 自主代码生成器 V2 (优化版)")
        print("=" * 80)
        print(f"📄 规则文件: {rules_path}")
        print(f"📄 Source: {source_path}")
        print(f"📄 Expected: {expected_path}")
        print(f"🔄 最大修复次数: {self.max_iterations}")
        print()

        # Step 0: 构建/加载Expected语义库
        self.logger.log_step("语义库构建", "开始")
        print("📚 Step 0: 构建/加载Expected语义库...")
        try:
            self.semantic_map = self.semantic_builder.build_or_load_semantic_map(
                expected_path, force_rebuild=not self.use_semantic_cache
            )
            print(
                f"   ✅ 语义库已加载: {len(self.semantic_map.get('fields', {}))} 个字段"
            )
            self.logger.info(
                f"   ✅ 语义库已加载: {len(self.semantic_map.get('fields', {}))} 个字段"
            )
        except Exception as e:
            print(f"   ⚠️  语义库加载失败: {e}")
            print(f"   ℹ️  继续执行（无语义库支持）")
            self.logger.warning(f"语义库加载失败: {e}")
            self.semantic_map = None
        self.logger.log_step("语义库构建", "完成")

        # Step 1: Agent分析规则生成规格书
        self.logger.log_step("规则分析", "开始")
        print("\n📝 Step 1: Agent分析规则...")
        spec = self._analyze_rules(rules_path, source_path, expected_path)
        print(f"   ✅ 生成规格书: {len(spec)} 字符")
        self.logger.info(f"生成规格书: {len(spec)} 字符")
        self.logger.log_step("规则分析", "完成")

        # Step 2-4: 代码生成-测试-修复循环
        code = None
        final_result = None

        for iteration in range(self.max_iterations):
            print(f"\n🔧 第 {iteration + 1}/{self.max_iterations} 轮代码生成/修复")
            self.logger.info(
                f"第 {iteration + 1}/{self.max_iterations} 轮代码生成/修复"
            )

            if iteration == 0:
                # 第一轮：使用模板生成代码
                self.logger.log_step("代码生成", "开始", iteration=iteration + 1)
                print("   📝 使用模板系统生成代码...")
                code = self._generate_code_with_template(
                    spec, rules_path, source_path, expected_path
                )
            else:
                # 后续轮：修复代码
                self.logger.log_step("代码修复", "开始", iteration=iteration + 1)
                print(
                    f"   🔧 修复代码 (基于错误: {final_result.get('error_type', 'Unknown')})..."
                )
                code = self._fix_code(spec, code, final_result)

            # 保存代码到文件
            code_path = self.output_dir / f"data_generator_v2_{iteration + 1}.py"
            with open(code_path, "w", encoding="utf-8") as f:
                f.write(code)
            print(f"   💾 代码已保存: {code_path}")
            self.logger.info(f"代码已保存: {code_path}")

            # 使用智能测试
            self.logger.log_step("代码测试", "开始", iteration=iteration + 1)
            print("   🧪 使用智能测试验证代码...")
            final_result = self._test_code_smart(code, spec, source_path, expected_path)

            if final_result["success"]:
                print(f"   ✅ 测试通过！代码在第 {iteration + 1} 轮验证成功！")
                self.logger.info(f"✅ 测试通过！代码在第 {iteration + 1} 轮验证成功！")
                self.logger.log_step(
                    "代码测试", "完成", iteration=iteration + 1, status="success"
                )
                break
            else:
                print(f"   ❌ 测试失败")
                print(f"      错误类型: {final_result.get('error_type', 'Unknown')}")
                print(
                    f"      错误信息: {final_result.get('error_message', 'No message')[:100]}..."
                )
                self.logger.error(
                    f"测试失败: {final_result.get('error_type', 'Unknown')}"
                )
                self.logger.log_step(
                    "代码测试",
                    "失败",
                    iteration=iteration + 1,
                    error_type=final_result.get("error_type", "Unknown"),
                )
        else:
            # 达到最大次数仍未成功
            print(f"\n❌ 达到最大修复次数 ({self.max_iterations})，代码仍无法运行")
            self.logger.error(
                f"达到最大修复次数 ({self.max_iterations})，代码仍无法运行"
            )
            self.logger.log_test_end(
                "自主代码生成",
                success=False,
                duration=0,
                error="Max iterations reached",
            )
            return {
                "success": False,
                "error": "Max iterations reached",
                "last_error": final_result,
                "code_path": str(code_path) if code else None,
                "version": "v2",
            }

        # Step 5: 执行最终代码生成数据
        self.logger.log_step("最终执行", "开始")
        print("\n🚀 Step 5: 执行最终代码生成数据...")
        data = self._execute_final_code(code)

        print("\n" + "=" * 80)
        print("✅ 自主代码生成完成 (V2)！")
        print("=" * 80)

        # 记录测试完成
        self.logger.log_test_end(
            "自主代码生成",
            success=True,
            duration=0,  # 实际应该记录时间
            iterations=iteration + 1,
            code_path=str(code_path),
            data_count=len(data) if data else 0,
        )

        return {
            "success": True,
            "code_path": str(code_path),
            "iterations": iteration + 1,
            "data": data,
            "spec": spec,
            "version": "v2",
        }

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

    def _analyze_rules(
        self, rules_path: str, source_path: str, expected_path: str
    ) -> str:
        """
        Agent分析规则生成规格书 (增强版 - 使用语义库)
        """
        # 🔥 关键修复: 读取文件实际内容
        file_contents = self._read_file_contents(rules_path, source_path, expected_path)

        # 构建提示词 - 如果有语义库则提供参考
        semantic_context = ""
        if self.semantic_map:
            # 提取关键字段的语义信息作为参考
            key_fields = ["PRODUCT_LINE", "FIRST_NAME", "LAST_NAME", "STATE", "CITY"]
            semantic_samples = {}
            for field in key_fields:
                field_sem = self.semantic_map.get("fields", {}).get(field)
                if field_sem:
                    semantic_samples[field] = {
                        "meaning": field_sem.get("business_meaning", ""),
                        "type": field_sem.get("data_type", ""),
                        "sources": field_sem.get("possible_source_fields", [])[:3],
                    }

            if semantic_samples:
                semantic_context = f"""

**Expected字段语义库参考** (示例关键字段):
{json.dumps(semantic_samples, ensure_ascii=False, indent=2)}

使用语义库时：
- 参考business_meaning理解字段含义
- 参考possible_source_fields推断映射关系
- 确保转换逻辑符合字段的业务含义
"""

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

3. **Expected模板 (expected.txt - 特殊格式 FIELD_NAME:default_value):**
```
{file_contents["expected"]}
```
注意：expected.txt使用特殊格式，每行一个字段，格式为 "FIELD_NAME:default_value"
需要提取所有字段名（冒号左侧）作为expected_fields列表。
{semantic_context}

**分析任务：**

1. **分析规则** - 从规则CSV中提取所有92条规则的字段映射关系
2. **识别字段** - 从Source CSV表头和Expected TXT表头提取所有字段名
3. **提取转换逻辑** - 分析每条规则的转换类型（direct/conditional/default/transform）
4. **计算使用字段** - 精确统计哪些Source字段被使用，哪些未使用

**输出JSON规格书：**
{{
    "source_fields": ["从Source CSV表头提取的所有字段名"],
    "expected_fields": ["从Expected TXT表头提取的所有字段名"],
    "field_mappings": [
        {{
            "source": "源字段名（如果有）",
            "target": "目标字段名",
            "type": "direct|conditional|default|transform",
            "logic": "转换逻辑的简短描述",
            "conditions": {{"条件": "值"}}  // 仅conditional类型需要
        }}
    ],
    "used_source_fields": ["field_mappings中实际引用的source字段去重列表"],
    "unused_source_fields": ["source_fields中未被使用的字段"],
    "test_scenarios": {{
        "normal": ["正常场景描述"],
        "abnormal": ["异常场景描述"],
        "boundary": ["边界场景描述"]
    }},
    "code_structure": {{
        "class_name": "SmartDataGenerator",
        "methods": ["需要的方法列表"]
    }}
}}

【关键要求】
- 只输出JSON，不要其他解释
- used_source_fields必须100%匹配field_mappings中的source字段
- 所有字段名必须从实际文件内容中提取，不要猜测
            """,
            expected_output="JSON格式的代码生成规格书",
            agent=self.agent,
        )

        crew = Crew(agents=[self.agent], tasks=[task], verbose=False)

        # 捕获Agent思考过程
        self.logger.info("🤖 Agent开始分析规则...")
        self.logger.start_agent_capture()
        result = crew.kickoff()
        agent_output = self.logger.stop_agent_capture()

        # 记录Agent思考过程
        if agent_output:
            # 将Agent输出记录到日志
            self.logger.info(f"{'=' * 60}")
            self.logger.info(f"🤖 规则分析Agent思考过程与执行详情")
            self.logger.info(f"{'=' * 60}")
            for line in agent_output.split("\n"):
                if line.strip():
                    self.logger.info(f"    {line}")
            self.logger.info(f"{'=' * 60}")

        self.logger.info(f"✅ Agent分析完成，生成了 {len(result.raw)} 字符的规格书")

        return result.raw

    def _generate_code_with_template(
        self, spec: str, rules_path: str, source_path: str, expected_path: str
    ) -> str:
        """
        使用模板系统生成代码 (V2核心优化)

        策略：
        1. 使用预定义模板 (~700行框架代码)
        2. Agent只需填充3个部分 (~800行业务逻辑)
        3. 参考语义库确保理解正确
        """
        # 解析spec
        try:
            spec_dict = json.loads(spec)
        except:
            # 如果spec包含markdown标记，提取JSON
            if "```json" in spec:
                spec = spec.split("```json")[1].split("```")[0]
            elif "```" in spec:
                spec = spec.split("```")[1].split("```")[0]
            spec_dict = json.loads(spec.strip())

        # 生成基础模板
        template_code = CodeTemplate.generate_code(spec_dict, rules_path)

        # 构建语义库上下文
        semantic_context = ""
        if self.semantic_map:
            semantic_context = f"""

**Expected字段语义库** (完整版已加载):
可用于验证转换逻辑的正确性。例如：
- PRODUCT_LINE应该是产品线代码枚举值
- FIRST_NAME应该是从Member字段拆分得到
- STATE应该是2字符州代码，需与CITY匹配

语义库包含{len(self.semantic_map.get("fields", {}))}个字段的详细信息。
"""

        # Agent填充3个关键部分
        task = Task(
            description=f"""
你将基于预定义的代码模板，填充3个关键部分的业务逻辑。

**基础模板** (已包含约700行框架代码):
```python
{template_code[:2000]}
...（共{len(template_code)}字符）
```

**规格书**:
{spec[:1000]}...
{semantic_context}

**你需要填充的3个部分**:

1. **SOURCE_GENERATION_NORMAL** (~200行)
   生成Source行数据的逻辑，基于used_source_fields：
   ```python
   return {{
       "Member": "LAST,FIRST M",
       "Product": self.fake.random_element(self._product_types),
       "State": state,
       "City": city,
       ...  # 其余28个字段
   }}
   ```

2. **TRANSFORMATION_METHODS** (~400行)
   为每个field_mapping生成转换方法：
   ```python
   def transform_PRODUCT_LINE(self, source: Dict) -> str:
       \"\"\"转换Product到PRODUCT_LINE\"\"\"
       product = source.get("Product", "")
       if product == "PDP":
           return "MD"
       elif product in ["LPPO", "LPPO SNP DE"]:
           return "MA/MAPD"
       ...
   ```

3. **FIELD_MAPPING_LOGIC** (~200行)
   主转换方法中的字段映射：
   ```python
   expected["PRODUCT_LINE"] = self.transform_PRODUCT_LINE(source)
   expected["FIRST_NAME"] = self.transform_FIRST_NAME(source)
   ...
   ```

**关键要求**:
1. 使用规格书中的field_mappings生成转换方法
2. 参考语义库确保转换逻辑符合业务含义
3. 所有方法必须有错误处理
4. 使用Faker生成真实数据
5. 州-城市要匹配（使用_get_random_state_city()）

**输出格式**:
请输出完整的Python代码（将3个PLACEHOLDER替换为实际代码）。
确保代码可以直接运行，不要遗漏任何部分。
            """,
            expected_output="完整的Python代码",
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

    def _test_code_smart(
        self, code: str, spec: str, source_path: str, expected_path: str
    ) -> Dict[str, Any]:
        """
        使用智能测试生成器测试代码 (V2核心优化)
        """
        # 解析spec
        try:
            spec_dict = json.loads(spec)
        except:
            if "```json" in spec:
                spec = spec.split("```json")[1].split("```")[0]
            elif "```" in spec:
                spec = spec.split("```")[1].split("```")[0]
            spec_dict = json.loads(spec.strip())

        # 生成智能测试
        test_generator = SmartTestGenerator(spec_dict)
        test_file_content = test_generator.generate_test_file(code)

        # 创建临时测试文件
        test_file = self.output_dir / "_test_runner_v2.py"
        with open(test_file, "w", encoding="utf-8") as f:
            f.write(test_file_content)

        # 执行测试
        try:
            result = subprocess.run(
                [sys.executable, "_test_runner_v2.py"],  # 只用文件名，因为cwd已经设置
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
        """Agent修复代码 (保持与V1兼容)"""
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
{current_code[:1000]}
...
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

3. **保持代码风格**
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
        """执行最终代码生成数据 (保持与V1兼容)"""
        exec_file = self.output_dir / "_final_executor_v2.py"

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
    with open("generated_data_v2.json", "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)
    
    print(f"Generated {{len(all_data)}} records")
    print("Data saved to generated_data_v2.json")
"""

        with open(exec_file, "w", encoding="utf-8") as f:
            f.write(exec_code)

        # 执行
        try:
            # 只使用文件名，因为cwd已经指定了目录
            exec_filename = "_final_executor_v2.py"
            result = subprocess.run(
                [sys.executable, exec_filename],
                capture_output=True,
                text=True,
                timeout=60,
                cwd=str(self.output_dir),
            )

            if result.returncode == 0:
                # 读取生成的数据
                data_file = self.output_dir / "generated_data_v2.json"
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
    print("测试自主代码生成器 V2...")

    generator = AutonomousCodeGeneratorV2(max_iterations=3)

    result = generator.run(
        rules_path="case/rules.xlsx",
        source_path="case/source.csv",
        expected_path="case/expected.txt",
    )

    if result["success"]:
        print(f"\n✅ 成功！迭代 {result['iterations']} 次 (V2)")
        print(f"📄 代码保存于: {result['code_path']}")
        print(f"📊 生成数据: {len(result['data'])} 条")
    else:
        print(f"\n❌ 失败: {result.get('error', 'Unknown error')}")
