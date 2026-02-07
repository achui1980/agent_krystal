#!/usr/bin/env python3
"""
Expected Output Generator Agent
基于Agent智能分析生成符合规范的Expected数据
"""

import os
import sys
import csv
import random
import string
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Tuple

from faker import Faker

# 添加项目路径
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv

# 加载环境变量
env_path = project_root / ".env"
if env_path.exists():
    load_dotenv(env_path)

from crewai import Agent, Task, Crew
from crewai.llm import LLM


class ExpectedOutputGenerator:
    """Expected输出生成器Agent"""

    def __init__(self):
        self.output_dir = Path("generated_autonomous/output")
        self.output_dir.mkdir(exist_ok=True, parents=True)

        # 初始化LLM
        api_key = os.getenv("OPENAI_API_KEY")
        model = os.getenv("OPENAI_MODEL", "gpt-4o")
        self.llm = LLM(model=model, api_key=api_key) if api_key else None

        if not self.llm:
            raise ValueError("OPENAI_API_KEY not found!")

    def run(self):
        """执行完整的生成流程"""
        print("=" * 80)
        print("🤖 Expected Output Generator Agent")
        print("=" * 80)
        print()

        # Step 1: 读取和分析文件
        print("📖 步骤1: 读取输入文件...")
        source_data = self._read_source_csv()
        expected_template = self._read_expected_template()
        print(f"   ✅ 读取Source数据: {len(source_data)} 行")
        print(f"   ✅ 读取Expected模板: {len(expected_template.split(chr(10)))} 行")
        print()

        # Step 2: Agent格式分析
        print("🔍 步骤2: Agent分析格式规范...")
        format_analysis = self._agent_analyze_format(expected_template)
        self._save_document("expected_format_analysis.md", format_analysis)
        print("   ✅ 格式分析完成")
        print()

        # Step 3: Agent映射分析
        print("🔗 步骤3: Agent分析字段映射...")
        mapping_analysis = self._agent_analyze_mapping(source_data, expected_template)
        self._save_document("field_mapping_analysis.md", mapping_analysis)
        print("   ✅ 映射分析完成")
        print()

        # Step 4: 生成数据
        print("🎯 步骤4: 生成20行Expected数据...")
        generated_content = self._generate_expected_data(
            source_data, format_analysis, mapping_analysis
        )
        print("   ✅ 数据生成完成")
        print()

        # Step 5: 验证和对比
        print("✅ 步骤5: 验证并与原始文件对比...")
        validation_report = self._validate_and_compare(
            generated_content, expected_template
        )
        self._save_document("validation_comparison_report.md", validation_report)
        print("   ✅ 验证完成")
        print()

        # Step 6: 保存最终输出
        print("💾 步骤6: 保存最终文件...")
        self._save_expected_output(generated_content)
        self._save_document(
            "data_generation_report.md",
            self._generate_report(source_data, generated_content),
        )
        print("   ✅ 文件保存完成")
        print()

        # 输出总结
        print("=" * 80)
        print("🎉 全部完成！")
        print("=" * 80)
        print()
        print("📁 输出文件:")
        print(f"  📄 expected_output.txt                     - 最终数据（20行）")
        print(f"  📄 expected_format_analysis.md             - 格式分析")
        print(f"  📄 field_mapping_analysis.md              - 字段映射")
        print(f"  📄 validation_comparison_report.md        - 验证对比报告")
        print(f"  📄 data_generation_report.md               - 生成报告")
        print()

    def _read_source_csv(self) -> List[Dict[str, str]]:
        """读取Source CSV文件"""
        source_path = Path("case/source.csv")
        data = []
        with open(source_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append(row)
        return data

    def _read_expected_template(self) -> str:
        """读取Expected模板文件"""
        expected_path = Path("case/expected.txt")
        with open(expected_path, "r", encoding="utf-8") as f:
            return f.read()

    def _agent_analyze_format(self, expected_template: str) -> str:
        """Agent任务: 分析格式规范"""
        agent = Agent(
            role="数据格式分析专家",
            goal="深入分析expected.txt文件格式，提取所有格式规范",
            backstory="你是一位资深的数据格式分析专家，擅长理解复杂的文本文件结构。你能够准确识别文件的组织方式、分隔符、字段类型和数据格式。",
            verbose=True,
            allow_delegation=False,
            llm=self.llm,
        )

        task = Task(
            description=f"""
请分析以下expected.txt文件内容，提取完整的格式规范：

【文件内容】
{expected_template[:2000]}...（显示前2000字符）

请分析并输出：
1. 文件整体结构（元数据行数、表头位置、数据起始行）
2. 分隔符类型和特殊处理
3. 总字段数统计
4. 表头字段完整列表（93个字段）
5. 数据类型分类（字符串、日期、数字、代码等）
6. 特殊值的处理方式（空值、日期格式、千分位数字等）
7. 固定值字段列表

输出格式：Markdown文档，包含清晰的章节和列表。
            """,
            expected_output="详细的格式分析Markdown文档",
            agent=agent,
        )

        crew = Crew(agents=[agent], tasks=[task], verbose=False)
        result = crew.kickoff()
        return result.raw

    def _agent_analyze_mapping(
        self, source_data: List[Dict], expected_template: str
    ) -> str:
        """Agent任务: 分析字段映射"""
        agent = Agent(
            role="数据映射分析专家",
            goal="建立Source.csv到Expected.txt的完整字段映射关系",
            backstory="你是一位ETL映射专家，擅长分析源系统和目标系统的字段对应关系。你能够识别数据转换逻辑和映射规则。",
            verbose=True,
            allow_delegation=False,
            llm=self.llm,
        )

        # 准备Source字段示例
        source_sample = source_data[0] if source_data else {}
        source_fields = list(source_sample.keys())[:15]

        task = Task(
            description=f"""
请分析Source.csv到Expected.txt的字段映射关系：

【Source字段】（共28个）
{chr(10).join([f"- {k}" for k in source_fields])}...

【Source数据示例】
{chr(10).join([f"{k}: {v}" for k, v in list(source_sample.items())[:10]])}...

【Expected表头】
从expected.txt第4行提取的93个字段

请分析并输出：
1. 完整的字段映射表（Source字段 → Expected字段）
2. 每个映射的转换逻辑：
   - 直接复制（无需转换）
   - 格式转换（如日期格式）
   - 数据映射（如产品代码）
   - 字段拆分（如姓名拆分为First/Last）
   - 字段合并（如Plan拆分为Contract/Plan）
3. 固定值字段及其值
4. 无法映射的字段（需要用空字符串填充）
5. 特殊处理逻辑说明

重点映射分析：
- Source.Product (PDP/HMO/PPO) → Expected.PRODUCT_LINE (MD/MA/MAPD/MS)
- Source.Member (Last,First) → Expected.FIRST_NAME + LAST_NAME
- Source.Plan_Name (S5884-197) → Expected.CMS_CONTRACT_ID + CMS_PLAN_ID
- Source.DOB (YYYY-MM-DD) → Expected.BIRTH_DATE (MM/DD/YYYY)

输出格式：Markdown表格和说明文档。
            """,
            expected_output="详细的字段映射分析Markdown文档",
            agent=agent,
        )

        crew = Crew(agents=[agent], tasks=[task], verbose=False)
        result = crew.kickoff()
        return result.raw

    def _generate_expected_data(
        self, source_data: List[Dict], format_analysis: str, mapping_analysis: str
    ) -> str:
        """生成20行Expected数据"""
        # 读取原始expected获取表头
        expected_lines = self._read_expected_template().split("\n")
        header_line = expected_lines[3] if len(expected_lines) > 3 else ""

        # 构建输出内容
        lines = []

        # 元数据
        lines.append("ACTION_ID:humana-s10-cs-data-integration")
        lines.append("SERVICE_MAP_ID:10003358")
        lines.append(f"SOURCE_TOKEN:{self._generate_source_token()}")
        lines.append("")  # 空行

        # 表头
        lines.append(header_line)

        # 生成20行数据
        base_row = source_data[0] if source_data else {}
        for i in range(20):
            row_data = self._generate_row_variant(base_row, i)
            formatted_row = self._format_expected_row(row_data, header_line)
            lines.append(formatted_row)

        return "\n".join(lines)

    def _generate_source_token(self) -> str:
        """生成符合示例格式的SOURCE_TOKEN"""
        random_part = "".join(random.choices(string.hexdigits.lower(), k=32))
        timestamp = str(int(datetime.now().timestamp() * 1000))
        return f"{random_part}_{timestamp}"

    def _generate_row_variant(
        self, base_row: Dict[str, str], variant_index: int
    ) -> Dict[str, Any]:
        """生成数据变体"""
        fake = __import__("faker").Faker("en_US")
        random.seed(variant_index)

        row = {}

        # 生成新的姓名（Last,First格式）
        first_name = fake.first_name()
        last_name = fake.last_name()
        row["member_name"] = f"{last_name},{first_name} "

        # 生成Agent姓名
        agent_first = fake.first_name()
        agent_last = fake.last_name()
        row["agent_name"] = f"{agent_last}, {agent_first}"

        # 生成地址
        row["address"] = fake.street_address()

        # 生成城市和州（匹配）
        state = random.choice(["MO", "CA", "NY", "TX", "FL"])
        city_map = {
            "MO": ["Saint Louis", "Kansas City", "Springfield"],
            "CA": ["Los Angeles", "San Francisco", "San Diego"],
            "NY": ["New York", "Buffalo", "Rochester"],
            "TX": ["Houston", "Dallas", "Austin"],
            "FL": ["Miami", "Orlando", "Tampa"],
        }
        row["city"] = random.choice(city_map.get(state, ["Unknown"]))
        row["state"] = state

        # 生成邮编（匹配州）
        row["zip"] = fake.zipcode_in_state(state_abbr=state)

        # 生成Medicare ID（11位字母数字）
        row["medicare_id"] = "".join(
            random.choices(string.ascii_uppercase + string.digits, k=11)
        )

        # 生成DOB（1940-2005年间）
        dob = fake.date_of_birth(minimum_age=18, maximum_age=85)
        row["dob"] = dob.strftime("%Y-%m-%d")

        # 生成Product（PDP, HMO, PPO）
        products = ["PDP", "HMO", "PPO"]
        row["product"] = random.choice(products)

        # 生成Plan（S####-###格式）
        contract = f"S{random.randint(1000, 9999)}"
        plan_id = str(random.randint(100, 999))
        row["plan_name"] = f"{contract}-{plan_id}"

        # 生成Eff_Date（2024-2026年）
        eff_date = fake.date_between(start_date="2024-01-01", end_date="2026-12-31")
        row["eff_date"] = eff_date.strftime("%Y-%m-%d")

        # 生成Term_Date（80%为9999-12-31，20%为具体日期）
        if random.random() < 0.8:
            row["term_date"] = "9999-12-31"
            row["status"] = "Active"
        else:
            term_date = fake.date_between(
                start_date="2025-01-01", end_date="2026-12-31"
            )
            row["term_date"] = term_date.strftime("%Y-%m-%d")
            row["status"] = "Termed"

        # 固定值
        row["aor_name"] = "EHEALTHINSURANCE SERVICES INC"
        row["aor_san"] = "1273481"
        row["san"] = str(random.randint(100000, 999999))
        row["npn"] = str(random.randint(10000000, 99999999))

        return row

    def _format_expected_row(self, row_data: Dict[str, Any], header_line: str) -> str:
        """格式化Expected行数据"""
        fields = header_line.split("|")
        values = []

        for field in fields:
            value = self._get_field_value(field, row_data)
            values.append(value)

        return "|".join(values)

    def _get_field_value(self, field: str, row_data: Dict[str, Any]) -> str:
        """根据字段名获取值"""
        field = field.strip()

        # 映射逻辑
        mapping = {
            "CARRIER_STATUS_MAP": lambda: row_data.get("status", "Active"),
            "CARRIER_FAMILY_ID": lambda: "66,175,206",
            "PARENT_CARRIER_ID": lambda: "",
            "IS_PAID": lambda: "1",
            "BUSINESS_LINE": lambda: "2",
            "APPLICATION_ID": lambda: "",
            "MEMBER_NUMBER": lambda: "1",
            "POLICY_ID": lambda: "",
            "CARRIER_ID": lambda: "",
            "PLAN_ID": lambda: "",
            "RIDER_ID": lambda: "1",
            "CATEGORY_CLASS_ID": lambda: "1",
            "REQUESTED_EFFECTIVE_DATE": lambda: "",
            "STATUS_CODE": lambda: "",
            "PRODUCT_LINE": lambda: self._map_product(row_data.get("product", "PDP")),
            "REVENUE_IMPACT_DATE": lambda: "",
            "POLICY_NUMBER": lambda: "",
            "MONTHLY_PREMIUM": lambda: "",
            "RATE_TIER": lambda: "",
            "NOT_CANCELLED": lambda: "",
            "IS_DELINQUENT": lambda: "",
            "DELINQUENCY_NOTE": lambda: "",
            "MEMBER_COUNT": lambda: "",
            "CARRIER_EFFECTIVE_DATE": lambda: "",
            "EFFECTIVE_START_DATE": lambda: self._format_date(
                row_data.get("eff_date", "")
            ),
            "CANCELLATION_DATE": lambda: self._format_date(
                row_data.get("term_date", "")
            ),
            "IS_ACTIVE": lambda: "",
            "IS_REVERSED": lambda: "",
            "IS_MASTER_POLICY": lambda: "",
            "HEALTH_RATE_FACTOR": lambda: "",
            "HOUSEHOLE_DISCOUNT": lambda: "",
            "PREMIUM_EFFECTIVE_DATE": lambda: "",
            "FUTURE_RATE_CHANGE_DATE": lambda: "",
            "FUTURE_PREMIUM": lambda: "",
            "FREQUENCY_TO_DEBIT": lambda: "",
            "CARRIER_PAID_THRU_DATE": lambda: "",
            "ADDRESS_TYPE": lambda: "",
            "ADDRESS_LINE_1": lambda: row_data.get("address", ""),
            "ADDRESS_LINE_2": lambda: "",
            "ADDRESS_LINE_3": lambda: "",
            "CITY": lambda: row_data.get("city", ""),
            "STATE": lambda: row_data.get("state", ""),
            "ZIP_CODE": lambda: row_data.get("zip", ""),
            "COUNTY": lambda: "",
            "FIRST_NAME": lambda: self._parse_first_name(
                row_data.get("member_name", "")
            ),
            "MIDDLE_NAME": lambda: "",
            "LAST_NAME": lambda: self._parse_last_name(row_data.get("member_name", "")),
            "GENDER": lambda: "",
            "BIRTH_DATE": lambda: self._format_date(row_data.get("dob", "")),
            "SSN": lambda: "",
            "EMAIL": lambda: "",
            "MEDICARE_ID": lambda: row_data.get("medicare_id", ""),
            "CARRIER_CONFIRMATION_NUMBER": lambda: "",
            "PHONE_TYPE": lambda: "",
            "AREA_CODE": lambda: "",
            "PHONE_NUMBER": lambda: "",
            "PHONE_EXTENSION": lambda: "",
            "STATUS_NOTE": lambda: "",
            "CATEGORY": lambda: "",
            "SUBSCRIBER_COUNT": lambda: "",
            "GROUP_NAME": lambda: "",
            "GROUP_ID": lambda: "",
            "RAF": lambda: "",
            "CARRIER_APPLICATION_ID": lambda: "",
            "CARRIER_POLICY_ID": lambda: "",
            "RECORD_TYPE": lambda: "",
            "CREATION_DATE": lambda: "",
            "SUBMIT_DATE": lambda: "",
            "SIGNITURE_DATE": lambda: "",
            "SEP_REASON_CODE": lambda: "",
            "EXTRA_HELP": lambda: "",
            "EXTRA_HELP_LEVEL": lambda: "",
            "CARRIER_NAME": lambda: "",
            "PLAN_NAME": lambda: self._parse_plan_name(row_data.get("plan_name", ""))[
                1
            ],
            "CMS_CONTRACT_ID": lambda: self._parse_plan_name(
                row_data.get("plan_name", "")
            )[0],
            "CMS_PLAN_ID": lambda: self._parse_plan_name(row_data.get("plan_name", ""))[
                2
            ],
            "CMS_SEGMENT_ID": lambda: "",
            "RENEWAL_DATE": lambda: "",
            "RENEWAL_TYPE": lambda: "",
            "INITIAL_PAYMENT_MODE": lambda: "",
            "CMS_DISENROLLMENT_CODE": lambda: "",
            "STATUS_DATE": lambda: "",
            "PLAN_CHANGE_NOTE": lambda: "",
            "CMS_CARRIER_POST_TERM": lambda: "",
            "CMS_CONTRACT_ID_POST_TERM": lambda: "",
            "CMS_PLAN_ID_POST_TERM": lambda: "",
            "CMS_SEGMENT_ID_POST_TERM": lambda: "",
            "BROKER_OF_RECORD": lambda: "",
            "AGENCY_NAME": lambda: row_data.get("aor_name", ""),
            "AGENCY_ID": lambda: row_data.get("aor_san", ""),
            "AGENT_NAME": lambda: row_data.get("agent_name", ""),
            "AGENT_ID": lambda: row_data.get("san", ""),
            "LAST_TOUCHED_DATE": lambda: "",
        }

        if field in mapping:
            return mapping[field]()
        return ""

    def _map_product(self, product: str) -> str:
        """产品代码映射"""
        mapping = {"PDP": "MD", "HMO": "MA/MAPD", "PPO": "MS"}
        return mapping.get(product, product)

    def _format_date(self, date_str: str) -> str:
        """日期格式转换 YYYY-MM-DD → MM/DD/YYYY"""
        if not date_str or date_str == "9999-12-31":
            return "12/31/9999"
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            return dt.strftime("%m/%d/%Y")
        except:
            return date_str

    def _parse_first_name(self, member_name: str) -> str:
        """从 Member (Last,First) 解析 First Name"""
        if "," in member_name:
            parts = member_name.split(",")
            if len(parts) > 1:
                return parts[1].strip()
        return member_name.strip()

    def _parse_last_name(self, member_name: str) -> str:
        """从 Member (Last,First) 解析 Last Name"""
        if "," in member_name:
            return member_name.split(",")[0].strip()
        return member_name.strip()

    def _parse_plan_name(self, plan_name: str) -> Tuple[str, str, str]:
        """解析 Plan_Name (S5884-197) → (Contract, PlanName, PlanId)"""
        if "-" in plan_name:
            parts = plan_name.split("-")
            return (parts[0], plan_name, parts[1])
        return ("", plan_name, "")

    def _validate_and_compare(
        self, generated_content: str, expected_template: str
    ) -> str:
        """Agent任务: 验证并对比"""
        agent = Agent(
            role="数据质量验证专家",
            goal="验证生成数据的正确性并与原始文件对比",
            backstory="你是一位数据质量专家，擅长数据验证和差异分析。",
            verbose=True,
            allow_delegation=False,
            llm=self.llm,
        )

        # 计算统计信息
        gen_lines = generated_content.split("\n")
        data_lines = [
            l for l in gen_lines if "|" in l and "CARRIER_STATUS_MAP" not in l
        ]
        field_count = len(data_lines[0].split("|")) if data_lines else 0

        task = Task(
            description=f"""
请验证生成的Expected数据并与原始文件对比：

【生成的数据统计】
- 总行数: {len(gen_lines)}
- 数据行数: {len(data_lines)}
- 每行字段数: {field_count}
- 分隔符: |

【生成的数据样本】（前3行）
{chr(10).join(gen_lines[:6])}

【原始文件样本】（case/expected.txt）
{chr(10).join(expected_template.split(chr(10))[:6])}

请进行以下验证：
1. ✅ 格式验证：
   - 元数据格式正确（3行）
   - 分隔符使用正确
   - 字段数量 = 93
   - 空值处理正确（空字符串）

2. 📊 数据验证：
   - 日期格式 = MM/DD/YYYY
   - 产品代码映射正确（PDP→MD等）
   - 姓名字段解析正确
   - Plan解析正确
   - 固定值与示例一致

3. 🔍 对比分析：
   - 与原始文件的字段级对比
   - 数据格式一致性
   - 特殊值处理对比

4. 📝 问题报告（如有）：
   - 发现的任何问题
   - 建议的改进

输出详细的验证报告（Markdown格式）。
            """,
            expected_output="详细的验证对比报告（Markdown格式）",
            agent=agent,
        )

        crew = Crew(agents=[agent], tasks=[task], verbose=False)
        result = crew.kickoff()
        return result.raw

    def _save_document(self, filename: str, content: str):
        """保存文档"""
        filepath = self.output_dir / filename
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"   💾 已保存: {filename}")

    def _save_expected_output(self, content: str):
        """保存最终的expected输出"""
        filepath = self.output_dir / "expected_output.txt"
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"   💾 已保存: expected_output.txt")

    def _generate_report(self, source_data: List[Dict], generated_content: str) -> str:
        """生成数据生成报告"""
        lines = generated_content.split("\n")
        data_lines = [l for l in lines if "|" in l and "CARRIER_STATUS_MAP" not in l]

        report = f"""# 数据生成报告

## 生成时间
{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## 输入文件
- Source: case/source.csv ({len(source_data)} 行)
- Template: case/expected.txt

## 输出统计
- 生成数据行数: {len(data_lines)}
- 目标字段数: 93
- 分隔符: |
- 日期格式: MM/DD/YYYY

## 数据特征
- 正常场景: Active状态 (约80%)
- 终止场景: Termed状态 (约20%)
- 产品分布: PDP→MD, HMO→MA/MAPD, PPO→MS
- 日期范围: 1940-2005年出生, 2024-2026年生效

## 字段映射
- 直接复制: ADDRESS_LINE_1, CITY, STATE, ZIP_CODE, MEDICARE_ID
- 格式转换: DOB→BIRTH_DATE, Eff_Date→EFFECTIVE_START_DATE
- 代码映射: Product→PRODUCT_LINE
- 字段拆分: Member→FIRST_NAME+LAST_NAME, Plan_Name→CMS_CONTRACT_ID+CMS_PLAN_ID
- 固定值: CARRIER_FAMILY_ID=66,175,206等

## 输出文件
- expected_output.txt
"""
        return report


if __name__ == "__main__":
    generator = ExpectedOutputGenerator()
    generator.run()
