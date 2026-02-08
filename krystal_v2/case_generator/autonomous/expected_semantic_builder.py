"""
Expected语义库构建器 - 自动分析Expected字段语义

这个模块会：
1. 读取expected.txt并分析所有字段
2. 基于字段名和样本数据推断业务含义
3. 识别常见模式和枚举值
4. 构建语义知识库（JSON格式）
5. 缓存以便后续使用
"""

import os
import json
import hashlib
from pathlib import Path
from typing import Dict, List, Any, Optional
from crewai import Agent, Task, Crew
from crewai.llm import LLM
from dotenv import load_dotenv


class ExpectedSemanticBuilder:
    """Expected语义库构建器"""

    def __init__(self, llm: Optional[LLM] = None):
        """
        初始化构建器

        Args:
            llm: CrewAI LLM实例（可选）
        """
        if llm is None:
            api_key = os.getenv("OPENAI_API_KEY")
            model = os.getenv("OPENAI_MODEL", "gpt-4o")
            llm = LLM(model=model, api_key=api_key) if api_key else None

        self.llm = llm
        self.agent = self._create_semantic_analyst_agent()
        self.cache_dir = Path("./generated_autonomous")
        self.cache_dir.mkdir(exist_ok=True)

    def _create_semantic_analyst_agent(self) -> Agent:
        """创建语义分析Agent"""
        return Agent(
            role="ETL数据语义分析专家",
            goal="分析数据字段的业务含义，构建准确的语义知识库",
            backstory="""
你是一位资深的数据架构师和业务分析师，拥有15年的ETL和数据建模经验。

你的核心能力：
1. 字段命名分析：通过字段名推断业务含义
2. 数据模式识别：从样本数据识别类型、格式、枚举值
3. 业务逻辑推断：理解字段间的关系和约束
4. 领域知识：熟悉医疗保险、金融、电商等常见领域

分析原则：
- 基于证据：只基于字段名和样本数据推断
- 保守推断：不确定时标记为"unknown"
- 结构化输出：使用标准JSON格式
- 详细文档：为每个字段提供清晰的描述
            """,
            verbose=True,
            allow_delegation=False,
            llm=self.llm,
        )

    def build_or_load_semantic_map(
        self, expected_path: str, force_rebuild: bool = False
    ) -> Dict[str, Any]:
        """
        构建或加载语义库

        Args:
            expected_path: expected.txt文件路径
            force_rebuild: 是否强制重建（忽略缓存）

        Returns:
            语义库字典
        """
        # 计算文件MD5作为缓存键
        file_hash = self._calculate_file_hash(expected_path)
        cache_file = self.cache_dir / f"expected_semantic_map_{file_hash[:8]}.json"

        # 检查缓存
        if not force_rebuild and cache_file.exists():
            print(f"📚 加载缓存的语义库: {cache_file}")
            with open(cache_file, "r", encoding="utf-8") as f:
                return json.load(f)

        # 构建新的语义库
        print(f"🔨 构建Expected语义库...")
        semantic_map = self._build_semantic_map(expected_path)

        # 保存缓存
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(semantic_map, f, ensure_ascii=False, indent=2)
        print(f"💾 语义库已缓存: {cache_file}")

        return semantic_map

    def _calculate_file_hash(self, filepath: str) -> str:
        """计算文件MD5哈希"""
        with open(filepath, "rb") as f:
            return hashlib.md5(f.read()).hexdigest()

    def _build_semantic_map(self, expected_path: str) -> Dict[str, Any]:
        """构建语义库"""
        # 读取expected.txt
        expected_data = self._read_expected_file(expected_path)

        # 准备分析材料
        fields = expected_data["headers"]
        sample_rows = expected_data["data"][:5]  # 取前5行样本

        # Agent分析
        task = Task(
            description=f"""
作为数据语义分析专家，请分析Expected数据的所有字段并构建语义知识库。

**输入数据：**
文件: {expected_path}

字段列表({len(fields)}个字段):
{", ".join(fields[:20])}...

样本数据(前5行):
{json.dumps(sample_rows[:2], ensure_ascii=False, indent=2)}

**分析任务：**

对每个字段，请分析并输出以下信息：

1. **business_meaning**: 业务含义（基于字段名推断）
   - 示例: "FIRST_NAME" → "会员名字"
   - 示例: "PRODUCT_LINE" → "产品线代码"
   - 示例: "CARRIER_EFFECTIVE_DATE" → "承保生效日期"

2. **data_type**: 数据类型
   - 选项: string, number, date, boolean, enum
   - 如果是string，标注长度范围: string(1-50)
   - 如果是date，标注格式: date(MM/DD/YYYY)

3. **common_patterns**: 常见模式/枚举值（从样本数据提取）
   - 如果是州代码: ["MO", "CA", "NY", "TX", "FL"]
   - 如果是产品类型: ["PDP", "LPPO", "HUM", "MD", "MS"]
   - 如果是状态: ["Active", "Termed"]
   - 如果没有明显模式: []

4. **possible_source_fields**: 可能的来源字段（基于命名相似性）
   - 示例: "FIRST_NAME" → ["Member", "FIRST_NAME", "Name"]
   - 示例: "STATE" → ["State", "STATE"]
   - 使用常见的命名变体（大小写、下划线、驼峰）

5. **validation_rules**: 验证规则
   - 示例: ["非空", "2字符州代码"]
   - 示例: ["日期格式MM/DD/YYYY"]
   - 示例: ["必须与CITY字段匹配"]

6. **derivation_hints**: 派生提示（如何从Source计算得到）
   - 示例: "FIRST_NAME" → "从Member字段拆分获取（格式: LAST,FIRST M）"
   - 示例: "PRODUCT_LINE" → "根据Product字段条件转换"
   - 示例: "IS_ACTIVE" → "根据STATUS字段计算（Active→1, Termed→0）"

**输出格式：**

请只输出JSON格式，不要其他内容：

{{
    "fields": {{
        "CARRIER_STATUS_MAP": {{
            "business_meaning": "承保状态映射",
            "data_type": "string",
            "common_patterns": ["Active", "Termed"],
            "possible_source_fields": ["Policy_Indicator", "STATUS"],
            "validation_rules": ["非空", "枚举值: Active|Termed"],
            "derivation_hints": "从Policy_Indicator字段映射"
        }},
        "FIRST_NAME": {{
            "business_meaning": "会员名字",
            "data_type": "string(1-50)",
            "common_patterns": [],
            "possible_source_fields": ["Member", "FIRST_NAME"],
            "validation_rules": ["字母字符"],
            "derivation_hints": "从Member字段拆分（格式: LAST,FIRST M）"
        }},
        ...（其余{len(fields)}个字段）
    }},
    "metadata": {{
        "total_fields": {len(fields)},
        "analyzed_samples": {len(sample_rows)},
        "confidence": "high|medium|low"
    }}
}}

**重要要求：**
1. 必须分析所有{len(fields)}个字段
2. 字段名必须与输入完全一致
3. 不确定时标记为"unknown"
4. 只输出JSON，不要markdown代码块标记
            """,
            expected_output="JSON格式的语义知识库",
            agent=self.agent,
        )

        crew = Crew(agents=[self.agent], tasks=[task], verbose=False)
        result = crew.kickoff()

        # 解析结果
        try:
            semantic_map = json.loads(result.raw)
            return semantic_map
        except json.JSONDecodeError:
            # 尝试提取JSON（如果Agent包含了markdown标记）
            raw = result.raw
            if "```json" in raw:
                raw = raw.split("```json")[1].split("```")[0]
            elif "```" in raw:
                raw = raw.split("```")[1].split("```")[0]

            try:
                semantic_map = json.loads(raw.strip())
                return semantic_map
            except:
                print("警告: 无法解析语义库JSON，返回空结构")
                return {"fields": {}, "metadata": {"error": "解析失败"}}

    def _read_expected_file(self, expected_path: str) -> Dict[str, Any]:
        """读取expected.txt文件"""
        with open(expected_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        # 前4行是元数据
        metadata = {}
        for i in range(min(4, len(lines))):
            if ":" in lines[i]:
                key, value = lines[i].strip().split(":", 1)
                metadata[key] = value.strip()

        # 第5行是表头
        if len(lines) > 4:
            headers = [h.strip() for h in lines[4].strip().split("|")]
        else:
            headers = []

        # 数据行
        data = []
        for line in lines[5:]:
            if line.strip():
                values = [v.strip() for v in line.strip().split("|")]
                if len(values) == len(headers):
                    row = dict(zip(headers, values))
                    data.append(row)

        return {"metadata": metadata, "headers": headers, "data": data}

    def get_field_semantics(
        self, semantic_map: Dict[str, Any], field_name: str
    ) -> Optional[Dict[str, Any]]:
        """
        获取特定字段的语义信息

        Args:
            semantic_map: 语义库
            field_name: 字段名

        Returns:
            字段语义信息，如果不存在返回None
        """
        return semantic_map.get("fields", {}).get(field_name)

    def search_source_candidates(
        self, semantic_map: Dict[str, Any], source_field: str
    ) -> List[str]:
        """
        根据Source字段名搜索可能的Expected字段

        Args:
            semantic_map: 语义库
            source_field: Source字段名

        Returns:
            可能的Expected字段列表
        """
        candidates = []
        fields = semantic_map.get("fields", {})

        source_field_lower = source_field.lower()

        for expected_field, semantics in fields.items():
            possible_sources = semantics.get("possible_source_fields", [])
            possible_sources_lower = [s.lower() for s in possible_sources]

            if source_field_lower in possible_sources_lower:
                candidates.append(expected_field)

        return candidates


if __name__ == "__main__":
    # 测试语义库构建器
    print("=" * 80)
    print("Expected语义库构建器测试")
    print("=" * 80)

    # 加载环境变量
    env_path = Path(__file__).parent.parent.parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)

    expected_path = "case/expected.txt"

    if not os.path.exists(expected_path):
        print(f"错误: 找不到文件 {expected_path}")
        print("请从项目根目录运行此脚本")
    else:
        builder = ExpectedSemanticBuilder()

        print(f"\n分析文件: {expected_path}")
        semantic_map = builder.build_or_load_semantic_map(expected_path)

        print(f"\n✅ 语义库构建完成")
        print(f"   总字段数: {semantic_map.get('metadata', {}).get('total_fields', 0)}")
        print(f"   已分析字段数: {len(semantic_map.get('fields', {}))}")

        # 显示几个示例字段
        fields = semantic_map.get("fields", {})
        if fields:
            print(f"\n示例字段语义:")
            for i, (field_name, semantics) in enumerate(list(fields.items())[:3]):
                print(f"\n{i + 1}. {field_name}:")
                print(f"   业务含义: {semantics.get('business_meaning', 'N/A')}")
                print(f"   数据类型: {semantics.get('data_type', 'N/A')}")
                print(f"   常见模式: {semantics.get('common_patterns', [])}")
