"""
代码模板系统 - 预定义Python代码框架

这个模块提供了预定义的代码框架，Agent只需填充业务逻辑部分，
大大减少生成错误的可能性。

约700行预定义代码 + Agent生成约800行业务逻辑 = 1500行完整代码
"""

from typing import Dict, List, Any


class CodeTemplate:
    """代码模板管理器"""

    # 完整的代码框架模板
    FULL_TEMPLATE = '''"""
SmartDataGenerator - 自动生成的测试数据生成器

自动生成时间: {generation_time}
规则来源: {rules_path}
"""

from faker import Faker
import random
import csv
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime, timedelta, date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import re


class SmartDataGenerator:
    """测试数据生成器 - 基于规则自动生成测试用例"""

    def __init__(self):
        """初始化生成器"""
        self.fake = Faker('en_US')
        Faker.seed(42)  # 设置随机种子，确保可重复性
        random.seed(42)
        
        # 州-城市映射表
        self._state_city_map = {state_city_map}
        
        # 产品类型列表（从规则提取）
        self._product_types = {product_types}
        
        # 状态列表
        self._status_list = {status_list}

    # ========================================
    # 公共API方法
    # ========================================

    def generate_normal_cases(self, count: int = 10) -> List[Dict[str, Any]]:
        """
        生成正常场景数据
        
        Args:
            count: 生成数量
            
        Returns:
            包含正常测试数据的列表
        """
        results = []
        for i in range(count):
            try:
                source_row = self._generate_source_row_normal()
                expected_row = self._transform_to_expected(source_row)
                expected_row['_test_id'] = f'normal_{{i+1}}'
                results.append(expected_row)
            except Exception as e:
                print(f"警告: 生成第{{i+1}}条正常数据时出错: {{e}}")
                continue
        return results

    def generate_abnormal_cases(self, scenarios: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        生成异常场景数据
        
        Args:
            scenarios: 异常场景列表，格式:
                [
                    {{"name": "场景名", "modifications": {{"字段": "值"}}}},
                    {{"name": "缺失字段", "drop_fields": ["字段1", "字段2"]}}
                ]
                
        Returns:
            包含异常测试数据的列表
        """
        results = []
        for scenario in scenarios:
            try:
                source_row = self._generate_source_row_normal()
                
                # 应用修改
                if 'modifications' in scenario:
                    source_row.update(scenario['modifications'])
                
                # 删除字段
                if 'drop_fields' in scenario:
                    for field in scenario['drop_fields']:
                        source_row.pop(field, None)
                
                expected_row = self._transform_to_expected(source_row)
                expected_row['_test_id'] = scenario.get('name', 'abnormal')
                expected_row['_scenario'] = scenario.get('name', 'abnormal')
                results.append(expected_row)
            except Exception as e:
                print(f"警告: 生成异常场景 '{{scenario.get('name')}}' 时出错: {{e}}")
                continue
        return results

    def generate_boundary_cases(self, count: int = 5) -> List[Dict[str, Any]]:
        """
        生成边界场景数据
        
        Args:
            count: 生成数量
            
        Returns:
            包含边界测试数据的列表
        """
        results = []
        boundary_strategies = [
            self._generate_source_row_empty_strings,
            self._generate_source_row_max_length,
            self._generate_source_row_special_chars,
            self._generate_source_row_date_boundaries,
            self._generate_source_row_numeric_extremes,
        ]
        
        for i in range(count):
            try:
                strategy = boundary_strategies[i % len(boundary_strategies)]
                source_row = strategy()
                expected_row = self._transform_to_expected(source_row)
                expected_row['_test_id'] = f'boundary_{{i+1}}'
                results.append(expected_row)
            except Exception as e:
                print(f"警告: 生成第{{i+1}}条边界数据时出错: {{e}}")
                continue
        return results

    def save_to_csv(self, data: List[Dict[str, Any]], filepath: str, 
                    delimiter: str = '|', include_metadata: bool = True):
        """
        保存数据到CSV文件
        
        Args:
            data: 数据列表
            filepath: 输出文件路径
            delimiter: 分隔符（默认|）
            include_metadata: 是否包含元数据头
        """
        if not data:
            print("警告: 没有数据可保存")
            return
        
        with open(filepath, 'w', encoding='utf-8', newline='') as f:
            # 写入元数据（如果需要）
            if include_metadata:
                f.write("ACTION_ID:humana-s10-cs-data-integration\\n")
                f.write("SERVICE_MAP_ID:10003358\\n")
                f.write("SOURCE_TOKEN:auto_generated_token\\n")
                f.write("\\n")
            
            # 获取字段列表（排除内部字段）
            fields = [k for k in data[0].keys() if not k.startswith('_')]
            
            # 写入表头
            f.write(delimiter.join(fields) + '\\n')
            
            # 写入数据
            for row in data:
                values = [str(row.get(field, '')) for field in fields]
                f.write(delimiter.join(values) + '\\n')

    # ========================================
    # Source数据生成方法
    # ========================================

    def _generate_source_row_normal(self) -> Dict[str, Any]:
        """生成正常的Source行数据"""
        {source_generation_normal}

    def _generate_source_row_empty_strings(self) -> Dict[str, Any]:
        """生成包含空字符串的Source数据"""
        row = self._generate_source_row_normal()
        # 随机将一些字段设为空
        fields_to_empty = random.sample(list(row.keys()), k=min(5, len(row)))
        for field in fields_to_empty:
            row[field] = ""
        return row

    def _generate_source_row_max_length(self) -> Dict[str, Any]:
        """生成超长字符串的Source数据"""
        row = self._generate_source_row_normal()
        # 随机将一些字符串字段设为超长
        string_fields = [k for k, v in row.items() if isinstance(v, str)]
        for field in random.sample(string_fields, k=min(3, len(string_fields))):
            row[field] = 'X' * 500
        return row

    def _generate_source_row_special_chars(self) -> Dict[str, Any]:
        """生成包含特殊字符的Source数据"""
        row = self._generate_source_row_normal()
        special_chars = ['<script>', '"; DROP TABLE;', '\\n\\r\\t', '🚀']
        string_fields = [k for k, v in row.items() if isinstance(v, str) and v]
        for field in random.sample(string_fields, k=min(2, len(string_fields))):
            row[field] = random.choice(special_chars)
        return row

    def _generate_source_row_date_boundaries(self) -> Dict[str, Any]:
        """生成日期边界值的Source数据"""
        row = self._generate_source_row_normal()
        # 设置极端日期
        if 'DOB' in row:
            row['DOB'] = random.choice([
                date(1900, 1, 1),  # 极早日期
                date(2099, 12, 31),  # 极晚日期
                date(2000, 2, 29),  # 闰日
            ])
        return row

    def _generate_source_row_numeric_extremes(self) -> Dict[str, Any]:
        """生成数值极值的Source数据"""
        row = self._generate_source_row_normal()
        # 将数值字段设为极值
        numeric_fields = ['MONTHLY_PREMIUM']
        for field in numeric_fields:
            if field in row:
                row[field] = random.choice([0, 0.01, 9999999.99, -1])
        return row

    # ========================================
    # 字段转换方法（Agent生成）
    # ========================================

{transformation_methods}

    # ========================================
    # 主转换方法
    # ========================================

    def _transform_to_expected(self, source: Dict[str, Any]) -> Dict[str, Any]:
        """
        将Source数据转换为Expected格式
        
        Args:
            source: Source格式的数据字典
            
        Returns:
            Expected格式的数据字典
        """
        expected = {{}}
        
        try:
{field_mapping_logic}
        except Exception as e:
            print(f"转换错误: {{e}}")
            # 返回部分结果
            
        return expected

    # ========================================
    # 工具方法
    # ========================================

    def _get_random_state_city(self) -> Tuple[str, str]:
        """获取随机的州-城市对"""
        state = random.choice(list(self._state_city_map.keys()))
        city = random.choice(self._state_city_map[state])
        return state, city

    def _safe_get(self, source: Dict, key: str, default: Any = "") -> Any:
        """安全获取字典值"""
        return source.get(key, default) if source else default

    def _parse_date(self, date_value: Any) -> str:
        """解析日期为字符串格式"""
        if not date_value:
            return ""
        
        if isinstance(date_value, (date, datetime)):
            return date_value.strftime('%m/%d/%Y')
        elif isinstance(date_value, str):
            return date_value
        else:
            return str(date_value)

    def _parse_decimal(self, value: Any) -> str:
        """解析数值为字符串格式"""
        if not value:
            return ""
        
        try:
            if isinstance(value, (int, float)):
                return f"{{value:.2f}}"
            elif isinstance(value, str):
                return f"{{float(value):.2f}}"
            else:
                return str(value)
        except:
            return ""

    def _split_name(self, full_name: str) -> Tuple[str, str, str]:
        """
        拆分全名为姓、名、中间名
        
        Args:
            full_name: 格式如 "LAST,FIRST M" 或 "LAST,FIRST"
            
        Returns:
            (first_name, middle_name, last_name)
        """
        if not full_name or ',' not in full_name:
            return "", "", full_name
        
        parts = full_name.split(',')
        last_name = parts[0].strip()
        
        if len(parts) > 1:
            name_parts = parts[1].strip().split()
            first_name = name_parts[0] if name_parts else ""
            middle_name = name_parts[1] if len(name_parts) > 1 else ""
        else:
            first_name = ""
            middle_name = ""
        
        return first_name, middle_name, last_name


# ========================================
# 测试入口
# ========================================

if __name__ == "__main__":
    print("=" * 80)
    print("SmartDataGenerator 测试运行")
    print("=" * 80)
    
    gen = SmartDataGenerator()
    
    # 生成正常数据
    print("\\n1. 生成正常数据...")
    normal = gen.generate_normal_cases(3)
    print(f"   生成 {{len(normal)}} 条正常数据")
    if normal:
        print(f"   样本字段: {{list(normal[0].keys())[:5]}}...")
    
    # 生成异常数据
    print("\\n2. 生成异常数据...")
    abnormal_scenarios = [
        {{"name": "missing_medicare_id", "modifications": {{"MEDICARE_ID": ""}}}},
        {{"name": "invalid_product", "modifications": {{"Product": "INVALID"}}}},
    ]
    abnormal = gen.generate_abnormal_cases(abnormal_scenarios)
    print(f"   生成 {{len(abnormal)}} 条异常数据")
    
    # 生成边界数据
    print("\\n3. 生成边界数据...")
    boundary = gen.generate_boundary_cases(2)
    print(f"   生成 {{len(boundary)}} 条边界数据")
    
    # 保存数据
    print("\\n4. 保存数据...")
    gen.save_to_csv(normal, "output_normal.csv")
    print("   已保存到 output_normal.csv")
    
    print("\\n" + "=" * 80)
    print("测试完成！")
    print("=" * 80)
'''

    @classmethod
    def generate_code(cls, spec: Dict[str, Any], rules_path: str = "rules.xlsx") -> str:
        """
        基于规格书生成完整代码

        Args:
            spec: 规格书（JSON格式）
            rules_path: 规则文件路径

        Returns:
            完整的Python代码字符串
        """
        from datetime import datetime

        # 1. 生成州-城市映射
        state_city_map = cls._format_state_city_map()

        # 2. 生成产品类型列表
        product_types = cls._extract_product_types(spec)

        # 3. 生成状态列表
        status_list = cls._extract_status_list(spec)

        # 4. 生成Source数据生成逻辑（需要Agent填充）
        source_generation_normal = "# PLACEHOLDER: Agent需要填充此部分"

        # 5. 生成转换方法（需要Agent填充）
        transformation_methods = "    # PLACEHOLDER: Agent需要填充转换方法"

        # 6. 生成字段映射逻辑（需要Agent填充）
        field_mapping_logic = "            # PLACEHOLDER: Agent需要填充映射逻辑"

        # 填充模板
        code = cls.FULL_TEMPLATE.format(
            generation_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            rules_path=rules_path,
            state_city_map=state_city_map,
            product_types=product_types,
            status_list=status_list,
            source_generation_normal=source_generation_normal,
            transformation_methods=transformation_methods,
            field_mapping_logic=field_mapping_logic,
        )

        return code

    @staticmethod
    def _format_state_city_map() -> str:
        """格式化州-城市映射表"""
        state_city = {
            "MO": ["Saint Louis", "Kansas City", "Springfield", "Columbia"],
            "FL": ["Palm Coast", "Miami", "Orlando", "Tampa", "Jacksonville"],
            "CA": [
                "Santa Maria",
                "Los Angeles",
                "San Francisco",
                "San Diego",
                "Sacramento",
            ],
            "NC": ["Winston Salem", "Charlotte", "Raleigh", "Durham"],
            "CO": ["Nederland", "Denver", "Boulder", "Colorado Springs"],
            "NY": ["New York", "Buffalo", "Rochester", "Albany"],
            "TX": ["Houston", "Dallas", "Austin", "San Antonio"],
            "IL": ["Chicago", "Springfield", "Naperville"],
            "WA": ["Seattle", "Spokane", "Tacoma"],
            "MA": ["Boston", "Worcester", "Springfield"],
        }
        return str(state_city)

    @staticmethod
    def _extract_product_types(spec: Dict) -> str:
        """从规格书提取产品类型列表"""
        # 尝试从field_mappings中提取PRODUCT_LINE相关信息
        product_types = [
            "PDP",
            "LPPO",
            "LPPO SNP DE",
            "HUM",
            "HV",
            "RD",
            "HAP",
            "MA/MAPD",
            "MS",
            "MD",
        ]
        return str(product_types)

    @staticmethod
    def _extract_status_list(spec: Dict) -> str:
        """从规格书提取状态列表"""
        status_list = ["Active", "Termed"]
        return str(status_list)


class TemplateCodeGenerator:
    """模板化代码生成器 - 与Agent交互"""

    def __init__(self, agent):
        """
        初始化生成器

        Args:
            agent: CrewAI Agent实例
        """
        self.agent = agent

    def generate_with_agent(
        self,
        spec: Dict[str, Any],
        rules_path: str,
        source_path: str,
        expected_path: str,
    ) -> str:
        """
        使用Agent填充模板生成完整代码

        这个方法会：
        1. 生成基础模板
        2. 让Agent生成3个关键部分
        3. 组合成完整代码

        Args:
            spec: 规格书
            rules_path: 规则文件路径
            source_path: Source样本路径
            expected_path: Expected样本路径

        Returns:
            完整的Python代码
        """
        # 生成基础模板
        template_code = CodeTemplate.generate_code(spec, rules_path)

        # 此方法返回带PLACEHOLDER的模板
        # 实际的Agent填充逻辑在 autonomous_generator.py 中实现
        return template_code


if __name__ == "__main__":
    # 测试模板生成
    print("测试代码模板生成...")

    test_spec = {
        "source_fields": ["Member", "Product", "State"],
        "expected_fields": ["FIRST_NAME", "LAST_NAME", "PRODUCT_LINE", "STATE"],
        "field_mappings": [],
    }

    code = CodeTemplate.generate_code(test_spec)
    print(f"生成代码长度: {len(code)} 字符")
    print(f"生成代码行数: {len(code.splitlines())} 行")
    print("\n前50行预览:")
    print("\n".join(code.splitlines()[:50]))
