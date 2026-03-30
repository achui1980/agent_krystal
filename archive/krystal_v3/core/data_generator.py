"""
数据生成器

生成 Source 和 Expected 数据
"""

from typing import Dict, List, Any
from pathlib import Path
import sys
import random

# 添加父目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from faker import Faker
from datetime import datetime, timedelta
from models.rule import TransformationConfig
from core.transformation_engine import TransformationEngine
from utils.logger import get_logger


class DataGenerator:
    """数据生成器"""

    def __init__(self, config: TransformationConfig):
        self.config = config
        self.logger = get_logger()
        self.fake = Faker("en_US")

        # 设置随机种子保证可重复性
        Faker.seed(42)
        random.seed(42)

        # 州-城市映射
        self.state_city_map = {
            "MO": ["Saint Louis", "Kansas City", "Springfield"],
            "FL": ["Palm Coast", "Miami", "Orlando", "Tampa"],
            "CA": ["Los Angeles", "San Francisco", "San Diego"],
            "PA": ["Pittsburgh", "Philadelphia", "Harrisburg"],
            "TX": ["Houston", "Dallas", "Austin"],
            "NY": ["New York", "Buffalo", "Rochester"],
            "IL": ["Chicago", "Springfield", "Naperville"],
        }

        # 产品类型
        self.product_types = ["PDP", "LPPO", "HUM", "HV", "MAPD"]

        # 创建转换引擎
        self.transform_engine = TransformationEngine(config)

    def generate_source_record(self) -> Dict[str, str]:
        """
        生成单条源数据记录

        Returns:
            源数据字典
        """
        record = {}

        # 根据配置中的源字段生成数据
        for field in self.config.source_fields:
            value = self._generate_field_value(field)
            record[field] = value

        return record

    def _generate_field_value(self, field: str) -> str:
        """
        根据字段名生成值

        Args:
            field: 字段名

        Returns:
            生成的值
        """
        field_lower = field.lower()

        # 姓名字段
        if (
            "member" in field_lower
            or "applicant" in field_lower
            and "name" in field_lower
        ):
            if "first" in field_lower:
                return self.fake.first_name()
            elif "last" in field_lower:
                return self.fake.last_name()
            else:
                # 全名格式: LAST,FIRST
                return f"{self.fake.last_name()},{self.fake.first_name()}"

        # 地址字段
        elif "address" in field_lower or "addr" in field_lower:
            return self.fake.street_address()

        # 城市
        elif "city" in field_lower:
            state = random.choice(list(self.state_city_map.keys()))
            return random.choice(self.state_city_map[state])

        # 州
        elif "state" in field_lower:
            return random.choice(list(self.state_city_map.keys()))

        # 邮编
        elif "zip" in field_lower:
            return self.fake.zipcode()

        # 日期字段
        elif "date" in field_lower or "dob" in field_lower:
            if "birth" in field_lower or "dob" in field_lower:
                # 生日，生成较早的日期
                birth_date = self.fake.date_of_birth(minimum_age=65, maximum_age=90)
                return birth_date.strftime("%m/%d/%Y")
            elif "eff" in field_lower:
                # 生效日期
                eff_date = datetime.now() + timedelta(days=random.randint(1, 365))
                return eff_date.strftime("%Y-%m-%d")
            elif "term" in field_lower:
                # 终止日期
                return "9999-12-31"
            else:
                return self.fake.date_this_decade().strftime("%Y-%m-%d")

        # 产品类型
        elif "product" in field_lower or "plan_type" in field_lower:
            return random.choice(self.product_types)

        # 计划名称 (CMS格式: S5884-197)
        elif "plan_name" in field_lower:
            prefix = random.choice(["S", "H"])
            return f"{prefix}{self.fake.random_int(1000, 9999)}-{self.fake.random_int(100, 999)}"

        # 合同ID (格式: XXXX-XXX)
        elif "contract" in field_lower or "plan_id" in field_lower:
            prefix = random.choice(["S", "H"])
            return f"{prefix}{self.fake.random_int(1000, 9999)}-{self.fake.random_int(100, 999)}"

        # ID 类字段
        elif "id" in field_lower or "hicn" in field_lower or "medicare" in field_lower:
            return str(self.fake.random_int(10000000, 99999999))

        # 电话号码
        elif "phone" in field_lower:
            return self.fake.phone_number()

        # 邮箱
        elif "email" in field_lower:
            return self.fake.email()

        # SSN
        elif "ssn" in field_lower:
            return self.fake.ssn()

        # 代理机构相关 (AOR_Name, AOR_SAN)
        elif "aor" in field_lower:
            if "name" in field_lower:
                return self.fake.company()
            elif "san" in field_lower or "id" in field_lower:
                return str(self.fake.random_int(1000000, 9999999))
            else:
                return self.fake.name()

        # Agent 相关 (Agent, SAN)
        elif "agent" in field_lower or field_lower == "san":
            if "name" in field_lower or field_lower == "agent":
                return self.fake.name()
            elif "san" in field_lower or field_lower == "san":
                return str(self.fake.random_int(1000000, 9999999))
            else:
                return self.fake.name()

        # 默认：生成随机字符串
        else:
            return self.fake.word()

    def generate_source_data(self, count: int = 10) -> List[Dict[str, str]]:
        """
        生成多条源数据

        Args:
            count: 生成数量

        Returns:
            源数据记录列表
        """
        self.logger.step(2, "生成 Source 数据")
        self.logger.info(f"🎲 生成 {count} 条源数据记录")

        records = []
        for i in range(count):
            record = self.generate_source_record()
            records.append(record)

        self.logger.success(f"生成完成: {len(records)} 条记录")
        return records

    def generate_expected_data(
        self, source_records: List[Dict[str, str]]
    ) -> List[Dict[str, str]]:
        """
        根据源数据生成预期数据

        Args:
            source_records: 源数据记录列表

        Returns:
            预期数据记录列表
        """
        return self.transform_engine.transform_batch(source_records)

    def export_to_pipe_format(
        self,
        records: List[Dict[str, str]],
        output_path: str,
        action_id: str = "test-data-integration",
    ):
        """
        导出为管道符分隔格式

        Args:
            records: 数据记录列表
            output_path: 输出文件路径
            action_id: 动作ID
        """
        self.logger.step(4, "导出数据")
        self.logger.info(f"💾 导出到: {output_path}")

        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, "w", encoding="utf-8") as f:
            # 写入元数据头
            f.write(f"ACTION_ID:{action_id}\n")
            f.write("SERVICE_MAP_ID:10003358\n")
            f.write(
                f"SOURCE_TOKEN:generated_{datetime.now().strftime('%Y%m%d_%H%M%S')}\n"
            )
            f.write("\n")

            # 写入字段头
            field_order = self.config.field_order
            f.write("|".join(field_order) + "\n")

            # 写入数据行
            for record in records:
                values = [str(record.get(field, "")) for field in field_order]
                f.write("|".join(values) + "\n")

        self.logger.success(f"导出完成: {len(records)} 行")

    def generate_and_export(
        self, count: int = 10, output_dir: str = "output"
    ) -> Dict[str, Path]:
        """
        生成并导出数据（Source + Expected）

        Args:
            count: 生成数量
            output_dir: 输出目录

        Returns:
            导出文件路径字典
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # 生成源数据
        source_records = self.generate_source_data(count)

        # 生成预期数据
        expected_records = self.generate_expected_data(source_records)

        # 导出 Expected
        case_name = self.config.case_name
        expected_file = output_path / f"expected_{case_name}.txt"
        self.export_to_pipe_format(
            expected_records,
            str(expected_file),
            action_id=f"{case_name}-cs-data-integration",
        )

        result = {
            "expected": expected_file,
            "record_count": len(expected_records),
            "field_count": len(self.config.field_order),
        }

        return result
