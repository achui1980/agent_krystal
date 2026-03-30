"""
SmartDataGenerator - 自动生成的测试数据生成器

自动生成时间: 2026-02-09 12:22:59
规则来源: case/highmark/rules.xlsx
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
        self._state_city_map = {
            'MO': ['Saint Louis', 'Kansas City', 'Springfield', 'Columbia'],
            'FL': ['Palm Coast', 'Miami', 'Orlando', 'Tampa', 'Jacksonville'],
            'CA': ['Santa Maria', 'Los Angeles', 'San Francisco', 'San Diego', 'Sacramento'],
            'NC': ['Winston Salem', 'Charlotte', 'Raleigh', 'Durham'],
            'CO': ['Nederland', 'Denver', 'Boulder', 'Colorado Springs'],
            'NY': ['New York', 'Buffalo', 'Rochester', 'Albany'],
            'TX': ['Houston', 'Dallas', 'Austin', 'San Antonio'],
            'IL': ['Chicago', 'Springfield', 'Naperville'],
            'WA': ['Seattle', 'Spokane', 'Tacoma'],
            'MA': ['Boston', 'Worcester', 'Springfield']
        }
        
        # 产品类型列表（从规则提取）
        self._product_types = ['PDP', 'LPPO', 'LPPO SNP DE', 'HUM', 'HV', 'RD', 'HAP', 'MA/MAPD', 'MS', 'MD']
        
        # 状态列表
        self._status_list = ['Active', 'Termed']

    def _get_random_state_city(self) -> Tuple[str, str]:
        """随机获取一个州和城市的组合，确保匹配"""
        try:
            state = self.fake.random_element(list(self._state_city_map.keys()))
            city = self.fake.random_element(self._state_city_map[state])
            return state, city
        except Exception as e:
            print(f"Error generating random state and city: {e}")
            return "UnknownState", "UnknownCity"

    # ========================================
    # SOURCE_GENERATION_NORMAL
    # ========================================
    def _generate_source_row_normal(self) -> Dict[str, Any]:
        """生成正常数据的Source行"""
        state, city = self._get_random_state_city()
        return {
            "RECORD_TYPE": "D",
            "HICN": self.fake.bothify(text='?#?#?#?#?#'),
            "SSN": self.fake.ssn(),
            "SUBSCRIBER_ID": str(self.fake.random_int(min=10000000, max=99999999)),
            "POLICY_ID": str(self.fake.random_int(min=100000, max=999999)),
            "AGENCY_MEMBER_ID": str(self.fake.random_int(min=100000, max=999999)),
            "CONFIRMATION_NUMBER": str(self.fake.random_int(min=10000000, max=99999999)),
            "APPLICANT_LAST_NAME": self.fake.last_name(),
            "APPLICANT_FIRST_NAME": self.fake.first_name(),
            "APPLICANT_MIDDLE_INITIAL": self.fake.random_letter().upper(),
            "APPLICANT_BIRTH_DATE": self.fake.date_of_birth(minimum_age=18, maximum_age=90),
            "APPLICANT_ADDR_1": self.fake.street_address(),
            "APPLICANT_STATE": state,
            "APPLICANT_ZIP": self.fake.zipcode_in_state(state),
            "APPLICANT_PHONE": self.fake.phone_number(),
            "APPLICANT_EMAIL_ADDRESS": self.fake.email(),
            "GROUP_ID": str(self.fake.random_int(min=1000, max=9999)),
            "PLAN_TYPE": self.fake.random_element(self._product_types),
            "PLAN_NAME": self.fake.word().capitalize(),
            "PLAN_ID": str(self.fake.random_int(min=100, max=999)),
            "CONTRACT_ID": self.fake.lexify(text='??') + str(self.fake.random_int(min=1000, max=9999)),
            "SEGMENT_ID": str(self.fake.random_int(min=1, max=99)),
            "COVERAGE_EFFECTIVE_DATE": self.fake.date_this_century(),
            "SUBSIDIARY_CARRIER_NAME": self.fake.company(),
            "AGENT_WRITING_NUMBER": str(self.fake.random_int(min=100000, max=999999)),
            "APPLICATION_STATUS_CODE": self.fake.random_element(['APPROVED', 'PENDING', 'REJECTED']),
            "APPLICATION_STATUS_NOTE": self.fake.sentence(),
            "COVERAGE_END_DATE": self.fake.date_this_century()
        }

    # ========================================
    # TRANSFORMATION_METHODS
    # ========================================
    def transform_PRODUCT_LINE(self, source: Dict[str, Any]) -> str:
        """转换Product到PRODUCT_LINE"""
        try:
            product = source.get("PLAN_TYPE", "")
            if product == "PDP":
                return "MD"
            elif product in ["LPPO", "LPPO SNP DE"]:
                return "MA/MAPD"
            elif product == "MS":
                return "MS"
            else:
                return "OTH"
        except Exception as e:
            print(f"Error transforming PRODUCT_LINE: {e}")
            return ""

    def transform_FIRST_NAME(self, source: Dict[str, Any]) -> str:
        """直接映射APPLICANT_FIRST_NAME到FIRST_NAME"""
        try:
            return source.get("APPLICANT_FIRST_NAME", "")
        except Exception as e:
            print(f"Error transforming FIRST_NAME: {e}")
            return ""

    def transform_LAST_NAME(self, source: Dict[str, Any]) -> str:
        """直接映射APPLICANT_LAST_NAME到LAST_NAME"""
        try:
            return source.get("APPLICANT_LAST_NAME", "")
        except Exception as e:
            print(f"Error transforming LAST_NAME: {e}")
            return ""

    def transform_STATE(self, source: Dict[str, Any]) -> str:
        """直接映射APPLICANT_STATE到STATE"""
        try:
            return source.get("APPLICANT_STATE", "")
        except Exception as e:
            print(f"Error transforming STATE: {e}")
            return ""

    def transform_ZIP_CODE(self, source: Dict[str, Any]) -> str:
        """直接映射APPLICANT_ZIP到ZIP_CODE"""
        try:
            return source.get("APPLICANT_ZIP", "")
        except Exception as e:
            print(f"Error transforming ZIP_CODE: {e}")
            return ""

    def transform_EMAIL(self, source: Dict[str, Any]) -> str:
        """直接映射APPLICANT_EMAIL_ADDRESS到EMAIL"""
        try:
            return source.get("APPLICANT_EMAIL_ADDRESS", "")
        except Exception as e:
            print(f"Error transforming EMAIL: {e}")
            return ""

    # ========================================
    # FIELD_MAPPING_LOGIC
    # ========================================
    def _transform_to_expected(self, source: Dict[str, Any]) -> Dict[str, Any]:
        """将Source行数据转换为Expected行数据"""
        expected = {}
        try:
            expected["PRODUCT_LINE"] = self.transform_PRODUCT_LINE(source)
            expected["FIRST_NAME"] = self.transform_FIRST_NAME(source)
            expected["LAST_NAME"] = self.transform_LAST_NAME(source)
            expected["STATE"] = self.transform_STATE(source)
            expected["ZIP_CODE"] = self.transform_ZIP_CODE(source)
            expected["EMAIL"] = self.transform_EMAIL(source)
            # Add more field transformations as needed...
        except Exception as e:
            print(f"Error in transforming expected row: {e}")
        return expected

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
                expected_row['_test_id'] = f'normal_{i+1}'
                results.append(expected_row)
            except Exception as e:
                print(f"Error generating case {i+1}: {e}")
        return results

# Additional utility or test functions can be added here if needed.