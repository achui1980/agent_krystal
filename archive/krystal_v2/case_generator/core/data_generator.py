"""
Data Generator - 生成测试数据
添加场景元数据和测试点描述
"""

from faker import Faker
import random
from typing import List, Dict, Any, Tuple
from datetime import datetime, timedelta


class DataGenerator:
    """生成测试数据"""

    def __init__(self):
        self.fake = Faker("en_US")

        # 产品类型枚举
        self.product_types = ["PDP", "HAP", "HUM", "HV", "RD", "LPPO", "LPPO SNP DE"]

        # 州-城市映射（简化版）
        self.locations = {
            "MO": {"cities": ["SAINT LOUIS", "KANSAS CITY"], "zip_prefix": "631"},
            "CA": {"cities": ["SANTA MARIA", "LOS ANGELES"], "zip_prefix": "934"},
            "FL": {"cities": ["PALM COAST", "MIAMI"], "zip_prefix": "321"},
            "NC": {"cities": ["WINSTON SALEM", "RALEIGH"], "zip_prefix": "271"},
            "CO": {"cities": ["NEDERLAND", "DENVER"], "zip_prefix": "804"},
        }

    def generate_normal_cases(self, count: int) -> Tuple[List[Dict], List[Dict]]:
        """
        生成正常场景数据

        Returns:
            (测试数据列表, 测试点描述列表)
        """
        cases = []
        test_points = []

        # 定义正常场景测试点
        scenarios = [
            {
                "name": "标准PDP产品-Active状态",
                "focus": "验证PDP产品正确映射到MD产品线",
                "rules": ["PRODUCT_LINE条件映射", "CARRIER_STATUS_MAP派生"],
                "business_value": "核心业务场景-处方药计划",
            },
            {
                "name": "标准LPPO产品-Active状态",
                "focus": "验证LPPO产品映射到MA/MAPD",
                "rules": ["PRODUCT_LINE条件映射", "CARRIER_STATUS_MAP派生"],
                "business_value": "核心业务场景-本地优选组织",
            },
            {
                "name": "HUM产品-Active状态",
                "focus": "验证HUM产品映射到MS",
                "rules": ["PRODUCT_LINE条件映射"],
                "business_value": "验证Humana产品线分类",
            },
            {
                "name": "含Provider_ID的完整数据",
                "focus": "验证Provider_ID直接映射",
                "rules": ["Provider_ID直接映射"],
                "business_value": "医生信息完整场景",
            },
            {
                "name": "无Provider_ID的数据",
                "focus": "验证可选字段为空处理",
                "rules": ["Provider_ID直接映射"],
                "business_value": "医生信息缺失场景",
            },
            {
                "name": "LIS=Y低收入补贴",
                "focus": "验证LIS标识正确传递",
                "rules": ["LIS_Indicator直接映射"],
                "business_value": "低收入补贴成员",
            },
            {
                "name": "LIS=N非补贴成员",
                "focus": "验证LIS标识正确传递",
                "rules": ["LIS_Indicator直接映射"],
                "business_value": "普通成员",
            },
            {
                "name": "含月保费数据",
                "focus": "验证MONTHLY_PREMIUM直接映射",
                "rules": ["MONTHLY_PREMIUM直接映射"],
                "business_value": "保费计算场景",
            },
            {
                "name": "无月保费数据",
                "focus": "验证空保费处理",
                "rules": ["MONTHLY_PREMIUM直接映射"],
                "business_value": "零保费场景",
            },
            {
                "name": "Termed状态保单",
                "focus": "验证非9999-12-31日期识别为Termed",
                "rules": ["CARRIER_STATUS_MAP派生"],
                "business_value": "已终止保单场景",
            },
        ]

        for i in range(min(count, len(scenarios))):
            scenario = scenarios[i]
            case = self._generate_case_for_scenario(scenario)

            # 添加元数据
            case["_scenario_type"] = "normal"
            case["_scenario_name"] = scenario["name"]

            cases.append(case)
            test_points.append(
                {
                    "row_id": i + 1,
                    "scenario_type": "normal",
                    "scenario_name": scenario["name"],
                    "test_focus": scenario["focus"],
                    "rules_covered": scenario["rules"],
                    "business_value": scenario["business_value"],
                    "data_characteristics": self._extract_characteristics(case),
                }
            )

        return cases, test_points

    def generate_abnormal_cases(
        self, scenarios: List[Dict]
    ) -> Tuple[List[Dict], List[Dict]]:
        """
        生成异常场景数据

        Returns:
            (测试数据列表, 测试点描述列表)
        """
        cases = []
        test_points = []

        # 定义异常场景测试点
        abnormal_scenarios = [
            {
                "id": "missing_medicare_id",
                "name": "缺失MEDICARE_ID",
                "focus": "验证必填字段缺失时的系统处理",
                "rules": ["MEDICARE_ID直接映射"],
                "risk_level": "高",
                "expected_behavior": "MEDICARE_ID为空字符串",
                "modifications": {"MEDICARE_ID": ""},
            },
            {
                "id": "invalid_product",
                "name": "无效Product类型",
                "focus": "验证无效枚举值走默认分支",
                "rules": ["PRODUCT_LINE条件映射"],
                "risk_level": "中",
                "expected_behavior": "PRODUCT_LINE=MA/MAPD",
                "modifications": {"Product": "INVALID_TYPE"},
            },
            {
                "id": "wrong_date_format",
                "name": "错误日期格式",
                "focus": "验证日期格式错误时的处理",
                "rules": ["DOB直接映射", "日期格式转换"],
                "risk_level": "中",
                "expected_behavior": "日期可能解析失败或原样传递",
                "modifications": {"DOB": "07/28/1949"},
            },
            {
                "id": "member_format_error",
                "name": "Member格式错误",
                "focus": "验证姓名字段拆分容错",
                "rules": ["Member字段拆分"],
                "risk_level": "中",
                "expected_behavior": "FIRST_NAME和LAST_NAME可能相同",
                "modifications": {"Member": "MICKEY MOUSE"},
            },
            {
                "id": "date_logic_error",
                "name": "日期逻辑错误",
                "focus": "验证Eff_Date>Term_Date时的状态判断",
                "rules": ["CARRIER_STATUS_MAP派生"],
                "risk_level": "低",
                "expected_behavior": "CARRIER_STATUS_MAP=Termed",
                "modifications": {"Eff_Date": "2025-12-31", "Term_Date": "2025-01-01"},
            },
        ]

        for i, scenario in enumerate(abnormal_scenarios[: len(scenarios)]):
            base = self._generate_base_case()

            # 应用异常修改
            for key, value in scenario["modifications"].items():
                base[key] = value

            base["_scenario_type"] = "abnormal"
            base["_scenario_id"] = scenario["id"]
            base["_scenario_name"] = scenario["name"]

            cases.append(base)
            test_points.append(
                {
                    "row_id": len(cases),
                    "scenario_type": "abnormal",
                    "scenario_id": scenario["id"],
                    "scenario_name": scenario["name"],
                    "test_focus": scenario["focus"],
                    "rules_covered": scenario["rules"],
                    "risk_level": scenario["risk_level"],
                    "expected_behavior": scenario["expected_behavior"],
                    "data_characteristics": self._extract_characteristics(
                        base, scenario["modifications"]
                    ),
                }
            )

        return cases, test_points

    def generate_boundary_cases(self, count: int) -> Tuple[List[Dict], List[Dict]]:
        """
        生成边界场景数据

        Returns:
            (测试数据列表, 测试点描述列表)
        """
        cases = []
        test_points = []

        # 定义边界场景
        boundaries = [
            {
                "name": "最早出生日期边界",
                "modifications": {"DOB": "1900-01-01"},
                "focus": "验证系统支持最早1900年出生日期",
                "rules": ["DOB直接映射"],
                "business_rule": "会员年龄上限边界",
            },
            {
                "name": "最晚出生日期边界",
                "modifications": {"DOB": "2005-12-31"},
                "focus": "验证年轻成员(18岁)数据处理",
                "rules": ["DOB直接映射"],
                "business_rule": "会员年龄下限边界",
            },
            {
                "name": "短期保单边界",
                "modifications": {"Term_Date": "2025-01-01", "Eff_Date": "2024-01-01"},
                "focus": "验证非标准年度保单处理",
                "rules": ["CARRIER_STATUS_MAP派生"],
                "business_rule": "短期保单状态判断",
            },
        ]

        for i in range(min(count, len(boundaries))):
            boundary = boundaries[i]
            base = self._generate_base_case()
            base.update(boundary["modifications"])

            base["_scenario_type"] = "boundary"
            base["_scenario_name"] = boundary["name"]

            cases.append(base)
            test_points.append(
                {
                    "row_id": len(cases),
                    "scenario_type": "boundary",
                    "scenario_name": boundary["name"],
                    "test_focus": boundary["focus"],
                    "rules_covered": boundary["rules"],
                    "business_rule": boundary["business_rule"],
                    "data_characteristics": self._extract_characteristics(
                        base, boundary["modifications"]
                    ),
                }
            )

        return cases, test_points

    def _generate_case_for_scenario(self, scenario: Dict) -> Dict:
        """根据场景生成特定数据"""
        base = self._generate_base_case()

        # 根据场景名称调整数据
        if "PDP" in scenario["name"]:
            base["Product"] = "PDP"
        elif "LPPO" in scenario["name"]:
            base["Product"] = "LPPO"
        elif "HUM" in scenario["name"]:
            base["Product"] = "HUM"
        elif "Termed" in scenario["name"]:
            # 生成终止日期
            eff_date = datetime.strptime(base["Eff_Date"], "%Y-%m-%d")
            term_date = eff_date + timedelta(days=random.randint(30, 365))
            base["Term_Date"] = term_date.strftime("%Y-%m-%d")
            base["Policy_Indicator"] = "Termed"

        if "Provider_ID" in scenario["name"]:
            if "无" in scenario["name"]:
                base["Provider_ID"] = ""
            else:
                base["Provider_ID"] = str(random.randint(100000000, 999999999))

        if "LIS" in scenario["name"]:
            if "Y" in scenario["name"]:
                base["LIS_Indicator"] = "Y"
            else:
                base["LIS_Indicator"] = "N"

        if "保费" in scenario["name"] or "PREMIUM" in scenario["name"]:
            if "无" in scenario["name"]:
                base["MONTHLY_PREMIUM"] = ""
            else:
                base["MONTHLY_PREMIUM"] = round(random.uniform(20, 150), 2)

        return base

    def _generate_base_case(self) -> Dict:
        """生成基础正常数据"""
        state = random.choice(list(self.locations.keys()))
        location = self.locations[state]

        # 生成日期
        dob = self.fake.date_of_birth(minimum_age=25, maximum_age=85)
        eff_date = datetime(2025, 1, 1)
        term_date = datetime(9999, 12, 31)  # Active

        return {
            "AOR_Name": "EHEALTHINSURANCE SERVICES INC",
            "AOR_SAN": "1273481",
            "Agent": self.fake.name().upper(),
            "SAN": str(random.randint(1000000, 9999999)),
            "NPN": str(random.randint(10000000, 99999999)),
            "Member": f"{self.fake.last_name().upper()},{self.fake.first_name().upper()}",
            "Address1": f"{random.randint(1, 9999)} {self.fake.street_name().upper()}",
            "City": random.choice(location["cities"]),
            "State": state,
            "Zip": f"{location['zip_prefix']}{random.randint(10, 99)}",
            "MEDICARE_ID": self._generate_medicare_id(),
            "DOB": dob.strftime("%Y-%m-%d"),
            "Product": random.choice(self.product_types),
            "SIGNATURE_DATE": self.fake.date_between(
                start_date="-1y", end_date="today"
            ).strftime("%Y-%m-%d"),
            "UMID": f"H{random.randint(10000000, 99999999)}",
            "Plan_Name": f"{self.fake.bothify(text='????-###').upper()}",
            "Eff_Date": eff_date.strftime("%Y-%m-%d"),
            "Term_Date": term_date.strftime("%Y-%m-%d"),
            "EndReasonCd": "",
            "EndReasonCodeDescription": "",
            "Solar_Group_Nbr": f"{random.randint(10000000000, 99999999999)}K",
            "New_P2P": "New",
            "LIS_Indicator": random.choice(["Y", "N"]),
            "Provider_ID": str(random.randint(100000000, 999999999))
            if random.random() > 0.5
            else "",
            "Published_Name": self.fake.name().upper(),
            "DOC_ID": str(random.randint(10000000, 99999999)),
            "Policy_Indicator": "Active",
            "MONTHLY_PREMIUM": round(random.uniform(20, 150), 2)
            if random.random() > 0.3
            else "",
        }

    def _generate_medicare_id(self) -> str:
        """生成Medicare ID"""
        parts = [
            str(random.randint(1, 9)),
            random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
            random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
            str(random.randint(0, 9)),
            random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
            random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
            str(random.randint(0, 9)),
            random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
            random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
            str(random.randint(0, 9)),
            str(random.randint(0, 9)),
        ]
        return "".join(parts)

    def _extract_characteristics(self, case: Dict, modifications: Dict = None) -> Dict:
        """提取数据特征用于报告"""
        chars = {
            "Product": case.get("Product", ""),
            "Term_Date": case.get("Term_Date", ""),
            "DOB": case.get("DOB", ""),
            "LIS_Indicator": case.get("LIS_Indicator", ""),
            "State": case.get("State", ""),
        }

        if modifications:
            chars["modified_fields"] = list(modifications.keys())

        return chars
