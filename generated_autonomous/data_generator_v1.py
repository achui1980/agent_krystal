"""
SmartDataGenerator - 可独立运行的ETL规则解析/映射生成/数据造数/转换执行/自测框架

说明：
1) 由于当前对话未提供 case/source.csv、case/expected.txt、case/rules.xlsx 的实际内容，
   本代码实现“可落地的解析骨架”：
   - 自动读取 source.csv 推断28列（实际以文件为准）
   - 自动读取 expected.txt 首行表头推断93列（实际以文件为准）
   - 自动读取 rules.xlsx（Sheet1, header=4）并解析为映射规则列表
   - 支持 direct / default / conditional / transform / unresolved
2) 同时提供 Faker 造数：normal/abnormal/boundary，并可保存CSV
3) 提供 apply_transformations 将 source DataFrame 转为 expected DataFrame
4) 内置单元测试（unittest）可在无外部文件时运行（使用内存样例规则/数据）

依赖：
- pandas
- openpyxl（读取xlsx用）
- faker

运行示例：
- 仅跑自测：python smart_data_generator.py
- 生成造数并保存：python smart_data_generator.py --generate --out out.csv
- 若存在case文件并想生成spec：python smart_data_generator.py --build-spec --case-dir case --spec-out spec.json
- 若存在case文件并执行转换：python smart_data_generator.py --apply --case-dir case --out expected_out.csv

作者定位：资深Python开发工程师与ETL测试专家（强调健壮性、可测试性）
"""

from __future__ import annotations

import csv
import json
import os
import random
import re
import sys
import argparse
import unittest
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import pandas as pd
from faker import Faker


# -----------------------------
# Exceptions
# -----------------------------
class MappingValidationError(Exception):
    """Raised when mapping validation fails with actionable details."""


class RuleParseError(Exception):
    """Raised when a rule row cannot be parsed safely."""


# -----------------------------
# Helpers
# -----------------------------
def _is_empty(value: Any) -> bool:
    """Treat '', None, NaN as empty."""
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _safe_str(value: Any) -> str:
    """Convert value to string (empty if None/NaN)."""
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value)


def _try_parse_date(value: str) -> Optional[datetime]:
    """Try parse common date formats; return None if fails."""
    v = value.strip()
    if not v:
        return None
    fmts = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%m/%d/%Y",
        "%Y%m%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
    ]
    for f in fmts:
        try:
            return datetime.strptime(v, f)
        except ValueError:
            continue
    return None


def _format_date(dt: datetime, fmt: str) -> str:
    """Format datetime using python strftime format."""
    try:
        return dt.strftime(fmt)
    except Exception:
        return ""


# -----------------------------
# Core class
# -----------------------------
class SmartDataGenerator:
    """
    负责：
    - 解析规则文件、推断schema
    - 构建字段映射并验证
    - 应用映射转换（source -> expected）
    - 生成 normal/abnormal/boundary 造数
    - 输出CSV与spec.json
    """

    def __init__(self, seed: int = 42):
        self.fake = Faker("en_US")
        Faker.seed(seed)
        random.seed(seed)

        # 用于城市/州匹配（示例：MO-Saint Louis）
        self.state_city_map: Dict[str, List[str]] = {
            "MO": ["Saint Louis", "Kansas City", "Springfield"],
            "CA": ["San Francisco", "Los Angeles", "San Diego"],
            "NY": ["New York", "Buffalo", "Rochester"],
            "TX": ["Houston", "Dallas", "Austin"],
            "FL": ["Miami", "Orlando", "Tampa"],
        }

    # -----------------------------
    # Spec/meta loading
    # -----------------------------
    def load_rules(self, path: str, sheet_name: str = "Sheet1", header_row: int = 4) -> pd.DataFrame:
        """
        读取规则文件rules.xlsx：
        - 按规格：pd.read_excel('case/rules.xlsx', sheet_name='Sheet1', header=4)
        - 对合并单元格导致NaN向下填充（ffill）
        """
        if not os.path.exists(path):
            raise FileNotFoundError(f"rules.xlsx not found: {path}")

        df = pd.read_excel(path, sheet_name=sheet_name, header=header_row, dtype=str)
        # 标准化列名
        df.columns = [self.normalize_field_name(c) for c in df.columns]
        # ffill 还原合并单元格分组规则
        df = df.ffill()
        # 将NaN转空串，便于后续处理
        df = df.fillna("")
        return df

    def load_source_schema(self, path: str, sample_rows: int = 50) -> List[str]:
        """
        读取source.csv前N行推断列名（通常期望28列，但以实际为准）
        dtype=str, keep_default_na=False：保留空字符串，不将其自动转NaN
        """
        if not os.path.exists(path):
            raise FileNotFoundError(f"source.csv not found: {path}")
        df_source = pd.read_csv(path, nrows=sample_rows, dtype=str, keep_default_na=False)
        return [self.normalize_field_name(c) for c in list(df_source.columns)]

    def load_expected_schema(self, path: str, encoding: str = "utf-8") -> List[str]:
        """
        读取expected.txt首行表头并识别分隔符，split 得到 expected_fields（通常期望93列）
        """
        if not os.path.exists(path):
            raise FileNotFoundError(f"expected.txt not found: {path}")
        with open(path, "r", encoding=encoding) as f:
            header_line = f.readline().rstrip("\n")
        if not header_line:
            raise ValueError("expected.txt header is empty")
        delim = self.infer_delimiter(header_line)
        fields = [self.normalize_field_name(x) for x in header_line.split(delim)]
        # 简单校验：至少应有>1列
        if len(fields) <= 1:
            snippet = header_line[:200]
            raise ValueError(f"Failed to split expected header. delim={delim!r}, header_snippet={snippet!r}")
        return fields

    def infer_delimiter(self, header_line: str) -> str:
        """
        分隔符识别：优先'|'，否则','，否则'\\t'
        """
        candidates = ["|", ",", "\t"]
        counts = {c: header_line.count(c) for c in candidates}
        # 选择出现次数最多且>0的
        best = max(counts.items(), key=lambda kv: kv[1])
        if best[1] <= 0:
            raise ValueError(f"Delimiter infer failed. header_line_snippet={header_line[:200]!r}")
        return best[0]

    def normalize_field_name(self, name: str) -> str:
        """
        字段标准化：去空格、大小写、全角半角（简化版）：
        - strip
        - 连续空白 -> 单下划线
        - 全角空格替换
        - 统一大写（便于匹配）
        """
        if name is None:
            return ""
        s = str(name)
        s = s.replace("\u3000", " ").strip()
        s = re.sub(r"\s+", "_", s)
        return s.upper()

    # -----------------------------
    # Rules parsing & mapping build
    # -----------------------------
    def parse_rule_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        将rules.xlsx的一行解析为内部rule dict（尽可能兼容未知列名）。

        期望抽取信息：
        - target: 目标字段
        - source: 源字段/表达式
        - rule_type: direct/default/conditional/transform/unresolved（先留空，后分类）
        - conditions: 条件分支列表（若存在）
        - default: 默认值
        - expression: transform表达式（若存在）
        - raw: 原始行（便于排查）
        """
        # 兼容列名：在实际规则中可能叫 Target/目标字段/Field 等
        def pick(*keys: str) -> str:
            for k in keys:
                nk = self.normalize_field_name(k)
                if nk in row and not _is_empty(row[nk]):
                    return _safe_str(row[nk]).strip()
            # 尝试模糊匹配
            for ck, cv in row.items():
                if any(self.normalize_field_name(k) in self.normalize_field_name(ck) for k in keys):
                    if not _is_empty(cv):
                        return _safe_str(cv).strip()
            return ""

        target = pick("TARGET", "TARGET_FIELD", "目标字段", "输出字段", "FIELD", "EXPECTED", "TO")
        source = pick("SOURCE", "SOURCE_FIELD", "源字段", "输入字段", "FROM")
        expr = pick("EXPRESSION", "TRANSFORM", "公式", "表达式", "LOGIC", "规则")
        default_val = pick("DEFAULT", "DEFAULT_VALUE", "固定值", "默认值", "CONST", "CONSTANT")
        cond = pick("CONDITION", "CONDITIONS", "条件", "IF", "WHEN")
        remark = pick("REMARK", "备注", "COMMENT", "NOTE", "DESCRIPTION", "DESC")

        rule: Dict[str, Any] = {
            "target": self.normalize_field_name(target),
            "source": self.normalize_field_name(source),
            "expression": expr.strip(),
            "default": default_val,
            "condition_text": cond,
            "remark": remark,
            "conditions": [],  # list of {when:..., then:...}
            "raw": dict(row),
        }

        # 解析条件文本（简易DSL）：
        # 支持示例：
        #   IF A=1 THEN X; ELIF A=2 THEN Y; ELSE Z
        #   A=1=>X;A=2=>Y;ELSE=>Z
        #   WHEN A IN (a,b) THEN X | ELSE Y
        if rule["condition_text"]:
            rule["conditions"] = self._parse_conditions(rule["condition_text"])

        return rule

    def _parse_conditions(self, text: str) -> List[Dict[str, str]]:
        """
        解析条件字符串为分支列表。尽量容错：
        返回格式：[{ "when": "A=1", "then": "X" }, ...]
        支持ELSE（when='ELSE'）。
        """
        t = " ".join(text.strip().split())
        # 统一分隔符为 ;
        t = t.replace("|", ";")
        parts = [p.strip() for p in re.split(r";+", t) if p.strip()]
        branches: List[Dict[str, str]] = []

        # 尝试模式1：A=1=>X
        arrow_like = any("=>" in p for p in parts)
        if arrow_like:
            for p in parts:
                if "=>" not in p:
                    continue
                left, right = p.split("=>", 1)
                branches.append({"when": left.strip().upper(), "then": right.strip()})
            return branches

        # 模式2：IF ... THEN ...
        # 简化：抽取 (IF|ELIF|WHEN) cond (THEN) val, ELSE val
        pattern = re.compile(r"^(IF|ELIF|WHEN)\s+(.*?)\s+THEN\s+(.*)$", re.IGNORECASE)
        else_pattern = re.compile(r"^ELSE\s+(.*)$", re.IGNORECASE)
        for p in parts:
            m = pattern.match(p)
            if m:
                branches.append({"when": m.group(2).strip().upper(), "then": m.group(3).strip()})
                continue
            em = else_pattern.match(p)
            if em:
                branches.append({"when": "ELSE", "then": em.group(1).strip()})
                continue

        # 若仍解析不到，退化为整段不可解析条件（unresolved）
        if not branches:
            branches.append({"when": "UNPARSED", "then": text})
        return branches

    def classify_mapping(self, rule: Dict[str, Any]) -> str:
        """
        分类规则：
        - direct: target & source 存在，且无条件/无表达式/无默认
        - default: target 存在，默认值存在，且source空
        - conditional: target存在，conditions非空
        - transform: target存在，expression存在（或source存在但包含函数符号）
        - unresolved: 其他
        """
        target = rule.get("target", "")
        source = rule.get("source", "")
        expr = rule.get("expression", "")
        default_val = rule.get("default", "")
        conditions = rule.get("conditions", [])

        if not target:
            return "unresolved"
        if conditions:
            return "conditional"
        if expr:
            return "transform"
        if default_val and not source:
            return "default"
        if source and not default_val and not expr:
            return "direct"

        # source像表达式，也算transform
        if source and re.search(r"[()+\-*/]", source):
            return "transform"

        return "unresolved"

    def build_field_mappings(
        self, rules_df: pd.DataFrame, source_fields: List[str], expected_fields: List[str]
    ) -> List[Dict[str, Any]]:
        """
        从rules_df构建 mappings：
        每条mapping dict至少包含：
        - source
        - target
        - type
        - logic
        - conditions
        - rule（保留解析后的rule）
        """
        mappings: List[Dict[str, Any]] = []
        norm_source_fields = {self.normalize_field_name(x) for x in source_fields}
        norm_expected_fields = {self.normalize_field_name(x) for x in expected_fields}

        for _, r in rules_df.iterrows():
            row = {c: r[c] for c in rules_df.columns}
            parsed = self.parse_rule_row(row)
            mtype = self.classify_mapping(parsed)

            target = parsed["target"]
            source = parsed.get("source", "")
            logic = parsed.get("expression") or parsed.get("condition_text") or parsed.get("default") or source

            mapping = {
                "source": source or "",
                "target": target or "",
                "type": mtype,
                "logic": logic,
                "conditions": {"branches": parsed.get("conditions", [])},
                "rule": parsed,
                "resolved": True,
            }

            # 基础可解析性检查
            if not mapping["target"]:
                mapping["resolved"] = False
            if mapping["target"] and mapping["target"] not in norm_expected_fields:
                # 目标不在expected字段里：标记unresolved但不立刻失败（由validate策略控制）
                mapping["resolved"] = False
            if mapping["type"] in ("direct", "transform", "conditional") and mapping["source"]:
                if mapping["source"] not in norm_source_fields:
                    # 条件/表达式可能引用多字段，单字段校验不够严谨；
                    # 这里仅对“看起来是单字段”的source做校验
                    if re.fullmatch(r"[A-Z0-9_]+", mapping["source"]) and mapping["source"] not in norm_source_fields:
                        mapping["resolved"] = False

            mappings.append(mapping)

        return mappings

    def validate_mappings(
        self,
        mappings: List[Dict[str, Any]],
        source_fields: List[str],
        expected_fields: List[str],
        strict: bool = False,
    ) -> None:
        """
        校验mapping：
        - target必须在expected_fields
        - direct/default/conditional/transform要有足够信息
        strict=True：发现问题直接抛异常
        """
        norm_source_fields = {self.normalize_field_name(x) for x in source_fields}
        norm_expected_fields = {self.normalize_field_name(x) for x in expected_fields}

        errors: List[str] = []
        for i, m in enumerate(mappings):
            t = self.normalize_field_name(m.get("target", ""))
            s = self.normalize_field_name(m.get("source", ""))
            mtype = m.get("type", "unresolved")

            if not t:
                errors.append(f"[{i}] missing target")
                continue
            if t not in norm_expected_fields:
                errors.append(f"[{i}] target not in expected_fields: {t}")

            if mtype == "direct":
                if not s:
                    errors.append(f"[{i}] direct mapping missing source for target={t}")
                elif s not in norm_source_fields:
                    errors.append(f"[{i}] source not in source_fields: {s} (target={t})")
            elif mtype == "default":
                if _is_empty(m.get("rule", {}).get("default")):
                    errors.append(f"[{i}] default mapping missing default value for target={t}")
            elif mtype == "conditional":
                branches = m.get("conditions", {}).get("branches", [])
                if not branches:
                    errors.append(f"[{i}] conditional mapping missing branches for target={t}")
            elif mtype == "transform":
                expr = m.get("rule", {}).get("expression", "") or m.get("logic", "")
                if not expr and not s:
                    errors.append(f"[{i}] transform mapping missing expression/source for target={t}")

        if errors and strict:
            raise MappingValidationError("Mapping validation failed:\n" + "\n".join(errors))

    def generate_spec_json(
        self, source_fields: List[str], expected_fields: List[str], mappings: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """生成可落盘的spec.json dict。"""
        spec = {
            "source_fields": source_fields,
            "expected_fields": expected_fields,
            "field_mappings": [
                {
                    "source": m.get("source", ""),
                    "target": m.get("target", ""),
                    "type": m.get("type", ""),
                    "logic": m.get("logic", ""),
                    "conditions": m.get("conditions", {}),
                    "resolved": bool(m.get("resolved", True)),
                    "remark": m.get("rule", {}).get("remark", ""),
                }
                for m in mappings
            ],
            "generated_at": datetime.utcnow().isoformat() + "Z",
        }
        return spec

    def save_spec(self, spec: Dict[str, Any], path: str) -> None:
        """保存spec为JSON文件。"""
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(spec, f, ensure_ascii=False, indent=2)

    # -----------------------------
    # Transform application
    # -----------------------------
    def apply_transformations(self, df_source: pd.DataFrame, mappings: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        将df_source应用mappings，生成目标DataFrame（expected schema列集合）：
        - 若同一target出现多条规则：后者覆盖前者（常见于规则表分段）
        - 遇到异常：返回空字符串，并记录在_errors列（JSON字符串）
        """
        if df_source is None or df_source.empty:
            return pd.DataFrame()

        # 标准化列名
        df = df_source.copy()
        df.columns = [self.normalize_field_name(c) for c in df.columns]
        df = df.fillna("")

        # 按target收敛（最后一条覆盖）
        by_target: Dict[str, Dict[str, Any]] = {}
        for m in mappings:
            t = self.normalize_field_name(m.get("target", ""))
            if not t:
                continue
            by_target[t] = m

        targets = list(by_target.keys())
        out = pd.DataFrame(index=df.index)
        error_records: List[Dict[str, Any]] = [{} for _ in range(len(df))]

        for t in targets:
            m = by_target[t]
            mtype = m.get("type", "unresolved")
            rule = m.get("rule", {})
            try:
                if mtype == "direct":
                    src = self.normalize_field_name(m.get("source", ""))
                    out[t] = df[src].astype(str)
                elif mtype == "default":
                    default_val = _safe_str(rule.get("default", m.get("logic", "")))
                    out[t] = default_val
                elif mtype == "conditional":
                    # 默认使用source字段作为条件输入（若缺失则用整行map）
                    src = self.normalize_field_name(m.get("source", rule.get("source", "")))
                    if src and src in df.columns:
                        series = df[src].astype(str)
                        out[t] = [
                            self.evaluate_conditional({src: series.iat[i]}, rule)
                            for i in range(len(df))
                        ]
                    else:
                        # 用整行map，允许 when 里引用多个字段（简化实现：仅支持单字段=值）
                        out[t] = [
                            self.evaluate_conditional(df.iloc[i].to_dict(), rule)
                            for i in range(len(df))
                        ]
                elif mtype == "transform":
                    out[t] = [
                        self._evaluate_transform_row(df.iloc[i].to_dict(), rule)
                        for i in range(len(df))
                    ]
                else:
                    out[t] = ""
            except Exception as e:
                # 逐行填空并记录错误
                out[t] = ""
                for idx in range(len(df)):
                    error_records[idx][t] = {"error": type(e).__name__, "message": str(e)}

        out["_ERRORS"] = [json.dumps(er, ensure_ascii=False) if er else "" for er in error_records]
        return out

    def evaluate_conditional(self, value_map: Dict[str, Any], rule: Dict[str, Any]) -> str:
        """
        执行条件分支：
        - branches: [{when:'A=1', then:'X'}, {when:'ELSE', then:'Z'}]
        支持when格式（简化）：
        - FIELD=VALUE
        - FIELD IN (A,B,C)
        - ELSE
        """
        branches = rule.get("conditions", []) or []
        if not branches:
            return ""

        # 先提取潜在source字段名：优先 rule.source
        # 但当when是FIELD=VALUE时，FIELD就是字段
        def get_field_value(field: str) -> str:
            key = self.normalize_field_name(field)
            # value_map keys may already normalized; try both
            if key in value_map:
                return _safe_str(value_map.get(key))
            # fallback: case-insensitive
            for k, v in value_map.items():
                if self.normalize_field_name(k) == key:
                    return _safe_str(v)
            return ""

        else_then = ""
        for b in branches:
            when = _safe_str(b.get("when", "")).strip().upper()
            then = _safe_str(b.get("then", ""))
            if when == "ELSE":
                else_then = then
                continue
            if when in ("UNPARSED", ""):
                # 无法解析：不命中，继续
                continue

            # FIELD=VALUE
            m_eq = re.match(r"^([A-Z0-9_]+)\s*=\s*(.*)$", when)
            if m_eq:
                field = m_eq.group(1).strip()
                expected = m_eq.group(2).strip().strip("'\"")
                actual = get_field_value(field).strip()
                if actual == expected:
                    return then

            # FIELD IN (...)
            m_in = re.match(r"^([A-Z0-9_]+)\s+IN\s*\((.*)\)$", when)
            if m_in:
                field = m_in.group(1).strip()
                items = [x.strip().strip("'\"") for x in m_in.group(2).split(",") if x.strip()]
                actual = get_field_value(field).strip()
                if actual in items:
                    return then

        return else_then

    def _evaluate_transform_row(self, row_map: Dict[str, Any], rule: Dict[str, Any]) -> str:
        """
        评估transform规则（简化可扩展）：
        - 支持表达式指令（不执行eval，避免注入），采用函数式DSL：

        支持示例（不区分大小写）：
        - TRIM(SRC_FIELD)
        - UPPER(SRC_FIELD)
        - LOWER(SRC_FIELD)
        - CONCAT(F1,'-',F2)
        - SUBSTR(F1,1,3)  # 1-based
        - REPLACE(F1,'a','b')
        - DATE_FORMAT(F1,'%Y%m%d')
        - COALESCE(F1,F2,'X')

        规则来源：
        - rule['expression'] 优先，否则尝试 rule['source']（当source像表达式）
        """
        expr = (rule.get("expression") or "").strip()
        if not expr:
            expr = _safe_str(rule.get("source", "")).strip()

        if not expr:
            # 退化：若有source字段则取值
            src = self.normalize_field_name(rule.get("source", ""))
            return _safe_str(row_map.get(src, ""))

        try:
            return self._eval_dsl(expr, row_map)
        except Exception:
            # 尝试把expr当作单字段名
            fld = self.normalize_field_name(expr)
            if fld in {self.normalize_field_name(k) for k in row_map.keys()}:
                return _safe_str(row_map.get(fld, ""))
            raise

    def _eval_dsl(self, expr: str, row_map: Dict[str, Any]) -> str:
        """
        解析并执行简易DSL：FUNC(arg1,arg2,...)
        参数支持：
        - 字段名：IDENT（A-Z0-9_）
        - 字符串字面量：'...' 或 "..."
        - 数字：123
        """
        s = expr.strip()

        # 若不是函数调用，直接当字段名
        if not re.match(r"^[A-Z_][A-Z0-9_]*\s*\(", s.strip(), flags=re.IGNORECASE):
            fld = self.normalize_field_name(s)
            return _safe_str(self._get_value(row_map, fld))

        func_m = re.match(r"^([A-Z_][A-Z0-9_]*)\s*\((.*)\)\s*$", s, flags=re.IGNORECASE)
        if not func_m:
            raise RuleParseError(f"Invalid transform expression: {expr!r}")

        func = func_m.group(1).upper()
        arg_str = func_m.group(2).strip()
        args = self._parse_args(arg_str)

        # Dispatch
        if func == "TRIM":
            v = self._arg_value(args, 0, row_map)
            return _safe_str(v).strip()
        if func == "UPPER":
            v = self._arg_value(args, 0, row_map)
            return _safe_str(v).upper()
        if func == "LOWER":
            v = self._arg_value(args, 0, row_map)
            return _safe_str(v).lower()
        if func == "CONCAT":
            parts = [self._safe_literal_or_field(a, row_map) for a in args]
            return "".join(parts)
        if func == "SUBSTR":
            v = _safe_str(self._arg_value(args, 0, row_map))
            start = int(self._safe_literal_or_field(args[1], row_map) or "1")
            length = int(self._safe_literal_or_field(args[2], row_map) or "0") if len(args) > 2 else 0
            # 1-based to 0-based
            start0 = max(start - 1, 0)
            if length <= 0:
                return v[start0:]
            return v[start0 : start0 + length]
        if func == "REPLACE":
            v = _safe_str(self._arg_value(args, 0, row_map))
            old = self._safe_literal_or_field(args[1], row_map)
            new = self._safe_literal_or_field(args[2], row_map)
            return v.replace(old, new)
        if func == "DATE_FORMAT":
            v = _safe_str(self._arg_value(args, 0, row_map))
            fmt = self._safe_literal_or_field(args[1], row_map)
            dt = _try_parse_date(v)
            if not dt:
                return ""
            return _format_date(dt, fmt)
        if func == "COALESCE":
            for a in args:
                vv = self._safe_literal_or_field(a, row_map, treat_as_value_if_field_missing=True)
                if not _is_empty(vv):
                    return _safe_str(vv)
            return ""

        raise RuleParseError(f"Unsupported function: {func} in expr={expr!r}")

    def _parse_args(self, arg_str: str) -> List[str]:
        """Parse function args, supporting quoted strings with commas."""
        if arg_str == "":
            return []
        args: List[str] = []
        buf = []
        in_quote = None  # type: Optional[str]
        i = 0
        while i < len(arg_str):
            ch = arg_str[i]
            if in_quote:
                buf.append(ch)
                if ch == in_quote:
                    in_quote = None
                i += 1
                continue
            if ch in ("'", '"'):
                in_quote = ch
                buf.append(ch)
                i += 1
                continue
            if ch == ",":
                args.append("".join(buf).strip())
                buf = []
                i += 1
                continue
            buf.append(ch)
            i += 1
        if buf:
            args.append("".join(buf).strip())
        return args

    def _get_value(self, row_map: Dict[str, Any], field: str) -> Any:
        """Get value by normalized field name."""
        nf = self.normalize_field_name(field)
        if nf in row_map:
            return row_map[nf]
        for k, v in row_map.items():
            if self.normalize_field_name(k) == nf:
                return v
        return ""

    def _arg_value(self, args: List[str], idx: int, row_map: Dict[str, Any]) -> Any:
        if idx >= len(args):
            return ""
        token = args[idx]
        return self._safe_literal_or_field(token, row_map, treat_as_value_if_field_missing=False)

    def _safe_literal_or_field(
        self, token: str, row_map: Dict[str, Any], treat_as_value_if_field_missing: bool = True
    ) -> str:
        """Return literal string/number or field value."""
        t = token.strip()
        if len(t) >= 2 and ((t[0] == "'" and t[-1] == "'") or (t[0] == '"' and t[-1] == '"')):
            return t[1:-1]
        # number
        if re.fullmatch(r"-?\d+(\.\d+)?", t):
            return t
        # identifier -> field
        fld = self.normalize_field_name(t)
        v = self._get_value(row_map, fld)
        if _is_empty(v) and treat_as_value_if_field_missing:
            # 如果字段不存在或为空，token本身可能就是值（容错）
            exists = fld in {self.normalize_field_name(k) for k in row_map.keys()}
            if not exists:
                return t
        return _safe_str(v)

    # -----------------------------
    # Data generation
    # -----------------------------
    def _random_state_city(self) -> Tuple[str, str]:
        state = random.choice(list(self.state_city_map.keys()))
        city = random.choice(self.state_city_map[state])
        return state, city

    def generate_normal_cases(self, count: int = 10) -> List[Dict[str, str]]:
        """
        生成正常场景数据（源数据行）：
        - 使用Faker生成姓名/地址
        - 州与城市匹配（如 MO - Saint Louis）
        - 日期格式正确
        返回：List[Dict]，键为“源字段名”（示例字段集合，实际可按source_schema调整）
        """
        rows: List[Dict[str, str]] = []
        for _ in range(count):
            state, city = self._random_state_city()
            dt = self.fake.date_between(start_date="-5y", end_date="today")
            row = {
                "FIRST_NAME": self.fake.first_name(),
                "LAST_NAME": self.fake.last_name(),
                "FULL_NAME": "",  # 可用于transform CONCAT
                "ADDRESS1": self.fake.street_address(),
                "CITY": city,
                "STATE": state,
                "ZIP": self.fake.postcode(),
                "EMAIL": self.fake.email(),
                "PHONE": self.fake.numerify("###-###-####"),
                "PRODUCT_LINE": random.choice(["A", "B", "C"]),
                "AMOUNT": str(random.randint(1, 10000)),
                "TXN_DATE": dt.strftime("%Y-%m-%d"),
            }
            row["FULL_NAME"] = f"{row['FIRST_NAME']} {row['LAST_NAME']}"
            rows.append(row)
        return rows

    def generate_abnormal_cases(self, scenarios: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        """
        生成异常场景数据：
        scenarios 示例：
        [
          {"name":"missing_field", "field":"EMAIL"},
          {"name":"bad_date", "field":"TXN_DATE", "value":"2024-13-40"},
          {"name":"illegal_enum", "field":"PRODUCT_LINE", "value":"UNKNOWN"},
        ]
        """
        base = self.generate_normal_cases(count=max(1, len(scenarios)))
        out: List[Dict[str, str]] = []
        for i, sc in enumerate(scenarios):
            row = dict(base[i % len(base)])
            name = sc.get("name", "")
            field = self.normalize_field_name(sc.get("field", ""))
            if name == "missing_field" and field:
                row.pop(field, None)
            elif name in ("bad_date", "illegal_enum", "bad_number"):
                if field:
                    row[field] = _safe_str(sc.get("value", ""))
            elif name == "encoding_weird":
                # 产生不可见字符/特殊符号
                row[field or "ADDRESS1"] = "ABC\u0000DEF\u200bGHI"
            else:
                # 默认：随机置空一个字段
                k = random.choice(list(row.keys()))
                row[k] = ""
            out.append(row)
        return out

    def generate_boundary_cases(self, count: int = 5) -> List[Dict[str, str]]:
        """
        生成边界场景：
        - 空字符串 vs NULL（这里用''模拟）
        - 超长字符串
        - 多字节字符
        - 连续分隔符
        """
        rows: List[Dict[str, str]] = []
        for i in range(count):
            state, city = self._random_state_city()
            row = {
                "FIRST_NAME": " " if i % 2 == 0 else "",
                "LAST_NAME": "测试用户" if i % 2 == 0 else "A" * 300,
                "FULL_NAME": "",
                "ADDRESS1": "AAA||BBB" if i % 2 == 0 else "|AAA|",
                "CITY": city,
                "STATE": state,
                "ZIP": "00000",
                "EMAIL": "x@example.com",
                "PHONE": "000-000-0000",
                "PRODUCT_LINE": "A",
                "AMOUNT": "999999999999999999999",
                "TXN_DATE": "2024-02-29" if i % 2 == 0 else "2024-13-40",
            }
            row["FULL_NAME"] = (row["FIRST_NAME"] + " " + row["LAST_NAME"]).strip()
            rows.append(row)
        return rows

    def save_to_csv(self, data: List[Dict[str, Any]], filepath: str) -> None:
        """
        保存数据到CSV（自动收集所有字段作为表头）。
        """
        os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
        if not data:
            with open(filepath, "w", newline="", encoding="utf-8") as f:
                f.write("")
            return

        # union headers
        headers: List[str] = []
        seen = set()
        for row in data:
            for k in row.keys():
                nk = self.normalize_field_name(k)
                if nk not in seen:
                    seen.add(nk)
                    headers.append(nk)

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for row in data:
                norm_row = {self.normalize_field_name(k): v for k, v in row.items()}
                writer.writerow({h: _safe_str(norm_row.get(h, "")) for h in headers})

    # -----------------------------
    # Unit tests
    # -----------------------------
    def run_unit_tests(self) -> None:
        """运行内置unittest。"""
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(_SmartDataGeneratorTests)
        runner = unittest.TextTestRunner(verbosity=2)
        result = runner.run(suite)
        if not result.wasSuccessful():
            raise SystemExit(1)


# -----------------------------
# Tests (self contained)
# -----------------------------
class _SmartDataGeneratorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.gen = SmartDataGenerator(seed=1)

    def test_infer_delimiter(self) -> None:
        self.assertEqual(self.gen.infer_delimiter("A|B|C"), "|")
        self.assertEqual(self.gen.infer_delimiter("A,B,C"), ",")
        self.assertEqual(self.gen.infer_delimiter("A\tB\tC"), "\t")

    def test_normalize_field_name(self) -> None:
        self.assertEqual(self.gen.normalize_field_name("  abc def "), "ABC_DEF")

    def test_parse_conditions_arrow(self) -> None:
        branches = self.gen._parse_conditions("A=1=>X;A=2=>Y;ELSE=>Z")
        self.assertEqual(branches[0]["when"], "A=1")
        self.assertEqual(branches[-1]["then"], "Z")

    def test_evaluate_conditional_eq_else(self) -> None:
        rule = {
            "conditions": [{"when": "PRODUCT_LINE=A", "then": "ALPHA"}, {"when": "ELSE", "then": "OTHER"}]
        }
        out1 = self.gen.evaluate_conditional({"PRODUCT_LINE": "A"}, rule)
        out2 = self.gen.evaluate_conditional({"PRODUCT_LINE": "B"}, rule)
        self.assertEqual(out1, "ALPHA")
        self.assertEqual(out2, "OTHER")

    def test_transform_concat_substr_replace_date(self) -> None:
        row = {"FIRST_NAME": "John", "LAST_NAME": "Doe", "TXN_DATE": "2024-01-31", "TEXT": "aabbcc"}
        rule_concat = {"expression": "CONCAT(FIRST_NAME,' ',LAST_NAME)"}
        self.assertEqual(self.gen._evaluate_transform_row(row, rule_concat), "John Doe")

        rule_substr = {"expression": "SUBSTR(TEXT,2,3)"}
        self.assertEqual(self.gen._evaluate_transform_row(row, rule_substr), "abb")

        rule_replace = {"expression": "REPLACE(TEXT,'bb','XX')"}
        self.assertEqual(self.gen._evaluate_transform_row(row, rule_replace), "aaXXcc")

        rule_date = {"expression": "DATE_FORMAT(TXN_DATE,'%Y%m%d')"}
        self.assertEqual(self.gen._evaluate_transform_row(row, rule_date), "20240131")

    def test_apply_transformations(self) -> None:
        df = pd.DataFrame([{"FIRST_NAME": "Jane", "LAST_NAME": "Roe", "PRODUCT_LINE": "A"}])
        mappings = [
            {
                "source": "FIRST_NAME",
                "target": "FIRST_NAME_OUT",
                "type": "direct",
                "logic": "FIRST_NAME",
                "conditions": {},
                "rule": {"target": "FIRST_NAME_OUT", "source": "FIRST_NAME"},
                "resolved": True,
            },
            {
                "source": "",
                "target": "CONST_OUT",
                "type": "default",
                "logic": "X",
                "conditions": {},
                "rule": {"target": "CONST_OUT", "default": "X"},
                "resolved": True,
            },
            {
                "source": "PRODUCT_LINE",
                "target": "PL_OUT",
                "type": "conditional",
                "logic": "IF PRODUCT_LINE=A THEN ALPHA; ELSE OTHER",
                "conditions": {"branches": []},
                "rule": {
                    "target": "PL_OUT",
                    "source": "PRODUCT_LINE",
                    "conditions": [{"when": "PRODUCT_LINE=A", "then": "ALPHA"}, {"when": "ELSE", "then": "OTHER"}],
                },
                "resolved": True,
            },
            {
                "source": "",
                "target": "FULL_NAME",
                "type": "transform",
                "logic": "CONCAT(FIRST_NAME,' ',LAST_NAME)",
                "conditions": {},
                "rule": {"target": "FULL_NAME", "expression": "CONCAT(FIRST_NAME,' ',LAST_NAME)"},
                "resolved": True,
            },
        ]
        out = self.gen.apply_transformations(df, mappings)
        self.assertEqual(out.loc[0, "FIRST_NAME_OUT"], "Jane")
        self.assertEqual(out.loc[0, "CONST_OUT"], "X")
        self.assertEqual(out.loc[0, "PL_OUT"], "ALPHA")
        self.assertEqual(out.loc[0, "FULL_NAME"], "Jane Roe")


# -----------------------------
# CLI
# -----------------------------
def _build_spec_flow(case_dir: str, spec_out: str) -> None:
    gen = SmartDataGenerator()
    source_path = os.path.join(case_dir, "source.csv")
    expected_path = os.path.join(case_dir, "expected.txt")
    rules_path = os.path.join(case_dir, "rules.xlsx")

    source_fields = gen.load_source_schema(source_path)
    expected_fields = gen.load_expected_schema(expected_path)
    rules_df = gen.load_rules(rules_path, sheet_name="Sheet1", header_row=4)

    mappings = gen.build_field_mappings(rules_df, source_fields, expected_fields)
    # 非strict校验：生成spec时允许unresolved存在
    gen.validate_mappings(mappings, source_fields, expected_fields, strict=False)

    spec = gen.generate_spec_json(source_fields, expected_fields, mappings)
    gen.save_spec(spec, spec_out)
    print(f"[OK] spec saved: {spec_out}")


def _apply_flow(case_dir: str, out_path: str) -> None:
    gen = SmartDataGenerator()
    source_path = os.path.join(case_dir, "source.csv")
    expected_path = os.path.join(case_dir, "expected.txt")
    rules_path = os.path.join(case_dir, "rules.xlsx")

    df_source = pd.read_csv(source_path, dtype=str, keep_default_na=False).fillna("")
    source_fields = [gen.normalize_field_name(c) for c in df_source.columns]
    expected_fields = gen.load_expected_schema(expected_path)
    rules_df = gen.load_rules(rules_path, sheet_name="Sheet1", header_row=4)
    mappings = gen.build_field_mappings(rules_df, source_fields, expected_fields)

    # apply前建议strict校验（如需强约束可打开）
    gen.validate_mappings(mappings, source_fields, expected_fields, strict=False)

    df_out = gen.apply_transformations(df_source, mappings)

    # 按expected_fields顺序输出（若缺失列补空）
    cols = [gen.normalize_field_name(c) for c in expected_fields]
    for c in cols:
        if c not in df_out.columns:
            df_out[c] = ""
    df_out = df_out[cols + [c for c in df_out.columns if c not in cols]]

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    df_out.to_csv(out_path, index=False, encoding="utf-8")
    print(f"[OK] transformed output saved: {out_path}")


def _generate_flow(out_path: str, normal: int, boundary: int) -> None:
    gen = SmartDataGenerator()
    data: List[Dict[str, Any]] = []
    data.extend(gen.generate_normal_cases(count=normal))
    data.extend(
        gen.generate_abnormal_cases(
            scenarios=[
                {"name": "bad_date", "field": "TXN_DATE", "value": "2024-13-40"},
                {"name": "illegal_enum", "field": "PRODUCT_LINE", "value": "UNKNOWN"},
                {"name": "missing_field", "field": "EMAIL"},
            ]
        )
    )
    data.extend(gen.generate_boundary_cases(count=boundary))
    gen.save_to_csv(data, out_path)
    print(f"[OK] generated data saved: {out_path}")


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="SmartDataGenerator - ETL mapping & test data generator")
    parser.add_argument("--test", action="store_true", help="run built-in unit tests")
    parser.add_argument("--generate", action="store_true", help="generate sample data to CSV")
    parser.add_argument("--out", type=str, default="out.csv", help="output file path for generate/apply")
    parser.add_argument("--normal", type=int, default=10, help="normal case count for generate")
    parser.add_argument("--boundary", type=int, default=5, help="boundary case count for generate")

    parser.add_argument("--build-spec", action="store_true", help="build spec.json from case files")
    parser.add_argument("--spec-out", type=str, default="spec.json", help="spec output path")
    parser.add_argument("--apply", action="store_true", help="apply mappings to source.csv to produce output csv")
    parser.add_argument("--case-dir", type=str, default="case", help="case directory containing source.csv/expected.txt/rules.xlsx")

    args = parser.parse_args(argv)

    if args.test or (not args.generate and not args.build_spec and not args.apply):
        # 默认行为：跑自测，保证代码可独立运行
        SmartDataGenerator().run_unit_tests()
        return

    if args.generate:
        _generate_flow(args.out, args.normal, args.boundary)

    if args.build_spec:
        _build_spec_flow(args.case_dir, args.spec_out)

    if args.apply:
        _apply_flow(args.case_dir, args.out)


if __name__ == "__main__":
    main()