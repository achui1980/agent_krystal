"""
Transformation executor tool for Krystal V4.
Applies transformation rules to source data records.
"""

from typing import Dict, Any, List
from crewai.tools import BaseTool
from pydantic import BaseModel, Field
from krystal_v4.transformers.transformer_registry import TransformerRegistry


class TransformationExecutorInput(BaseModel):
    """Input schema for TransformationExecutorTool"""

    source_record: str = Field(description="Source record as JSON string")
    transformation_type: str = Field(
        default="",
        description="Transformation type for single mode (fixed, direct, etc.). Leave empty for batch mode."
    )
    config: str = Field(
        default="",
        description="For single mode: transformation config as JSON string. For batch mode: JSON array of rules, each with target_field, transformation_type, config."
    )
    batch_rules: str = Field(
        default="",
        description="JSON array of transformation rules for batch mode. Each rule: {target_field, transformation_type, config}. When provided, processes ALL rules in one call and returns JSON result."
    )


class TransformationExecutorTool(BaseTool):
    name: str = "Transformation Executor"
    description: str = (
        "Executes transformation rules on a source record. "
        "Supports BATCH MODE (recommended): pass source_record + batch_rules (JSON array of {target_field, transformation_type, config}). "
        "Returns JSON dict of all transformed values in one call. "
        "Also supports single mode: pass source_record + transformation_type + config."
    )
    args_schema: type[BaseModel] = TransformationExecutorInput

    def _run(
        self,
        source_record: str,
        transformation_type: str = "",
        config: str = "",
        batch_rules: str = "",
    ) -> str:
        """
        Execute transformation(s) on source record.
        Supports batch mode (batch_rules) and single mode (transformation_type + config).
        """
        try:
            import json

            record = json.loads(source_record)

            # Batch mode: process all rules at once
            if batch_rules:
                rules = json.loads(batch_rules)
                result = {}
                for rule in rules:
                    t_field = rule.get("target_field", "")
                    t_type = rule.get("transformation_type", "")
                    t_config = rule.get("config", {})
                    try:
                        value = self.apply_transformation(record, t_type, t_config)
                        result[t_field] = value
                    except Exception as e:
                        result[t_field] = f"ERROR: {str(e)}"
                return json.dumps(result, ensure_ascii=False)

            # Single mode (backward compatible)
            if transformation_type and config:
                config_dict = json.loads(config)
                result = self.apply_transformation(record, transformation_type, config_dict)
                return f"Transformation result: {result}"

            return "ERROR: Provide either batch_rules or (transformation_type + config)"

        except Exception as e:
            return f"ERROR executing transformation: {str(e)}"

    def apply_transformation(
        self,
        source_record: Dict[str, Any],
        transformation_type: str,
        config: Dict[str, Any],
    ) -> Any:
        """
        Apply transformation (programmatic interface).

        Args:
            source_record: Source record dictionary
            transformation_type: Transformation type
            config: Configuration dictionary

        Returns:
            Transformed value
        """
        transformer = TransformerRegistry.get_transformer(transformation_type, config)
        return transformer.transform(source_record)

    def apply_multiple_transformations(
        self, source_record: Dict[str, Any], transformation_rules: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Apply multiple transformation rules to create output record.

        Args:
            source_record: Source record dictionary
            transformation_rules: List of transformation rule dictionaries
                Each rule should have: target_field, transformation_type, config

        Returns:
            Dictionary of transformed values
        """
        result = {}

        for rule in transformation_rules:
            target_field = rule.get("target_field")
            trans_type = rule.get("transformation_type")
            config = rule.get("config", {})

            try:
                value = self.apply_transformation(source_record, trans_type, config)
                result[target_field] = value
            except Exception as e:
                # Log error but continue processing other rules
                result[target_field] = f"ERROR: {str(e)}"

        return result
