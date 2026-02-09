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
        description="Transformation type (fixed, direct, etc.)"
    )
    config: str = Field(description="Transformation config as JSON string")


class TransformationExecutorTool(BaseTool):
    name: str = "Transformation Executor"
    description: str = (
        "Executes a single transformation rule on a source record. "
        "Uses the transformer registry to apply the specified transformation. "
        "Input: source_record (JSON string), transformation_type (string), config (JSON string)"
    )
    args_schema: type[BaseModel] = TransformationExecutorInput

    def _run(self, source_record: str, transformation_type: str, config: str) -> str:
        """
        Execute transformation on source record.

        Args:
            source_record: Source record as JSON string
            transformation_type: Transformation type
            config: Configuration as JSON string

        Returns:
            Transformed value or error message
        """
        try:
            import json

            # Parse inputs
            record = json.loads(source_record)
            config_dict = json.loads(config)

            # Execute transformation
            result = self.apply_transformation(record, transformation_type, config_dict)

            return f"Transformation result: {result}"

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
