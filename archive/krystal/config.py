"""
Configuration Manager for Krystal

Handles loading and managing configurations for different environments.
"""

import os
import yaml
from typing import Dict, List, Any, Optional
from pathlib import Path
from dataclasses import dataclass, field
from dotenv import load_dotenv


@dataclass
class SFTPConfig:
    """SFTP connection configuration"""

    host: str
    port: int = 22
    username: str = ""
    password: str = ""
    private_key: Optional[str] = None
    remote_base_path: str = "/"


@dataclass
class DataField:
    """Data field schema definition"""

    name: str
    type: str  # uuid, string, int, float, datetime, enum, etc.
    required: bool = True
    min: Optional[Any] = None
    max: Optional[Any] = None
    values: List[Any] = field(default_factory=list)
    pattern: Optional[str] = None


@dataclass
class DataGenerationConfig:
    """Data generation configuration"""

    template: Optional[str] = None
    output_filename: str = "test_data_{timestamp}.csv"
    row_count: int = 100
    data_schema: List[DataField] = field(default_factory=list)


@dataclass
class TriggerConfig:
    """API trigger configuration"""

    type: str = "api"
    endpoint: str = ""
    method: str = "POST"
    headers: Dict[str, str] = field(default_factory=dict)
    body_template: str = ""
    task_id_extractor: str = ""


@dataclass
class PollingConfig:
    """Polling configuration"""

    enabled: bool = True
    max_attempts: int = 30
    interval: int = 10
    status_check_endpoint: str = ""
    success_statuses: List[str] = field(
        default_factory=lambda: ["completed", "success"]
    )
    failure_statuses: List[str] = field(default_factory=lambda: ["failed", "error"])


@dataclass
class ValidationRule:
    """Validation rule definition"""

    field: str
    rule: str  # equals, not_empty, regex, range, etc.
    reference_field: Optional[str] = None
    expected_value: Optional[Any] = None


@dataclass
class ValidationConfig:
    """Result validation configuration"""

    download_from_sftp: bool = True
    remote_result_path: str = ""
    local_temp_path: str = "/tmp/krystal/downloads"
    comparison_mode: str = "csv"
    expected_schema: List[DataField] = field(default_factory=list)
    validation_rules: List[ValidationRule] = field(default_factory=list)


@dataclass
class ServiceConfig:
    """Service test configuration"""

    name: str
    description: str = ""
    enabled: bool = True
    data_generation: DataGenerationConfig = field(default_factory=DataGenerationConfig)
    upload: Dict[str, Any] = field(default_factory=dict)
    trigger: TriggerConfig = field(default_factory=TriggerConfig)
    polling: PollingConfig = field(default_factory=PollingConfig)
    validation: ValidationConfig = field(default_factory=ValidationConfig)
    retry_attempts: int = 3


@dataclass
class ReportConfig:
    """Report generation configuration"""

    format: str = "markdown"
    output_path: str = "reports/"
    include_passed: bool = True
    include_failed: bool = True
    include_details: bool = True


@dataclass
class KrystalConfig:
    """Main Krystal configuration"""

    environment: str = "dev"
    sftp: SFTPConfig = field(default_factory=lambda: SFTPConfig(host=""))
    services: List[ServiceConfig] = field(default_factory=list)
    report: ReportConfig = field(default_factory=ReportConfig)


class ConfigManager:
    """Manages loading and accessing configurations"""

    def __init__(self, environment: Optional[str] = None):
        self.environment = environment or os.getenv("KRYSTAL_ENV", "dev")
        self.config_path = Path("config") / self.environment
        self.env_file = self.config_path / "secrets.env"
        self.services_file = self.config_path / "services.yaml"

        # Load environment variables
        load_dotenv()
        if self.env_file.exists():
            load_dotenv(self.env_file)

        self._config: Optional[KrystalConfig] = None

    def load(self) -> KrystalConfig:
        """Load configuration for the current environment"""
        if self._config is not None:
            return self._config

        # Load base configuration from environment variables
        sftp_config = SFTPConfig(
            host=os.getenv("SFTP_HOST", "localhost"),
            port=int(os.getenv("SFTP_PORT", "22")),
            username=os.getenv("SFTP_USERNAME", ""),
            password=os.getenv("SFTP_PASSWORD", ""),
            private_key=os.getenv("SFTP_PRIVATE_KEY"),
            remote_base_path=os.getenv("SFTP_REMOTE_BASE_PATH", "/uploads"),
        )

        # Load services configuration
        services = []
        if self.services_file.exists():
            with open(self.services_file, "r", encoding="utf-8") as f:
                services_data = yaml.safe_load(f)
                services = self._parse_services(services_data.get("services", []))

        # Load report configuration
        report_config = ReportConfig(
            format=os.getenv("REPORT_FORMAT", "markdown"),
            output_path=os.getenv("REPORT_OUTPUT_PATH", "reports/"),
            include_passed=os.getenv("REPORT_INCLUDE_PASSED", "true").lower() == "true",
            include_failed=os.getenv("REPORT_INCLUDE_FAILED", "true").lower() == "true",
            include_details=os.getenv("REPORT_INCLUDE_DETAILS", "true").lower()
            == "true",
        )

        self._config = KrystalConfig(
            environment=self.environment,
            sftp=sftp_config,
            services=services,
            report=report_config,
        )

        return self._config

    def _parse_services(self, services_data: List[Dict]) -> List[ServiceConfig]:
        """Parse service configurations from YAML data"""
        services = []

        for service_data in services_data:
            # Parse data generation config
            data_gen_data = service_data.get("data_generation", {})
            data_schema = []
            for field_data in data_gen_data.get("data_schema", []):
                data_schema.append(
                    DataField(
                        name=field_data["name"],
                        type=field_data["type"],
                        required=field_data.get("required", True),
                        min=field_data.get("min"),
                        max=field_data.get("max"),
                        values=field_data.get("values", []),
                        pattern=field_data.get("pattern"),
                    )
                )

            data_generation = DataGenerationConfig(
                template=data_gen_data.get("template"),
                output_filename=data_gen_data.get(
                    "output_filename", "test_data_{timestamp}.csv"
                ),
                row_count=data_gen_data.get("row_count", 100),
                data_schema=data_schema,
            )

            # Parse trigger config
            trigger_data = service_data.get("trigger", {})
            trigger = TriggerConfig(
                type=trigger_data.get("type", "api"),
                endpoint=trigger_data.get("endpoint", ""),
                method=trigger_data.get("method", "POST"),
                headers=trigger_data.get("headers", {}),
                body_template=trigger_data.get("body_template", ""),
                task_id_extractor=trigger_data.get("task_id_extractor", ""),
            )

            # Parse polling config
            polling_data = service_data.get("polling", {})
            polling = PollingConfig(
                enabled=polling_data.get("enabled", True),
                max_attempts=polling_data.get("max_attempts", 30),
                interval=polling_data.get("interval", 10),
                status_check_endpoint=polling_data.get("status_check", {}).get(
                    "endpoint", ""
                ),
                success_statuses=polling_data.get("status_check", {}).get(
                    "success_statuses", ["completed", "success"]
                ),
                failure_statuses=polling_data.get("status_check", {}).get(
                    "failure_statuses", ["failed", "error"]
                ),
            )

            # Parse validation config
            validation_data = service_data.get("validation", {})
            expected_schema = []
            for field_data in validation_data.get("expected_schema", []):
                expected_schema.append(
                    DataField(
                        name=field_data["name"],
                        type=field_data["type"],
                        required=field_data.get("required", True),
                        values=field_data.get("values", []),
                    )
                )

            validation_rules = []
            for rule_data in validation_data.get("validation_rules", []):
                validation_rules.append(
                    ValidationRule(
                        field=rule_data["field"],
                        rule=rule_data["rule"],
                        reference_field=rule_data.get("reference_field"),
                        expected_value=rule_data.get("expected_value"),
                    )
                )

            validation = ValidationConfig(
                download_from_sftp=validation_data.get("download_from_sftp", True),
                remote_result_path=validation_data.get("remote_result_path", ""),
                local_temp_path=validation_data.get(
                    "local_temp_path", "/tmp/krystal/downloads"
                ),
                comparison_mode=validation_data.get("comparison_mode", "csv"),
                expected_schema=expected_schema,
                validation_rules=validation_rules,
            )

            service = ServiceConfig(
                name=service_data["name"],
                description=service_data.get("description", ""),
                enabled=service_data.get("enabled", True),
                data_generation=data_generation,
                upload=service_data.get("upload", {}),
                trigger=trigger,
                polling=polling,
                validation=validation,
                retry_attempts=service_data.get("retry_attempts", 3),
            )

            services.append(service)

        return services

    def get_service(self, name: str) -> Optional[ServiceConfig]:
        """Get a specific service configuration by name"""
        config = self.load()
        for service in config.services:
            if service.name == name:
                return service
        return None

    def get_enabled_services(self) -> List[ServiceConfig]:
        """Get all enabled service configurations"""
        config = self.load()
        return [s for s in config.services if s.enabled]

    def get_environment_context(self) -> str:
        """Get a string describing the current environment for agents"""
        config = self.load()
        return f"""
当前执行环境: {self.environment}
配置文件路径: {self.config_path}
SFTP服务器: {config.sftp.host}:{config.sftp.port}
报告输出路径: {config.report.output_path}
可用服务: {", ".join(s.name for s in config.services if s.enabled)}
"""
