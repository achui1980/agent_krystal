"""
Krystal Crew - End-to-End Testing Crew

Orchestrates 5 agents to execute complete testing workflow:
1. Generate test data → 2. Upload to SFTP → 3. Trigger process → 4. Poll for completion → 5. Validate results
"""

from datetime import datetime
from typing import Dict, Any, List
import uuid

from crewai import Crew, Task, Process
from crewai.llm import LLM

from krystal.agents import (
    DataGeneratorAgent,
    SFTPOperatorAgent,
    APITriggerAgent,
    PollingMonitorAgent,
    ResultValidatorAgent,
)
from krystal.config import ServiceConfig, SFTPConfig, KrystalConfig


class KrystalCrew:
    """End-to-end testing crew for a single service"""

    def __init__(
        self,
        service_config: ServiceConfig,
        sftp_config: SFTPConfig,
        environment: str = "dev",
        llm=None,
    ):
        """
        Initialize Krystal Crew for a service

        Args:
            service_config: Configuration for the service to test
            sftp_config: SFTP connection configuration
            environment: Current environment name
            llm: Language model to use (optional)
        """
        self.service_config = service_config
        self.sftp_config = sftp_config
        self.environment = environment
        self.batch_id = (
            f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        )

        # Initialize LLM if not provided
        if llm is None:
            import os
            from crewai.llm import LLM

            llm = LLM(
                model=os.getenv("OPENAI_MODEL", "gpt-4o"),
                api_key=os.getenv("OPENAI_API_KEY"),
            )
        self.llm = llm

        # Build environment context
        self.environment_context = f"""
当前执行环境: {environment}
服务名称: {service_config.name}
批次ID: {self.batch_id}
SFTP服务器: {sftp_config.host}:{sftp_config.port}
"""

        # Store results
        self.results: Dict[str, Any] = {}

    def create_crew(self) -> Crew:
        """Create and configure the Crew with all agents and tasks"""
        # Create agents
        data_generator = DataGeneratorAgent.create(self.llm, self.environment_context)
        sftp_operator = SFTPOperatorAgent.create(self.llm, self.environment_context)
        api_trigger = APITriggerAgent.create(self.llm, self.environment_context)
        polling_monitor = PollingMonitorAgent.create(self.llm, self.environment_context)
        result_validator = ResultValidatorAgent.create(
            self.llm, self.environment_context
        )

        # Create tasks
        generate_task = self._create_generate_task(data_generator)
        upload_task = self._create_upload_task(sftp_operator)
        trigger_task = self._create_trigger_task(api_trigger)
        poll_task = self._create_poll_task(polling_monitor)
        validate_task = self._create_validate_task(result_validator)

        # Create crew with sequential process
        crew = Crew(
            agents=[
                data_generator,
                sftp_operator,
                api_trigger,
                polling_monitor,
                result_validator,
            ],
            tasks=[
                generate_task,
                upload_task,
                trigger_task,
                poll_task,
                validate_task,
            ],
            process=Process.sequential,
            verbose=True,
        )

        return crew

    def _create_generate_task(self, agent) -> Task:
        """Create data generation task"""
        data_gen = self.service_config.data_generation

        # Build data schema description
        schema_fields = []
        for field in data_gen.data_schema:
            field_desc = f"- {field.name} ({field.type})"
            if field.values:
                field_desc += f", values: {field.values}"
            if field.pattern:
                field_desc += f", pattern: {field.pattern}"
            schema_fields.append(field_desc)

        description = f"""
生成测试数据CSV文件用于服务: {self.service_config.name}

数据生成配置:
- 输出行数: {data_gen.row_count}
- 输出文件名: {data_gen.output_filename}
- 模板路径: {data_gen.template or "未指定（使用schema生成）"}

数据模式定义:
{chr(10).join(schema_fields)}

批次ID: {self.batch_id}

请使用csv_generator工具生成数据:
1. 如果有模板，使用模板路径
2. 如果没有模板，根据data_schema生成
3. 保存到临时目录: /tmp/krystal/{self.service_config.name}/
4. 文件名中包含批次ID

预期输出:
- 生成的CSV文件完整路径
        """

        return Task(
            description=description,
            expected_output="生成的CSV文件完整路径（绝对路径）",
            agent=agent,
        )

    def _create_upload_task(self, agent) -> Task:
        """Create SFTP upload task"""
        upload = self.service_config.upload

        description = f"""
将生成的CSV文件上传到SFTP服务器。

SFTP配置:
- 服务器: {self.sftp_config.host}:{self.sftp_config.port}
- 用户名: {self.sftp_config.username}
- 远程基础路径: {self.sftp_config.remote_base_path}

上传配置:
- 远程路径: {upload.get("remote_path", "/uploads")}
- 服务名称: {self.service_config.name}

批次ID: {self.batch_id}

请使用sftp_client工具上传文件:
1. 连接SFTP服务器（使用配置中的凭证）
2. 确保远程目录存在
3. 上传文件到远程路径
4. 验证文件大小
5. 返回远程文件完整路径

注意: 文件路径从上一步任务中获取（context传递）
        """

        return Task(
            description=description,
            expected_output="远程文件的完整路径和上传确认信息",
            agent=agent,
            context=[self._create_generate_task.__name__],
        )

    def _create_trigger_task(self, agent) -> Task:
        """Create API trigger task"""
        trigger = self.service_config.trigger

        # Build headers description
        headers_desc = "\n".join([f"  {k}: {v}" for k, v in trigger.headers.items()])

        description = f"""
调用API触发业务流程。

API触发配置:
- 端点: {trigger.endpoint}
- 方法: {trigger.method}
- 请求头:
{headers_desc}

请求体模板:
```
{trigger.body_template}
```

任务ID提取器: {trigger.task_id_extractor}

批次ID: {self.batch_id}

变量替换:
- {{remote_file_path}} - 从上传任务获取
- {{batch_id}} - 使用当前批次ID: {self.batch_id}
- {{row_count}} - 数据行数: {self.service_config.data_generation.row_count}

请使用api_client和template_renderer工具:
1. 渲染模板，替换变量
2. 发送HTTP请求
3. 从响应中提取任务ID（使用json_extractor）
4. 返回任务ID和响应详情
        """

        return Task(
            description=description,
            expected_output="任务ID（用于后续轮询）和API响应详情",
            agent=agent,
            context=[self._create_upload_task.__name__],
        )

    def _create_poll_task(self, agent) -> Task:
        """Create polling task"""
        polling = self.service_config.polling

        if not polling.enabled:
            return Task(
                description="轮询已禁用，跳过此步骤。",
                expected_output="轮询跳过确认",
                agent=agent,
                context=[self._create_trigger_task.__name__],
            )

        description = f"""
轮询检查任务执行状态直到完成。

轮询配置:
- 最大尝试次数: {polling.max_attempts}
- 轮询间隔: {polling.interval}秒
- 成功状态: {", ".join(polling.success_statuses)}
- 失败状态: {", ".join(polling.failure_statuses)}

状态检查端点: {polling.status_check_endpoint or "使用与触发相同的端点"}

任务ID: 从触发任务获取

请使用polling_service工具:
1. 定期检查任务状态
2. 判断是否达到终态（成功/失败/超时）
3. 记录尝试次数和耗时
4. 返回最终状态和详细信息

注意:
- 状态检查端点可能包含 {{task_id}} 占位符，需要替换
- 使用jsonpath提取状态字段
- 正确处理超时情况
        """

        return Task(
            description=description,
            expected_output="最终任务状态、尝试次数、完成时间",
            agent=agent,
            context=[self._create_trigger_task.__name__],
        )

    def _create_validate_task(self, agent) -> Task:
        """Create result validation task"""
        validation = self.service_config.validation

        description = f"""
下载结果文件并验证数据正确性。

验证配置:
- 下载结果文件: {"是" if validation.download_from_sftp else "否"}
- 远程结果路径: {validation.remote_result_path}
- 本地临时路径: {validation.local_temp_path}
- 对比模式: {validation.comparison_mode}

批次ID: {self.batch_id}

远程路径变量:
- {{batch_id}} - 替换为当前批次ID

请使用以下工具执行验证:
1. sftp_client (如果需要下载)
   - 从远程路径下载文件
   - 保存到本地临时路径

2. file_validator
   - 验证文件存在
   - 验证文件大小（如有配置）

3. data_validator (如果是CSV对比模式)
   - 对比预期和实际数据
   - 应用验证规则
   - 生成差异报告

预期输出:
详细的验证报告，包括：
- 测试通过/失败状态
- 验证详情
- 错误列表
- 数据行数统计
        """

        return Task(
            description=description,
            expected_output="详细的测试验证报告（JSON格式）",
            agent=agent,
            context=[self._create_poll_task.__name__],
        )

    def run(self) -> Dict[str, Any]:
        """
        Execute the testing workflow

        Returns:
            Dictionary with execution results
        """
        import os
        from dotenv import load_dotenv

        load_dotenv()

        print(f"\n{'=' * 60}")
        print(f"开始测试服务: {self.service_config.name}")
        print(f"环境: {self.environment}")
        print(f"批次ID: {self.batch_id}")
        print(f"{'=' * 60}\n")

        try:
            # Create and run crew
            crew = self.create_crew()
            result = crew.kickoff()

            # Parse result
            self.results = {
                "service": self.service_config.name,
                "batch_id": self.batch_id,
                "environment": self.environment,
                "success": True,
                "result": result,
                "timestamp": datetime.now().isoformat(),
            }

            print(f"\n{'=' * 60}")
            print(f"服务 {self.service_config.name} 测试完成")
            print(f"{'=' * 60}\n")

            return self.results

        except Exception as e:
            self.results = {
                "service": self.service_config.name,
                "batch_id": self.batch_id,
                "environment": self.environment,
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
            }

            print(f"\n{'=' * 60}")
            print(f"服务 {self.service_config.name} 测试失败")
            print(f"错误: {str(e)}")
            print(f"{'=' * 60}\n")

            return self.results
