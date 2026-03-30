"""
Krystal E2E Test API Stub

模拟真实的业务流程：
1. 接收 trigger 请求，创建任务
2. 状态轮询：前2次返回 processing，第3次返回 completed
3. 任务完成后在 SFTP 目录生成结果文件
"""

import os
import csv
import uuid
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
import uvicorn


app = FastAPI(title="Krystal E2E API Stub", version="1.0.0")

# 内存任务存储
tasks: Dict[str, dict] = {}

# 配置
SFTP_BASE_PATH = Path(os.getenv("SFTP_BASE_PATH", "/home/testuser"))
PROCESSING_DELAY_SECONDS = int(os.getenv("PROCESSING_DELAY_SECONDS", "3"))


class TriggerRequest(BaseModel):
    """Trigger API request body"""

    file_path: str = Field(description="上传的文件路径（相对于SFTP根目录）")
    batch_id: str = Field(description="批次ID")
    service: str = Field(description="服务名称")
    row_count: int = Field(default=0, description="数据行数")


class TriggerResponse(BaseModel):
    """Trigger API response"""

    task_id: str = Field(description="任务ID")
    status: str = Field(description="任务状态")
    message: str = Field(description="响应消息")
    file_path: str = Field(description="接收的文件路径")


class StatusResponse(BaseModel):
    """Status check response"""

    task_id: str = Field(description="任务ID")
    status: str = Field(description="当前状态")
    progress: int = Field(default=0, description="处理进度百分比")
    message: str = Field(description="状态描述")
    created_at: str = Field(description="任务创建时间")
    updated_at: str = Field(description="最后更新时间")


@app.get("/health")
def health_check():
    """健康检查端点"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


@app.post("/api/v1/trigger", response_model=TriggerResponse)
def trigger_task(request: TriggerRequest, background_tasks: BackgroundTasks):
    """
    触发业务流程

    - 创建任务
    - 后台异步处理
    - 返回 task_id 用于轮询
    """
    task_id = f"task_{uuid.uuid4().hex[:12]}"
    now = datetime.now().isoformat()

    # 创建任务记录
    tasks[task_id] = {
        "task_id": task_id,
        "status": "pending",
        "progress": 0,
        "file_path": request.file_path,
        "batch_id": request.batch_id,
        "service": request.service,
        "row_count": request.row_count,
        "created_at": now,
        "updated_at": now,
        "poll_count": 0,
    }

    # 启动后台处理任务
    background_tasks.add_task(process_task, task_id)

    return TriggerResponse(
        task_id=task_id,
        status="pending",
        message=f"Task created successfully for {request.service}",
        file_path=request.file_path,
    )


@app.get("/api/v1/status/{task_id}", response_model=StatusResponse)
def check_status(task_id: str):
    """
    检查任务状态

    状态流转：pending → processing → completed
    """
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")

    task = tasks[task_id]

    # 模拟渐进式处理
    if task["status"] == "pending":
        task["status"] = "processing"
        task["progress"] = 25
        task["updated_at"] = datetime.now().isoformat()
    elif task["status"] == "processing":
        task["poll_count"] += 1
        if task["poll_count"] >= 2:
            # 模拟处理完成
            task["status"] = "completed"
            task["progress"] = 100
        else:
            task["progress"] = 50 + task["poll_count"] * 25
        task["updated_at"] = datetime.now().isoformat()

    return StatusResponse(
        task_id=task_id,
        status=task["status"],
        progress=task["progress"],
        message=f"Task is {task['status']}",
        created_at=task["created_at"],
        updated_at=task["updated_at"],
    )


def process_task(task_id: str):
    """
    后台处理任务

    模拟真实业务流程：
    1. 读取上传的CSV文件
    2. 处理数据（模拟耗时操作）
    3. 生成结果文件到SFTP output目录
    """
    import paramiko

    task = tasks.get(task_id)
    if not task:
        return

    try:
        # 模拟处理延迟
        time.sleep(PROCESSING_DELAY_SECONDS)

        # 连接SFTP读取源文件
        input_file_path = task["file_path"]
        batch_id = task["batch_id"]
        service = task["service"]

        # 构建结果文件路径
        # 假设服务名为 payment-service，则输出路径为 /uploads/payment/output/{batch_id}_result.csv
        service_short = service.replace("-service", "")
        output_dir = SFTP_BASE_PATH / "uploads" / service_short / "output"
        output_file = output_dir / f"{batch_id}_result.csv"

        # 确保输出目录存在
        output_dir.mkdir(parents=True, exist_ok=True)

        # 读取源文件并生成结果
        input_full_path = SFTP_BASE_PATH / input_file_path.lstrip("/")

        if input_full_path.exists():
            # 读取输入文件
            input_rows = []
            with open(input_full_path, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames
                for row in reader:
                    input_rows.append(row)

            # 生成结果数据（模拟处理）
            result_fieldnames = fieldnames + [
                "status",
                "processed_amount",
                "transaction_id",
            ]

            with open(output_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=result_fieldnames)
                writer.writeheader()

                for i, row in enumerate(input_rows):
                    result_row = row.copy()
                    result_row["status"] = "success"
                    result_row["processed_amount"] = (
                        float(row.get("amount", 0)) * 0.95
                    )  # 模拟手续费
                    result_row["transaction_id"] = f"txn_{uuid.uuid4().hex[:16]}"
                    writer.writerow(result_row)

            print(f"[API Stub] Generated result file: {output_file}")
            print(f"[API Stub] Processed {len(input_rows)} rows")

        else:
            # 如果找不到源文件，创建一个 dummy 结果
            output_file.parent.mkdir(parents=True, exist_ok=True)
            with open(output_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(
                    ["order_id", "status", "processed_amount", "transaction_id"]
                )
                writer.writerow(
                    [
                        f"order_{uuid.uuid4().hex[:8]}",
                        "success",
                        "100.00",
                        f"txn_{uuid.uuid4().hex[:16]}",
                    ]
                )

            print(f"[API Stub] Generated dummy result file: {output_file}")

        # 更新任务状态
        task["result_file"] = str(output_file.relative_to(SFTP_BASE_PATH))
        task["status"] = "completed"
        task["progress"] = 100
        task["updated_at"] = datetime.now().isoformat()

    except Exception as e:
        print(f"[API Stub] Error processing task {task_id}: {e}")
        task["status"] = "failed"
        task["error"] = str(e)
        task["updated_at"] = datetime.now().isoformat()


@app.get("/api/v1/tasks")
def list_tasks():
    """列出所有任务（用于调试）"""
    return {"tasks": list(tasks.values()), "count": len(tasks)}


@app.delete("/api/v1/tasks/{task_id}")
def delete_task(task_id: str):
    """删除任务（用于清理）"""
    if task_id in tasks:
        del tasks[task_id]
        return {"message": f"Task {task_id} deleted"}
    raise HTTPException(status_code=404, detail="Task not found")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
