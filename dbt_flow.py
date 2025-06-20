# dbt_flow.py

from prefect import flow, task
from prefect_shell import ShellOperation
from pathlib import Path

@task(log_prints=True, name="Run dbt build")
def run_dbt_build():
    """
    Task này sẽ chạy lệnh `dbt build` và kiểm tra kết quả đúng cách.
    """
    print("Bắt đầu chạy dbt build...")
    dbt_project_path = Path.cwd()
    shell_op = ShellOperation(
        commands=["dbt build"],
        working_dir=dbt_project_path,
        stream_output=True
    )

    # .run() trả về một danh sách các process, một cho mỗi lệnh
    completed_processes = shell_op.run()

    # Lấy process đầu tiên và duy nhất từ danh sách
    dbt_process = completed_processes[0]

    # Kiểm tra return_code từ đối tượng process đó
    if dbt_process.return_code != 0:
        # Lấy log lỗi để hiển thị
        error_logs = "\n".join(dbt_process.fetch_result())
        raise Exception(f"dbt build failed with exit code {dbt_process.return_code}.\nLogs:\n{error_logs}")

    print("dbt build hoàn tất thành công!")
    return True


@flow(name="Hourly dbt Build From Git")
def dbt_hourly_run_flow():
    run_dbt_build()


if __name__ == "__main__":
    from prefect.flows import flow as prefect_flow

    deployment = prefect_flow.from_source(
        source="https://github.com/long1234hcc/dbt-smart-bi.git",
        entrypoint="dbt_flow.py:dbt_hourly_run_flow"
    ).to_deployment(
        name="dbt-run-from-git-final",
        work_pool_name="dbt-pool"
    )
    
    deployment.apply()
    
    print("Deployment 'dbt-run-from-git-final' đã được tạo/cập nhật thành công!")