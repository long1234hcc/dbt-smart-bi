# dbt_flow.py

from prefect import flow, task
from prefect_shell import ShellOperation
from pathlib import Path

@task(log_prints=True, name="Run dbt build")
def run_dbt_build():
    """
    Task này sẽ chạy lệnh `dbt build` và in ra log chi tiết.
    """
    print("Bắt đầu chạy dbt build...")
    
    dbt_project_path = Path.cwd()

    # Tạo đối tượng để chạy lệnh shell
    shell_op = ShellOperation(
        commands=["dbt build --debug"], # Thêm flag --debug để có log chi tiết nhất
        working_dir=dbt_project_path,
        stream_output=True
    )
    
    # Chạy lệnh và chờ nó hoàn thành
    process = shell_op.run()
    
    # In ra toàn bộ log output để gỡ lỗi
    # .fetch_result() sẽ lấy toàn bộ những gì đã được in ra terminal
    output = "\n".join(process.fetch_result())
    print("--- DBT COMMAND OUTPUT ---")
    print(output)
    print("--- END DBT COMMAND OUTPUT ---")
    
    # Kiểm tra nếu lệnh dbt thất bại thì báo lỗi toàn bộ flow
    if process.return_code != 0:
        raise Exception(f"dbt build failed with exit code {process.return_code}. Check logs above for details.")
    
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
        name="dbt-run-from-git-debug", 
        work_pool_name="dbt-pool"
    )
    
    deployment.apply()
    
    print("Deployment 'dbt-run-from-git-debug' đã được tạo/cập nhật thành công!")