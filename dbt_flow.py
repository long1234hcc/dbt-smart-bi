# dbt_flow.py

from prefect import flow, task
from prefect_shell import ShellOperation
from pathlib import Path

@task(log_prints=True, name="Run dbt build")
def run_dbt_build():
    """
    Task này sẽ chạy lệnh `dbt build` cho toàn bộ dự án,
    với đường dẫn được chỉ định rõ ràng.
    """
    print("Bắt đầu chạy dbt build với đường dẫn tường minh...")
    
    # Lấy đường dẫn tuyệt đối đến thư mục dự án dbt
    # Worker sẽ chạy code từ một thư mục tạm, nên chúng ta cần đường dẫn tuyệt đối
    # tới nơi chứa file profiles.yml
    dbt_project_path = "/home/long/workspace/dbt-smart-bi" # <-- Sửa lại đường dẫn tuyệt đối của bạn
    profiles_path = "/home/long/.dbt" # <-- Đường dẫn đến thư mục chứa profiles.yml

    # Xây dựng lệnh dbt hoàn chỉnh
    dbt_command = (
        "dbt build "
        f"--project-dir {dbt_project_path} "
        f"--profiles-dir {profiles_path}"
    )

    print(f"Executing command: {dbt_command}")

    shell_op = ShellOperation(
        commands=[dbt_command], # Chạy lệnh đã được xây dựng
        stream_output=True
    )
    
    result = shell_op.run()
    
    if result.return_code != 0:
        raise Exception("dbt build failed!")
    
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