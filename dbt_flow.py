# dbt_flow.py

from pathlib import Path
from prefect import flow, task
from prefect_shell import ShellOperation
from prefect.schedules import Cron

@task(name="Run dbt build", log_prints=True, retries=1, retry_delay_seconds=60)
def run_dbt_build_task():
    """
    Task này sẽ chạy lệnh `dbt build` cho toàn bộ dự án,
    với đường dẫn tường minh để đảm bảo Worker luôn tìm thấy.
    """
    # QUAN TRỌNG: Hãy đảm bảo các đường dẫn tuyệt đối này là chính xác trên máy bạn
    dbt_project_path = "/home/long/workspace/dbt-smart-bi"
    profiles_path = "/home/long/.dbt"

    # Xây dựng lệnh dbt hoàn chỉnh với các flag chỉ định đường dẫn
    dbt_command = (
        "dbt build "
        f"--project-dir {dbt_project_path} "
        f"--profiles-dir {profiles_path}"
    )

    print(f"Executing command: {dbt_command}")

    # ShellOperation sẽ tự động báo lỗi (raise exception) nếu return code khác 0.
    # Chúng ta không cần kiểm tra thủ công, giúp code sạch hơn.
    ShellOperation(
        commands=[dbt_command],
        stream_output=True
    ).run()
    
    print("dbt build completed successfully!")


@flow(name="Production Social Media ETL")
def dbt_etl_pipeline():
    """
    Flow chính để điều phối việc chạy dbt pipeline.
    """
    run_dbt_build_task()


if __name__ == "__main__":
    # Import flow object dưới một tên khác để tránh nhầm lẫn
    from prefect.flows import flow as prefect_flow

    # Sử dụng cú pháp deploy cuối cùng và đúng chuẩn nhất cho Prefect 3
    prefect_flow.from_source(
        source="https://github.com/long1234hcc/dbt-smart-bi.git",
        entrypoint="dbt_flow.py:dbt_etl_pipeline"
    ).deploy(
        name="dbt-prod-run",
        work_pool_name="dbt-pool",
        # Sử dụng đối tượng Cron để có thể chỉ định múi giờ
        schedule=Cron(cron="0 * * * *", timezone="Asia/Ho_Chi_Minh")
    )
    
    print("Deployment 'dbt-prod-run' đã được tạo/cập nhật thành công!")