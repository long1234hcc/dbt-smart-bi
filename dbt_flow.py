# dbt_flow.py

# Import các thư viện cần thiết
from prefect import flow, task
# Import lớp Cron từ prefect.schedules
from prefect.schedules import Cron
from prefect_shell import ShellOperation
from pathlib import Path

# ===============================================
# === CÁC TASK VÀ FLOW LOGIC GIỮ NGUYÊN =========
# ===============================================

@task(log_prints=True, name="Run dbt build")
def run_dbt_build():
    """
    Task này sẽ chạy lệnh `dbt build` cho toàn bộ dự án.
    """
    print("Bắt đầu chạy dbt build từ code trên Git...")
    dbt_project_path = Path.cwd()
    shell_op = ShellOperation(
        commands=["dbt build"],
        working_dir=dbt_project_path,
        stream_output=True
    )
    result = shell_op.run()
    if result.return_code != 0:
        raise Exception("dbt build failed!")
    print("dbt build hoàn tất thành công!")
    return True

@flow(name="Hourly dbt Build From Git")
def dbt_hourly_run_flow():
    """
    Flow chỉ có một nhiệm vụ: chạy dbt build.
    """
    run_dbt_build()

# ========================================================
# ===== KHỐI DEPLOYMENT THEO CHUẨN GIT STORAGE CỦA PREFECT 3 =====
# ========================================================

if __name__ == "__main__":
    # Import flow object dưới một tên khác để tránh nhầm lẫn
    from prefect.flows import flow as prefect_flow

    # Gọi trực tiếp phương thức .deploy() từ flow đã được load từ source
    prefect_flow.from_source(
        # Chỉ định nguồn code là một kho chứa Git
        source="https://github.com/long1234hcc/dbt-smart-bi.git",
        # Chỉ định file và tên hàm flow cần chạy bên trong kho chứa đó
        entrypoint="dbt_flow.py:dbt_hourly_run_flow"
    ).deploy(
        name="dbt-run-from-git",
        work_pool_name="default-agent-pool",
        # Lịch trình chạy
        schedule=Cron(cron="0 * * * *", timezone="Asia/Ho_Chi_Minh"),
        # Các tham số khác có thể thêm ở đây nếu cần
    )
    
    print("Deployment 'dbt-run-from-git' từ nguồn GitHub đã được tạo/cập nhật thành công!")