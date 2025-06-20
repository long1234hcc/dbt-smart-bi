from prefect import flow, task
import time

# Định nghĩa task đầu tiên
@task
def say_hello(name: str):
    """Một task đơn giản để in lời chào."""
    print(f"Hello, {name}!")
    return name  # Trả về kết quả để task khác có thể dùng

# Định nghĩa task thứ hai
@task
def say_goodbye(name: str):
    """Một task đơn giản để in lời tạm biệt."""
    print(f"Running goodbye task for {name}...")
    # Thêm một chút delay để chúng ta có thể thấy flow chạy trong UI
    time.sleep(5) 
    print(f"Goodbye, {name}!")

# Định nghĩa Flow chính để điều phối các task
@flow(name="Hello Flow")
def hello_world_flow(user_name: str = "World"):
    # Gọi task đầu tiên và lưu kết quả của nó
    hello_result = say_hello(user_name)

    # Gọi task thứ hai. 
    # Prefect đủ thông minh để biết task này phụ thuộc vào kết quả của task trước.
    say_goodbye(hello_result)

# Khối này cho phép chúng ta chạy file Python này trực tiếp
if __name__ == "__main__":
    # Chạy flow với một tên cụ thể
    hello_world_flow("dbt Learner")