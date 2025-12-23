FROM python:3.9-slim

WORKDIR /app

# 设置时区
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# ==================== 👇 加这一行 👇 ====================
# 强制 Python 实时打印日志 (Unbuffered Mode)
ENV PYTHONUNBUFFERED=1
# =======================================================

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000   # 新增这行

CMD ["python", "main.py"]
