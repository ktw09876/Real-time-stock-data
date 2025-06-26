# 1. 공식 PySpark 이미지 사용
FROM apache/spark:3.5.1-python3

# 2. 모든 설치 작업을 위해 root 사용자로 전환
USER root

# 3. [핵심 수정] .ivy2 디렉토리를 미리 만들고 소유자를 spark로 변경
# spark-submit이 패키지를 다운로드할 캐시 디렉토리에 대한 쓰기 권한을 부여합니다.
RUN mkdir -p /home/spark/.ivy2 && \
    chown -R spark:spark /home/spark/.ivy2

# 4. 애플리케이션 작업 디렉토리 설정
WORKDIR /app

# 5. 파이썬 의존성 설치 (캐시 활용을 위해 먼저 복사)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 6. 소스코드 복사
COPY . .

# 7. 컨테이너의 기본 사용자를 spark로 전환
USER spark
