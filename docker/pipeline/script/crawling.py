import os
import requests
import logging
import sys
import json
import time
import re

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager

# --- 로거(Logger) 설정 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

class ChromeDriver:
    def __init__(self):    
        self.driver = None
        self.options = Options()
        self._setup_options()
        self.driver = self._start_driver()


    def _setup_options(self):
        logger.info("ChromeDriver 옵션을 설정합니다...")
        webdriver.ChromeOptions()
        self.options.add_argument("--headless")  # GUI 없이 백그라운드에서 실행
        self.options.add_argument("--start-maximized")
        # self.options.add_argument('--ignore-certificate-errors')   # SSL 인증서 오류 무시
        self.options.add_argument('--disable-gpu') # GPU 가속 사용 안함(리눅스/서버 환경에서 종종 필요)
        self.options.add_argument('--disable-dev-shm-usage') # /dev/shm 용량 제한 우회(특히 Docker 환경)
        self.options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36') # User-Agent 문자열 강제 지정 (크롤링 차단 우회 등)
        self.options.add_argument('--no-sandbox') # 샌드박스 비활성화 (권장X, 도커 등에서만 사용)
        self.options.add_argument('--ignore-ssl-errors') # SSL 관련 기타 에러 무시
        self.options.add_experimental_option('excludeSwitches', ['enable-logging']) # 불필요한 로그 제거

    def _start_driver(self):
        logger.info("ChromeDriver를 시작합니다...")
        try:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=self.options)
            logger.info(f"ChromeDriver 시작 완료 - Chrome 버전: {driver.capabilities['browserVersion']}")
            return driver
        except Exception as e:
            logger.error(f"ChromeDriver 시작 중 오류 발생: {e}")
            return None
    
    """
    생성된 드라이버 객체를 반환
    """
    def get_driver(self):
        return self.driver
    
    """
    WebDriver를 안전하게 종료합니다.
    """
    def _close_driver(self):
        if self.driver:
            logger.info("ChromeDriver를 종료합니다...")
            self.driver.quit()
            logger.info("ChromeDriver가 종료되었습니다.")



def _get_sector_info(driver):
    """
    네이버 금융의 '테마' 페이지를 크롤링하여,
    모든 종목의 {종목코드: {"sector": 섹터명, "name": 종목명}} 형태의
    매핑 정보를 생성하고 JSON 파일로 저장합니다.
    """
    base_url = "https://finance.naver.com"
    theme_list_url = f"{base_url}/sise/sise_group.naver?type=theme"
    stock_data_map = {}

    try:
        # 1. 페이지 로딩
        logger.info(f"전체 테마 목록 페이지를 크롤링합니다: {theme_list_url}")
        driver.get(theme_list_url)
        logger.info(f"페이지 로드 완료: {theme_list_url}")
        time.sleep(1)

        # 2. 크롤링 전체 대상 파악
        select = f"#contentarea_left > table > tbody > tr > td:nth-child(1) > a" # 해당 요소를 정확히 반복하면서 클릭하기 위해서는 ... > a 와 같이 a 속성까지 정확하게 지정해야 함
        sector_nums = len(driver.find_elements(By.CSS_SELECTOR, select))
        logger.info(f"총 {sector_nums}개의 테마를 발견했습니다.")

        if sector_nums == 0:
            logger.warning("크롤링할 테마를 찾지 못했습니다.")
            return

        sector_name = []
        for i  in range(sector_nums):
            try:
                found_elements = driver.find_elements(By.CSS_SELECTOR, select)
                link_to_click = found_elements[i]
                sector_name = link_to_click.text
                logger.info(f"[{i+1}/{sector_nums}] '{sector_name}' 테마 처리 중...")

                link_to_click.click() 
                time.sleep(1)

                #요소 하나씩 상세 페이지에서 크롤링
                detail_select = f"#contentarea > div:nth-child(5) > table > tbody > tr > td.name > div > a"
                stock_items = driver.find_elements(By.CSS_SELECTOR, detail_select)
                
                for stock_item in stock_items:
                    stock_name = stock_item.text
                    href = stock_item.get_attribute('href')

                    match = re.search(r'code=(\d{6})', href)
                    if match:
                        stock_code = match.group(1)

                        if stock_code not in stock_data_map:
                            stock_data_map[stock_code] = {
                                "name": stock_name,
                                "sectors": [sector_name]
                            }
                        else:
                            # 이미 다른 테마에서 추가된 종목이면, 현재 섹터만 추가
                            stock_data_map[stock_code]["sectors"].append(sector_name)

                driver.back()
                time.sleep(1)

            except StaleElementReferenceException:
                logger.warning("페이지 변경으로 요소를 잃었습니다. 재시도합니다.")
                driver.get(theme_list_url) # 페이지를 새로고침하여 복구
                time.sleep(1)
                continue # 현재 루프를 다시 시작

        # 그룹화된 딕셔너리를 최종 JSON 리스트 형태로 변환
        final_data = []
        for code, data in stock_data_map.items():
            final_data.append({
                "stock_code": code,
                "name": data["name"],
                "sectors": data["sectors"]
            })

        # 5. 수집된 모든 데이터를 JSON 파일로 저장
        # output_path = "/app/pipeline/sector/sector_info_test.json"
        output_path = r"D:\tool\work_space\Real-time_stock_data\docker\pipeline\sector\sector_info.json"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, ensure_ascii=False, indent=4)

        logger.info(f"총 {len(final_data)}개의 종목 정보를 '{output_path}' 파일에 저장했습니다.")
        
    except requests.exceptions.RequestException as e:
        logger.error(f"크롤링 중 오류 발생: {e}", exc_info=True)
    
def main():
    chrome_manager = ChromeDriver()
    chrome_driver = chrome_manager.get_driver()

    if chrome_driver:
        try:
            _get_sector_info(chrome_driver)
        finally:
            # 작업이 성공하든 실패하든 항상 드라이버를 종료합니다.
            chrome_manager._close_driver()
    else:
        logger.critical("WebDriver를 시작할 수 없어 프로그램을 종료합니다.")

if __name__ == "__main__":
    main()
    
