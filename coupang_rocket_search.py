from bs4 import BeautifulSoup
import requests
import csv
import warnings
import time
import json
from urllib.parse import quote
from datetime import datetime
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# 경고 무시 (urllib3 InsecureRequestWarning 등)
warnings.filterwarnings(
    "ignore", category=requests.packages.urllib3.exceptions.InsecureRequestWarning
)

# 전역 설정
BASE_DIR = "/Users/hakyeongkim/Desktop/Coupang_crawling"
DEFAULT_INPUT_CSV_FILE = "Discovery_헤어액세서리_20251112214121.csv"

# CSV 헤더 정의
RESULTS_CSV_HEADER = [
    "키워드", "순위", "상품명", "원가", "최종가격",
    "로켓배지", "도착일", "무료배송", "리뷰수", "포인트",
    "재고현황", "링크", "이미지URL"
]

SUMMARY_CSV_HEADER = [
    "키워드", "평균최종가격", "로켓배지개수", "평균리뷰수", "상품개수"
]

# Bright Data API 설정 (main.py 스타일 재사용)
BRD_API_URL = "https://api.brightdata.com/request"
# 환경변수 우선, 없으면 기존 값 사용
BRD_API_TOKEN = os.getenv(
    "BRD_API_TOKEN",
    "05aef2b4090457d4596657a92e2cab7ce602375b4d5e90ea0cdf8b49781df158",
)
BRD_ZONE = os.getenv("BRD_ZONE", "web_unlocker_251031")


def fetch_html_via_brightdata(target_url, retries=3, backoff=5):
    encoded_url = requests.utils.requote_uri(target_url)
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {BRD_API_TOKEN}",
    }
    payload = {
        "zone": BRD_ZONE,
        "url": encoded_url,
        "method": "GET",
        "format": "raw",
        "country": "kr",
        "headers": {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/128.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://www.coupang.com/",
            "Upgrade-Insecure-Requests": "1",
        },
    }

    for attempt in range(1, retries + 1):
        try:
            response = requests.post(
                BRD_API_URL, headers=headers, data=json.dumps(payload), timeout=30
            )
            response.raise_for_status()
            return response.text
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            print(f"[경고] Bright Data 지연 (시도 {attempt}/{retries}) - {e}")
            if attempt < retries:
                time.sleep(backoff * attempt)
            else:
                raise


def parse_search_results(html):
    """
    Coupang 검색결과에서 최대 36개 항목을 확장 파싱
    반환: [rank, name, original_price, final_price, rocket_badge, arrival, free_shipping, review_count, points, stock_status, link, img_url]
    """
    soup = BeautifulSoup(html, "html.parser")

    # 신 구조 우선
    items = soup.select("#product-list .ProductUnit_productUnit__Qd6sv")
    if not items:
        # 폴백 선택자
        items = soup.select("[class*=search-product], .baby-product, .search-product-wrap, li[class*=product]")

    results = []
    for idx, item in enumerate(items, start=1):
        # 이름
        name_node = item.select_one(".ProductUnit_productNameV2__cV9cw") or item.select_one(".name")

        # 가격 영역
        import re
        price_container = item.select_one(".PriceArea_priceArea__NntJz")
        original_price = ""
        final_price = ""
        if price_container:
            # 원가 <del>
            del_node = price_container.select_one("del")
            if del_node:
                m = re.search(r"([0-9][0-9,\.]+)\s*원?", del_node.get_text(" ", strip=True))
                if m:
                    original_price = m.group(1) + "원"
            # 최종가: 컨테이너 전체 텍스트에서 금액 패턴들 추출 후 마지막 항목(보통 최종가)을 사용
            all_txt = price_container.get_text(" ", strip=True)
            amounts = re.findall(r"([0-9][0-9,\.]+)\s*원", all_txt)
            if amounts:
                final_price = amounts[-1] + "원"
            # 구 구조 폴백
            if not final_price:
                price_node = price_container.select_one(".price-value")
                if price_node:
                    m = re.search(r"([0-9][0-9,\.]+)\s*원?", price_node.get_text(" ", strip=True))
                    if m:
                        final_price = m.group(1) + "원"
        else:
            # 구 구조 폴백
            price_node = item.select_one(".price-value")
            if price_node:
                txt = price_node.get_text(" ", strip=True)
                m = re.search(r"([0-9][0-9,\.]+)\s*원?", txt)
                if m:
                    final_price = m.group(1) + "원"
        if not final_price and not original_price:
            # 가격 전혀 없으면 스킵
            continue

        # 링크
        a_tag = item.select_one("a")
        href = a_tag.get("href") if a_tag else None
        link = f"https://www.coupang.com{href}" if href and href.startswith("/") else (href or "")

        # 썸네일
        thumb = item.select_one(".search-product-wrap-img")
        if thumb and thumb.get("data-img-src"):
            img_url = f"https:{thumb.get('data-img-src')}"
        elif thumb and thumb.get("src"):
            img_url = f"https:{thumb.get('src')}"
        else:
            img_url = ""
        img_url = img_url.replace("230x230ex", "700x700ex")

        # 랭킹
        rank = ""
        rank_node = item.select_one("[class*=RankMark_rank]")
        if rank_node:
            r = re.search(r"\d+", rank_node.get_text(" ", strip=True))
            if r:
                rank = r.group(0)
        if not rank:
            # 링크의 rank 파라미터 폴백
            if href:
                r = re.search(r"[?&]rank=(\d+)", href)
                if r:
                    rank = r.group(1)

        # 로켓 배지 감지 (기존 로직 + 새 로직 병행)
        rocket_badge = ""
        name_text = "" if not name_node else name_node.get_text(strip=True)
        
        # 모든 img 태그 수집
        all_imgs = item.select("img")
        debug_srcs = []
        for img in all_imgs:
            src_val = img.get("src") or img.get("data-src") or ""
            if src_val:
                debug_srcs.append(src_val)
        
        # ===== 기존 로직 (단순 img 태그 검색) =====
        print(f"[DEBUG 배지] 기존 로직 시작 - 상품: {name_text[:30]}... (img 개수: {len(all_imgs)})")
        for img in all_imgs:
            src = (img.get("src") or "") + (img.get("data-src") or "")
            if not src:
                continue
            # 기존 로직: 단순 키워드 매칭 (순서: 판매자로켓 > 로켓프레시 > 로켓설치 > 로켓직구 > 로켓배송)
            if "logoRocketMerchant" in src or "badge_199559e56f7" in src:
                rocket_badge = "판매자로켓"
                print(f"[DEBUG 배지] 기존 로직 → 판매자로켓 감지 (src: {src[:100]})")
                break  # 판매자로켓은 우선 처리
            if "rocket-fresh" in src:
                rocket_badge = "로켓프레시"
                print(f"[DEBUG 배지] 기존 로직 → 로켓프레시 감지 (src: {src[:100]})")
            if "rocket_install" in src:
                rocket_badge = "로켓설치"
                print(f"[DEBUG 배지] 기존 로직 → 로켓설치 감지 (src: {src[:100]})")
            if "logo_jikgu" in src:
                rocket_badge = "로켓직구"
                print(f"[DEBUG 배지] 기존 로직 → 로켓직구 감지 (src: {src[:100]})")
            # 로켓배송은 마지막에 체크 (다른 배지가 없을 때만, badge_199559e56f7 제외)
            if not rocket_badge and ("logo_rocket" in src or ("delivery_badge_ext" in src and "badge_199559e56f7" not in src) or ("badge_" in src and "badge_199559e56f7" not in src)):
                rocket_badge = "로켓배송"
                print(f"[DEBUG 배지] 기존 로직 → 로켓배송 감지 (src: {src[:100]})")
                break
        
        # ===== 새 로직 (ImageBadge 컨테이너 기반) =====
        if not rocket_badge:
            print(f"[DEBUG 배지] 새 로직 시작 - 상품: {name_text[:30]}...")
            # 우선 ImageBadge 영역에서 찾기
            badge_container = item.select_one(".ImageBadge_default__JWaYp")
            if badge_container:
                badge_img = badge_container.select_one("img")
                if badge_img:
                    src = (badge_img.get("src") or "") + (badge_img.get("data-src") or "")
                    print(f"[DEBUG 배지] ImageBadge 발견 - 상품: {name_text[:30]}... src: {src[:100]}")
                    # 순서: 판매자로켓 > 로켓프레시 > 로켓설치 > 로켓직구 > 로켓배송
                    if "logoRocketMerchant" in src or "RocketMerchant" in src or "badge_199559e56f7" in src:
                        rocket_badge = "판매자로켓"
                        print(f"[DEBUG 배지] 새 로직 → 판매자로켓 감지")
                    elif "rocket-fresh" in src or "rocket_fresh" in src:
                        rocket_badge = "로켓프레시"
                        print(f"[DEBUG 배지] 새 로직 → 로켓프레시 감지")
                    elif "rocket_install" in src or "rocket-install" in src:
                        rocket_badge = "로켓설치"
                        print(f"[DEBUG 배지] 새 로직 → 로켓설치 감지")
                    elif "logo_jikgu" in src or "jikgu" in src:
                        rocket_badge = "로켓직구"
                        print(f"[DEBUG 배지] 새 로직 → 로켓직구 감지")
                    elif "logo_rocket" in src or "logo_rocket_large" in src or ("delivery_badge_ext" in src and "badge_199559e56f7" not in src) or ("badge_" in src and "badge_199559e56f7" not in src):
                        rocket_badge = "로켓배송"
                        print(f"[DEBUG 배지] 새 로직 → 로켓배송 감지")
                    else:
                        print(f"[DEBUG 배지] 새 로직 → 배지 타입 미확인 (src: {src[:100]})")
            else:
                print(f"[DEBUG 배지] ImageBadge 컨테이너 없음 - 상품: {name_text[:30]}...")
            
            # ImageBadge에서 못 찾으면 전체 img 태그에서 검색 (폴백)
            if not rocket_badge:
                print(f"[DEBUG 배지] 새 로직 폴백 검색 시작 - 상품: {name_text[:30]}... (img 개수: {len(all_imgs)})")
                for img in all_imgs:
                    src = (img.get("src") or "") + (img.get("data-src") or "")
                    if not src:
                        continue
                    # 우선순위: 판매자로켓 > 로켓프레시 > 로켓설치 > 로켓직구 > 로켓배송
                    if "logoRocketMerchant" in src or "RocketMerchant" in src or "badge_199559e56f7" in src:
                        rocket_badge = "판매자로켓"
                        print(f"[DEBUG 배지] 새 로직 폴백 → 판매자로켓 감지 (src: {src[:100]})")
                        break  # 판매자로켓은 우선 처리
                    elif ("rocket-fresh" in src or "rocket_fresh" in src) and not rocket_badge:
                        rocket_badge = "로켓프레시"
                        print(f"[DEBUG 배지] 새 로직 폴백 → 로켓프레시 감지 (src: {src[:100]})")
                    elif ("rocket_install" in src or "rocket-install" in src) and not rocket_badge:
                        rocket_badge = "로켓설치"
                        print(f"[DEBUG 배지] 새 로직 폴백 → 로켓설치 감지 (src: {src[:100]})")
                    elif ("logo_jikgu" in src or "jikgu" in src) and not rocket_badge:
                        rocket_badge = "로켓직구"
                        print(f"[DEBUG 배지] 새 로직 폴백 → 로켓직구 감지 (src: {src[:100]})")
                    # 로켓배송은 마지막에 체크 (다른 배지가 없을 때만, badge_199559e56f7 제외)
                    elif not rocket_badge and ("logo_rocket" in src or "logo_rocket_large" in src or ("delivery_badge_ext" in src and "badge_199559e56f7" not in src) or ("badge_" in src and "badge_199559e56f7" not in src)):
                        rocket_badge = "로켓배송"
                        print(f"[DEBUG 배지] 새 로직 폴백 → 로켓배송 감지 (src: {src[:100]})")
                        break
        
        if not rocket_badge:
            print(f"[DEBUG 배지] 배지 없음 - 상품: {name_text[:30]}... (전체 img src: {debug_srcs[:3]})")
        else:
            print(f"[DEBUG 배지] 최종 배지: {rocket_badge} - 상품: {name_text[:30]}...")

        # 도착일/도착보장
        arrival = ""
        # .fw-leading-[15px] 클래스를 가진 요소에서 도착 정보 추출 (CSS 특수문자 포함 → regex 사용)
        arrival_containers = item.find_all(
            lambda tag: tag.has_attr("class")
            and any(re.search(r"fw-leading-\[15px\]", cls) for cls in tag["class"])
        )
        for container in arrival_containers:
            container_text = container.get_text(" ", strip=True)
            # "도착 예정" 또는 "도착 보장"이 포함된 경우
            if "도착 예정" in container_text or "도착 보장" in container_text:
                arrival = container_text
                break
        
        # 위 방법으로 못 찾으면 기존 방법 시도
        if not arrival:
            arrival_node = item.find(string=re.compile(r"(도착 예정|도착 보장)"))
            if arrival_node:
                # 부모 요소에서 전체 텍스트 추출 (날짜 포함)
                parent = arrival_node.parent
                if parent:
                    arrival = parent.get_text(" ", strip=True)
                else:
                    arrival = arrival_node.strip()
        
        # 여전히 못 찾으면 전체 텍스트에서 패턴 검색
        if not arrival:
            txt = item.get_text(" ", strip=True)
            # 다양한 패턴: "12/1 도착 예정", "모레(금) 도착 예정", "내일(목) 도착 보장" 등
            m = re.search(r"([0-9]{1,2}/[0-9]{1,2}|모레\([^)]+\)|내일\([^)]+\)|[가-힣]+\([^)]+\))\s*도착\s*(예정|보장)", txt)
            if m:
                arrival = m.group(0)
            else:
                # 간단한 패턴
                m = re.search(r"(도착\s*예정|도착\s*보장)", txt)
                if m:
                    arrival = m.group(0)

        # 무료배송
        free_shipping = "무료배송" if item.find(string=re.compile(r"무료배송")) else ""

        # 리뷰수
        review_count = ""
        rc_node = item.select_one(".ProductRating_ratingCount__R0Vhz")
        if rc_node:
            # HTML 주석이 포함된 경우를 대비해 여러 방법 시도
            # 방법 1: get_text()로 추출
            rc_text = rc_node.get_text(" ", strip=True)
            m = re.search(r"\((\d+)\)", rc_text)
            if m:
                review_count = m.group(1)
            else:
                # 방법 2: HTML 문자열에서 직접 추출 (주석 포함)
                rc_html = str(rc_node)
                m = re.search(r"\([^)]*?(\d+)[^)]*?\)", rc_html)
                if m:
                    review_count = m.group(1)
                else:
                    # 방법 3: 모든 숫자 찾기
                    all_numbers = re.findall(r"\d+", rc_text)
                    if all_numbers:
                        review_count = all_numbers[0]

        # 포인트 적립
        points = ""
        points_node = item.select_one(".BenefitBadge_cash-benefit__SmkrN")
        if points_node:
            pts_txt = points_node.get_text(" ", strip=True)
            m = re.search(r"([0-9][0-9,\.]+)\s*원\s*적립", pts_txt)
            if m:
                points = m.group(1) + "원"
            else:
                points = pts_txt

        # 재고/품절 현황
        stock_status = ""
        # 예: '품절임박', '일시품절', '일부 옵션 품절' 등 텍스트 탐색
        status_text = item.get_text(" ", strip=True)
        if "품절임박" in status_text:
            stock_status = "품절임박"
        elif "품절" in status_text:
            stock_status = "품절"

        name_text = "" if not name_node else name_node.get_text(strip=True)
        results.append([
            rank or "",
            name_text,
            original_price,
            final_price,
            rocket_badge,
            arrival,
            "Y" if free_shipping else "N",
            review_count,
            points,
            stock_status,
            link,
            img_url,
        ])

        if len(results) >= 36:
            break

    return results


def search_coupang_for_keyword(keyword):
    encoded_keyword = quote(keyword, safe="")
    searchProductListSize = 36
    url = f"https://www.coupang.com/np/search?component=&q={encoded_keyword}&page=1&listSize={searchProductListSize}"
    try:
        html = fetch_html_via_brightdata(url)
        return parse_search_results(html)
    except Exception as e:
        # Bright Data 실패 시 특별한 예외 발생
        raise Exception(f"Bright Data API 실패: {str(e)}")


def detect_columns(header_row):
    """
    header_row에서 '브랜드'가 포함된 컬럼과 '키워드' 컬럼을 추정
    """
    brand_idx = None
    keyword_idx = None

    for i, h in enumerate(header_row):
        h_norm = (h or "").replace("\n", "").replace("\r", "").strip()
        if brand_idx is None and "브랜드" in h_norm:
            brand_idx = i
        # '키워드'가 정확히 일치하면 우선
        if h_norm == "키워드":
            keyword_idx = i
        elif keyword_idx is None and "키워드" in h_norm:
            keyword_idx = i

    return brand_idx, keyword_idx


def load_keywords_from_csv(input_csv_path, limit=None):  # limit=5 주석처리
    keywords = []
    with open(input_csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return []

        brand_idx, keyword_idx = detect_columns(header)
        if brand_idx is None or keyword_idx is None:
            raise ValueError("CSV 헤더에서 '브랜드' 또는 '키워드' 컬럼을 찾지 못했습니다.")

        for row in reader:
            # 멀티라인 셀을 csv가 알아서 처리하므로 그대로 사용
            if not row:
                continue
            brand_val = (row[brand_idx] if len(row) > brand_idx else "").strip()
            keyword_val = (row[keyword_idx] if len(row) > keyword_idx else "").strip()

            # 브랜드 값이 'X'인 항목만
            if brand_val == "X" and keyword_val:
                keywords.append(keyword_val)
                # limit이 None이 아니고 지정된 개수에 도달하면 중단
                if limit is not None and len(keywords) >= limit:
                    break
    return keywords


def process_keyword(kw, writer, sum_writer, csvfile, sumfile, lock):
    """
    단일 키워드를 처리하고 CSV에 기록하는 함수 (스레드 안전)
    """
    print(f"\n[키워드] {kw} - 검색 시작")
    # 요청 간 딜레이 1.5초
    time.sleep(1.5)
    try:
        results = search_coupang_for_keyword(kw)
    except Exception as e:
        print(f"[오류] 검색 실패: {kw} - {e}")
        # 에러 정보를 CSV에 기록 (스레드 안전)
        error_row = [
            "",  # Rank
            "데이터를 못 받아 왔습니다",  # Name
            "",  # OriginalPrice
            "",  # FinalPrice
            "",  # RocketBadge
            "",  # Arrival
            "",  # FreeShipping
            "",  # ReviewCount
            "",  # Points
            "",  # StockStatus
            "",  # Link
            ""   # ImgUrl
        ]
        lock.acquire()
        try:
            writer.writerow([kw] + error_row)
            csvfile.flush()
            # 요약 CSV에도 에러 정보 기록
            sum_writer.writerow([kw, 0, 0, "0.00", 0])
            sumfile.flush()
        finally:
            lock.release()
        print(f"[키워드] {kw} - 에러 정보 기록 완료")
        return
    
    # 집계: 평균 최종가, 로켓 배지 개수, 평균 리뷰수
    def _to_int(num_str: str) -> int:
        try:
            import re as _re
            digits = _re.sub(r"[^\d]", "", num_str or "")
            return int(digits) if digits else 0
        except Exception:
            return 0
    
    num_items = len(results)
    final_prices = [_to_int(r[3]) for r in results]  # row: [Rank, Name, OriginalPrice, FinalPrice, RocketBadge, ...]
    avg_final_price = int(sum(final_prices) / num_items) if num_items else 0
    rocket_badge_count = sum(1 for r in results if ("로켓" in (r[4] or "")))
    review_counts = [_to_int(r[7]) for r in results]
    avg_review_count = float(sum(review_counts) / num_items) if num_items else 0.0
    
    # CSV에 기록 (스레드 안전)
    lock.acquire()
    try:
        for row in results:
            writer.writerow([kw] + row)
        csvfile.flush()
        sum_writer.writerow([kw, avg_final_price, rocket_badge_count, f"{avg_review_count:.2f}", num_items])
        sumfile.flush()
    finally:
        lock.release()
    
    print(f"[키워드] {kw} - {len(results)}건 기록 완료")


def get_file_paths(input_csv_file):
    """
    입력 CSV 파일명으로부터 관련 파일 경로들을 반환
    """
    input_csv = f'{BASE_DIR}/{input_csv_file}'
    base_name = input_csv_file.split('.')[0]
    output_csv = f"{BASE_DIR}/{base_name}_rocket_results.csv"
    summary_csv = f"{BASE_DIR}/{base_name}_rocket_result_summary.csv"
    return input_csv, output_csv, summary_csv


def main():
    input_csv_file = DEFAULT_INPUT_CSV_FILE
    input_csv, output_csv, summary_csv = get_file_paths(input_csv_file)

    start_time = datetime.now()
    print(f"시작: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # 1) 키워드 로드 (브랜드 == 'X', 최대 5개)
    keywords = load_keywords_from_csv(input_csv)  # limit=5 주석처리
    print('keywords:', keywords)
    print(f"키워드({len(keywords)}): {keywords}")

    # 2) 각 키워드별 검색 36개 수집 후 CSV 저장
    with open(output_csv, "w", encoding="utf-8", newline="") as csvfile, \
         open(summary_csv, "w", encoding="utf-8", newline="") as sumfile:
        writer = csv.writer(csvfile)
        sum_writer = csv.writer(sumfile)
        writer.writerow(RESULTS_CSV_HEADER)
        sum_writer.writerow(SUMMARY_CSV_HEADER)

        # 스레드 안전을 위한 Lock
        lock = threading.Lock()
        
        # 4개의 워커 스레드로 병렬 처리
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for kw in keywords:
                future = executor.submit(process_keyword, kw, writer, sum_writer, csvfile, sumfile, lock)
                futures.append(future)
            
            # 완료된 작업 확인
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"[오류] 작업 실행 중 예외 발생: {e}")

    end_time = datetime.now()
    print(f"\n종료: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"총 소요: {end_time - start_time}")


if __name__ == "__main__":
    main()
