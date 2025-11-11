from bs4 import BeautifulSoup
import requests
import csv
import warnings
import time
import json
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import threading

# 경고를 무시
warnings.filterwarnings(
    "ignore", category=requests.packages.urllib3.exceptions.InsecureRequestWarning
)

# Bright Data API 설정 (Proxies 대체)
BRD_API_URL = "https://api.brightdata.com/request"
BRD_API_TOKEN = "05aef2b4090457d4596657a92e2cab7ce602375b4d5e90ea0cdf8b49781df158"
BRD_ZONE = "web_unlocker_251031"

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
                BRD_API_URL, headers=headers, data=json.dumps(payload), timeout=60
            )
            response.raise_for_status()
            return response.text
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            print(f"[경고] Bright Data 응답 지연 (시도 {attempt}/{retries}) - {e}")
            if attempt < retries:
                time.sleep(backoff * attempt)  # 지수적 대기
            else:
                raise


def find_list(page_num, url, writer, csvfile=None):
    print(f"\n[DEBUG] 페이지 {page_num} 시작 - URL: {url}")
    
    try:
        html = fetch_html_via_brightdata(url)
        print(f"[DEBUG] HTML 응답 길이: {len(html)} bytes")
    except Exception as e:
        print(f"[오류] 페이지 {page_num} HTML 가져오기 실패: {e}")
        return []
    
    soup = BeautifulSoup(html, "html.parser")
    # 신 구조: <ul id="product-list"> 내의 li.ProductUnit_productUnit__Qd6sv
    items = soup.select("#product-list .ProductUnit_productUnit__Qd6sv")
    
    print(f"[DEBUG] '.search-product' 셀렉터로 찾은 항목 수: {len(items)}")
    
    # 다른 셀렉터들도 시도해봅니다
    alternative_selectors = [
        "[class*=search-product]",
        ".baby-product",
        ".search-product-wrap",
        "li[class*=product]",
    ]
    for selector in alternative_selectors:
        alt_items = soup.select(selector)
        if alt_items:
            print(f"[DEBUG] 대체 셀렉터 '{selector}': {len(alt_items)}개 발견")

    link_list = []

    rank = 1
    for idx, item in enumerate(items):
        print(f"[DEBUG] 항목 {idx + 1}/{len(items)} 처리 중...")
        
        # 이름: 신 구조 클래스 우선, 없으면 구 구조 폴백
        name = item.select_one(".ProductUnit_productNameV2__cV9cw") or item.select_one(".name")
        # 가격: 신 구조 영역 내 텍스트에서 가격 추출, 없으면 구 구조 폴백
        price_container = item.select_one(".PriceArea_priceArea__NntJz")
        price = None
        if price_container:
            price_text = price_container.get_text(" ", strip=True)
            # 금액 패턴 추출 (숫자와 , 포함, '원' 포함 가능)
            import re
            m = re.search(r"([0-9][0-9,\.]+)\s*원?", price_text)
            if m:
                price = m.group(1) + "원"
        if not price:
            price_node = item.select_one(".price-value")
            price = None if not price_node else price_node.text
        
        if not price:
            print(f"[DEBUG] 항목 {idx + 1}: 가격 정보 없음 (리퍼 제품일 수 있음) - 건너뜀")
            continue

        try:
            a_tag = item.select_one("a")
            href = a_tag.get("href") if a_tag else None
            link = f"https://www.coupang.com{href}" if href and href.startswith("/") else (href or "")
            thumb = item.select_one(".search-product-wrap-img")

            name_text = "" if not name else name.text
            final_price = price if isinstance(price, str) else ("" if not price else price.text)
            
            if thumb and thumb.get("data-img-src"):
                img_url = f"https:{thumb.get('data-img-src')}"
            elif thumb and thumb.get("src"):
                img_url = f"https:{thumb.get('src')}"
            else:
                img_url = ""
                print(f"[DEBUG] 항목 {idx + 1}: 이미지 URL 없음")
            
            img_url = img_url.replace("230x230ex", "700x700ex")

            # CSV에 기록될 데이터 출력
            csv_data = [name_text, final_price, link, img_url]
            print(f"[CSV 데이터] Name: {name_text}, Price: {final_price}, Link: {link}, Img_url: {img_url}")
            
            writer.writerow(csv_data)
            # CSV 파일에 즉시 기록되도록 flush
            if csvfile:
                csvfile.flush()
                print(f"[DEBUG] CSV 기록 완료: {name_text[:30]}...")

            print(f"{page_num}페이지: {rank}위 {name_text} {final_price}, {link}")

            link_list.append(link)
            rank += 1
            
        except Exception as e:
            print(f"[DEBUG] 항목 {idx + 1} 처리 중 오류: {e}")
            continue

    if not link_list:
        print("\n[DEBUG] 검색 결과를 찾지 못했습니다.")
        print(f"[DEBUG] HTML 응답 길이: {len(html)} bytes")
        print(f"[DEBUG] 응답 시작 부분 (처음 1000자):")
        print("=" * 80)
        print(html[:1000])
        print("=" * 80)
        
        # HTML에 특정 키워드가 있는지 확인
        keywords = ["상품", "product", "검색", "결과", "쿠팡", "coupang"]
        found_keywords = [kw for kw in keywords if kw in html]
        print(f"[DEBUG] HTML에 포함된 키워드: {found_keywords}")
        
        # title 태그 확인
        title_tag = soup.find("title")
        if title_tag:
            print(f"[DEBUG] 페이지 제목: {title_tag.text}")
        
    else:
        print(f"[DEBUG] 페이지 {page_num}에서 {len(link_list)}개 링크 수집 완료")
    
    return link_list


def pdp(url, csv_writer, lock, csvfile=None, index=None, total=None):
    if index and total:
        print(f"\n[PDP DEBUG] ({index}/{total}) 시작 - URL: {url}")
    else:
        print(f"\n[PDP DEBUG] 시작 - URL: {url}")
    
    try:
        html = fetch_html_via_brightdata(url)
        print(f"[PDP DEBUG] 전체 HTML 응답 길이: {len(html)} bytes")
    except Exception as e:
        print(f"[오류] PDP 페이지 HTML 가져오기 실패: {e}")
        return
    
    # 전체 HTML을 파싱 후 필요한 부분만 추출 (최적화)
    full_soup = BeautifulSoup(html, "html.parser")
    prod_atf_content = full_soup.select_one(".prod-atf-contents")
    
    if prod_atf_content:
        # 필요한 부분만 사용하여 새로운 soup 생성
        optimized_html = str(prod_atf_content)
        print(f"[PDP DEBUG] 최적화된 HTML 길이: {len(optimized_html)} bytes (원본의 {len(optimized_html)/len(html)*100:.1f}%)")
        soup = BeautifulSoup(optimized_html, "html.parser")
        # 전체 soup도 유지하여 필요한 경우 폴백으로 사용
        soup._full_soup = full_soup  # 폴백용 전체 soup 저장
    else:
        # .prod-atf-contents가 없으면 전체 soup 사용
        print("[PDP DEBUG] .prod-atf-contents를 찾지 못했습니다. 전체 HTML 사용")
        soup = full_soup
    
    print("[PDP DEBUG] BeautifulSoup 파싱 완료")

    # 제목: h1.product-title span 기준
    title_node = soup.select_one("h1.product-title span") or soup.select_one(".product-title, h1")
    title = "" if not title_node else title_node.get_text(strip=True)
    print(f"[PDP DEBUG] 제목 노드: {title_node}")
    print(f"[PDP DEBUG] 제목: {title}")

    # 가격: 최종 가격 .final-price-amount
    sale_node = soup.select_one(".final-price-amount")
    sale_price_text = "" if not sale_node else sale_node.get_text(strip=True)
    print(f"[PDP DEBUG] 가격 노드: {sale_node}")
    print(f"[PDP DEBUG] 가격: {sale_price_text}")

    # 회원 할인가(없을 수 있음) -> 빈 문자열 유지
    coupon_price_text = ""
    print(f"[PDP DEBUG] 회원 할인가: {coupon_price_text}")

    # 판매자: .seller-info a 텍스트
    seller_node = soup.select_one(".seller-info a")
    seller = "" if not seller_node else seller_node.get_text(strip=True)
    print(f"[PDP DEBUG] 판매자 노드: {seller_node}")
    print(f"[PDP DEBUG] 판매자: {seller}")

    # 다른 판매자 수: 페이지 텍스트에서 '새 상품 (N)' 패턴 추출
    prod_other_seller_count = ""
    try:
        import re
        page_text = soup.get_text(" ", strip=True)
        m = re.search(r"새\s*상품\s*\((\d+)\)", page_text)
        if m:
            prod_other_seller_count = m.group(1)
        print(f"[PDP DEBUG] 다른 판매자 수: {prod_other_seller_count}")
    except Exception as e:
        print(f"[PDP DEBUG] 다른 판매자 수 추출 실패: {e}")
        prod_other_seller_count = ""

    # 옵션: .option-picker-container 내부 첫 두 span (이름:값)
    prod_option_item = ""
    option_container = soup.select_one(".option-picker-container")
    print(f"[PDP DEBUG] 옵션 컨테이너: {option_container}")
    if option_container:
        spans = option_container.select("span")
        print(f"[PDP DEBUG] 옵션 스팬 개수: {len(spans)}")
        if len(spans) >= 2:
            key = spans[0].get_text(strip=True).rstrip(":")
            val = spans[1].get_text(strip=True)
            print(f"[PDP DEBUG] 옵션 키: '{key}', 값: '{val}'")
            if key and val:
                prod_option_item = f"{key}: {val}"
    print(f"[PDP DEBUG] 최종 옵션: {prod_option_item}")

    # 상세정보: .product-description li 리스트 합치기
    # .prod-atf-contents 밖에 있을 수 있으므로 전체 soup에서도 검색
    prod_description = ""
    li_nodes = soup.select(".product-description li")
    if not li_nodes and hasattr(soup, '_full_soup'):
        # 최적화된 soup에서 찾지 못했으면 전체 soup에서 검색
        li_nodes = soup._full_soup.select(".product-description li")
        print(f"[PDP DEBUG] 상세정보 (전체 soup에서 검색): li 개수: {len(li_nodes)}")
    else:
        print(f"[PDP DEBUG] 상세정보 li 개수: {len(li_nodes)}")
    if li_nodes:
        prod_description = ", ".join([li.get_text(strip=True) for li in li_nodes])
        print(f"[PDP DEBUG] 상세정보: {prod_description[:100]}..." if len(prod_description) > 100 else f"[PDP DEBUG] 상세정보: {prod_description}")
    
    print(f"[PDP DEBUG] 최종 데이터: title={title}, price={sale_price_text}, seller={seller}, options={prod_option_item}, description_len={len(prod_description)}")

    # CSV 기록 (브랜드는 빈 문자열 유지) - 스레드 안전하게
    row_data = [
        "",
        title or "",
        sale_price_text or "",
        coupon_price_text or "",
        seller or "",
        prod_other_seller_count or "",
        prod_option_item or "",
        prod_description or "",
        url,
    ]
    print(f"[PDP DEBUG] CSV 쓰기: {len(row_data)}개 필드")
    
    # CSV에 기록될 데이터 출력
    print(f"[CSV 데이터] 브랜드: '', 제품명: {title or ''}, 현재 판매가: {sale_price_text or ''}, "
          f"회원 할인가: {coupon_price_text or ''}, 판매자: {seller or ''}, "
          f"다른 판매자: {prod_other_seller_count or ''}, 옵션: {prod_option_item or ''}, "
          f"상세정보: {prod_description[:50] if prod_description else ''}..., URL: {url}")
    
    lock.acquire()
    try:
        csv_writer.writerow(row_data)
        # CSV 파일에 즉시 기록되도록 flush
        if csvfile:
            csvfile.flush()
    finally:
        lock.release()
    if index and total:
        print(f"[PDP DEBUG] ({index}/{total}) CSV 기록 완료")
    else:
        print("[PDP DEBUG] CSV 기록 완료")


# 프로그램 시작 시간 기록
start_time = datetime.now()
print(f"프로그램 시작: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

keyword = input("Enter product: ")
page_num = 1

link_list = []

with open(
    f"coupang_discovery_{keyword}.csv", "w", newline="", encoding="utf-8"
) as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["Name", "Price", "Link", "Img_url"])
    csvfile.flush()  # 헤더도 즉시 기록
    # 1 페이지만 스크랩
    for page_num in range(1, 2):
        encoded_keyword = quote(keyword, safe="")
        searchProductListSize = 36
        url = f"https://www.coupang.com/np/search?component=&q={encoded_keyword}&page={page_num}&listSize={searchProductListSize}"
        if not page_num:
            break
        print("page_num:", page_num)
        link_list += find_list(page_num, url, writer, csvfile)

print("link_list:", link_list)

print(f"{len(link_list)}개 {keyword} 상제페이지 스크랩 시작")
print()

# 멀티스레드로 PDP 스크랩
with open(f"coupang_pdp_{keyword}.csv", "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(
        ["브랜드", "제품명", "현재 판매가", "회원 할인가", "판매자", "다른 판매자", "옵션", "상세정보", "URL"]
    )
    csvfile.flush()  # 헤더도 즉시 기록
    
    # 스레드 안전을 위한 Lock
    lock = threading.Lock()
    
    # 4개의 워커 스레드로 병렬 처리
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for e, url in enumerate(link_list, 1):
            print(f"작업 큐에 추가: {e}/{len(link_list)} - {url}")
            future = executor.submit(pdp, url, writer, lock, csvfile, e, len(link_list))
            futures.append(future)
        
        # 완료된 작업 확인
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"[오류] 작업 실행 중 예외 발생: {e}")

# 프로그램 종료 시간 및 소요 시간 계산
end_time = datetime.now()
elapsed_time = end_time - start_time
print(f"\n프로그램 종료: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"총 소요 시간: {elapsed_time}")