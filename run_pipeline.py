#!/usr/bin/env python3
"""
Excel 파일을 CSV로 변환한 후 Coupang 크롤링을 실행하는 파이프라인 스크립트
"""
import argparse
import subprocess
import sys
import os
from pathlib import Path

BASE_DIR = "/Users/hakyeongkim/Desktop/Coupang_crawling"

# 기본 파일명 변수 (필요시 수정)
DEFAULT_FILE_NAME = "Discovery_생활가전_20251118183128"


def run_excel_to_csv(input_file_path):
    """
    excel_to_csv.py를 실행하여 Excel 파일을 CSV로 변환
    """
    print("="*60)
    print("[1단계] Excel → CSV 변환 시작")
    print("="*60)
    
    # 입력 파일 경로 확인
    if not os.path.exists(input_file_path):
        print(f"[오류] 입력 파일을 찾을 수 없습니다: {input_file_path}")
        return None
    
    # 출력 CSV 파일 경로 생성 (입력 파일명 기반)
    input_path = Path(input_file_path)
    output_csv_path = input_path.with_suffix('.csv')
    
    # excel_to_csv.py 실행
    excel_to_csv_script = os.path.join(BASE_DIR, "excel_to_csv.py")
    
    try:
        result = subprocess.run(
            [
                sys.executable,
                excel_to_csv_script,
                "--input", str(input_file_path),
                "--output", str(output_csv_path)
            ],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8"
        )
        
        # 출력 메시지 표시
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        
        # CSV 파일 생성 확인
        if os.path.exists(output_csv_path):
            print(f"[완료] CSV 파일 생성됨: {output_csv_path}")
            return str(output_csv_path)
        else:
            print(f"[오류] CSV 파일이 생성되지 않았습니다: {output_csv_path}")
            return None
            
    except subprocess.CalledProcessError as e:
        print(f"[오류] excel_to_csv.py 실행 실패: {e}")
        if e.stdout:
            print("STDOUT:", e.stdout)
        if e.stderr:
            print("STDERR:", e.stderr, file=sys.stderr)
        return None
    except Exception as e:
        print(f"[오류] 예상치 못한 오류 발생: {e}")
        return None


def run_coupang_rocket_search(csv_file_path):
    """
    coupang_rocket_search.py를 실행하여 Coupang 크롤링 수행
    """
    print("\n" + "="*60)
    print("[2단계] Coupang 크롤링 시작")
    print("="*60)
    
    # CSV 파일 경로 확인
    if not os.path.exists(csv_file_path):
        print(f"[오류] CSV 파일을 찾을 수 없습니다: {csv_file_path}")
        return False
    
    # CSV 파일명만 추출 (경로가 아닌 파일명)
    csv_filename = os.path.basename(csv_file_path)
    
    # 작업 디렉토리를 BASE_DIR로 변경하여 모듈 import 경로 문제 해결
    original_cwd = os.getcwd()
    os.chdir(BASE_DIR)
    
    try:
        # coupang_rocket_search 모듈 import
        import coupang_rocket_search as crs
        
        # DEFAULT_INPUT_CSV_FILE을 임시로 변경
        original_default = crs.DEFAULT_INPUT_CSV_FILE
        crs.DEFAULT_INPUT_CSV_FILE = csv_filename
        
        try:
            # main 함수 실행
            crs.main()
            print("\n[완료] Coupang 크롤링 완료")
            return True
        finally:
            # 원래 값으로 복원
            crs.DEFAULT_INPUT_CSV_FILE = original_default
            
    except ImportError as e:
        print(f"[오류] coupang_rocket_search 모듈을 import할 수 없습니다: {e}")
        print(f"[시도] subprocess로 실행합니다...")
        os.chdir(original_cwd)
        
        # subprocess로 실행 (대체 방법)
        rocket_search_script = os.path.join(BASE_DIR, "coupang_rocket_search.py")
        try:
            # 환경변수로 CSV 파일명 전달 시도
            env = os.environ.copy()
            env["COUPANG_INPUT_CSV"] = csv_filename
            
            result = subprocess.run(
                [sys.executable, rocket_search_script],
                check=True,
                cwd=BASE_DIR,
                env=env
            )
            print("\n[완료] Coupang 크롤링 완료")
            return True
        except subprocess.CalledProcessError as e:
            print(f"[오류] coupang_rocket_search.py 실행 실패: {e}")
            return False
    except Exception as e:
        print(f"[오류] 예상치 못한 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        os.chdir(original_cwd)


def main():
    parser = argparse.ArgumentParser(
        description="Excel 파일을 CSV로 변환한 후 Coupang 크롤링을 실행하는 파이프라인"
    )
    parser.add_argument(
        "input_file",
        nargs="?",
        default=None,
        help=f"입력 Excel 파일 경로 (.xlsx) - 지정하지 않으면 기본값 사용: {DEFAULT_FILE_NAME}.xlsx"
    )
    parser.add_argument(
        "--skip-excel",
        action="store_true",
        help="Excel → CSV 변환 단계를 건너뛰고 CSV 파일이 이미 있다고 가정"
    )
    
    args = parser.parse_args()
    
    # 입력 파일 경로 설정 (인자가 없으면 기본 파일명 사용)
    if args.input_file is None:
        input_file_path = os.path.join(BASE_DIR, f"{DEFAULT_FILE_NAME}.xlsx")
        print(f"[정보] 파일명이 지정되지 않아 기본 파일명을 사용합니다: {DEFAULT_FILE_NAME}.xlsx")
    else:
        input_file_path = args.input_file
    
    # 절대 경로로 변환
    if not os.path.isabs(input_file_path):
        input_file_path = os.path.join(BASE_DIR, input_file_path)
    
    # 1단계: Excel → CSV 변환
    csv_file_path = None
    if not args.skip_excel:
        csv_file_path = run_excel_to_csv(input_file_path)
        if not csv_file_path:
            print("\n[중단] CSV 변환 실패로 인해 파이프라인을 중단합니다.")
            sys.exit(1)
    else:
        # CSV 파일 경로 추정
        csv_file_path = Path(input_file_path).with_suffix('.csv')
        if not os.path.exists(csv_file_path):
            print(f"[오류] CSV 파일을 찾을 수 없습니다: {csv_file_path}")
            sys.exit(1)
        csv_file_path = str(csv_file_path)
    
    # 2단계: Coupang 크롤링
    success = run_coupang_rocket_search(csv_file_path)
    
    if success:
        print("\n" + "="*60)
        print("[완료] 전체 파이프라인 실행 완료")
        print("="*60)
        sys.exit(0)
    else:
        print("\n" + "="*60)
        print("[실패] Coupang 크롤링 단계에서 오류가 발생했습니다.")
        print("="*60)
        sys.exit(1)


if __name__ == "__main__":
    main()

