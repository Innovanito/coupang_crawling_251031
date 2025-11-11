import argparse
import pandas as pd
import csv
import re
import os

original_file_name = "Discovery_코트_20251111003804"
input_file = f"/Users/hakyeongkim/Desktop/Coupang_crawling/{original_file_name}.xlsx"
output_file = f"/Users/hakyeongkim/Desktop/Coupang_crawling/{original_file_name}.csv"

def excel_to_csv(input_path: str, output_path: str, sheet_name=None) -> None:
	# openpyxl 엔진 명시 (설치 필요: pip install openpyxl)
	# sheet_name이 None이면 pandas는 모든 시트를 dict로 반환하므로 첫 시트를 사용하도록 보정
	actual_sheet = 0 if sheet_name is None else sheet_name
	df = pd.read_excel(
		input_path,
		sheet_name=actual_sheet,
		engine="openpyxl",
		dtype=str,           # 모든 값을 문자열로 읽어 통일
		na_filter=False,     # NaN 대신 빈 문자열 유지
	)
	if isinstance(df, dict):
		# 사용자가 sheet_name을 None으로 넘겼거나, 엔진 동작으로 dict가 온 경우 첫 시트 선택
		df = next(iter(df.values()))

	# 컬럼명(헤더)의 줄바꿈도 제거
	import re as _re
	df.columns = [
		_re.sub(r"[\r\n\u2028\u2029]+", " ", str(c)).strip()
		for c in df.columns
	]

	# 모든 셀의 줄바꿈 및 유니코드 줄분리자 치환, 다중 공백 축약
	def _clean_cell(x: str) -> str:
		if x is None:
			return ""
		s = str(x)
		# 개행, 캐리지리턴, 유니코드 줄분리자 제거
		s = re.sub(r"[\r\n\u2028\u2029]+", " ", s)
		# 탭 -> 공백
		s = s.replace("\t", " ")
		# 연속 공백 축약
		s = re.sub(r"[ ]{2,}", " ", s)
		return s.strip()

	df = df.applymap(_clean_cell)

	# Python csv 모듈로 엄격하게 기록 (lineterminator 고정, QUOTE_ALL)
	# 출력 폴더가 없으면 생성
	out_dir = os.path.dirname(output_path) or "."
	os.makedirs(out_dir, exist_ok=True)

	with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
		writer = csv.writer(f, quoting=csv.QUOTE_ALL, lineterminator="\n")
		# 헤더
		writer.writerow([_clean_cell(col) for col in df.columns.tolist()])
		# 본문
		for _, row in df.iterrows():
			writer.writerow([_clean_cell(val) for val in row.tolist()])

	# 생성 확인 로그
	try:
		size = os.path.getsize(output_path)
		print(f"[DONE] CSV 생성: {output_path} ({size} bytes, {len(df)} rows)")
	except Exception:
		print(f"[DONE] CSV 생성: {output_path}")

def main():
	parser = argparse.ArgumentParser(description="XLSX → CSV 변환 (셀 내부 개행 제거)")
	parser.add_argument(
		"--input",
		required=False,
		default=input_file,
		help="입력 XLSX 경로",
	)
	parser.add_argument(
		"--output",
		required=False,
		default=output_file,
		help="출력 CSV 경로",
	)
	parser.add_argument(
		"--sheet",
		required=False,
		default="0",
		help="시트 이름 또는 인덱스 (기본: 0 = 첫 시트)",
	)
	args = parser.parse_args()

	# sheet이 숫자 문자열이면 int로 변환
	sheet = args.sheet
	try:
		sheet = int(sheet)
	except (TypeError, ValueError):
		# 이름 문자열로 사용
		pass

	excel_to_csv(args.input, args.output, sheet)


if __name__ == "__main__":
	main()
