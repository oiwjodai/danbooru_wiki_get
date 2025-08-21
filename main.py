import csv
import requests
from bs4 import BeautifulSoup
import gspread
import time
import os
import json

# --- 設定 ---
# GoogleサービスアカウントのJSONキーファイルへのパス (ワークフローで作成されるファイル名)
SERVICE_ACCOUNT_JSON_PATH = "service-account-key.json"

# Danbooruタグが記載されたCSVファイルへのパス (リポジトリのルートに置いた場合)
CSV_FILE_PATH = "danbooru.csv" 

# 書き込み先のGoogleスプレッドシート名とワークシート名
GOOGLE_SHEET_NAME = 'prompt'
WORKSHEET_NAME = 'danbooru'

# Danbooru Wikiへのリクエスト間の待機時間（秒） - サイトへの負荷軽減とIPブロック回避のため重要
REQUEST_DELAY_SECONDS = 5

# --- 処理するタグ数の設定 (ここを変更して調整してください) ---
# Noneに設定すると、未処理のすべてのタグを処理します。
# 例: 1000 に設定すると、未処理のタグの中から最初の1000個のみを処理します。
LIMIT_TAGS_TO_PROCESS = 1000 # <-- ここを調整してください (例: 100, 500, None)
# --- 設定終わり ---

# --- Googleスプレッドシート認証 ---
try:
    key_content_string = os.environ['GCP_SA_KEY_CONTENT']
    credentials_dict = json.loads(key_content_string)
    gc = gspread.service_account_from_dict(credentials_dict)
    
    spreadsheet = gc.open(GOOGLE_SHEET_NAME)
    worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
    print("✅ Googleスプレッドシートに接続しました。")
except Exception as e:
    print(f"❌ Googleスプレッドシートへの接続エラー: {e}")
    raise e

# --- 関数定義 ---

def get_danbooru_wiki_content(tag_name):
    """
    Danbooru Wikiからタグの説明（英語原文）とURLを取得します。
    """
    danbooru_tag_slug = tag_name.replace(' ', '_').replace('/', '_').lower()
    wiki_url = f"https://danbooru.donmai.us/wiki_pages/{danbooru_tag_slug}"

    try:
        response = requests.get(wiki_url, timeout=10)
        response.raise_for_status() 
        soup = BeautifulSoup(response.text, 'html.parser')

        description_div = soup.find('div', id='wiki-page-body')
        if description_div:
            description_text = description_div.get_text(separator=' ', strip=True)
            return description_text, wiki_url
        else:
            print(f"⚠️ 警告: タグ '{tag_name}' の説明が見つかりませんでした。")
            return "", wiki_url 

    except requests.exceptions.RequestException as e:
        print(f"❌ エラー: Danbooru Wikiからのデータ取得に失敗しました ({tag_name}): {e}")
        return "", wiki_url
    except Exception as e:
        print(f"❌ エラー: Danbooru Wikiの説明解析中に問題が発生しました ({tag_name}): {e}")
        return "", wiki_url

# --- メイン処理 ---
if __name__ == '__main__':
    all_tags_from_csv = [] 
    try:
        if not os.path.isabs(CSV_FILE_PATH):
            csv_path = os.path.join(os.getcwd(), CSV_FILE_PATH)
        else:
            csv_path = CSV_FILE_PATH

        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if row: 
                    all_tags_from_csv.append(row[0].strip()) 
        print(f"📄 CSVファイルから **{len(all_tags_from_csv)}** 個のタグを読み込みました。")
    except FileNotFoundError:
        print(f"❌ エラー: CSVファイル '{csv_path}' が見つかりません。")
        raise FileNotFoundError 
    except Exception as e:
        print(f"❌ エラー: CSVファイルの読み込み中に問題が発生しました: {e}")
        raise e 

    if LIMIT_TAGS_TO_PROCESS is not None:
        tags_to_process_initial_count = LIMIT_TAGS_TO_PROCESS
        print(f"✨ 処理するタグ数の上限を **{tags_to_process_initial_count}** 個に設定しました。")
    else:
        tags_to_process_initial_count = len(all_tags_from_csv) 

    results_to_sheet = [] 

    # スプレッドシートの既存データを読み込み、処理済みタグをスキップできるようにする
    print("🔍 スプレッドシートの既存データを読み込んでいます...")
    existing_data_all = worksheet.get_all_values()
    
    # ヘッダー行をスキップして、データ行のみを処理済みタグの識別に使う
    # existing_data_all が空でなければ、最初の行をスキップ
    # 空のシート、またはヘッダー行しかないシートの場合、data_rowsは空になる
    if existing_data_all and len(existing_data_all) > 0:
        data_rows = existing_data_all[1:] 
    else:
        data_rows = [] 

    # 既存データの3列目（元のタグ名）を処理済みタグとして利用
    # Pythonが書き込むのはA, B, C列なので、C列（インデックス2）を使用
    processed_tags_in_sheet = {row[2] for row in data_rows if len(row) > 2 and row[2].strip()} 
    print(f"✅ スプレッドシートには既に **{len(processed_tags_in_sheet)}** 個のタグが記録されています（ヘッダーを除く）。")

    # 未処理のタグリストを作成
    unprocessed_tags = [tag for tag in all_tags_from_csv if tag not in processed_tags_in_sheet]
    print(f"✨ 未処理のタグが **{len(unprocessed_tags)}** 個あります。")

    # 今回の実行で実際に処理するタグリスト（未処理タグからLIMIT数を取得）
    tags_to_process_this_run = unprocessed_tags[:tags_to_process_initial_count]
    print(f"✨ 今回の実行で、未処理のタグの中から **{len(tags_to_process_this_run)}** 個を処理します。")

    # --- ヘッダーが既に存在することを確認し、追加処理は行わない ---
    # Pythonスクリプトはヘッダーの自動追加を行わないため、スプレッドシートに事前にヘッダーが存在することを確認してください。
    # ヘッダーは ['danbooru_text', 'danbooru_url', 'tag', 'danbooru_translation'] としてください。
    
    # ここにヘッダーチェックロジックを再構築し、想定されるヘッダーに'danbooru_translation'を含める
    expected_headers_with_translation = ['danbooru_text', 'danbooru_url', 'tag', 'danbooru_translation']
    
    # シートが空、または最初の行が想定されるヘッダーと完全に一致しない場合のみ警告を出す
    if not existing_data_all or existing_data_all[0] != expected_headers_with_translation:
        print("⚠️ 警告: スプレッドシートのヘッダーが期待される形式と一致しないか、シートが空です。")
        print("         以下のヘッダーを手動で設定してください:")
        print(f"         {expected_headers_with_translation}")
        # 強制終了したい場合は以下の行のコメントを外す
        # raise ValueError("ヘッダーが正しく設定されていません。")


    for i, tag in enumerate(tags_to_process_this_run):
        print(f"\n--- 処理中: **{i+1}/{len(tags_to_process_this_run)}** - タグ: '**{tag}**' ---")

        if tag in processed_tags_in_sheet:
            print(f"➡️ このタグはフィルタリング済みでしたが、念のため再度スキップします: '{tag}'")
            continue

        # Danbooru Wikiから説明とURLを取得
        description, wiki_url = get_danbooru_wiki_content(tag)
        time.sleep(REQUEST_DELAY_SECONDS) 

        # スプレッドシートに格納するデータ: [danbooru_text, danbooru_url, tag]
        # D列のdanbooru_translationは空のままにするため、リストには含めない
        results_to_sheet.append([description, wiki_url, tag])

        if (i + 1) % 50 == 0:
            try:
                # 既存の最終行の下に追加 (ヘッダーは1行目にあるため、2行目以降に追加される)
                # 書き込むのは3列なので、D列は影響を受けない
                worksheet.append_rows(results_to_sheet, value_input_option='RAW')
                print(f"🚀 **{len(results_to_sheet)}** 件のデータをスプレッドシートに書き込みました。")
                for row in results_to_sheet:
                    processed_tags_in_sheet.add(row[2]) 
                results_to_sheet = [] 
            except Exception as e:
                print(f"❌ スプレッドシートへの書き込みエラー: {e}")
                raise e 

    if results_to_sheet:
        try:
            worksheet.append_rows(results_to_sheet, value_input_option='RAW')
            print(f"🚀 残り **{len(results_to_sheet)}** 件のデータをスプレッドシートに書き込みました。")
        except Exception as e:
            print(f"❌ 最終スプレッドシートへの書き込みエラー: {e}")
            raise e 

    print("\n🎉 すべてのタグの処理が完了しました。")
    print("Googleスプレッドシートを確認してください。")
