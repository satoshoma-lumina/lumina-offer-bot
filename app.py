import os
import json
import gspread
import pandas as pd
import google.generativeai as genai
import re
from datetime import datetime
import traceback
import pkg_resources

from flask import Flask, request, abort, jsonify
from flask_cors import CORS
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi,
    PushMessageRequest, ReplyMessageRequest, TextMessage,
    FlexMessage, FlexContainer
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent

app = Flask(__name__)
CORS(app)

# --- 定数定義 ---
SCHEDULE_LIFF_ID = "2008066763-X5mxymoj"
QUESTIONNAIRE_LIFF_ID = "2008066763-JAkGQkmw"
SATO_EMAIL = "sato@lumina-beauty.co.jp"

# --- 認証設定 ---
creds_path = '/etc/secrets/delta-wonder-471708-u1-93f8d5bbdf1c.json'

# LINE API
configuration = Configuration(access_token=os.environ.get('YOUR_CHANNEL_ACCESS_TOKEN'))
handler = WebhookHandler(os.environ.get('YOUR_CHANNEL_SECRET'))

# ( ... 中略 ... )

@app.route("/trigger-offer", methods=['POST'])
def trigger_offer():
    # ★★★★★ ここからが最終診断コード ★★★★★
    print("\n\n--- 最終診断テスト開始 ---")
    try:
        # 1. サーバーの現在時刻を確認
        server_time_utc = datetime.utcnow()
        print(f"✅ Renderサーバーの現在時刻 (UTC): {server_time_utc.strftime('%Y-%m-%d %H:%M:%S')}")

        # 2. 認証情報ファイルの中身を確認
        print(f"🔍 認証ファイルを '{creds_path}' から読み込みます...")
        with open(creds_path, 'r') as f:
            creds_content = f.read()
        
        # 3. JSONとして有効か確認
        creds_json = json.loads(creds_content)
        print("✅ ファイルは有効なJSON形式です。")
        
        # 4. 中身の一部をログに出力して破損がないか確認
        project_id = creds_json.get('project_id')
        private_key = creds_json.get('private_key', '')
        print(f"✅ project_id: {project_id}")
        print(f"✅ private_key の先頭15文字: {private_key[:15]}")
        print(f"✅ private_key の末尾15文字: {private_key[-15:]}")
        print("--- 診断項目は正常に見えます。これからスプレッドシートへの接続を試みます... ---")

    except FileNotFoundError:
        print(f"❌ 致命的エラー: 認証ファイル '{creds_path}' が見つかりません。RenderのSecret Filesの設定を確認してください。")
        return jsonify({"status": "error", "message": "Credential file not found"}), 500
    except json.JSONDecodeError:
        print("❌ 致命的エラー: 認証ファイルの中身がJSONとして破損しています。RenderのSecret Filesに貼り付けた内容を再確認してください。")
        return jsonify({"status": "error", "message": "Credential file is corrupted"}), 500
    except Exception as e:
        print(f"❌ 予期せぬ診断エラー: {e}")
        return jsonify({"status": "error", "message": f"Unexpected diagnostic error: {e}"}), 500
    # ★★★★★ ここまでが最終診断コード ★★★★★


    data = request.get_json()
    if not data: return jsonify({"status": "error", "message": "No data provided"}), 400
    user_id = data.get('userId')
    user_wishes = data.get('wishes')
    if not user_id or not user_wishes: return jsonify({"status": "error", "message": "Missing userId or wishes"}), 400

    try:
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            welcome_message = ( "ご登録いただき、誠にありがとうございます！\n" "LUMINA Offerが、あなたにプロフィールを拝見してピッタリな『好待遇サロンの公認オファー』を、このLINEアカウントを通じてご連絡いたします。\n" "楽しみにお待ちください！" )
            line_bot_api.push_message(PushMessageRequest( to=user_id, messages=[TextMessage(text=welcome_message)] ))
    except Exception as e:
        print(f"ウェルカムメッセージの送信エラー: {e}")

    if 'birthdate' in user_wishes and user_wishes['birthdate']:
        try:
            age = get_age_from_birthdate(user_wishes.get('birthdate'))
            user_wishes['age'] = f"{ (age // 10) * 10 }代"
        except (ValueError, TypeError):
            user_wishes['age'] = '' # 不正な日付形式の場合は空にする

    try:
        gc = gspread.service_account(filename=creds_path)
        user_management_sheet = gc.open("店舗マスタ_LUMINA Offer用").worksheet("ユーザー管理")

        user_headers = user_management_sheet.row_values(1)

        user_row_dict = {
            "ユーザーID": user_id, "登録日": datetime.today().strftime('%Y/%m/%d'), "ステータス": 'オファー中',
            "氏名": user_wishes.get('full_name'), "性別": user_wishes.get('gender'), "生年月日": user_wishes.get('birthdate'),
            "電話番号": user_wishes.get('phone_number'), "MBTI": user_wishes.get('mbti'), "役職": user_wishes.get('role'),
            "希望エリア": user_wishes.get('area_prefecture'), "希望勤務地": user_wishes.get('area_detail'),
            "職場満足度": user_wishes.get('satisfaction'), "興味のある待遇": user_wishes.get('perk'),
            "現在の状況": user_wishes.get('current_status'), "転職希望時期": user_wishes.get('timing'), "美容師免許": user_wishes.get('license')
        }

        profile_headers = user_headers[:16]
        profile_row_values = [user_row_dict.get(h, '') for h in profile_headers]

        cell = user_management_sheet.find(user_id, in_column=1)
        if cell:
            range_to_update = f'A{cell.row}:{chr(ord("A") + len(profile_row_values) - 1)}{cell.row}'
            user_management_sheet.update(range_to_update, [profile_row_values])
        else:
            full_row = profile_row_values + [''] * 8
            user_management_sheet.append_row(full_row)

    except Exception as e:
        print(f"ユーザー管理シートへの書き込みエラー: {e}")
        # エラーが発生しても、AI処理に進むためにprocess_and_send_offerを呼び出す
        process_and_send_offer(user_id, user_wishes)
        return jsonify({"status": "success_with_db_error", "message": "Offer task processed, but failed to write to user sheet"})

    process_and_send_offer(user_id, user_wishes)

    return jsonify({"status": "success", "message": "Offer task processed immediately"})

# ( ... 以降のコードは、前回提供した「delta-wonder-...json 専用版」と全く同じです ... )
# ( ... 省略 ... )
def process_and_send_offer(user_id, user_wishes):
    try:
        ranked_ids, matched_salon, result_or_reason = find_and_generate_offer(user_wishes)

        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)

            if matched_salon:
                offer_text = result_or_reason
                today_str = datetime.today().strftime('%Y/%m/%d')
                
                try:
                    gc = gspread.service_account(filename=creds_path)
                    offer_management_sheet = gc.open("店舗マスタ_LUMINA Offer用").worksheet("オファー管理")
                    
                    offer_headers = ['ユーザーID', '店舗ID', 'オファー送信日', 'オファー状況']
                    initial_offer_data = { "ユーザーID": user_id, "店舗ID": matched_salon.get('店舗ID'), "オファー送信日": today_str, "オファー状況": "送信済み" }
                    new_offer_row = [initial_offer_data.get(h, '') for h in offer_headers]
                    offer_management_sheet.append_row(new_offer_row, value_input_option='USER_ENTERED')
                except Exception as e:
                    print(f"オファー管理シートへの書き込み中にエラー: {e}")

                flex_container = FlexContainer.from_dict(create_salon_flex_message(matched_salon, offer_text))
                messages = [FlexMessage(alt_text=f"{matched_salon['店舗名']}からのオファー", contents=flex_container)]
                line_bot_api.push_message(PushMessageRequest(to=user_id, messages=messages))
            else:
                reason = result_or_reason
                print(f"ユーザーID {user_id} にマッチするサロンが見つからなかったため、オファーは送信されませんでした。詳細: {reason}")


    except Exception as e:
        print(f"オファー送信中のエラー: {e}")
        traceback.print_exc()

def find_and_generate_offer(user_wishes):
    # STEP 1: 最初にGemini APIの処理を完全に終わらせる
    try:
        genai.configure(
            api_key=os.environ.get('GEMINI_API_KEY'),
            transport="rest"
        )
        model = genai.GenerativeModel('gemini-1.5-flash')
    except Exception as e:
        print(f"Gemini APIの初期化エラー: {e}")
        traceback.print_exc()
        return None, None, "AIサービスの初期化に失敗しました。"

    # STEP 2: 次にgspreadを初期化してスプレッドシートを読み込む
    try:
        gc = gspread.service_account(filename=creds_path)
        salon_master_sheet = gc.open("店舗マスタ_LUMINA Offer用").worksheet("店舗マスタ")
        all_salons_data = salon_master_sheet.get_all_records()
    except gspread.exceptions.SpreadsheetNotFound:
        return None, None, "スプレッドシート「店舗マスタ_LUMINA Offer用」が見つかりません。"
    except Exception as e:
        print(f"スプレッドシート読み込みエラー: {e}")
        return None, None, "サロン情報の読み込みに失敗しました。"

    if not all_salons_data: return None, None, "サロン情報が見つかりません。"

    salons_df = pd.DataFrame(all_salons_data)

    try:
        prefecture = user_wishes.get("area_prefecture", "")
        detail_area = user_wishes.get("area_detail", "")
        full_area = f"{prefecture} {detail_area}"

        geolocator = Nominatim(user_agent="lumina_offer_geocoder")
        location = geolocator.geocode(full_area, timeout=10)

        if not location:
            print(f"ジオコーディング失敗: {full_area}")
            return None, None, "希望勤務地の位置情報を特定できませんでした。"
        user_coords = (location.latitude, location.longitude)
    except Exception as e:
        print(f"ジオコーディング中にエラーが発生: {e}")
        return None, None, "位置情報取得中にエラーが発生しました。"

    salons_df['緯度'] = pd.to_numeric(salons_df['緯度'], errors='coerce')
    salons_df['経度'] = pd.to_numeric(salons_df['経度'], errors='coerce')
    salons_df.dropna(subset=['緯度', '経度'], inplace=True)

    distances = [geodesic(user_coords, (salon['緯度'], salon['経度'])).kilometers for _, salon in salons_df.iterrows()]

    salons_df['距離'] = distances
    nearby_salons = salons_df[salons_df['距離'] <= 25].copy()
    if nearby_salons.empty: return None, None, "希望勤務地の25km以内に条件に合うサロンが見つかりませんでした。"

    user_role = user_wishes.get("role")
    user_license = user_wishes.get("license")

    salons_to_consider = nearby_salons[nearby_salons['募集状況'] == '募集中']
    if salons_to_consider.empty: return None, None, "募集中のサロンがありません。"

    def role_matcher(salon_roles):
        roles_list = [r.strip() for r in str(salon_roles).split(',')]
        return user_role in roles_list

    salons_to_consider = salons_to_consider[salons_to_consider['役職'].apply(role_matcher)]
    if salons_to_consider.empty: return None, None, "役職に合うサロンがありません。"

    if user_license == "取得済み":
        salons_to_consider = salons_to_consider[salons_to_consider['美容師免許'] == '取得']
    else:
        salons_to_consider = salons_to_consider[salons_to_consider['美容師免許'].isin(['取得', '未取得'])]
    if salons_to_consider.empty: return None, None, "免許条件に合うサロンがありません。"

    salons_json_string = salons_to_consider.to_json(orient='records', force_ascii=False)

    prompt = f"""
    あなたは、美容師向けのスカウトサービス「LUMINA Offer」の優秀なAIアシスタントです。
    # 候補者プロフィール:
    {json.dumps(user_wishes, ensure_ascii=False)}
    # 候補となる求人リスト:
    {salons_json_string}
    # あなたのタスク:
    1. **スコアリング**: 以下の基準で各求人を評価し、合計スコアが高い順に最大3件まで選んでください。
        - 候補者が「最も興味のある待遇」（プロフィール内'perk'）を、求人が提供している（求人リスト内'待遇'に文字列として含まれている）場合: +10点
        - 候補者のMBTIの性格特性が、求人の「特徴」と相性が良い場合: +5点
    2. **オファー文章生成**: スコアが最も高かった1件目のサロンについてのみ、ルールを厳守し、候補者がカジュアル面談に行きたくなるようなオファー文章を150字以内で作成してください。
        - 冒頭は必ず「LUMINA Offerから、あなたに特別なオファーが届いています。」で始めること。
        - 候補者が「最も興味のある待遇」が、なぜそのサロンで満たされるのかを説明すること。
        - 候補者のMBTIの性格特性が、どのようにそのサロンの文化や特徴と合致するのかを説明すること。
        - 最後は必ず「まずは、サロンから話を聞いてみませんか？」という一文で締めること。
        - 禁止事項: サロンが直接オファーを送っているかのような表現は避けること。
    # 回答フォーマット:
    以下のJSON形式で、厳密に回答してください。
    {{
      "ranked_store_ids": [ (ここにスコア上位の'店舗ID'を数値のリストで記述。例: [101, 108, 125]) ],
      "first_offer_message": "(ここに1件目のサロン用のオファー文章を記述)"
    }}
    """

    response = model.generate_content(prompt)

    try:
        response_text = response.text
        json_str_match = re.search(r'\{.*\}', response_text, re.DOTALL)
        if not json_str_match: raise ValueError("Response does not contain a valid JSON object.")
        json_str = json_str_match.group(0)
        gemini_response = json.loads(json_str)

        ranked_ids = gemini_response.get("ranked_store_ids")
        first_offer_message = gemini_response.get("first_offer_message")

        if not ranked_ids: return None, None, "AIによるスコアリングの結果、最適なサロンが見つかりませんでした。"

        first_match_id = ranked_ids[0]
        matched_salon_info_series = salons_to_consider[salons_to_consider['店舗ID'].astype(int) == int(first_match_id)]

        if matched_salon_info_series.empty: return None, None, "マッチしたサロン情報が見つかりませんでした。"

        matched_salon_info = matched_salon_info_series.iloc[0].to_dict()

        return ranked_ids, matched_salon_info, first_offer_message
    except Exception as e:
        print(f"Geminiからの応答解析エラー: {e}")
        print(f"Geminiからの元テキスト: {response.text}")
        return None, None, "AIからの応答解析中にエラーが発生しました。"

def create_salon_flex_message(salon, offer_text):
    db_role = salon.get("役職", "")
    if "アシスタント" in db_role:
        display_role = "アシスタント"
    else:
        display_role = "スタイリスト"

    recruitment_type = salon.get("募集", "")
    salon_id = salon.get('店舗ID')
    liff_url = f"https://liff.line.me/{SCHEDULE_LIFF_ID}?salonId={salon_id}"

    return {
        "type": "bubble", "hero": { "type": "image", "url": salon.get("画像URL", ""), "size": "full", "aspectRatio": "20:13", "aspectMode": "cover" },
        "body": { "type": "box", "layout": "vertical", "contents": [
            { "type": "text", "text": salon.get("店舗名", ""), "weight": "bold", "size": "xl" },
            { "type": "box", "layout": "vertical", "margin": "lg", "spacing": "sm", "contents": [
                { "type": "box", "layout": "baseline", "spacing": "sm", "contents": [
                    { "type": "text", "text": "勤務地", "color": "#aaaaaa", "size": "sm", "flex": 2 },
                    { "type": "text", "text": salon.get("住所", ""), "wrap": True, "color": "#666666", "size": "sm", "flex": 5 } ]},
                { "type": "box", "layout": "baseline", "spacing": "sm", "contents": [
                    { "type": "text", "text": "募集役職", "color": "#aaaaaa", "size": "sm", "flex": 2 },
                    { "type": "text", "text": display_role, "wrap": True, "color": "#666666", "size": "sm", "flex": 5 } ]},
                { "type": "box", "layout": "baseline", "spacing": "sm", "contents": [
                    { "type": "text", "text": "募集形態", "color": "#aaaaaa", "size": "sm", "flex": 2 },
                    { "type": "text", "text": recruitment_type, "wrap": True, "color": "#666666", "size": "sm", "flex": 5 } ]},
                { "type": "box", "layout": "baseline", "spacing": "sm", "contents": [
                    { "type": "text", "text": "メッセージ", "color": "#aaaaaa", "size": "sm", "flex": 2 },
                    { "type": "text", "text": offer_text, "wrap": True, "color": "#666666", "size": "sm", "flex": 5 } ]}
            ]}
        ]},
        "footer": { "type": "box", "layout": "vertical", "spacing": "sm", "contents": [
            { "type": "button", "style": "link", "height": "sm", "action": { "type": "uri", "label": "詳しく見る", "uri": "https://example.com" }},
            { "type": "button", "style": "primary", "height": "sm", "action": { "type": "uri", "label": "サロンから話を聞いてみる", "uri": liff_url }, "color": "#FF6B6B"}
        ], "flex": 0 }
    }

def get_age_from_birthdate(birthdate):
    today = datetime.today()
    birth_date = datetime.strptime(birthdate, '%Y-%m-%d')
    return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))

@app.route("/callback", methods=['POST'])
def callback():
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)
    try: handler.handle(body, signature)
    except InvalidSignatureError: abort(400)
    return 'OK'

@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    with ApiClient(configuration) as api_client:
        line_bot_api = MessagingApi(api_client)
        line_bot_api.reply_message_with_http_info(
            ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text="ご登録ありがとうございます。リッチメニューからプロフィールをご入力ください。")])
        )

@app.route("/submit-schedule", methods=['POST'])
def submit_schedule():
    data = request.get_json()
    user_id = data.get('userId')
    salon_id = data.get('salonId')

    try:
        gc = gspread.service_account(filename=creds_path)
        offer_management_sheet = gc.open("店舗マスタ_LUMINA Offer用").worksheet("オファー管理")
        
        user_cells = offer_management_sheet.findall(user_id, in_column=1)
        row_to_update = -1

        for cell in user_cells:
            record_salon_id = offer_management_sheet.cell(cell.row, 2).value
            if str(record_salon_id) == str(salon_id):
                row_to_update = cell.row
                break

        if row_to_update != -1:
            update_values = [
                '日程調整中',
                data['interviewMethod'],
                data['date1'], data['startTime1'], data['endTime1'],
                data['date2'], data['startTime2'], data['endTime2'],
                data['date3'], data['startTime3'], data['endTime3']
            ]
            offer_management_sheet.update(f'D{row_to_update}:N{row_to_update}', [update_values])

            subject = "【LUMINAオファー】面談日程の新規登録がありました"
            body = f"""
            以下の内容で、ユーザーから面談希望日時の登録がありました。
            速やかにサロンとの日程調整を開始してください。

            ■ ユーザーID: {user_id}
            ■ サロンID: {salon_id}
            ■ 希望の面談方法: {data['interviewMethod']}
            ■ 第1希望: {data['date1']} {data['startTime1']}〜{data['endTime1']}
            ■ 第2希望: {data.get('date2', '')} {data.get('startTime2', '')}〜{data.get('endTime2', '')}
            ■ 第3希望: {data.get('date3', '')} {data.get('startTime3', '')}〜{data.get('endTime3', '')}
            """
            send_notification_email(subject, body)

            next_liff_url = f"https://liff.line.me/{QUESTIONNAIRE_LIFF_ID}"
            return jsonify({ "status": "success", "message": "Schedule submitted successfully", "nextLiffUrl": next_liff_url })
        else:
            return jsonify({"status": "error", "message": "Offer not found"}), 404
    except Exception as e:
        print(f"スプレッドシート更新エラー: {e}")
        return jsonify({"status": "error", "message": "Failed to update spreadsheet"}), 500

@app.route("/submit-questionnaire", methods=['POST'])
def submit_questionnaire():
    data = request.get_json()
    user_id = data.get('userId')

    try:
        gc = gspread.service_account(filename=creds_path)
        user_management_sheet = gc.open("店舗マスタ_LUMINA Offer用").worksheet("ユーザー管理")
        
        cell = user_management_sheet.find(user_id, in_column=1)
        if cell:
            row_to_update = cell.row

            update_values = [
                data.get('q1_area'), data.get('q2_job_changes'), data.get('q3_current_employment'),
                data.get('q4_experience_years'), data.get('q5_desired_employment'),
                data.get('q6_priorities'), data.get('q7_improvement_point'),
                data.get('q8_ideal_beautician')
            ]
            user_management_sheet.update(f'Q{row_to_update}:X{row_to_update}', [update_values])

            user_name = user_management_sheet.cell(row_to_update, 4).value
            subject = f"【LUMINAオファー】{user_name}様からアンケート回答がありました"
            body = f"""
            {user_name}様（ユーザーID: {user_id}）から、面談前アンケートへの回答がありました。
            内容を確認し、面談の準備を進めてください。

            ---
            1. お住まいエリア: {data.get('q1_area')}
            2. 転職回数: {data.get('q2_job_changes')}
            3. 現雇用形態: {data.get('q3_current_employment')}
            4. 現役職経験年数: {data.get('q4_experience_years')}
            5. 希望雇用形態: {data.get('q5_desired_employment')}
            6. サロン選びの重視点: {data.get('q6_priorities')}
            7. 現職場の改善点: {data.get('q7_improvement_point')}
            8. 理想の美容師像: {data.get('q8_ideal_beautician')}
            """
            send_notification_email(subject, body)

            return jsonify({"status": "success", "message": "Questionnaire submitted successfully"})
        else:
            return jsonify({"status": "error", "message": "User not found"}), 404
    except Exception as e:
        print(f"アンケート更新エラー: {e}")
        return jsonify({"status": "error", "message": "Failed to update questionnaire"}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port)