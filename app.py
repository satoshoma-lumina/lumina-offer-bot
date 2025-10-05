import os
import json
import gspread
import pandas as pd
import re
from datetime import datetime, timedelta, timezone
import traceback
import requests

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
from linebot.v3.webhooks import MessageEvent, TextMessageContent, FollowEvent

app = Flask(__name__)
CORS(app)

# --- 定数定義 ---
SCHEDULE_LIFF_ID = "2008066763-X5mxymoj"
QUESTIONNAIRE_LIFF_ID = "2008066763-JAkGQkmw"
LINE_CONTACT_LIFF_ID = "2008066763-Rv0z80wl"
SATO_EMAIL = "sato@lumina-beauty.co.jp"

# --- 認証設定 ---
creds_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', '/etc/secrets/delta-wonder-471708-u1-93f8d5bbdf1c.json')

# LINE API
configuration = Configuration(access_token=os.environ.get('YOUR_CHANNEL_ACCESS_TOKEN'))
handler = WebhookHandler(os.environ.get('YOUR_CHANNEL_SECRET'))

# タイムゾーンの定義
JST = timezone(timedelta(hours=+9))

def send_notification_email(subject, body):
    from_email = os.environ.get('MAIL_USERNAME')
    api_key = os.environ.get('SENDGRID_API_KEY')
    if not from_email or not api_key:
        print("メール送信用の環境変数が設定されていません。")
        return
    message = Mail(from_email=from_email, to_emails=SATO_EMAIL, subject=subject, html_content=body.replace('\n', '<br>'))
    try:
        sg = SendGridAPIClient(api_key)
        response = sg.send(message)
        print(f"メール送信成功: Status Code {response.status_code}")
    except Exception as e:
        print(f"メール送信エラー: {e}")

def generate_single_offer_message(user_wishes, salon_info):
    prompt_text = f"""
    あなたは、美容師向けのスカウトサービス「LUMINA Offer」の優秀なAIアシスタントです。
    # 候補者プロフィール:
    {json.dumps(user_wishes, ensure_ascii=False)}
    # オファーを送るサロン情報:
    {json.dumps(salon_info, ensure_ascii=False, indent=2)}
    # あなたのタスク:
    提示されたサロン情報と候補者プロフィールを基に、ルールを厳守し、候補者がカジュアル面談に行きたくなるようなオファー文章を150字以内で作成してください。
    - 冒頭は必ず「LUMINA Offerから、あなたに特別なオファーが届いています。」で始めること。
    - 候補者が「最も興味のある待遇」が、なぜそのサロンで満たされるのかを説明すること。
    - 候補者のMBTIの性格特性が、どのようにそのサロンの文化や特徴と合致するのかを説明すること。
    - 最後は必ず「まずは、サロンから話を聞いてみませんか？」という一文で締めること。
    - 禁止事項: サロンが直接オファーを送っているかのような表現は避けること。
    # 回答フォーマット:
    オファー文章のテキストのみを回答してください。JSON形式は不要です。
    """
    try:
        api_key = os.environ.get('GEMINI_API_KEY')
        if not api_key: return "AIサービスのAPIキーが設定されていません。"
        model_name = "gemini-2.5-flash"
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}"
        headers = {"Content-Type": "application/json"}
        data = { "contents": [{ "parts": [{"text": prompt_text}] }] }
        response = requests.post(url, headers=headers, json=data, timeout=60)
        response.raise_for_status()
        response_json = response.json()
        return response_json['candidates'][0]['content']['parts'][0]['text'].strip()
    except Exception as e:
        print(f"単一オファーメッセージの生成中にエラー: {e}")
        return "サロンの特徴を基に、あなたにぴったりのオファーをご用意しました。ぜひ詳細をご覧ください。"

def find_and_select_top_salons(user_wishes):
    try:
        gc = gspread.service_account(filename=creds_path)
        salon_master_sheet = gc.open("店舗マスタ_LUMINA Offer用").worksheet("店舗マスタ")
        all_salons_data = salon_master_sheet.get_all_records()
        offer_management_sheet = gc.open("店舗マスタ_LUMINA Offer用").worksheet("オファー管理")
        offer_history = offer_management_sheet.get_all_records()
    except Exception as e:
        print(f"スプレッドシート読み込みエラー: {e}")
        return [], "スプレッドシート読み込みエラー"

    if not all_salons_data: return [], "サロン情報が見つかりません。"
    salons_df = pd.DataFrame(all_salons_data)

    try:
        prefecture = user_wishes.get("area_prefecture", "")
        detail_area = user_wishes.get("area_detail", "")
        words_to_remove = ["周辺", "中心部", "あたり"]
        cleaned_detail_area = detail_area
        for word in words_to_remove:
            cleaned_detail_area = cleaned_detail_area.replace(word, "")
        full_area = f"{prefecture} {cleaned_detail_area.strip()}"
        
        geolocator = Nominatim(user_agent="lumina_offer_geocoder")
        location = geolocator.geocode(full_area, timeout=10)
        if not location:
            print(f"ジオコーディング失敗: {full_area}")
            return [], "希望勤務地の位置情報を特定できませんでした。"
        user_coords = (location.latitude, location.longitude)
    except Exception as e:
        print(f"ジオコーディング中にエラーが発生: {e}")
        return [], "位置情報取得中にエラーが発生しました。"

    salons_df['緯度'] = pd.to_numeric(salons_df['緯度'], errors='coerce')
    salons_df['経度'] = pd.to_numeric(salons_df['経度'], errors='coerce')
    salons_df.dropna(subset=['緯度', '経度'], inplace=True)

    distances = [geodesic(user_coords, (salon['緯度'], salon['経度'])).kilometers for _, salon in salons_df.iterrows()]
    salons_df['距離'] = distances
    
    conditionally_matched_salons = salons_df[salons_df['距離'] <= 25].copy()
    user_role = user_wishes.get("role")
    user_license = user_wishes.get("license")
    conditionally_matched_salons = conditionally_matched_salons[conditionally_matched_salons['募集状況'] == '募集中']
    if conditionally_matched_salons.empty: return [], "募集中のサロンがありません。"

    def role_matcher(salon_roles):
        roles_list = [r.strip() for r in str(salon_roles).split(',')]
        return user_role in roles_list
    conditionally_matched_salons = conditionally_matched_salons[conditionally_matched_salons['役職'].apply(role_matcher)]
    if conditionally_matched_salons.empty: return [], "役職に合うサロンがありません。"

    if user_license == "取得済み":
        conditionally_matched_salons = conditionally_matched_salons[conditionally_matched_salons['美容師免許'] == '取得']
    else:
        conditionally_matched_salons = conditionally_matched_salons[conditionally_matched_salons['美容師免許'].isin(['取得', '未取得'])]
    if conditionally_matched_salons.empty: return [], "免許条件に合うサロンがありません。"
    
    user_gender = user_wishes.get("gender")
    if user_gender:
        conditionally_matched_salons = conditionally_matched_salons[
            (conditionally_matched_salons['ターゲット性別'].isnull()) |
            (conditionally_matched_salons['ターゲット性別'] == '') |
            (conditionally_matched_salons['ターゲット性別'] == '指定なし') |
            (conditionally_matched_salons['ターゲット性別'] == user_gender)
        ]
    if conditionally_matched_salons.empty: return [], "性別の条件に合うサロンがありません。"

    user_age_group = user_wishes.get("age")
    if user_age_group:
        conditionally_matched_salons = conditionally_matched_salons[
            (conditionally_matched_salons['ターゲット年齢'].isnull()) |
            (conditionally_matched_salons['ターゲット年齢'] == '') |
            (conditionally_matched_salons['ターゲット年齢'] == '指定なし') |
            (conditionally_matched_salons['ターゲット年齢'].str.contains(user_age_group, na=False))
        ]
    if conditionally_matched_salons.empty: return [], "年齢の条件に合うサロンがありません。"
    
    already_sent_salon_ids = [ record['店舗ID'] for record in offer_history if record['ユーザーID'] == user_wishes.get('userId') ]
    if already_sent_salon_ids:
        conditionally_matched_salons = conditionally_matched_salons[ ~conditionally_matched_salons['店舗ID'].isin(already_sent_salon_ids) ]
        if conditionally_matched_salons.empty:
            return [], "条件に合うサロンはありますが、すべて過去にオファー済みです。"

    top_salons = []
    
    within_5km_salons = conditionally_matched_salons[conditionally_matched_salons['距離'] <= 5]
    if not within_5km_salons.empty:
        closest_salon = within_5km_salons.sort_values(by='距離').iloc[0]
        top_salons.append(closest_salon.to_dict())
        conditionally_matched_salons = conditionally_matched_salons[conditionally_matched_salons['店舗ID'] != closest_salon['店舗ID']]

    remaining_slots = 5 - len(top_salons)
    if remaining_slots > 0 and not conditionally_matched_salons.empty:
        salons_json_string = conditionally_matched_salons.to_json(orient='records', force_ascii=False)
        prompt_text = f"""
        # あなたのタスク
        以下の候補者プロフィールと求人リストを基に、最もマッチ度が高い順に最大{remaining_slots}件のサロンの「店舗ID」をリストで回答してください。
        # 候補者プロフィール:
        {json.dumps(user_wishes, ensure_ascii=False)}
        # 候補となる求人リスト:
        {salons_json_string}
        # 回答フォーマット
        JSON形式のリストのみを回答してください。例: [101, 108, 125]
        """
        try:
            api_key = os.environ.get('GEMINI_API_KEY')
            if not api_key: raise Exception("GEMINI_API_KEY is not set.")
            model_name = "gemini-2.5-flash"
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}"
            headers = {"Content-Type": "application/json"}
            data = { "contents": [{ "parts": [{"text": prompt_text}] }] }
            response = requests.post(url, headers=headers, json=data, timeout=60)
            response.raise_for_status()
            response_json = response.json()
            response_text = response_json['candidates'][0]['content']['parts'][0]['text']
            
            json_str_match = re.search(r'\[.*\]', response_text, re.DOTALL)
            if json_str_match:
                ranked_ids = json.loads(json_str_match.group(0))
                for salon_id in ranked_ids:
                    salon_info = conditionally_matched_salons[conditionally_matched_salons['店舗ID'] == salon_id].iloc[0].to_dict()
                    top_salons.append(salon_info)
        except Exception as e:
            print(f"AIによるサロンランキング選出中にエラー: {e}")

    return top_salons, "サロン選出完了"

def create_salon_flex_message(salon, offer_text):
    db_role = salon.get("役職", "")
    display_role = "アシスタント" if "アシスタント" in db_role else "スタイリスト"
    recruitment_type = salon.get("募集", "")
    salon_id = salon.get('店舗ID')
    liff_url = f"https://liff.line.me/{SCHEDULE_LIFF_ID}?salonId={salon_id}"
    return { "type": "bubble", "hero": { "type": "image", "url": salon.get("画像URL", ""), "size": "full", "aspectRatio": "20:13", "aspectMode": "cover" }, "body": { "type": "box", "layout": "vertical", "contents": [ { "type": "text", "text": salon.get("店舗名", ""), "weight": "bold", "size": "xl" }, { "type": "box", "layout": "vertical", "margin": "lg", "spacing": "sm", "contents": [ { "type": "box", "layout": "baseline", "spacing": "sm", "contents": [ { "type": "text", "text": "勤務地", "color": "#aaaaaa", "size": "sm", "flex": 2 }, { "type": "text", "text": salon.get("住所", ""), "wrap": True, "color": "#666666", "size": "sm", "flex": 5 } ]}, { "type": "box", "layout": "baseline", "spacing": "sm", "contents": [ { "type": "text", "text": "募集役職", "color": "#aaaaaa", "size": "sm", "flex": 2 }, { "type": "text", "text": display_role, "wrap": True, "color": "#666666", "size": "sm", "flex": 5 } ]}, { "type": "box", "layout": "baseline", "spacing": "sm", "contents": [ { "type": "text", "text": "募集形態", "color": "#aaaaaa", "size": "sm", "flex": 2 }, { "type": "text", "text": recruitment_type, "wrap": True, "color": "#666666", "size": "sm", "flex": 5 } ]}, { "type": "box", "layout": "baseline", "spacing": "sm", "contents": [ { "type": "text", "text": "メッセージ", "color": "#aaaaaa", "size": "sm", "flex": 2 }, { "type": "text", "text": offer_text, "wrap": True, "color": "#666666", "size": "sm", "flex": 5 } ]} ]} ]}, "footer": { "type": "box", "layout": "vertical", "spacing": "sm", "contents": [ { "type": "button", "style": "link", "height": "sm", "action": { "type": "uri", "label": "詳しく見る", "uri": "https://example.com" }}, { "type": "button", "style": "primary", "height": "sm", "action": { "type": "uri", "label": "サロンから話を聞いてみる", "uri": liff_url }, "color": "#FF6B6B"} ], "flex": 0 } }

# ▼▼▼▼▼ 欠落していた関数をここに追加しました ▼▼▼▼▼
def get_age_from_birthdate(birthdate):
    today = datetime.today()
    birth_date = datetime.strptime(birthdate, '%Y-%m-%d')
    return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
# ▲▲▲▲▲ ここまで ▲▲▲▲▲

@app.route("/callback", methods=['POST'])
def callback():
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return 'OK'

@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    with ApiClient(configuration) as api_client:
        line_bot_api = MessagingApi(api_client)
        line_bot_api.reply_message_with_http_info( ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text="ご登録ありがとうございます。リッチメニューからプロフィールをご入力ください。")]) )

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
            interview_location = data.get('interviewLocation', '') 
            update_values = [ '日程調整中', data.get('interviewMethod', ''), data.get('date1', ''), data.get('startTime1', ''), data.get('endTime1', ''), data.get('date2', ''), data.get('startTime2', ''), data.get('endTime2', ''), data.get('date3', ''), data.get('startTime3', ''), data.get('endTime3', ''), interview_location ]
            offer_management_sheet.update(f'D{row_to_update}:O{row_to_update}', [update_values])
            subject = "【LUMINAオファー】面談日程の新規登録がありました"
            body = f"以下の内容で、ユーザーから面談希望日時の登録がありました。\n速やかにサロンとの日程調整を開始してください。\n\n■ ユーザーID: {user_id}\n■ サロンID: {salon_id}\n■ 希望の面談方法: {data.get('interviewMethod', '')}\n{f'■ 希望の場所: {interview_location}' if interview_location else ''}\n■ 第1希望: {data.get('date1', '')} {data.get('startTime1', '')} 〜 {data.get('endTime1', '')}\n■ 第2希望: {data.get('date2', '')} {data.get('startTime2', '')} 〜 {data.get('endTime2', '')}\n■ 第3希望: {data.get('date3', '')} {data.get('startTime3', '')} 〜 {data.get('endTime3', '')}"
            send_notification_email(subject, body)
            next_liff_url = f"https://liff.line.me/{QUESTIONNAIRE_LIFF_ID}"
            return jsonify({ "status": "success", "message": "Schedule submitted successfully", "nextLiffUrl": next_liff_url })
        else:
            return jsonify({"status": "error", "message": "Offer not found"}), 404
    except Exception as e:
        print(f"スプレッドシート更新エラー: {e}"); traceback.print_exc()
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
            update_values = [ data.get('q1_area'), data.get('q2_job_changes'), data.get('q3_current_employment'), data.get('q4_experience_years'), data.get('q5_desired_employment'), data.get('q6_priorities'), data.get('q7_improvement_point'), data.get('q8_ideal_beautician') ]
            user_management_sheet.update(f'Q{row_to_update}:X{row_to_update}', [update_values])
            user_name = user_management_sheet.cell(row_to_update, 4).value
            subject = f"【LUMINAオファー】{user_name}様からアンケート回答がありました"
            body = f"{user_name}様（ユーザーID: {user_id}）から、面談前アンケートへの回答がありました。\n内容を確認し、面談の準備を進めてください。\n\n---\n1. お住まいエリア: {data.get('q1_area')}\n2. 転職回数: {data.get('q2_job_changes')}\n3. 現雇用形態: {data.get('q3_current_employment')}\n4. 現役職経験年数: {data.get('q4_experience_years')}\n5. 希望雇用形態: {data.get('q5_desired_employment')}\n6. サロン選びの重視点: {data.get('q6_priorities')}\n7. 現職場の改善点: {data.get('q7_improvement_point')}\n8. 理想の美容師像: {data.get('q8_ideal_beautician')}"
            send_notification_email(subject, body)
            
            try:
                with ApiClient(configuration) as api_client:
                    line_bot_api = MessagingApi(api_client)
                    liff_url_for_contact = f"https://liff.line.me/{LINE_CONTACT_LIFF_ID}"
                    flex_message_body = {"type": "bubble","body": {"type": "box","layout": "vertical","contents": [{"type": "text","text": "アンケートへのご回答、ありがとうございます！","weight": "bold","size": "md","wrap": True},{"type": "text","text": "最後に、サロン担当者があなたに直接連絡できるよう、ご自身のLINE連絡先をご登録ください。","margin": "md","size": "sm","color": "#666666","wrap": True}]},"footer": {"type": "box","layout": "vertical","spacing": "sm","contents": [{"type": "button","style": "primary","height": "sm","action": {"type": "uri","label": "LINE連絡先を登録する","uri": liff_url_for_contact},"color": "#FF6B6B"}],"flex": 0}}
                    messages = [FlexMessage(alt_text="LINE連絡先の登録をお願いします。", contents=FlexContainer.from_dict(flex_message_body))]
                    line_bot_api.push_message(PushMessageRequest(to=user_id, messages=messages))
                    print(f"ユーザーID {user_id} に連絡先登録を促すプッシュメッセージを送信しました。")
            except Exception as e:
                print(f"プッシュメッセージ送信エラー: {e}")

            return jsonify({"status": "success", "message": "Questionnaire submitted successfully"})
        else:
            return jsonify({"status": "error", "message": "User not found"}), 404
    except Exception as e:
        print(f"アンケート更新エラー: {e}"); traceback.print_exc()
        return jsonify({"status": "error", "message": "Failed to update questionnaire"}), 500

@app.route("/submit-line-contact", methods=['POST'])
def submit_line_contact():
    data = request.get_json()
    user_id = data.get('userId')
    line_url = data.get('lineUrl')
    if not user_id or not line_url: return jsonify({"status": "error", "message": "Invalid data"}), 400
    try:
        gc = gspread.service_account(filename=creds_path)
        sheet = gc.open("店舗マスタ_LUMINA Offer用").worksheet("ユーザー管理")
        cell = sheet.find(user_id, in_column=1)
        if cell:
            sheet.update_cell(cell.row, 25, line_url) # Y列
            user_name = sheet.cell(cell.row, 4).value
            subject = f"【LUMINAオファー】{user_name}様からLINE連絡先の登録がありました"
            body = f"{user_name}様（ユーザーID: {user_id}）から、LINE連絡先の登録がありました。\nサロン担当者へ以下のURLを共有してください。\n\n<hr><b>▼ 友だち追加URL</b><br><a href=\"{line_url}\">{line_url}</a><hr>"
            send_notification_email(subject, body)
            
            try:
                with ApiClient(configuration) as api_client:
                    line_bot_api = MessagingApi(api_client)
                    confirmation_text = "ご連絡先の登録、ありがとうございました。\nサロンとの日程調整が完了しましたら、担当者から直接あなたのLINEにご連絡がありますので、もうしばらくお待ちください。"
                    messages = [TextMessage(text=confirmation_text)]
                    line_bot_api.push_message(PushMessageRequest(to=user_id, messages=messages))
                    print(f"ユーザーID {user_id} に最終確認のプッシュメッセージを送信しました。")
            except Exception as e:
                print(f"プッシュメッセージ送信エラー: {e}")

            return jsonify({"status": "success", "message": "LINE contact submitted successfully"})
        else:
            return jsonify({"status": "error", "message": "User not found"}), 404
    except Exception as e:
        print(f"LINE連絡先更新エラー: {e}"); traceback.print_exc()
        return jsonify({"status": "error", "message": "Failed to update LINE contact"}), 500

@app.route("/trigger-offer", methods=['POST'])
def trigger_offer():
    data = request.get_json()
    if not data: return jsonify({"status": "error", "message": "No data provided"}), 400
    user_id = data.get('userId')
    user_wishes = data.get('wishes')
    if not user_id or not user_wishes: return jsonify({"status": "error", "message": "Missing userId or wishes"}), 400
    
    try:
        user_name = user_wishes.get('full_name', '不明なユーザー')
        subject = f"【LUMINAオファー】{user_name}様から新規プロフィール登録がありました"
        body = f"""
            新しいユーザーからプロフィールの登録がありました。
            内容を確認し、システムが自動送信するオファーの妥当性を確認してください。
            <hr>
            <b>▼ 登録情報</b><br>
            - 氏名: {user_wishes.get('full_name', '')}<br>
            - 性別: {user_wishes.get('gender', '')}<br>
            - 生年月日: {user_wishes.get('birthdate', '')}<br>
            - 電話番号: {user_wishes.get('phone_number', '')}<br>
            - 美容師免許: {user_wishes.get('license', '')}<br>
            - MBTI: {user_wishes.get('mbti', '')}<br>
            - 役職: {user_wishes.get('role', '')}<br>
            - 希望エリア: {user_wishes.get('area_prefecture', '')} {user_wishes.get('area_detail', '')}<br>
            - 職場満足度: {user_wishes.get('satisfaction', '')}<br>
            - 興味のある待遇: {user_wishes.get('perk', '')}<br>
            - 今の状況: {user_wishes.get('current_status', '')}<br>
            - 転職希望時期: {user_wishes.get('timing', '')}<br>
            - ユーザーID: {user_id}
            <hr>
        """
        send_notification_email(subject, body)
    except Exception as e:
        print(f"初回登録の通知メール送信中にエラーが発生: {e}")

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
            user_wishes['age'] = f"{(age // 10) * 10}代"
        except (ValueError, TypeError):
            user_wishes['age'] = ''
            
    try:
        gc = gspread.service_account(filename=creds_path)
        user_management_sheet = gc.open("店舗マスタ_LUMINA Offer用").worksheet("ユーザー管理")
        user_headers = user_management_sheet.row_values(1)
        user_row_dict = { "ユーザーID": user_id, "登録日": datetime.now(JST).strftime('%Y/%m/%d'), "ステータス": 'オファー中', "氏名": user_wishes.get('full_name'), "性別": user_wishes.get('gender'), "生年月日": user_wishes.get('birthdate'), "電話番号": user_wishes.get('phone_number'), "MBTI": user_wishes.get('mbti'), "役職": user_wishes.get('role'), "希望エリア": user_wishes.get('area_prefecture'), "希望勤務地": user_wishes.get('area_detail'), "職場満足度": user_wishes.get('satisfaction'), "興味のある待遇": user_wishes.get('perk'), "現在の状況": user_wishes.get('current_status'), "転職希望時期": user_wishes.get('timing'), "美容師免許": user_wishes.get('license') }
        profile_headers = user_headers[:16]
        profile_row_values = [user_row_dict.get(h, '') for h in profile_headers]
        cell = user_management_sheet.find(user_id, in_column=1)
        if cell:
            range_to_update = f'A{cell.row}:{chr(ord("A") + len(profile_row_values) - 1)}{cell.row}'
            user_management_sheet.update(range_to_update, [profile_row_values])
        else:
            full_row = profile_row_values + [''] * (len(user_headers) - len(profile_headers))
            user_management_sheet.append_row(full_row, value_input_option='USER_ENTERED')
    except Exception as e:
        print(f"ユーザー管理シートへの書き込みエラー: {e}"); traceback.print_exc()

    try:
        user_wishes['userId'] = user_id
        top_salons, reason = find_and_select_top_salons(user_wishes)
        
        if not top_salons:
            print(f"ユーザーID {user_id} のオファー予約に失敗しました。理由: {reason}")
            return jsonify({"status": "error", "message": "No salons found to schedule"}), 404

        now_jst = datetime.now(JST)
        cutoff_time = now_jst.replace(hour=19, minute=30, second=0, microsecond=0)
        
        if now_jst >= cutoff_time:
            first_send_date = now_jst.date() + timedelta(days=1)
        else:
            first_send_date = now_jst.date()
        
        schedule = [
            (first_send_date, "21:30"),
            (first_send_date + timedelta(days=1), "12:30"),
            (first_send_date + timedelta(days=1), "20:00"),
            (first_send_date + timedelta(days=3), "12:30"),
            (first_send_date + timedelta(days=4), "21:30")
        ]
        
        rows_to_append = []
        for i, salon in enumerate(top_salons):
            if i < len(schedule):
                send_date, send_time_str = schedule[i]
                send_time_obj = datetime.strptime(send_time_str, "%H:%M").time()
                send_at_datetime = datetime.combine(send_date, send_time_obj, tzinfo=JST)
                send_at_iso = send_at_datetime.isoformat()
                new_row = [user_id, salon['店舗ID'], send_at_iso, 'pending']
                rows_to_append.append(new_row)

        if rows_to_append:
            gc = gspread.service_account(filename=creds_path)
            queue_sheet = gc.open("店舗マスタ_LUMINA Offer用").worksheet("Offer Queue")
            queue_sheet.append_rows(rows_to_append, value_input_option='USER_ENTERED')
            print(f"ユーザーID {user_id} のために {len(rows_to_append)}件のオファーを予約しました。")

    except Exception as e:
        print(f"オファーの予約処理中にエラー: {e}")
        traceback.print_exc()

    return jsonify({"status": "success", "message": "Offer tasks scheduled successfully"})

@app.route("/process-offer-queue", methods=['GET'])
def process_offer_queue():
    cron_secret = request.args.get('secret')
    if cron_secret != os.environ.get('CRON_SECRET'):
        return "Unauthorized", 401
    
    try:
        now_iso = datetime.now(JST).isoformat()
        
        gc = gspread.service_account(filename=creds_path)
        queue_sheet = gc.open("店舗マスタ_LUMINA Offer用").worksheet("Offer Queue")
        user_sheet = gc.open("店舗マスタ_LUMINA Offer用").worksheet("ユーザー管理")
        salon_sheet = gc.open("店舗マスタ_LUMINA Offer用").worksheet("店舗マスタ")
        
        all_queue = queue_sheet.get_all_records()
        all_users = user_sheet.get_all_records(value_render_option='UNFORMATTED_VALUE')
        all_salons = salon_sheet.get_all_records()

        users_dict = {str(u['ユーザーID']): u for u in all_users}
        salons_dict = {str(s['店舗ID']): s for s in all_salons}
        
        for idx, record in enumerate(all_queue):
            row_num = idx + 2
            if record.get('status') == 'pending' and record.get('send_at') <= now_iso:
                user_id = str(record.get('user_id'))
                salon_id = str(record.get('salon_id'))
                
                user_wishes = users_dict.get(user_id)
                salon_info = salons_dict.get(salon_id)

                if user_wishes and salon_info:
                    print(f"{row_num}行目のオファーを処理します: {user_id} -> Salon {salon_id}")
                    
                    offer_message = generate_single_offer_message(user_wishes, salon_info)
                    
                    with ApiClient(configuration) as api_client:
                        line_bot_api = MessagingApi(api_client)
                        flex_container = FlexContainer.from_dict(create_salon_flex_message(salon_info, offer_message))
                        messages = [FlexMessage(alt_text=f"{salon_info['店舗名']}からのオファー", contents=flex_container)]
                        line_bot_api.push_message(PushMessageRequest(to=user_id, messages=messages))
                    
                    queue_sheet.update_cell(row_num, 4, 'sent')
                else:
                    print(f"ユーザー({user_id})またはサロン({salon_id})の情報が見つからず、スキップします。")
                    queue_sheet.update_cell(row_num, 4, 'error')
        
        return "Offer queue processed.", 200
    except Exception as e:
        print(f"オファーキューの処理中にエラー: {e}")
        traceback.print_exc()
        return "An error occurred.", 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port)