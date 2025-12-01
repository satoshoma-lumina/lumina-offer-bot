import os
import json
import gspread
import pandas as pd
import re
from datetime import datetime, timedelta, timezone
import traceback
import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import threading  # ★追加: バックグラウンド処理用

from flask import Flask, request, abort, jsonify
from flask_cors import CORS
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
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
CALL_REQUEST_LIFF_ID = "2008066763-d13XV3gO"
SCHEDULE_LIFF_ID = "2008066763-X5mxymoj"
QUESTIONNAIRE_LIFF_ID = "2008066763-JAkGQkmw"
LINE_CONTACT_LIFF_ID = "2008066763-Rv0z80wl"
SALON_DETAIL_LIFF_ID = "2008066763-Exlv1lLY"
SATO_EMAIL = "sato@lumina-beauty.co.jp"

# --- 認証設定 ---
creds_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', '/etc/secrets/delta-wonder-471708-u1-93f8d5bbdf1c.json')

# LINE API
configuration = Configuration(access_token=os.environ.get('YOUR_CHANNEL_ACCESS_TOKEN'))
handler = WebhookHandler(os.environ.get('YOUR_CHANNEL_SECRET'))

# タイムゾーン
JST = timezone(timedelta(hours=+9))
GAS_WEBHOOK_URL = os.environ.get('GAS_WEBHOOK_URL')

# --- Helper Functions ---

def send_notification_email(subject, body):
    # 環境変数の取得
    from_email = os.environ.get('MAIL_USERNAME')
    password = os.environ.get('MAIL_PASSWORD')
    to_email = SATO_EMAIL

    if not from_email or not password:
        print("メール送信用の環境変数(MAIL_USERNAME, MAIL_PASSWORD)が設定されていません。")
        return

    try:
        # メールデータの作成
        msg = MIMEMultipart()
        msg['From'] = from_email
        msg['To'] = to_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body.replace('\n', '<br>'), 'html'))

        # GmailのSMTPサーバーに接続 (ポート587, STARTTLS, タイムアウト設定)
        server = smtplib.SMTP('smtp.gmail.com', 587, timeout=10)
        server.starttls()
        server.login(from_email, password)
        server.send_message(msg)
        server.quit()
        
        print(f"メール送信成功: {subject}")
    except Exception as e:
        print(f"メール送信エラー: {e}")
        traceback.print_exc()

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
    - 禁止事項1: サロンが直接オファーを送っているかのような表現は避けること。
    - 禁止事項2: 文章内に具体的な「サロン名（店舗名）」は絶対に含まないこと。「当サロン」「こちらのサロン」などの表現を使用すること。
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
        primary_area = detail_area.replace("　", " ").split(" ")[0]
        words_to_remove = ["周辺", "中心部", "あたり"]
        cleaned_detail_area = primary_area
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
    
    already_sent_salon_ids_str = [ str(record['店舗ID']) for record in offer_history if record['ユーザーID'] == user_wishes.get('userId') ]
    if already_sent_salon_ids_str:
        conditionally_matched_salons = conditionally_matched_salons[
            ~conditionally_matched_salons['店舗ID'].astype(str).isin(already_sent_salon_ids_str)
        ]
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
    
    address_full = salon.get("住所", "")
    masked_address = "エリア: " + address_full.split(" ")[0] if address_full else "エリア: 非公開"

    detail_liff_url = f"https://liff.line.me/{SALON_DETAIL_LIFF_ID}?salonId={salon_id}"
    call_request_liff_url = f"https://liff.line.me/{CALL_REQUEST_LIFF_ID}?salonId={salon_id}"
    
    original_image_url = salon.get("画像URL", "")
    if original_image_url:
        blurred_image_url = f"https://wsrv.nl/?url={original_image_url}&blur=10&output=jpg"
    else:
        blurred_image_url = "https://placehold.co/600x400/333333/FFFFFF/png?text=No+Image"
    
    display_salon_name = salon.get("公開用店名", "非公開サロン")

    return {
        "type": "bubble",
        "hero": {
            "type": "image",
            "url": blurred_image_url,
            "size": "full",
            "aspectRatio": "20:13",
            "aspectMode": "cover"
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "contents": [
                { "type": "text", "text": display_salon_name, "weight": "bold", "size": "xl" },
                {
                    "type": "box", "layout": "vertical", "margin": "lg", "spacing": "sm",
                    "contents": [
                        { "type": "box", "layout": "baseline", "spacing": "sm", "contents": [ { "type": "text", "text": "勤務地", "color": "#aaaaaa", "size": "sm", "flex": 2 }, { "type": "text", "text": masked_address, "wrap": True, "color": "#666666", "size": "sm", "flex": 5 } ]},
                        { "type": "box", "layout": "baseline", "spacing": "sm", "contents": [ { "type": "text", "text": "募集役職", "color": "#aaaaaa", "size": "sm", "flex": 2 }, { "type": "text", "text": display_role, "wrap": True, "color": "#666666", "size": "sm", "flex": 5 } ]},
                        { "type": "box", "layout": "baseline", "spacing": "sm", "contents": [ { "type": "text", "text": "募集形態", "color": "#aaaaaa", "size": "sm", "flex": 2 }, { "type": "text", "text": recruitment_type, "wrap": True, "color": "#666666", "size": "sm", "flex": 5 } ]},
                        { "type": "box", "layout": "baseline", "spacing": "sm", "contents": [ { "type": "text", "text": "メッセージ", "color": "#aaaaaa", "size": "sm", "flex": 2 }, { "type": "text", "text": offer_text, "wrap": True, "color": "#666666", "size": "sm", "flex": 5 } ]}
                    ]
                }
            ]
        },
        "footer": {
            "type": "box", "layout": "vertical", "spacing": "sm",
            "contents": [
                { "type": "button", "style": "primary", "height": "sm", "action": { "type": "uri", "label": "待遇を見る", "uri": detail_liff_url }, "color": "#59A5D8" },
                { "type": "button", "style": "primary", "height": "sm", "action": { "type": "uri", "label": "サロン名を確認する", "uri": call_request_liff_url }, "color": "#F37335" }
            ],
            "flex": 0
        }
    }

def get_age_from_birthdate(birthdate):
    today = datetime.today()
    birth_date = datetime.strptime(birthdate, '%Y-%m-%d')
    return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))

# --- Background Task ---
def process_offer_background(user_id, user_wishes):
    """
    時間のかかる処理（メール、スプレッドシート、AI、LINE）をバックグラウンドで実行
    """
    print(f"Start background process for user: {user_id}")
    # アプリケーションコンテキスト内で実行（念のため）
    with app.app_context():
        try:
            # 1. 管理者へメール通知
            try:
                user_name = user_wishes.get('full_name', '不明なユーザー')
                subject = f"【LUMINAオファー】{user_name}様から新規プロフィール登録がありました"
                body = f"新規ユーザー登録: {user_name}様 (ID: {user_id})"
                send_notification_email(subject, body)
            except Exception as e:
                print(f"[Background] メール送信エラー: {e}")
                traceback.print_exc()

            # 2. ユーザーへウェルカムメッセージ送信
            try:
                with ApiClient(configuration) as api_client:
                    line_bot_api = MessagingApi(api_client)
                    welcome_message = ( "ご登録ありがとうございます！\nLUMINA Offerが、あなたにピッタリな『好待遇サロンの公認オファー』をご連絡いたします。\n楽しみにお待ちください！" )
                    line_bot_api.push_message(PushMessageRequest( to=user_id, messages=[TextMessage(text=welcome_message)] ))
            except Exception as e:
                print(f"[Background] LINE送信エラー: {e}")

            # 3. 年齢計算
            if 'birthdate' in user_wishes and user_wishes['birthdate']:
                try:
                    age = get_age_from_birthdate(user_wishes.get('birthdate'))
                    user_wishes['age'] = f"{(age // 10) * 10}代"
                except: user_wishes['age'] = ''

            # 4. ユーザー管理シートへの保存
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
                print(f"[Background] シート保存エラー: {e}"); traceback.print_exc()

            # 5. オファーマッチングと予約
            try:
                user_wishes['userId'] = user_id
                top_salons, reason = find_and_select_top_salons(user_wishes)
                
                if not top_salons:
                    print(f"[Background] ユーザーID {user_id} にマッチするサロンなし: {reason}")
                else:
                    now_jst = datetime.now(JST)
                    cutoff_time = now_jst.replace(hour=19, minute=30, second=0, microsecond=0)
                    first_send_date = now_jst.date() + timedelta(days=1) if now_jst >= cutoff_time else now_jst.date()
                    
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
                        # gspreadインスタンスを再取得（セッション切れ防止）
                        gc_q = gspread.service_account(filename=creds_path)
                        queue_sheet = gc_q.open("店舗マスタ_LUMINA Offer用").worksheet("Offer Queue")
                        queue_sheet.append_rows(rows_to_append, value_input_option='USER_ENTERED')
                        print(f"[Background] {len(rows_to_append)} offers scheduled.")

            except Exception as e:
                print(f"[Background] オファー予約エラー: {e}"); traceback.print_exc()

        except Exception as e:
            print(f"[Background] 予期せぬエラー: {e}")
            traceback.print_exc()

# --- Routes ---

@app.route("/callback", methods=['POST'])
def callback():
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return 'OK'

@handler.add(FollowEvent)
def handle_follow(event):
    if GAS_WEBHOOK_URL:
        try:
            params_to_gas = { 'userId': event.source.user_id, 'timestamp': event.timestamp }
            requests.get(GAS_WEBHOOK_URL, params=params_to_gas, timeout=5)
            print(f"GASへのFollowイベント通知成功: {event.source.user_id}")
        except Exception as e:
            print(f"GASへのFollowイベント通知に失敗: {e}")
    
    try:
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            PROFILE_LIFF_URL = "https://liff.line.me/2008066763-ZJ72p7OJ"
            YOUR_NEW_IMAGE_URL = "https://raw.githubusercontent.com/satoshoma-lumina/lumina-offer-bot/4c57f959238f64d2254550c2347db1d9a625a435/%E3%82%B9%E3%82%AD%E3%83%9E%C3%97MBTI%E8%A8%B4%E6%B1%82_%E6%95%B0%E5%AD%97%E5%A4%89%E6%9B%B4Vr.png"
            flex_message_json = {
                "type": "bubble",
                "hero": { "type": "image", "url": YOUR_NEW_IMAGE_URL, "size": "full", "aspectRatio": "1024:678", "aspectMode": "fit" },
                "body": {
                    "type": "box", "layout": "vertical",
                    "contents": [
                        { "type": "text", "text": "”3分”でオファーが届く！", "weight": "bold", "size": "xl", "align": "center" },
                        { "type": "text", "text": "業界初！MBTIで相性マッチ", "wrap": True, "margin": "lg", "size": "md", "color": "#666666", "align": "center" }
                    ],
                    "paddingTop": "xl", "paddingBottom": "lg"
                },
                "footer": {
                    "type": "box", "layout": "vertical",
                    "contents": [ { "type": "button", "action": { "type": "uri", "label": "今すぐMBTI入力▶▶", "uri": PROFILE_LIFF_URL }, "style": "primary", "color": "#F37335", "height": "sm", "margin": "sm" } ],
                    "spacing": "sm", "flex": 0, "paddingAll": "md"
                }
            }
            line_bot_api.reply_message_with_http_info(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[FlexMessage(alt_text="LUMINA Offer プロフィール登録", contents=FlexContainer.from_dict(flex_message_json))]
                )
            )
    except Exception as e:
        print(f"Followイベントへの返信メッセージ送信エラー: {e}"); traceback.print_exc()

@app.route("/api/salon-detail/<int:salon_id>", methods=['GET'])
def get_salon_detail(salon_id):
    try:
        gc = gspread.service_account(filename=creds_path)
        salon_master_sheet = gc.open("店舗マスタ_LUMINA Offer用").worksheet("店舗マスタ")
        all_salons = salon_master_sheet.get_all_records()
        salon_info = next((s for s in all_salons if str(s['店舗ID']) == str(salon_id)), None)
        if salon_info: return jsonify(salon_info)
        else: return jsonify({"error": "Salon not found"}), 404
    except Exception as e:
        print(f"サロン詳細の取得中にエラー: {e}"); traceback.print_exc()
        return jsonify({"error": "Internal server error"}), 500

@app.route("/submit-schedule", methods=['POST'])
def submit_schedule(): return jsonify({"status": "error", "message": "Deprecated"}), 410

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
            body = f"{user_name}様（ユーザーID: {user_id}）からアンケート回答がありました。\n内容を確認してください。"
            send_notification_email(subject, body)
            return jsonify({"status": "success", "message": "Questionnaire submitted successfully"})
        else: return jsonify({"status": "error", "message": "User not found"}), 404
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
            sheet.update_cell(cell.row, 25, line_url)
            user_name = sheet.cell(cell.row, 4).value
            subject = f"【LUMINAオファー】{user_name}様からLINE連絡先の登録がありました"
            body = f"{user_name}様（ユーザーID: {user_id}）からLINE連絡先登録。\nURL: {line_url}"
            send_notification_email(subject, body)
            return jsonify({"status": "success", "message": "LINE contact submitted successfully"})
        else: return jsonify({"status": "error", "message": "User not found"}), 404
    except Exception as e:
        print(f"LINE連絡先更新エラー: {e}"); traceback.print_exc()
        return jsonify({"status": "error", "message": "Failed to update LINE contact"}), 500

@app.route("/submit-call-request", methods=['POST'])
def submit_call_request():
    data = request.get_json()
    user_id = data.get('userId')
    salon_id = data.get('salonId')
    time_slot = data.get('timeSlot')

    if not user_id or not salon_id or not time_slot:
        return jsonify({"status": "error", "message": "Missing required fields"}), 400

    try:
        gc = gspread.service_account(filename=creds_path)
        user_sheet = gc.open("店舗マスタ_LUMINA Offer用").worksheet("ユーザー管理")
        user_cell = user_sheet.find(user_id, in_column=1)
        user_phone = "不明"; user_name = "不明"
        if user_cell:
            user_name = user_sheet.cell(user_cell.row, 4).value
            user_phone = user_sheet.cell(user_cell.row, 7).value

        salon_sheet = gc.open("店舗マスタ_LUMINA Offer用").worksheet("店舗マスタ")
        all_salons = salon_sheet.get_all_records()
        salon_info = next((s for s in all_salons if str(s['店舗ID']) == str(salon_id)), None)
        salon_name = salon_info['店舗名'] if salon_info else "サロンID: " + str(salon_id)

        offer_sheet = gc.open("店舗マスタ_LUMINA Offer用").worksheet("オファー管理")
        today_str = datetime.now(JST).strftime('%Y/%m/%d %H:%M:%S')
        new_row = [user_id, salon_id, today_str, "電話希望: " + time_slot]
        offer_sheet.append_row(new_row, value_input_option='USER_ENTERED')

        priority_marker = "【至急】" if "今すぐ" in time_slot else ""
        subject = f"{priority_marker}【LUMINA】サロン名確認・電話依頼（{user_name}様）"
        body = f"""ユーザーからサロン名確認のための電話連絡依頼がありました。
指定の時間帯に、以下の電話番号へ連絡し、サロン名と詳細を伝えてください。<hr>
<b>■ ユーザー情報</b><br>氏名: {user_name}<br><b>電話番号: <a href="tel:{user_phone}">{user_phone}</a></b><br>ユーザーID: {user_id}<br><br>
<b>■ 希望連絡時間</b><br><span style="font-size:16px; font-weight:bold; color:red;">{time_slot}</span><br><br>
<b>■ 対象サロン</b><br>{salon_name} (ID: {salon_id})<hr>"""
        send_notification_email(subject, body)
        return jsonify({"status": "success", "message": "Call request submitted"})

    except Exception as e:
        print(f"電話依頼処理エラー: {e}"); traceback.print_exc()
        return jsonify({"status": "error", "message": "Server Error"}), 500

# ★★★ 変更点: バックグラウンド処理への引き渡し ★★★
@app.route("/trigger-offer", methods=['POST'])
def trigger_offer():
    data = request.get_json()
    if not data: return jsonify({"status": "error", "message": "No data provided"}), 400
    user_id = data.get('userId')
    user_wishes = data.get('wishes')
    if not user_id or not user_wishes: return jsonify({"status": "error", "message": "Missing userId or wishes"}), 400

    # スレッドを開始して、すぐにレスポンスを返す
    thread = threading.Thread(target=process_offer_background, args=(user_id, user_wishes))
    thread.start()

    return jsonify({"status": "success", "message": "Accepted"}), 200

@app.route("/process-offer-queue", methods=['GET'])
def process_offer_queue():
    cron_secret = request.args.get('secret')
    if cron_secret != os.environ.get('CRON_SECRET'): return "Unauthorized", 401
    try:
        now_iso = datetime.now(JST).isoformat()
        gc = gspread.service_account(filename=creds_path)
        queue_sheet = gc.open("店舗マスタ_LUMINA Offer用").worksheet("Offer Queue")
        user_sheet = gc.open("店舗マスタ_LUMINA Offer用").worksheet("ユーザー管理")
        salon_sheet = gc.open("店舗マスタ_LUMINA Offer用").worksheet("店舗マスタ")
        offer_management_sheet = gc.open("店舗マスタ_LUMINA Offer用").worksheet("オファー管理")
        
        all_queue = queue_sheet.get_all_records()
        all_users = user_sheet.get_all_records(value_render_option='UNFORMATTED_VALUE')
        all_salons = salon_sheet.get_all_records()
        users_dict = {str(u['ユーザーID']): u for u in all_users}
        salons_dict = {str(s['店舗ID']): s for s in all_salons}
        
        for idx, record in enumerate(all_queue):
            row_num = idx + 2
            if record.get('status') == 'pending' and record.get('send_at') <= now_iso:
                user_id = str(record.get('user_id')); salon_id = str(record.get('salon_id'))
                user_wishes = users_dict.get(user_id); salon_info = salons_dict.get(salon_id)

                if user_wishes and salon_info:
                    print(f"Processing: {user_id} -> {salon_id}")
                    offer_message = generate_single_offer_message(user_wishes, salon_info)
                    
                    with ApiClient(configuration) as api_client:
                        line_bot_api = MessagingApi(api_client)
                        flex_container = FlexContainer.from_dict(create_salon_flex_message(salon_info, offer_message))
                        messages = [FlexMessage(alt_text=f"非公開サロンからのオファー", contents=flex_container)]
                        line_bot_api.push_message(PushMessageRequest(to=user_id, messages=messages))
                    
                    try:
                        today_str = datetime.now(JST).strftime('%Y/%m/%d')
                        new_offer_row = [ user_id, salon_info.get('店舗ID'), today_str, "送信済み" ] + [''] * 11 
                        offer_management_sheet.append_row(new_offer_row, value_input_option='USER_ENTERED')
                    except Exception as e: print(f"オファー管理シートへの書き込み中にエラー: {e}")

                    queue_sheet.update_cell(row_num, 4, 'sent')
                else: queue_sheet.update_cell(row_num, 4, 'error')
        return "Offer queue processed.", 200
    except Exception as e:
        print(f"Queue Error: {e}"); traceback.print_exc()
        return "An error occurred.", 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port)