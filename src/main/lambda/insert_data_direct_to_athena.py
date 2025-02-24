# bật terminal gõ lệnh pip install psycopg2 boto3 pandas để install lib
import psycopg2
import json
import logging
import os
import datetime
import boto3
import pandas as pd
from io import BytesIO

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def send_email_notification(subject, body, to_email):
    sender_email = "ngoccuongbich6972@gmail.com"
    sender_password = "irhj rbzg zxak fxrq"  # Thay bằng App Password nếu dùng xác thực 2 bước

    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = to_email
    msg["Subject"] = subject

    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()  # Bật mã hóa TLS
            server.login(sender_email, sender_password)  # Đăng nhập
            server.sendmail(sender_email, to_email, msg.as_string())  # Gửi email

        logger.info("✅ Email đã được gửi thành công qua Gmail!")
    except Exception as e:
        logger.error(f"❌ Lỗi khi gửi email qua Gmail: {str(e)}")

def check_directory_and_files_in_s3(bucket_name, prefix):
    """
    Kiểm tra thư mục có tồn tại và có chứa file không trong S3.
    :param bucket_name: Tên bucket
    :param prefix: Đường dẫn thư mục (kết thúc bằng "/")
    :return: Tuple (has_file, file_count, file_names)
             has_file: True nếu có ít nhất một file
             file_count: Số lượng file
             file_names: Danh sách tên file
    """
    s3 = boto3.client('s3')

    try:
        # Lấy danh sách đối tượng trong thư mục
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        if 'Contents' not in response:
            logger.info(f"Thư mục {prefix} không tồn tại hoặc rỗng.")
            return False, 0, []

        # Lọc ra các file (bỏ qua thư mục có size = 0)
        files = [obj for obj in response['Contents'] if obj['Size'] > 0]
        file_names = [obj['Key'].split('/')[-1] for obj in files]
        file_count = len(files)

        return file_count > 0, file_count, file_names
    except Exception as e:
        logger.error(f"Lỗi khi kiểm tra thư mục trong S3: {str(e)}")
        return False, 0, []

# Hàm gửi email thông qua AWS SES
# def send_email_notification(subject, body, to_email):
#     ses = boto3.client('ses', region_name='ap-southeast-1')
#     RECIPIENT = "cuongnv37@tnexfinance.com.vn"
#     try:
#         response = ses.send_email(
#             Source='ittnexfinance@tnexfinance.com.vn',
#             Destination={
#                 'ToAddresses': [RECIPIENT],
#             },
#             Message={
#                 'Subject': {'Data': subject},
#                 'Body': {'Text': {'Data': body}},
#             },
#         )
#         logger.info(f"Email đã được gửi thành công: {response}")
#     except Exception as e:
#         logger.error(f"Lỗi khi gửi email: {str(e)}")

def handle_string(value):
    """Xử lý chuỗi, giữ lại số 0 ở đầu nếu có."""
    if pd.isna(value) or value == "":
        return None  # Nếu giá trị là NaN hoặc rỗng, trả về None
    # Kiểm tra nếu giá trị là chuỗi có chứa số 0 ở đầu và trả lại chuỗi ban đầu
    if isinstance(value, str):
        return value.strip()  # Trả lại chuỗi ban đầu mà không thay đổi gì
    return str(value)  # Nếu không phải là chuỗi, chuyển thành chuỗi bình thường

def handle_number(value):
    """Xử lý số, thay NaN hoặc None thành 0 hoặc giá trị hợp lệ."""
    if pd.isna(value) or value is None:
        return 0  # Thay NaN hoặc None thành 0
    try:
        # Nếu là chuỗi số có dấu thập phân, thử chuyển thành float
        if isinstance(value, str):
            value = value.replace(",", "")  # Xử lý nếu có dấu phẩy trong số
        return float(value)  # Chuyển đổi giá trị thành kiểu float nếu có thể
    except ValueError:
        return 0  # Nếu không thể chuyển đổi thành float, trả về 0

def handle_timestamp(value):
    if pd.isna(value) or value is None:
        return None  # Hoặc giá trị mặc định nếu cần
    elif isinstance(value, int):  # Nếu giá trị là kiểu integer, có thể là timestamp dạng số
        return datetime.fromtimestamp(value)  # Chuyển từ integer sang timestamp
    return value

# 📌 Hàm INSERT cho bảng case_collection_debt
def insert_into_case_collection_debt(df, cursor):
    logger.info(f"=============start insert_into_case_collection_debt =======================")
    try:
        logger.info(f"DataFrame columns: {df.columns}")
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO collection_debt.case_collection_debt (
                    customer_id
                    , contract_number
                    , product_name
                    , dpd
                    , to_collect
                    , total_paid
                    , principle_outstanding
                    , principle_overdue
                    , interest_overdue
                    , tenor
                    , disbursement_date
                    , disbursement_amount
                    , approved_amount
                    , overdue_amount
                    , interest_outstanding
                    , remain_total
                    , fee_overdue
                    , emi
                    , due_date
                    , created_time
                    , updated_time
                    , terminal_fee
                    , interest_rate
                    , default_risk_segment
                    , bucket
                    , start_date
                    , end_date
                    , next_due_date
                    , first_due_date
                    , last_payment_date
                    , last_payment_amount
                    , last_action_code
                    , debt_sale_date
                    , balance_amount
                    , period_bucket
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                handle_string(row["cus_id"]),
				handle_string(row["case_code"]),
				handle_string(row["product_name"]),
				handle_number(row["dpd"]),
				handle_number(row["to_collect"]),
				handle_number(row["total_paid"]),
				handle_number(row["principle_outstanding"]),
				handle_number(row["principle_overdue"]),
				handle_number(row["interest_overdue"]),
				handle_number(row["tenor"]),
				handle_timestamp(row["disbursement_date"]),
				handle_number(row["disbursement_amount"]),
				handle_number(row["approved_amount"]),
				handle_number(row["overdue_amount"]),
				handle_number(row["interest_outstanding"]),
				handle_number(row["remain_total"]),
				handle_number(row["fee_overdue"]),
				handle_number(row["emi"]),
				handle_timestamp(row["due_date"]),
				handle_number(row["other_charges"]),
				handle_number(row["interest_product"]),
				handle_string(row["risk_segment"]),
                handle_string(row["bucket"]),
                handle_timestamp(row["start_date"]),
                handle_timestamp(row["end_date"]),
                handle_timestamp(row["next_due_date"]),
                handle_timestamp(row["first_due_date"]),
                handle_timestamp(row["last_payment_date"]),
                handle_number(row["last_payment_amount"]),
                handle_string(row["last_action_code"]),
                handle_timestamp(row["debt_sale_date"]),
                handle_number(row["balance_amount"]),
                handle_string(row["period_bucket"])
        ))
    except Exception as e:
        logger.error(f"Lỗi insert_into_case_collection_debt: {str(e)}")
    logger.info(f"======================End insert_into_case_collection_debt =======================")

# 📌 Hàm UPSERT (INSERT nếu chưa có, UPDATE nếu đã có) cho bảng case_customer_info_debt
def upsert_case_customer_info_debt(df, cursor):
    logger.info(f"======================Start upsert_case_customer_info_debt =======================")
    for index, row in df.iterrows():
        try:
            logger.info(f"Customer Mobile: {row['customer_mobile']}")

            id_issue_date = pd.to_datetime(row['id_issue_date']).strftime('%Y-%m-%d %H:%M:%S') if pd.notna(row['id_issue_date']) else None
            logger.info(f"======================value row {row}")
            logger.info(f"Customer Mobile: {row['customer_mobile']}")
            cursor.execute("""
                INSERT INTO collection_debt.case_customer_info_debt (
                    customer_id, customer_name, phone, gender, birth_day, national_id_number,
                    id_issue_date, id_issue_place, citizen_id_number, citizen_issue_date, citizen_issue_place,
                    per_address, per_ward, per_district, per_province, cur_address, cur_ward, cur_district, cur_province,
                    ref1_name, ref1_number, ref1_relationship, ref2_name, ref2_number, ref2_relationship, tenor,
                    casa_account, ref1_work_add, ref2_work_add, work_name, work_type, martial_status,
                    fb_number, fb_owner,
                    created_time, updated_time, account_state, is_col_contact, customer_channel
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        NOW(), NOW(), 'ACTIVE_IN_ARREARS', FALSE, 'DIRECT')
                ON CONFLICT (customer_id)
                DO UPDATE SET
                    customer_name = EXCLUDED.customer_name,
                    phone = EXCLUDED.phone,
                    gender = EXCLUDED.gender,
                    birth_day = EXCLUDED.birth_day,
                    national_id_number = EXCLUDED.national_id_number,
                    id_issue_date = EXCLUDED.id_issue_date,
                    id_issue_place = EXCLUDED.id_issue_place,
                    citizen_id_number = EXCLUDED.citizen_id_number,
                    citizen_issue_date = EXCLUDED.citizen_issue_date,
                    citizen_issue_place = EXCLUDED.citizen_issue_place,
                    per_address = EXCLUDED.per_address,
                    per_ward = EXCLUDED.per_ward,
                    per_district = EXCLUDED.per_district,
                    per_province = EXCLUDED.per_province,
                    cur_address = EXCLUDED.cur_address,
                    cur_ward = EXCLUDED.cur_ward,
                    cur_district = EXCLUDED.cur_district,
                    cur_province = EXCLUDED.cur_province,
                    ref1_name = EXCLUDED.ref1_name,
                    ref1_number = EXCLUDED.ref1_number,
                    ref1_relationship = EXCLUDED.ref1_relationship,
                    ref2_name = EXCLUDED.ref2_name,
                    ref2_number = EXCLUDED.ref2_number,
                    ref2_relationship = EXCLUDED.ref2_relationship,
                    tenor = EXCLUDED.tenor,
                    casa_account = EXCLUDED.casa_account,
                    ref1_work_add = EXCLUDED.ref1_work_add,
                    ref2_work_add = EXCLUDED.ref2_work_add,
                    work_name = EXCLUDED.work_name,
                    work_type = EXCLUDED.work_type,
                    martial_status = EXCLUDED.martial_status,
                    fb_number = EXCLUDED.fb_number,
                    fb_owner = EXCLUDED.fb_owner,
                    updated_time = NOW()
                """,
                (
                    handle_string(row['cus_id']),
                    handle_string(row['full_name']),
                    handle_string(row['customer_mobile']),
                    handle_string(row['gender']),
                    handle_timestamp(row['dob']),
                    handle_string(row['national_id_number']),
                    id_issue_date,
                    handle_string(row['id_issue_place']),
                    handle_string(row['citizen_id_number']),
                    handle_timestamp(row['citizen_issue_date']),
                    handle_string(row['citizen_issue_place']),
                    handle_string(row['per_address']),
                    handle_string(row['per_ward']),
                    handle_string(row['per_district']),
                    handle_string(row['per_province']),
                    handle_string(row['cur_address']),
                    handle_string(row['cur_ward']),
                    handle_string(row['cur_district']),
                    handle_string(row['cur_province']),
                    handle_string(row['ref1_name']),
                    handle_string(row['ref1_number']),
                    handle_string(row['ref1_relationship']),
                    handle_string(row['ref2_name']),
                    handle_string(row['ref2_number']),
                    handle_string(row['ref2_relationship']),
                    handle_number(row['tenor']),
                    handle_string(row['casa']),
                    handle_string(row['ref1_workadd']),
                    handle_string(row['ref2_workadd']),
                    handle_string(row['work_name']),
                    handle_string(row['work_type']),
                    handle_string(row['martial_status']),
                    handle_string(row['fb_number']),
                    handle_string(row['fb_owner'])
                )
            )
        except Exception as e:
            print(f"❌ Lỗi khi insert customer_id {row['cus_id']}: {e}")
            cursor.connection.rollback()  # Rollback nếu có lỗi
    logger.info(f"======================End upsert_case_customer_info_debt =======================")

# 📌 Hàm INSERT cho bảng case_payment_history
def insert_into_case_payment_history(df, cursor):
    logger.info(f"======================Start insert_into_case_payment_history =======================")

    # Bỏ cột đầu tiên nếu cần thiết
    df = df.iloc[:, 1:]

    for _, row in df.iterrows():

        if pd.isna(row["payment_date"]) or pd.isna(row["payment_amount"]):
            logger.warning(f"Bỏ qua bản ghi do thiếu dữ liệu: {row.to_dict()}")
            continue  # Bỏ qua bản ghi này và tiếp tục vòng lặp

        cursor.execute("""
            INSERT INTO collection_debt.case_payment_history (
                customer_id, contract_number, payment_date, ref_transaction_id,
                payment_amount, note, created_time, updated_time
            ) VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())  -- created_time & updated_time = NOW()
        """, (
            handle_string(row["cus_id"]),
            handle_string(row["case_code"]),
            handle_timestamp(row["payment_date"]),
            handle_string(row["ref_transaction_id"]),
            handle_number(row["payment_amount"]),
            handle_string(row["note"])
        ))

    logger.info(f"======================End insert_into_case_payment_history =======================")

# 📌 Hàm INSERT cho bảng case_payment_schedule
def insert_into_case_payment_schedule(df, cursor):
    logger.info(f"======================Start insert_into_case_payment_schedule =======================")

    # Bỏ cột đầu tiên và cột thứ năm
    df = df.iloc[:, 1:]  # Bỏ cột đầu tiên
    df = df.drop(df.columns[4], axis=1)  # Bỏ cột thứ năm (tính từ 0)

    # Định nghĩa hàm kiểm tra giá trị numeric hợp lệ
    def check_numeric(value):
        # Nếu giá trị là None hoặc NaN, trả về 0
        if value is None or pd.isna(value):
            return 0.0
        # Nếu giá trị là kiểu string với phần thập phân, ép kiểu thành float
        try:
            return float(value)
        except ValueError:
            return 0.0

    for _, row in df.iterrows():
        # Lấy các giá trị từ hàng và xử lý giá trị NULL
        customer_id = row.get('cus_id')  # Thay 'cus_id' bằng tên cột thực tế trong DataFrame
        contract_number = row.get('case_code')  # Thay 'case_code' bằng tên cột thực tế
        installment_number = row.get('installment_number')
        due_date = row.get('end_date')  # Thay 'end_date' bằng tên cột thực tế

        # Sử dụng hàm check_numeric để đảm bảo giá trị đúng định dạng
        principle_amt = check_numeric(row.get('principle_amt'))
        interest_amt = check_numeric(row.get('interest_amt'))
        other_fees = check_numeric(row.get('other_fees'))
        sum_amt = check_numeric(row.get('sum_amt'))
        principle_balance = check_numeric(row.get('principle_balance'))

        # Tạo tuple cho giá trị
        values = (
            customer_id,
            contract_number,
            installment_number,
            due_date,
            principle_amt,
            interest_amt,
            other_fees,
            sum_amt,
            principle_balance
        )

        # Thực hiện chèn
        cursor.execute("""
            INSERT INTO collection_debt.case_payment_schedule (
                customer_id, contract_number, installment_number, due_date,
                principle_amt, interest_amt, other_fees, sum_amt,
                principle_balance, created_time, updated_time
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        """, values)

    logger.info(f"======================End insert_into_case_payment_schedule =======================")

def check_file_exists_in_s3(bucket_name, key):
    s3 = boto3.client('s3')
    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        return True
    except Exception as e:
        logger.warning(f"File {key} không tồn tại trong S3 sẽ được bỏ qua: {str(e)}")
        return False

def lambda_handler(event, context):
    logger.info(f"Lambda function bắt đầu thực thi.")
    try:
        current_time = datetime.datetime.utcnow() + datetime.timedelta(hours=7)
        logger.info(f"current_time-----------{current_time}")
        formatted_time2y = current_time.strftime("%d%m%y")
        bucket_name = 'collection-partners-documents'
        prefix = f'data_direct_debt/data_{formatted_time2y}/'

        # Kiểm tra thư mục có tồn tại và có chứa file hay không
        has_file, file_count, file_names = check_directory_and_files_in_s3(bucket_name, prefix)

        if not has_file:
            logger.warning(f"Thư mục {prefix} không tồn tại hoặc không có file trong S3. Gửi email thông báo.")
            subject = f"Cảnh Báo Data Direct Athena"
            body = f"Thư mục {prefix} không tồn tại hoặc không chứa file trong S3. Vui lòng kiểm tra lại."
            send_email_notification(subject, body, "cuongnv37@tnexfinance.com.vn")
        else:
            logger.info(f"Thư mục {prefix} tồn tại và có {file_count} file trong S3.")

            # Kiểm tra nếu số lượng file nhỏ hơn 3 nhưng lớn hơn hoặc bằng 1
            if 1 <= file_count < 3:
                subject = f"Cảnh báo: Thư mục {prefix} có ít hơn 3 file"
                body = f"Thư mục {prefix} có {file_count} file. Các file trong thư mục là: {', '.join(file_names)}"
                send_email_notification(subject, body, "cuongnv37@tnexfinance.com.vn")

            # Thiết lập kết nối cơ sở dữ liệu PostgreSQL
            logger.info("Kết nối đến cơ sở dữ liệu PostgreSQL.")
            conn = psycopg2.connect(
                dbname=os.environ['DB_NAME'],
                user=os.environ['DB_USER'],
                password=os.environ['DB_PASSWORD'],
                host=os.environ['DB_HOST'],
                port=os.environ['DB_PORT']
            )
            conn.autocommit = False
            cursor = conn.cursor()

            try:
                current_time = datetime.datetime.utcnow()
                logger.info(f"current_time-----------{current_time}")
                # previous_day = current_time - datetime.timedelta(0)
                formatted_time2y = current_time.strftime("%d%m%y")

                logger.info(f"Formatted UTC time: {formatted_time2y}")

                s3 = boto3.client('s3')

                xlsx_files = [
                    f'data_direct_debt/data_{formatted_time2y}/case_schedule_{formatted_time2y}.xlsx',
                    f'data_direct_debt/data_{formatted_time2y}/cus_information_{formatted_time2y}.xlsx',
                    f'data_direct_debt/data_{formatted_time2y}/payment_history_{formatted_time2y}.xlsx'
                ]

                # Mapping tên file -> Bảng database
                table_mappings = {
                    "case_schedule": ["case_payment_schedule"],
                    "cus_information": ["case_collection_debt","case_customer_info_debt"],
                    "payment_history": ["case_payment_history"]
                }

                for file_path in xlsx_files:
                    if not check_file_exists_in_s3(bucket_name, file_path):
                        logger.warning(f"File {file_path} không tồn tại, bỏ qua.")
                        continue
                    logger.info(f"file path name =============: {file_path}")
                    file_name_with_ext = file_path.split("/")[-1]  # Lấy tên file đầy đủ
                    logger.info(f"file full name =============: {file_name_with_ext}")
                    if formatted_time2y not in file_name_with_ext:
                        logger.warning(f"File {file_name_with_ext} không chứa ngày {formatted_time2y}, gửi email cảnh báo và bỏ qua.")
                        subject = f"Cảnh báo: File {file_name_with_ext} không hợp lệ"
                        body = f"File {file_name_with_ext} không chứa ngày {formatted_time2y}. Vui lòng kiểm tra lại."
                        send_email_notification(subject, body, "cuongnv37@tnexfinance.com.vn")
                        continue
                    file_name = "_".join(file_name_with_ext.rsplit("_", 1)[:-1])  # Loại bỏ phần ngày tháng

                    logger.info(f"file name cuted=============: {file_name}")

                    table_names = table_mappings.get(file_name, None)
                    logger.info(f"table name =============: {table_names}")
                    if not table_names:
                        logger.warning(f"Không có bảng tương ứng cho file {file_name}, bỏ qua.")
                        continue

                    # Tải file từ S3
                    obj = s3.get_object(Bucket=bucket_name, Key=file_path)
                    file_content = obj["Body"].read()
                    df = pd.read_excel(BytesIO(file_content), engine="openpyxl",
                     dtype={'customer_mobile': str
                     ,'national_id_number': str
                     ,'citizen_id_number':str
                     ,'ref2_number':str
                     ,'ref1_number':str
                     })

                    # Gọi các hàm xử lý cho từng bảng
                    for table in table_names:
                        if table == "case_collection_debt":
                            insert_into_case_collection_debt(df, cursor)
                        elif table == "case_customer_info_debt":
                            upsert_case_customer_info_debt(df, cursor)
                        elif table == "case_payment_history":
                            insert_into_case_payment_history(df, cursor)
                        elif table == "case_payment_schedule":
                            insert_into_case_payment_schedule(df, cursor)

                # conn.commit()
                logger.info(f"Dữ liệu đã được ghi thành công vào PostgreSQL.")

            except Exception as e:
                conn.rollback()
                logger.error(f"Lỗi trong quá trình xử lý file: {str(e)}")
            finally:
                cursor.close()
                conn.close()
                logger.info("Đã đóng kết nối với PostgreSQL.")

    except Exception as e:
        logger.error(f"Lỗi kết nối đến PostgreSQL: {str(e)}")

