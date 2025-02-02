import os
import re
import json
import hashlib
import shutil
import subprocess
import argparse
from googletrans import Translator
import pysrt

# ===== CẤU HÌNH BAN ĐẦU =====
IT_TERMS_FILE = "it_terms.txt"   # File chứa từ cần giữ nguyên (nên đặt trong cùng folder với file script hoặc đường dẫn đầy đủ)
OVERWRITE_ORIGINAL = True         # True: ghi đè file gốc / False: tạo file mới
# ==========================

def load_it_terms():
    """Đọc từ khóa cần giữ nguyên từ file IT_TERMS_FILE với encoding UTF-8"""
    try:
        with open(IT_TERMS_FILE, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        return ["server", "API", "cloud"]  # Danh sách mặc định nếu không có file

def compute_checksum(file_path):
    """Tính MD5 checksum của file"""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def load_translated_checksums(checksum_file):
    """Tải thông tin file đã dịch từ file lưu checksum"""
    if os.path.exists(checksum_file):
        with open(checksum_file, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}  # trả về dict rỗng nếu file không tồn tại

def save_translated_checksums(data, checksum_file):
    """Lưu thông tin file đã dịch vào file lưu checksum với encoding UTF-8"""
    with open(checksum_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def translate_subtitle_file(input_path, output_path, it_terms):
    """Dịch 1 file phụ đề (.srt) từ tiếng Anh sang tiếng Việt và giữ nguyên các từ khóa IT"""
    # Đảm bảo mở và lưu file với encoding UTF-8 để xử lý tốt font chữ
    subs = pysrt.open(input_path, encoding='utf-8')
    translator = Translator()
    # Tạo regex pattern để đánh dấu từ khóa (không phân biệt hoa thường)
    pattern = re.compile(r'\b(' + '|'.join(map(re.escape, it_terms)) + r')\b', flags=re.IGNORECASE)
    
    for sub in subs:
        # Bước 1: Đánh dấu từ khóa (ví dụ: API → __API__)
        marked_text = pattern.sub(lambda x: f"__{x.group()}__", sub.text)
        # Bước 2: Dịch từ tiếng Anh sang tiếng Việt
        translated = translator.translate(marked_text, src='en', dest='vi').text
        # Bước 3: Khôi phục từ khóa
        sub.text = re.sub(r'__({})__'.format('|'.join(map(re.escape, it_terms))), r'\1', translated, flags=re.IGNORECASE)
    
    subs.save(output_path, encoding='utf-8')

def batch_translate(input_folder):
    """
    Dịch hàng loạt file phụ đề (.srt) trong folder theo cơ chế transaction và kiểm tra checksum.
    Các file backup, file tạm và file lưu checksum sẽ được tạo trong folder đã chọn.
    """
    # Kiểm tra quyền ghi vào folder đầu vào
    if not os.access(input_folder, os.W_OK):
        print(f"Lỗi: Không có quyền ghi vào folder {input_folder}!")
        return

    it_terms = load_it_terms()

    # Xác định các folder con dùng để lưu file tạm và backup trong folder đã chọn
    temp_folder = os.path.join(input_folder, "temp_translated")
    backup_folder = os.path.join(input_folder, "backup")
    dest_folder = os.path.join(input_folder, "translated")  # Dùng nếu OVERWRITE_ORIGINAL == False
    os.makedirs(temp_folder, exist_ok=True)
    if OVERWRITE_ORIGINAL:
        os.makedirs(backup_folder, exist_ok=True)
    else:
        os.makedirs(dest_folder, exist_ok=True)

    # File lưu checksum được đặt trong folder đã chọn
    checksum_file = os.path.join(input_folder, "translated_files.json")
    translated_checksums = load_translated_checksums(checksum_file)

    # Duyệt các file .srt trong folder đã chọn
    files_to_process = []
    for filename in os.listdir(input_folder):
        if filename.lower().endswith(".srt"):
            file_path = os.path.join(input_folder, filename)
            current_checksum = compute_checksum(file_path)
            # Nếu file đã được dịch (với checksum khớp) thì bỏ qua
            if filename in translated_checksums and translated_checksums[filename] == current_checksum:
                print(f"Bỏ qua {filename} vì đã được dịch.")
                continue
            files_to_process.append((filename, file_path))
    
    if not files_to_process:
        print("Không có file nào cần dịch.")
        return

    all_success = True

    try:
        # Dịch từng file và lưu kết quả vào folder tạm
        for filename, input_path in files_to_process:
            temp_output_path = os.path.join(temp_folder, filename)
            print(f"Dịch file {filename}...")
            translate_subtitle_file(input_path, temp_output_path, it_terms)
    except Exception as e:
        print("Có lỗi xảy ra trong quá trình dịch:", e)
        all_success = False

    if all_success:
        # Nếu dịch thành công, tiến hành commit thay đổi
        for filename, input_path in files_to_process:
            temp_output_path = os.path.join(temp_folder, filename)
            if OVERWRITE_ORIGINAL:
                # Backup file gốc vào folder backup
                backup_path = os.path.join(backup_folder, filename + ".bak")
                shutil.copy2(input_path, backup_path)
                shutil.move(temp_output_path, input_path)
                print(f"Ghi đè file {filename} thành công. Backup được lưu tại {backup_path}.")
            else:
                shutil.move(temp_output_path, os.path.join(dest_folder, filename))
                print(f"Đã lưu file dịch {filename} vào folder {dest_folder}.")

            # Cập nhật checksum của file đã dịch (sau khi ghi đè hay lưu file dịch)
            final_path = input_path if OVERWRITE_ORIGINAL else os.path.join(dest_folder, filename)
            new_checksum = compute_checksum(final_path)
            translated_checksums[filename] = new_checksum

        # Lưu lại thông tin checksum
        save_translated_checksums(translated_checksums, checksum_file)
        print("Tất cả file đã được dịch thành công và ghi đè/lưu file mới.")
    else:
        print("Quá trình dịch gặp lỗi. Không ghi đè file gốc.")
        # Xoá các file tạm nếu cần
        for filename, _ in files_to_process:
            temp_output_path = os.path.join(temp_folder, filename)
            if os.path.exists(temp_output_path):
                os.remove(temp_output_path)

def select_folder_zenity():
    """Chọn folder bằng Zenity (GUI native của Linux, hỗ trợ hiển thị ổ đĩa ngoài)"""
    try:
        result = subprocess.check_output(
            ["zenity", "--file-selection", "--directory", "--title=Chọn folder chứa file phụ đề (.srt)"],
            text=True
        ).strip()
        return result if os.path.isdir(result) else None
    except subprocess.CalledProcessError:
        return None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--folder", type=str, help="Đường dẫn folder chứa file phụ đề (.srt)")
    args = parser.parse_args()

    if args.folder:
        input_folder = args.folder
    else:
        # Ưu tiên dùng Zenity nếu có, nếu không có sẽ báo lỗi và thoát
        input_folder = select_folder_zenity()
        if not input_folder:
            print("Không chọn được folder qua Zenity. Bạn hãy chỉ định folder qua tham số --folder hoặc kiểm tra cài đặt Zenity.")
            return

    print("Folder được chọn:", input_folder)
    batch_translate(input_folder)

if __name__ == "__main__":
    main()
