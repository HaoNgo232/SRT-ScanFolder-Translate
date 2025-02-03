# === IMPORTS ===
import os
import re
import json
import hashlib
import shutil
import subprocess
import argparse
import logging
import threading
import tkinter as tk
from tkinter import filedialog, ttk
from time import sleep
from random import uniform
from functools import partial
from googletrans import Translator
import pysrt
import time
import multiprocessing 
# === THAY ĐỔI TRONG PHẦN CONCURRENCY ===
from concurrent.futures import ThreadPoolExecutor  # Thay ProcessPool
from queue import Queue, Empty

# Thêm import cho Hugging Face (nếu sử dụng)
from transformers import MarianMTModel, MarianTokenizer, pipeline

# === LOGGING SETUP ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('translation.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# === CONFIGURATION DEFAULTS ===
IT_TERMS_FILE = "it_terms.txt"
OVERWRITE_ORIGINAL = True
EXCLUDED_DIRS = {"temp_translated", "backup", "translated"}
MAX_RETRIES = 3
CHUNK_SIZE = 4  # Số file xử lý đồng thời
USE_HF_TRANSLATOR = False  # Mặc định dùng Google Translate, option --hf bật transformer

# === UTILITY FUNCTIONS & HF TRANSLATOR ===
def get_hf_translator():
    """Khởi tạo pipeline dịch sử dụng model Helsinki-NLP/opus-mt-en-vi, sử dụng GPU nếu có"""
    try:
        model_name = "Helsinki-NLP/opus-mt-en-vi"
        translator_pipeline = pipeline("translation_en_to_vi", model=model_name, device=0)
        return translator_pipeline
    except Exception as e:
        logging.error(f"Lỗi khi khởi tạo HF Translator: {e}")
        raise

def get_translator():
    """Trả về đối tượng translator tùy theo chế độ được chọn"""
    if USE_HF_TRANSLATOR:
        return get_hf_translator()
    else:
        return Translator()

def translate_with_retry(translator, text, src='en', dest='vi', max_retries=MAX_RETRIES):
    """Dịch văn bản với cơ chế retry cho Google Translate"""
    for attempt in range(max_retries):
        try:
            result = translator.translate(text, src=src, dest=dest)
            return result.text
        except Exception as e:
            if attempt == max_retries - 1:
                logging.error(f"Lỗi dịch sau {max_retries} lần thử: {e}")
                raise
            logging.warning(f"Lỗi dịch lần {attempt + 1}, thử lại sau: {e}")
            sleep(uniform(1, 3))
    return text

def hf_translate_with_retry(translator, text, max_retries=MAX_RETRIES):
    """Dịch văn bản với cơ chế retry cho HF translator"""
    for attempt in range(max_retries):
        try:
            result = translator(text)[0]['translation_text']
            return result
        except Exception as e:
            if attempt == max_retries - 1:
                logging.error(f"Lỗi dịch HF sau {max_retries} lần thử: {e}")
                raise
            logging.warning(f"Lỗi dịch HF lần {attempt + 1}, thử lại sau: {e}")
            sleep(uniform(1, 3))
    return text

# === CLASSES ===

# Thêm class TransactionLog để quản lý rollback
class TransactionLog:
    def __init__(self):
        self.log = []
    
    def add_entry(self, action, src, dest):
        self.log.append((action, src, dest))
    
    def rollback(self):
        for action, src, dest in reversed(self.log):
            if action == "copy":
                if os.path.exists(dest):
                    shutil.move(dest, src)
            elif action == "move":
                shutil.move(src, dest)

class TranslationHistory:
    def __init__(self):
        self.history = {}
        self.current_index = {}
        
    def add_version(self, file_path, content):
        if file_path not in self.history:
            self.history[file_path] = []
            self.current_index[file_path] = -1
            
        current = self.current_index[file_path]
        if current < len(self.history[file_path]) - 1:
            self.history[file_path] = self.history[file_path][:current + 1]
            
        self.history[file_path].append(content)
        self.current_index[file_path] = len(self.history[file_path]) - 1
        
    def undo(self, file_path):
        if file_path in self.current_index and self.current_index[file_path] > 0:
            self.current_index[file_path] -= 1
            return self.history[file_path][self.current_index[file_path]]
        return None
        
    def redo(self, file_path):
        if (file_path in self.current_index and 
            self.current_index[file_path] < len(self.history[file_path]) - 1):
            self.current_index[file_path] += 1
            return self.history[file_path][self.current_index[file_path]]
        return None

class PreviewDialog:
    def __init__(self, parent, original_text, translated_text):
        self.dialog = tk.Toplevel(parent)
        self.dialog.title("Translation Preview")
        
        # Original text
        original_frame = ttk.LabelFrame(self.dialog, text="Original")
        original_frame.pack(padx=5, pady=5, fill="both", expand=True)
        
        self.original_text = tk.Text(original_frame, height=10)
        self.original_text.insert("1.0", original_text)
        self.original_text.config(state="disabled")
        self.original_text.pack(padx=5, pady=5, fill="both", expand=True)
        
        # Translated text
        translated_frame = ttk.LabelFrame(self.dialog, text="Translated")
        translated_frame.pack(padx=5, pady=5, fill="both", expand=True)
        
        self.translated_text = tk.Text(translated_frame, height=10)
        self.translated_text.insert("1.0", translated_text)
        self.translated_text.pack(padx=5, pady=5, fill="both", expand=True)
        
        # Buttons
        button_frame = ttk.Frame(self.dialog)
        button_frame.pack(padx=5, pady=5)
        
        ttk.Button(button_frame, text="Accept", command=self.accept).pack(side="left", padx=5)
        ttk.Button(button_frame, text="Edit", command=self.edit).pack(side="left", padx=5)
        ttk.Button(button_frame, text="Cancel", command=self.cancel).pack(side="left", padx=5)
        
        self.result = None
        
    def accept(self):
        self.result = self.translated_text.get("1.0", "end-1c")
        self.dialog.destroy()
        
    def edit(self):
        self.translated_text.config(state="normal")
        
    def cancel(self):
        self.dialog.destroy()

class TranslatorGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Subtitle Translator")
        self.history = TranslationHistory()
        # self.cancel_event = multiprocessing.Event()
        self.cancel_flag = threading.Event()  # Dùng threading.Event thay multiprocessing
        self.progress_queue = Queue()  # Queue cập nhật tiến trình
        
        # Input folder selection
        self.folder_frame = ttk.LabelFrame(root, text="Input Folder")
        self.folder_frame.pack(padx=5, pady=5, fill="x")
        
        self.folder_path = tk.StringVar()
        self.folder_entry = ttk.Entry(self.folder_frame, textvariable=self.folder_path)
        self.folder_entry.pack(side="left", padx=5, pady=5, fill="x", expand=True)
        
        self.browse_btn = ttk.Button(self.folder_frame, text="Browse", command=self.browse_folder)
        self.browse_btn.pack(side="right", padx=5, pady=5)
        
        # Options frame
        self.options_frame = ttk.LabelFrame(root, text="Options")
        self.options_frame.pack(padx=5, pady=5, fill="x")
        
        self.overwrite_var = tk.BooleanVar(value=OVERWRITE_ORIGINAL)
        self.overwrite_check = ttk.Checkbutton(
            self.options_frame, 
            text="Overwrite original files",
            variable=self.overwrite_var
        )
        self.overwrite_check.pack(padx=5, pady=5)
        
        self.hf_var = tk.BooleanVar(value=USE_HF_TRANSLATOR)
        self.hf_check = ttk.Checkbutton(
            self.options_frame,
            text="Use HF Transformer",
            variable=self.hf_var
        )
        self.hf_check.pack(padx=5, pady=5)
        
        # Progress frame
        self.progress_frame = ttk.LabelFrame(root, text="Progress")
        self.progress_frame.pack(padx=5, pady=5, fill="x")
        
        self.progress_var = tk.DoubleVar()
        self.progress = ttk.Progressbar(
            self.progress_frame, 
            variable=self.progress_var, 
            maximum=100
        )
        self.progress.pack(padx=5, pady=5, fill="x")
        
        # Status
        self.status_var = tk.StringVar(value="Ready")
        self.status_label = ttk.Label(
            self.progress_frame, 
            textvariable=self.status_var
        )
        self.status_label.pack(padx=5, pady=5)
        
        # Buttons frame
        self.button_frame = ttk.Frame(root)
        self.button_frame.pack(padx=5, pady=5)
        
        self.translate_btn = ttk.Button(
            self.button_frame, 
            text="Translate", 
            command=self.start_translation
        )
        self.translate_btn.pack(side="left", padx=5)
        
        self.cancel_btn = ttk.Button(
            self.button_frame, 
            text="Cancel", 
            command=self.cancel_translation,
            state="disabled"
        )
        self.cancel_btn.pack(side="left", padx=5)
        
        # Translation state
        self.translation_running = False

    def browse_folder(self):
        folder = filedialog.askdirectory()
        if folder:
            self.folder_path.set(folder)
            self.status_var.set("Folder selected")

    def update_progress(self, current, total):
        progress = (current / total) * 100
        self.progress_var.set(progress)
        self.root.update_idletasks()

    def start_translation(self):
        folder = self.folder_path.get()
        if not folder:
            self.status_var.set("Please select a folder")
            return
            
        self.translation_running = True
        self.translate_btn.state(['disabled'])
        self.cancel_btn.state(['!disabled'])
        self.status_var.set("Translating...")
        
        # # Start translation in a separate thread
        # self.translation_thread = threading.Thread(
        #     target=self.translate_folder,
        #     args=(folder,)
        # )
        # self.translation_thread.start()

        # Khởi chạy listener cho queue
        self.start_progress_listener()
        self.translation_thread = threading.Thread(
            target=self.translate_folder, args=(folder,)
        )
        self.translation_thread.start()

    # def cancel_translation(self):
    #     if self.translation_running:
    #         self.translation_running = False
    #         self.status_var.set("Canceling translation...")
    #         self.cancel_btn.state(['disabled'])

    # def translate_folder(self, folder):
    #      progress_queue = Queue()
    
    # def progress_listener():
    #     while True:
    #         try:
    #             current, total = progress_queue.get_nowait()
    #             self.progress_var.set((current/total)*100)
    #             self.root.update()
    #         except Empty:
    #             if not self.translation_running:
    #                 break
    #             time.sleep(0.1)
    
    #     listener_thread = threading.Thread(target=progress_listener)
    #     listener_thread.start()
    
    #     try:
    #         batch_translate(
    #             folder,
    #             progress_callback=lambda c,t: progress_queue.put((c,t)),
    #             cancel_event=self.cancel_event
    #         )
    #     finally:
    #         self.translation_running = False
    #         listener_thread.join()

    def start_progress_listener(self):
        def listener():
            while True:
                try:
                    current, total = self.progress_queue.get_nowait()
                    self.progress_var.set((current / total) * 100)
                    self.root.update()
                except Empty:
                    if not self.translation_running:
                        break
                    sleep(0.1)
        threading.Thread(target=listener, daemon=True).start()

    def translate_folder(self, folder):
        try:
            batch_translate(
                folder,
                progress_callback=lambda c, t: self.progress_queue.put((c, t)),
                cancel_flag=self.cancel_flag  # Truyền Event hủy
            )
        finally:
            self.translation_running = False

    def cancel_translation(self):
        self.cancel_flag.set()  # Kích hoạt hủy
        self.status_var.set("Đang dừng...")



# === UTILITY FUNCTIONS (Duplicate definitions removed for brevity; use the ones above) ===
def load_it_terms():
    try:
        with open(IT_TERMS_FILE, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        logging.warning(f"Không tìm thấy file {IT_TERMS_FILE}, sử dụng danh sách mặc định")
        return ["server", "API", "cloud"]

def compute_checksum(file_path):
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def load_translated_checksums(checksum_file):
    if os.path.exists(checksum_file):
        with open(checksum_file, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_translated_checksums(data, checksum_file):
    with open(checksum_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# === TRANSLATION FUNCTIONS ===
def translate_with_retry(translator, text, src='en', dest='vi', max_retries=MAX_RETRIES):
    for attempt in range(max_retries):
        try:
            result = translator.translate(text, src=src, dest=dest)
            return result.text
        except Exception as e:
            if attempt == max_retries - 1:
                logging.error(f"Lỗi dịch sau {max_retries} lần thử: {e}")
                raise
            logging.warning(f"Lỗi dịch lần {attempt + 1}, thử lại sau: {e}")
            sleep(uniform(1, 3))
    return text

def hf_translate_with_retry(translator, text, max_retries=MAX_RETRIES):
    for attempt in range(max_retries):
        try:
            result = translator(text)[0]['translation_text']
            return result
        except Exception as e:
            if attempt == max_retries - 1:
                logging.error(f"Lỗi dịch HF sau {max_retries} lần thử: {e}")
                raise
            logging.warning(f"Lỗi dịch HF lần {attempt + 1}, thử lại sau: {e}")
            sleep(uniform(1, 3))
    return text

def translate_subtitle_file(translator, input_path, output_path, it_terms):
    subs = pysrt.open(input_path, encoding='utf-8')
    pattern = re.compile(r'\b(' + '|'.join(map(re.escape, it_terms)) + r')\b', flags=re.IGNORECASE)
    
    for sub in subs:
        try:
            marked_text = pattern.sub(lambda x: f"__{x.group()}__", sub.text)
            if USE_HF_TRANSLATOR:
                translated = hf_translate_with_retry(translator, marked_text)
            else:
                translated = translate_with_retry(translator, marked_text)
            sub.text = re.sub(
                r'__({})__'.format('|'.join(map(re.escape, it_terms))), 
                r'\1', 
                translated, 
                flags=re.IGNORECASE
            )
        except Exception as e:
            logging.error(f"Lỗi khi dịch subtitle: {e}")
            sub.text = sub.text
    subs.save(output_path, encoding='utf-8')

def translate_batch(files_chunk, it_terms, temp_folder, progress_callback=None, cancel_flag=None, transaction_log=None):
    translator = get_translator()
    results = []
    # for file in files_chunk:
    for i, (rel_path, full_path) in enumerate(files_chunk):  # Giải nén rel_path và full_path
        if cancel_flag and cancel_flag.is_set():
            raise Exception("Người dùng hủy")
        try:
            temp_output_path = os.path.join(temp_folder, rel_path)
            os.makedirs(os.path.dirname(temp_output_path), exist_ok=True)
            translate_subtitle_file(translator, full_path, temp_output_path, it_terms)
            results.append((True, rel_path, temp_output_path))  # Thêm temp_output_path vào kết quả
            logging.info(f"Đã dịch xong file {rel_path}")
            transaction_log.add_entry("move", temp_output_path, full_path)
        except Exception as e:
            logging.error(f"Lỗi file {file}: {e}")
            transaction_log.add_entry("delete", temp_output_path, None)
            raise
        if progress_callback:
            progress_callback(i + 1, len(files_chunk))
    return results

def batch_translate(input_folder, progress_callback=None, cancel_flag=None):
    transaction_log = TransactionLog()  # Thêm log transaction
    if not os.access(input_folder, os.W_OK):
        raise PermissionError(f"Không có quyền ghi vào folder {input_folder}")
    it_terms = load_it_terms()
    logging.info(f"Đã tải {len(it_terms)} từ khóa IT")
    temp_folder = os.path.join(input_folder, "temp_translated")
    backup_folder = os.path.join(input_folder, "backup")
    dest_folder = os.path.join(input_folder, "translated")
    os.makedirs(temp_folder, exist_ok=True)
    if OVERWRITE_ORIGINAL:
        os.makedirs(backup_folder, exist_ok=True)
    else:
        os.makedirs(dest_folder, exist_ok=True)
    checksum_file = os.path.join(input_folder, "translated_files.json")
    translated_checksums = load_translated_checksums(checksum_file)
    files_to_process = []
    for root, dirs, files in os.walk(input_folder):
        dirs[:] = [d for d in dirs if d not in EXCLUDED_DIRS]
        for filename in files:
            if filename.lower().endswith(".srt"):
                full_path = os.path.join(root, filename)
                rel_path = os.path.relpath(full_path, input_folder)
                current_checksum = compute_checksum(full_path)
                if rel_path in translated_checksums and translated_checksums[rel_path] == current_checksum:
                    logging.info(f"Bỏ qua {rel_path} vì đã được dịch")
                    continue
                files_to_process.append((rel_path, full_path))
    if not files_to_process:
        logging.info("Không có file nào cần dịch")
        return
    logging.info(f"Tìm thấy {len(files_to_process)} file cần dịch")
    files_chunks = [
        files_to_process[i:i + CHUNK_SIZE] 
        for i in range(0, len(files_to_process), CHUNK_SIZE)
    ]
    all_success = True
    processed_files = []
    try:
         with ThreadPoolExecutor(max_workers=CHUNK_SIZE) as executor:
            futures = []
            for chunk in files_chunks:
                # if cancel_flag and cancel_flag():
                if cancel_flag and cancel_flag.is_set():
                    logging.info("Người dùng hủy quá trình dịch")
                    break
                future = executor.submit(
                    lambda: translate_batch(
                        chunk,
                        it_terms,
                        temp_folder,
                        progress_callback=progress_callback,
                        cancel_flag=cancel_flag,
                        transaction_log=transaction_log
                    )
                )
                futures.append(future)
            # Xử lý kết quả
            for future in futures:
                if cancel_flag and cancel_flag.is_set():
                    break
                results = future.result()
                for success, rel_path, temp_output_path in results:
                    if success:
                        final_path = os.path.join(input_folder, rel_path)
                        transaction_log.add_entry("move", temp_output_path, final_path)
                    else:
                        logging.error("Không thể thêm entry vào transaction_log vì thiếu temp_output_path hoặc final_path")
                if not files_to_process:
                    logging.info("Không có file nào cần dịch")
                    return
                # # Thêm vào transaction log
                # for result in results:
                #     if result[0]:
                #         transaction_log.add_entry("move", temp_output_path, final_path)
    except Exception as e:
        logging.error(f"Lỗi chính: {e}")
        transaction_log.rollback()  # Rollback toàn bộ nếu lỗi
        raise
    if all_success and not (cancel_flag and cancel_flag()):
        for rel_path in processed_files:
            temp_output_path = os.path.join(temp_folder, rel_path)
            full_path = os.path.join(input_folder, rel_path)
            if OVERWRITE_ORIGINAL:
                backup_path = os.path.join(backup_folder, rel_path + ".bak")
                os.makedirs(os.path.dirname(backup_path), exist_ok=True)
                shutil.copy2(full_path, backup_path)
                shutil.move(temp_output_path, full_path)
                logging.info(f"Đã ghi đè file {rel_path}")
            else:
                dest_output_path = os.path.join(dest_folder, rel_path)
                os.makedirs(os.path.dirname(dest_output_path), exist_ok=True)
                shutil.move(temp_output_path, dest_output_path)
                logging.info(f"Đã lưu file dịch {rel_path} vào {dest_folder}")
            final_path = full_path if OVERWRITE_ORIGINAL else dest_output_path
            new_checksum = compute_checksum(final_path)
            translated_checksums[rel_path] = new_checksum
        save_translated_checksums(translated_checksums, checksum_file)
        logging.info("Hoàn thành quá trình dịch")
    else:
        for rel_path in processed_files:
            temp_output_path = os.path.join(temp_folder, rel_path)
            if os.path.exists(temp_output_path):
                os.remove(temp_output_path)
        logging.warning("Quá trình dịch bị hủy hoặc có lỗi")
    if os.path.isdir("/usr/lib") and input_folder.startswith("/usr"):
        logging.error("Không thể ghi vào system directory không có sudo")
        raise PermissionError
    
    try:
        # Trong phần move file
        if OVERWRITE_ORIGINAL:
            rollback_manager.add_backup(full_path, backup_path)
            shutil.move(...)
    except Exception as e:
        rollback_manager.rollback()
        logging.error("Đã rollback toàn bộ thay đổi do lỗi")

def select_folder_zenity():
    try:
        # Thêm tham số --filename=/ để hiển thị root
        result = subprocess.check_output(
            ["zenity", "--file-selection", "--directory", 
             "--title=Chọn folder chứa file phụ đề (.srt)",
             "--filename=/"],
            text=True
        ).strip()
        return result if os.path.isdir(result) else None
    except subprocess.CalledProcessError:
        return None
def main():
    global OVERWRITE_ORIGINAL, MAX_RETRIES, CHUNK_SIZE, USE_HF_TRANSLATOR
    parser = argparse.ArgumentParser(description="Công cụ dịch phụ đề với bảo toàn thuật ngữ IT")
    parser.add_argument("--folder", type=str, help="Đường dẫn folder chứa file phụ đề (.srt)")
    parser.add_argument("--gui", action="store_true", help="Chạy với giao diện đồ họa")
    parser.add_argument("--overwrite", action="store_true", help="Bật chế độ ghi đè file gốc (mặc định là bật)")
    parser.add_argument("--max-retries", type=int, default=MAX_RETRIES, help="Số lần thử khi dịch (mặc định: 3)")
    parser.add_argument("--chunk-size", type=int, default=CHUNK_SIZE, help="Số file xử lý đồng thời (mặc định: 4)")
    parser.add_argument("--hf", action="store_true", help="Sử dụng Hugging Face Transformer thay cho Google Translate")
    args = parser.parse_args()

    OVERWRITE_ORIGINAL = args.overwrite or OVERWRITE_ORIGINAL
    MAX_RETRIES = args.max_retries
    CHUNK_SIZE = args.chunk_size
    USE_HF_TRANSLATOR = args.hf

    if args.gui:
        root = tk.Tk()
        app = TranslatorGUI(root)
        root.mainloop()
    else:
        if args.folder:
            input_folder = args.folder
        else:
            input_folder = select_folder_zenity()
            if not input_folder:
                logging.error("Không chọn được folder. Hãy chỉ định folder qua tham số --folder")
                return
        logging.info(f"Folder được chọn: {input_folder}")
        try:
            batch_translate(input_folder)
        except Exception as e:
            logging.error(f"Lỗi: {e}")

if __name__ == "__main__":
    main()
