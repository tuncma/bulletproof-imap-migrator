import imaplib
import re
import os
import time
import json
import shlex
import atexit
import signal
from datetime import datetime, timedelta
from dotenv import load_dotenv

# .env dosyasını yükle / Load .env file
load_dotenv()

# --- AYARLAR / SETTINGS ---
SRC_HOST = os.getenv('SRC_HOST')
SRC_USER = os.getenv('SRC_USER')
SRC_PASS = os.getenv('SRC_PASS')

DST_HOST = os.getenv('DST_HOST')
DST_USER = os.getenv('DST_USER')
DST_PASS = os.getenv('DST_PASS')

# --- LOG DOSYALARI / LOG FILES ---
STATE_JSON = 'migration_state.json'
SEEN_TXT = 'seen_message_ids.txt'

# --- BEKLEME AYARLARI / BACKOFF SETTINGS ---
BATCH_SIZE = 1000                       # Her 1000 mailde bir bekleme / Wait every 1000 emails
BATCH_WAIT = 30                         # Batch bekleme süresi (sn) / Batch wait time (sec)
BACKOFF_SEQUENCE = [30, 60, 120, 300]   # Bağlantı kopması bekleme dizisi / Connection drop wait sequence


# ============================================================
#  LOG FONKSİYONU / LOG FUNCTION
# ============================================================

def log(msg, level="INFO"):
    """Zaman damgalı log satırı yazdır / Print timestamped log line."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{level}] {msg}")


def format_size(size_bytes):
    """Byte değerini okunabilir formata çevir / Convert bytes to readable format."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"


def format_duration(seconds):
    """Saniye değerini okunabilir süre formatına çevir / Convert seconds to readable duration."""
    if seconds < 60:
        return f"{seconds:.0f} sn/sec"
    elif seconds < 3600:
        m = int(seconds // 60)
        s = int(seconds % 60)
        return f"{m} dk/min {s} sn/sec"
    else:
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        return f"{h} saat/hr {m} dk/min"


# ============================================================
#  DURUM YÖNETİMİ / STATE MANAGEMENT
# ============================================================

def load_seen_ids():
    """Daha önce aktarılan Message-ID'leri diskten oku / Load previously migrated Message-IDs."""
    if os.path.exists(SEEN_TXT):
        with open(SEEN_TXT, 'r', encoding='utf-8') as f:
            ids = set(line.strip() for line in f if line.strip())
        log(f"Daha önce aktarılmış {len(ids)} adet Message-ID yüklendi. / Loaded {len(ids)} previously migrated Message-IDs.", "INFO")
        return ids
    log("İlk çalıştırma — henüz aktarılmış mail kaydı yok. / First run — no previous records.", "INFO")
    return set()


seen_file_handle = None


def init_seen_file():
    """Seen dosyasını append modunda aç / Open seen file in append mode."""
    global seen_file_handle
    seen_file_handle = open(SEEN_TXT, 'a', encoding='utf-8')


def close_seen_file():
    """Seen dosyasını güvenli şekilde kapat / Safely close seen file."""
    global seen_file_handle
    if seen_file_handle:
        try:
            seen_file_handle.flush()
            seen_file_handle.close()
        except Exception:
            pass
        seen_file_handle = None


# Program çökse bile dosya handle'ını kapat / Close handle even on crash
atexit.register(close_seen_file)


# ============================================================
#  CTRL+C GÜVENLİ KAPANMA / GRACEFUL SHUTDOWN
# ============================================================

shutdown_requested = False


def handle_shutdown_signal(signum, frame):
    """Ctrl+C yakalandığında temiz çıkış yap / Handle Ctrl+C for graceful exit."""
    global shutdown_requested
    if shutdown_requested:
        log("İKİNCİ CTRL+C ALGILANDI — Acil çıkış! / SECOND CTRL+C — Emergency exit!", "ACİL/EMERGENCY")
        save_state(folder_state)
        close_seen_file()
        raise SystemExit(1)
    shutdown_requested = True
    log("", "")
    log("═" * 55, "CTRL+C")
    log("CTRL+C ALGILANDI! / CTRL+C DETECTED!", "CTRL+C")
    log("Mevcut işlem tamamlanıyor... / Finishing current task...", "CTRL+C")
    log("Durum kaydedilecek ve güvenli çıkış yapılacak. / Saving state for safe exit.", "CTRL+C")
    log("(Acil çıkış için tekrar Ctrl+C basın / Press Ctrl+C again for emergency exit)", "CTRL+C")
    log("═" * 55, "CTRL+C")


signal.signal(signal.SIGINT, handle_shutdown_signal)


def save_seen_id(msg_id):
    """Tek bir Message-ID'yi diske yaz / Write a single Message-ID to disk."""
    if seen_file_handle:
        seen_file_handle.write(msg_id + '\n')
        seen_file_handle.flush()


def load_state():
    """Klasör bazlı ilerleme durumunu JSON'dan oku / Load folder state from JSON."""
    if os.path.exists(STATE_JSON):
        with open(STATE_JSON, 'r', encoding='utf-8') as f:
            state = json.load(f)
        total_prev = sum(v.get("migrated", 0) for v in state.values())
        log(f"Önceki oturum durumu yüklendi. / Previous state loaded. Toplam/Total: {total_prev} mail.", "INFO")
        return state
    log("İlk çalıştırma — önceki durum bulunamadı. / First run — no previous state.", "INFO")
    return {}


def save_state(state_dict):
    """Durumu atomik olarak diske yaz / Atomically save state to disk."""
    tmp_path = STATE_JSON + '.tmp'
    with open(tmp_path, 'w', encoding='utf-8') as f:
        json.dump(state_dict, f, indent=4, ensure_ascii=False)
    os.replace(tmp_path, STATE_JSON)


# Global durum / Global state
seen_ids = load_seen_ids()
folder_state = load_state()


# ============================================================
#  IMAP YARDIMCI FONKSİYONLARI / IMAP HELPERS
# ============================================================

def parse_list_response(list_response_bytes):
    """Güvenli IMAP LIST ayrıştırıcı / Safe IMAP LIST parser."""
    if isinstance(list_response_bytes, bytes):
        raw_str = list_response_bytes.decode('utf-8', errors='ignore')
    else:
        raw_str = str(list_response_bytes)

    paren_match = re.match(r'\((.*?)\)\s+(.*)', raw_str)
    if paren_match:
        flags = paren_match.group(1).split()
        rest = paren_match.group(2)
        try:
            parts = shlex.split(rest)
            delim = parts[0]
            if delim.upper() == 'NIL':
                delim = '.'
            name = parts[1] if len(parts) > 1 else ''
            return flags, delim, name
        except Exception:
            pass

    flags_match = re.search(r'\((.*?)\)', raw_str)
    flags = flags_match.group(1).split() if flags_match else []
    name_match = re.search(r'"\s+"?(.*?)"?$', raw_str)
    if name_match:
        name = name_match.group(1)
    else:
        name = raw_str.split(' "/" ')[-1].split(' "|" ')[-1].strip('"')
    return flags, '|', name


def get_dst_delimiter(dst):
    """Hedef sunucu klasör ayırıcısını öğren / Get destination delimiter."""
    try:
        typ, data = dst.list('""', '*')
        if typ == 'OK' and data and data[0]:
            _flags, delim, _name = parse_list_response(data[0])
            log(f"Hedef ayırıcı / Destination delimiter: '{delim}'", "INFO")
            return delim if delim else '.'
    except Exception:
        pass
    return '.'


def determine_target_folder(flags, src_folder, src_delim, dst_delim):
    """Klasör adını hedefe göre eşle / Map folder name for destination."""
    parts = src_folder.split(src_delim) if src_delim in src_folder else [src_folder]
    base_folder = parts[0]
    rest_path = parts[1:]

    if base_folder.upper() == 'INBOX':
        mapped_base = 'INBOX'
    else:
        mapped_base = base_folder
        flag_map = {
            '\\Sent': 'Sent',
            '\\Trash': 'Trash',
            '\\Drafts': 'Drafts',
            '\\Junk': 'Junk',
            '\\Archive': 'Archive',
        }
        ignored_flags = {
            '\\HasNoChildren', '\\HasChildren',
            '\\Marked', '\\Unmarked',
            '\\Noselect', '\\NonExistent',
        }
        for flag in flags:
            clean_flag = flag.strip()
            if clean_flag in ignored_flags:
                continue
            if clean_flag in flag_map:
                mapped_base = flag_map[clean_flag]
                break

    if rest_path:
        return mapped_base + dst_delim + dst_delim.join(rest_path)
    return mapped_base


def connect_imap(host, user, password):
    """IMAP SSL bağlantısı kur / Establish IMAP SSL connection."""
    conn = imaplib.IMAP4_SSL(host)
    conn.login(user, password)
    try:
        conn.enable("UTF8=ACCEPT")
    except Exception:
        pass
    return conn


def safe_logout(conn):
    """Bağlantıyı kapat / Close connection safely."""
    if conn is None:
        return
    try:
        conn.logout()
    except Exception:
        pass


def build_flag_string(raw_response_info):
    """Bayrakları APPEND string'ine çevir / Convert flags to APPEND string."""
    flags_tuple = imaplib.ParseFlags(raw_response_info)
    if not flags_tuple:
        return None

    parsed = []
    for f in flags_tuple:
        flag_str = f.decode('utf-8', errors='ignore') if isinstance(f, bytes) else str(f)
        flag_str = flag_str.strip()
        if not flag_str:
            continue
        if flag_str.lower() == '\\recent':
            continue
        parsed.append(flag_str)

    if not parsed:
        return None
    return '(' + ' '.join(parsed) + ')'


def parse_internal_date(raw_response_info):
    """INTERNALDATE parse et / Parse INTERNALDATE."""
    date_val = imaplib.Internaldate2tuple(raw_response_info)
    if date_val is not None:
        return date_val
    return time.time()


def extract_subject(raw_email, max_len=60):
    """Subject başlığını çıkar / Extract Subject."""
    try:
        subj_match = re.search(rb'(?i)^Subject:\s*(.+)', raw_email, re.MULTILINE)
        if subj_match:
            raw_subj = subj_match.group(1).decode('utf-8', errors='ignore').strip()
            if '=?' in raw_subj:
                try:
                    import email.header
                    decoded_parts = email.header.decode_header(raw_subj)
                    subj = ''
                    for part, charset in decoded_parts:
                        if isinstance(part, bytes):
                            subj += part.decode(charset or 'utf-8', errors='ignore')
                        else:
                            subj += part
                    raw_subj = subj
                except Exception:
                    pass
            if len(raw_subj) > max_len:
                return raw_subj[:max_len] + "..."
            return raw_subj
    except Exception:
        pass
    return "(Konu okunamadı / Subject unreadable)"


def extract_from(raw_email):
    """From başlığını çıkar / Extract From."""
    try:
        from_match = re.search(rb'(?i)^From:\s*(.+)', raw_email, re.MULTILINE)
        if from_match:
            raw_from = from_match.group(1).decode('utf-8', errors='ignore').strip()
            if len(raw_from) > 50:
                return raw_from[:50] + "..."
            return raw_from
    except Exception:
        pass
    return "(Gönderen okunamadı / Sender unreadable)"


# ============================================================
#  ANA AKTARIM FONKSİYONU / MAIN MIGRATION LOOP
# ============================================================

def sync_all_emails():
    global seen_ids, folder_state

    migration_start = time.time()

    print("\n" + "█" * 65)
    print("█  IMAP EMAIL MIGRATION SYSTEM (Source → Target)")
    print(f"█  Kaynak/Source:  {SRC_USER} @ {SRC_HOST}")
    print(f"█  Hedef/Target :  {DST_USER} @ {DST_HOST}")
    print(f"█  Zaman/Time   :  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("█" * 65 + "\n")

    init_seen_file()

    # --- İlk Bağlantı / Initial Connection ---
    try:
        log(f"Kaynak sunucuya bağlanılıyor / Connecting to Source: {SRC_HOST}...", "BAĞLANTI/CONN")
        src = connect_imap(SRC_HOST, SRC_USER, SRC_PASS)
        log(f"Kaynak sunucu bağlantısı başarılı. ✓ / Source connected. ✓", "BAĞLANTI/CONN")

        log(f"Hedef sunucuya bağlanılıyor / Connecting to Target: {DST_HOST}...", "BAĞLANTI/CONN")
        dst = connect_imap(DST_HOST, DST_USER, DST_PASS)
        log(f"Hedef sunucu bağlantısı başarılı. ✓ / Target connected. ✓", "BAĞLANTI/CONN")

        log(f"Hafızadaki mail sayısı / Mails in memory: {len(seen_ids)}", "DURUM/STATE")
    except Exception as e:
        log(f"Bağlantı kurulamadı / Connection failed: {e}", "KRİTİK/CRITICAL")
        return

    dst_delim = get_dst_delimiter(dst)

    typ, folders = src.list()
    if typ != 'OK' or not folders:
        log("Klasör listesi alınamadı! / Failed to retrieve folder list!", "KRİTİK/CRITICAL")
        safe_logout(src)
        safe_logout(dst)
        return

    log(f"Kaynak sunucuda {len(folders)} klasör bulundu / Found {len(folders)} folders:", "KLASÖR/FOLDER")
    folder_info_list = []
    for fd in folders:
        if not fd:
            continue
        fl, sd, sf = parse_list_response(fd)
        noselect = '\\Noselect' in fl or '\\NonExistent' in fl
        folder_info_list.append((fl, sd, sf, noselect))
        status_mark = "⊘ ATLA/SKIP" if noselect else "✓"
        log(f"  {status_mark}  {sf}  (Bayraklar/Flags: {' '.join(fl)})", "KLASÖR/FOLDER")

    total_migrated_session = 0
    total_skipped_session = 0
    total_bytes_session = 0
    total_errors_session = 0
    folder_index = 0
    active_folders = [f for f in folder_info_list if not f[3]]

    for flags, src_delim, src_folder, is_noselect in folder_info_list:
        if is_noselect:
            log(f"[{src_folder}] Noselect bayrağı var — atlanıyor / Skipping Noselect folder.", "ATLANDI/SKIPPED")
            continue

        folder_index += 1
        dst_folder = determine_target_folder(flags, src_folder, src_delim, dst_delim)

        print(f"\n{'═'*65}")
        log(f"KLASÖR/FOLDER {folder_index}/{len(active_folders)}: [{src_folder}] → [{dst_folder}]", "KLASÖR/FOLDER")
        print(f"{'═'*65}")

        # --- Hedef Klasör Kontrolü / Destination Folder Check ---
        try:
            status, _ = dst.select(f'"{dst_folder}"')
            if status != 'OK':
                log(f"Hedef klasör oluşturuluyor / Creating destination folder: '{dst_folder}'...", "KLASÖR/FOLDER")
                dst.create(f'"{dst_folder}"')
                try:
                    dst.subscribe(f'"{dst_folder}"')
                except Exception:
                    pass
                status, _ = dst.select(f'"{dst_folder}"')
                if status != 'OK':
                    log(f"Klasör oluşturulamadı! / Failed to create folder! '{dst_folder}'", "HATA/ERROR")
                    total_errors_session += 1
                    continue
                log(f"Hedef klasör oluşturuldu. ✓ / Folder created. ✓", "KLASÖR/FOLDER")
            else:
                log(f"Hedef klasör mevcut. ✓ / Folder exists. ✓", "KLASÖR/FOLDER")
        except Exception as e:
            log(f"Hedef klasör erişim sorunu / Folder access issue: {e}", "HATA/ERROR")
            total_errors_session += 1
            continue

        # --- Kaynak Klasörü Aç / Open Source Folder ---
        try:
            status, select_data = src.select(f'"{src_folder}"', readonly=True)
            if status != 'OK':
                log(f"Kaynak klasör açılamadı / Failed to open source folder: '{src_folder}'", "HATA/ERROR")
                total_errors_session += 1
                continue
            total_in_mailbox = int(select_data[0].decode('utf-8')) if select_data and select_data[0] else 0
            log(f"[{src_folder}] Toplam mail / Total emails: {total_in_mailbox}", "BİLGİ/INFO")
        except Exception as e:
            log(f"Klasör hatası / Folder error: {e}", "HATA/ERROR")
            total_errors_session += 1
            continue

        # --- Klasör Durumu / Folder State ---
        if src_folder not in folder_state:
            folder_state[src_folder] = {"last_yandex_uid": 0, "migrated": 0, "skipped": 0}

        prev_migrated = folder_state[src_folder]["migrated"]
        prev_skipped = folder_state[src_folder]["skipped"]
        if prev_migrated > 0 or prev_skipped > 0:
            log(f"[{src_folder}] Devam ediliyor / Resuming: {prev_migrated} aktarıldı/migrated, "
                f"{prev_skipped} atlandı/skipped.", "DEVAM/RESUME")

        last_uid = folder_state[src_folder]["last_yandex_uid"]
        if last_uid > 0:
            log(f"[{src_folder}] Son UID / Last UID: {last_uid} — buradan devam / resuming here.", "DEVAM/RESUME")

        # --- UID Araması / UID Search ---
        search_criteria = f"UID {last_uid + 1}:*" if last_uid > 0 else "ALL"
        try:
            typ, src_data = src.uid('search', None, search_criteria)
        except Exception as e:
            log(f"[{src_folder}] UID arama hatası / UID search error: {e}", "HATA/ERROR")
            total_errors_session += 1
            continue

        if not src_data or not src_data[0]:
            log(f"[{src_folder}] Yeni mail yok. ✓ / No new emails to process. ✓", "TAMAM/DONE")
            continue

        src_uids = src_data[0].split()
        uids_to_process = [uid for uid in src_uids if int(uid.decode('utf-8')) > last_uid]

        if not uids_to_process:
            log(f"[{src_folder}] Zaten senkronize. ✓ / Already synced. ✓", "TAMAM/DONE")
            continue

        remaining_in_folder = len(uids_to_process)
        log(f"[{src_folder}] {remaining_in_folder} işlenmemiş mail / unprocessed emails. "
            f"(Toplam/Total: {total_in_mailbox} | Aktarılan/Migrated: {prev_migrated})", "BAŞLADI/START")

        # --- Mail Döngüsü / Mail Loop ---
        processed_count = 0
        folder_migrated = 0
        folder_skipped = 0
        folder_bytes = 0
        folder_start = time.time()

        for uid_bytes in uids_to_process:
            processed_count += 1
            yandex_uid = uid_bytes.decode('utf-8')
            yandex_uid_int = int(yandex_uid)
            success = False
            retry_attempt = 0

            while not success:
                try:
                    typ, msg_data = src.uid('fetch', yandex_uid, '(FLAGS INTERNALDATE BODY.PEEK[])')

                    if not msg_data or not msg_data[0] or not isinstance(msg_data[0], tuple):
                        log(f"[{src_folder}] {prev_migrated + processed_count}/{total_in_mailbox} | "
                            f"UID:{yandex_uid} | ⚠ Boş yanıt / Empty response — atlanıyor/skipping.", "UYARI/WARN")
                        success = True
                        continue

                    raw_response_info = msg_data[0][0]
                    raw_email = msg_data[0][1]

                    if not raw_email:
                        log(f"[{src_folder}] {prev_migrated + processed_count}/{total_in_mailbox} | "
                            f"UID:{yandex_uid} | ⚠ Mail gövdesi boş / Body empty — atlanıyor/skipping.", "UYARI/WARN")
                        success = True
                        continue

                    mail_size = len(raw_email)

                    # --- Message-ID Çıkar / Extract Message-ID ---
                    msg_id_match = re.search(rb'(?i)^Message-ID:\s*(<[^>]+>)', raw_email, re.MULTILINE)
                    if msg_id_match:
                        current_msg_id = msg_id_match.group(1).decode('utf-8', errors='ignore').strip()
                    else:
                        current_msg_id = f"<NO-ID-{src_folder}-{yandex_uid}>"

                    subject = extract_subject(raw_email)
                    sender = extract_from(raw_email)

                    # --- Mükerrer Kontrolü / Deduplication Check ---
                    if current_msg_id in seen_ids:
                        folder_state[src_folder]["skipped"] += 1
                        total_skipped_session += 1
                        folder_skipped += 1
                        success = True
                        log(f"[{src_folder}] {prev_migrated + processed_count}/{total_in_mailbox} | "
                            f"UID:{yandex_uid} | ♻ MÜKERRER/DUPLICATE | "
                            f"ID: {current_msg_id}", "ATLANDI/SKIP")
                    else:
                        flag_string = build_flag_string(raw_response_info)
                        date_val = parse_internal_date(raw_response_info)

                        # Mailcow'a ekle / Append to Target
                        app_status, app_response = dst.append(
                            f'"{dst_folder}"',
                            flag_string,
                            date_val,
                            raw_email,
                        )

                        if app_status == 'OK':
                            seen_ids.add(current_msg_id)
                            save_seen_id(current_msg_id)
                            folder_state[src_folder]["migrated"] += 1
                            total_migrated_session += 1
                            folder_migrated += 1
                            folder_bytes += mail_size
                            total_bytes_session += mail_size
                            success = True

                            elapsed = time.time() - folder_start
                            speed = folder_migrated / elapsed if elapsed > 0 else 0

                            log(f"[{src_folder}] {prev_migrated + processed_count}/{total_in_mailbox} | "
                                f"UID:{yandex_uid} | ✓ AKTARILDI/MIGRATED | "
                                f"{format_size(mail_size)} | "
                                f"Bayrak/Flag: {flag_string or '(yok/none)'} | "
                                f"ID: {current_msg_id}", "AKTARIM/OK")
                            log(f"  └─ Gönderen/From: {sender}", "DETAY/DETAIL")
                            log(f"  └─ Konu/Subj    : {subject}", "DETAY/DETAIL")
                        else:
                            raise Exception(f"APPEND reddedildi / rejected: {app_response}")

                    folder_state[src_folder]["last_yandex_uid"] = yandex_uid_int

                    if processed_count % 100 == 0:
                        save_state(folder_state)

                    # --- Batch Bekleme / Batch Sleep ---
                    if processed_count % BATCH_SIZE == 0 and processed_count < remaining_in_folder:
                        save_state(folder_state)
                        elapsed = time.time() - folder_start
                        speed = folder_migrated / elapsed if elapsed > 0 else 0
                        remaining = remaining_in_folder - processed_count
                        eta = remaining / speed if speed > 0 else 0

                        print(f"\n{'─'*65}")
                        log(f"[⏸] MOLA / PAUSE — {BATCH_WAIT} sn/sec...", "BEKLEME/WAIT")
                        log(f"  ├─ İlerleme/Progress: {processed_count}/{remaining_in_folder} "
                            f"({processed_count * 100 / remaining_in_folder:.1f}%)", "BEKLEME/WAIT")
                        log(f"  ├─ Bu Oturum/Session: {folder_migrated} aktarıldı/migrated, {folder_skipped} atlandı/skipped", "BEKLEME/WAIT")
                        log(f"  ├─ Veri/Data: {format_size(folder_bytes)}", "BEKLEME/WAIT")
                        log(f"  ├─ Hız/Speed: {speed:.1f} mail/sn(s)", "BEKLEME/WAIT")
                        log(f"  ├─ Süre/Elapsed: {format_duration(elapsed)}", "BEKLEME/WAIT")
                        log(f"  └─ Kalan/ETA: {format_duration(eta)}", "BEKLEME/WAIT")

                        for remaining_sec in range(BATCH_WAIT, 0, -10):
                            log(f"  ⏳ {remaining_sec} sn/sec...", "BEKLEME/WAIT")
                            time.sleep(min(10, remaining_sec))

                        log(f"[▶] Devam ediliyor / Resuming...", "BEKLEME/WAIT")
                        print(f"{'─'*65}\n")

                    retry_attempt = 0

                    if shutdown_requested:
                        break

                except Exception as e:
                    total_errors_session += 1
                    log(f"HATA/ERROR! UID: {yandex_uid} | Detay/Info: {e}", "HATA/ERROR")
                    log(f"  ├─ Klasör/Folder: {src_folder} → {dst_folder}", "HATA/ERROR")
                    log(f"  ├─ İşlenen/Processed: {processed_count}/{remaining_in_folder}", "HATA/ERROR")
                    log(f"  └─ Toplam Hata/Total Errors: {total_errors_session}", "HATA/ERROR")

                    save_state(folder_state)
                    log("Durum dosyası kaydedildi. ✓ / State saved. ✓", "KAYIT/SAVE")

                    if retry_attempt < len(BACKOFF_SEQUENCE):
                        wait_time = BACKOFF_SEQUENCE[retry_attempt]
                    else:
                        wait_time = BACKOFF_SEQUENCE[-1]

                    print(f"\n{'─'*65}")
                    log(f"[!] KOPTU/DROPPED — Deneme/Try #{retry_attempt + 1}", "YENİDEN/RECONNECT")
                    log(f"  ├─ Bekleme/Wait: {wait_time} sn/sec", "YENİDEN/RECONNECT")
                    
                    for remaining_sec in range(wait_time, 0, -10):
                        log(f"  ⏳ Bekleniyor/Waiting... {remaining_sec} sn/sec", "BEKLEME/WAIT")
                        time.sleep(min(10, remaining_sec))

                    safe_logout(src)
                    safe_logout(dst)

                    try:
                        log("Kaynak bağlanıyor / Reconnecting Source...", "YENİDEN/RECONNECT")
                        src = connect_imap(SRC_HOST, SRC_USER, SRC_PASS)
                        src.select(f'"{src_folder}"', readonly=True)
                        
                        log("Hedef bağlanıyor / Reconnecting Target...", "YENİDEN/RECONNECT")
                        dst = connect_imap(DST_HOST, DST_USER, DST_PASS)
                        dst.select(f'"{dst_folder}"')
                        
                        log("Yeniden bağlantı başarılı / Reconnection successful.", "YENİDEN/RECONNECT")
                    except Exception as reconnect_error:
                        log(f"Yeniden bağlanma BAŞARISIZ / Reconnect FAILED: {reconnect_error}", "KRİTİK/CRITICAL")

                    print(f"{'─'*65}\n")
                    retry_attempt += 1

            if shutdown_requested:
                log("Ctrl+C — döngüden çıkılıyor / breaking loop.", "CTRL+C")
                break

        # --- Klasör Sonu / Folder End ---
        save_state(folder_state)
        folder_elapsed = time.time() - folder_start
        stats = folder_state[src_folder]

        status_text = "DURDURULDU/STOPPED" if shutdown_requested else "TAMAMLANDI/DONE"
        print(f"\n{'─'*65}")
        log(f"KLASÖR/FOLDER {status_text}: [{src_folder}]", "ÖZET/SUMMARY")
        log(f"  ├─ Toplam/Total (Source): {total_in_mailbox}", "ÖZET/SUMMARY")
        log(f"  ├─ Bu Oturum Aktarılan/Session Migrated: {folder_migrated}", "ÖZET/SUMMARY")
        log(f"  ├─ Bu Oturum Atlanan/Session Skipped: {folder_skipped}", "ÖZET/SUMMARY")
        log(f"  ├─ Tüm Zamanlar Aktarılan/All-time Migrated: {stats['migrated']}", "ÖZET/SUMMARY")
        log(f"  ├─ Tüm Zamanlar Atlanan/All-time Skipped: {stats['skipped']}", "ÖZET/SUMMARY")
        log(f"  ├─ Aktarılan Veri/Data: {format_size(folder_bytes)}", "ÖZET/SUMMARY")
        log(f"  └─ Süre/Elapsed: {format_duration(folder_elapsed)}", "ÖZET/SUMMARY")
        print(f"{'─'*65}")

        if shutdown_requested:
            log("Kalan klasörler atlanıyor / Skipping remaining folders.", "CTRL+C")
            break

    # --- Temizlik / Cleanup ---
    close_seen_file()
    safe_logout(src)
    safe_logout(dst)

    migration_elapsed = time.time() - migration_start

    # --- Genel Özet / Global Summary ---
    exit_reason = "KULLANICI DURDURDU / USER STOPPED (Ctrl+C)" if shutdown_requested else "AKTARIM TAMAMLANDI / MIGRATION COMPLETE!"

    print(f"\n{'█'*65}")
    print(f"█  {exit_reason}")
    print(f"█{'─'*63}█")
    print(f"█  Oturum Aktarılan / Session Migrated  : {total_migrated_session:>10}")
    print(f"█  Oturum Atlanan / Session Skipped     : {total_skipped_session:>10}")
    print(f"█  Toplam Hata / Total Errors           : {total_errors_session:>10}")
    print(f"█  Aktarılan Veri / Total Data Migrated : {format_size(total_bytes_session):>10}")
    print(f"█  Toplam Süre / Total Time             : {format_duration(migration_elapsed):>10}")
    if total_migrated_session > 0 and migration_elapsed > 0:
        avg_speed = total_migrated_session / migration_elapsed
        print(f"█  Ortalama Hız / Average Speed         : {avg_speed:>7.1f} mail/sn")
    print(f"█{'─'*63}█")

    all_time_migrated = sum(v.get("migrated", 0) for v in folder_state.values())
    all_time_skipped = sum(v.get("skipped", 0) for v in folder_state.values())
    print(f"█  [TÜM ZAMANLAR / ALL-TIME]")
    print(f"█  Aktarılan / Migrated: {all_time_migrated} | Atlanan / Skipped: {all_time_skipped}")
    print(f"█{'─'*63}█")

    if shutdown_requested:
        print(f"█  ⚠ Durum dosyaları kaydedildi — tekrar çalıştırınca")
        print(f"█    kaldığı yerden devam edecektir.")
        print(f"█  ⚠ State saved — script will resume from where it left off.")
        print(f"█    ├─ {STATE_JSON}")
        print(f"█    └─ {SEEN_TXT}")
        print(f"█{'─'*63}█")

    print(f"█  Bitiş/End: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'█'*65}\n")


if __name__ == "__main__":
    sync_all_emails()