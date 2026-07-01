\# 🚀 Bulletproof IMAP Migrator (English / Türkçe)



> \*\*EN:\*\* A fault-tolerant, stateful, and production-ready Python script for migrating IMAP email accounts. Designed to handle server connection drops (EOF) seamlessly by utilizing exponential backoff and precise state tracking.

> 

> \*\*TR:\*\* Bağlantı kopmalarına dayanıklı, kaldığı yerden devam edebilen ve sıfır veri kaybı garantisi sunan, üretim ortamı standartlarında IMAP e-posta taşıma aracı. Yüksek hacimli e-posta taşımalarında karşılaşılan sunucu kesintilerini (EOF) akıllıca yönetir.



\---



\## ✨ Features / Özellikler



\### 🇬🇧 English

\* 🛡️ \*\*Exponential Backoff:\*\* Gracefully handles server rate-limits and connection drops. Delays incrementally (30s, 60s, 120s) and resumes exactly where it left off.

\* 💾 \*\*Stateful Architecture:\*\* Simultaneously tracks the `Message-ID` and folder IMAP `UID` on disk (JSON \& TXT). Never rescans folders from scratch.

\* 🛑 \*\*Graceful Shutdown:\*\* Pressing `Ctrl+C` does not corrupt the database. It finishes the current email, flushes logs safely to disk, and halts.

\* 🚫 \*\*Deduplication:\*\* Prevents duplicate emails even if folder structures change. Automatically assigns pseudo-IDs to draft emails lacking a valid `Message-ID`.

\* 📊 \*\*Live Dashboard:\*\* Turns your terminal into a monitoring hub with real-time speed (emails/sec), ETA, and data size metrics.



\### 🇹🇷 Türkçe

\* 🛡️ \*\*Akıllı Bekleme:\*\* Sunucu bağlantıyı kestiğinde script çökmez. Süreleri kademeli olarak artırarak yeniden bağlanmayı dener ve tam kaldığı mailden devam eder.

\* 💾 \*\*Durum Hafızası:\*\* İnen her mailin `Message-ID`'sini ve `UID`'sini diskte tutar. İşlem yarıda kesilirse, baştan tarama yapmaz; hedefe doğrudan ışınlanır.

\* 🛑 \*\*Zarif Kapanış:\*\* `Ctrl+C`'ye bastığınızda veritabanını bozmaz. Mevcut aktarımı tamamlar ve temiz bir çıkış yapar.

\* 🚫 \*\*Mükerrer Mail Koruması:\*\* Daha önce aktarılan bir e-posta asla ikinci kez kopyalanmaz. Kimliksiz maillere (taslaklar) otomatik sahte kimlik atanır.

\* 📊 \*\*Canlı Kontrol Paneli:\*\* İşlem hızı, kalan süre ve aktarılan boyut detayları ile terminal ekranınızı bir gösterge paneline dönüştürür.



\---



\## 🛠️ Installation / Kurulum



\*\*1. Clone the repository / Depoyu klonlayın:\*\*

```bash

git clone \[https://github.com/YOUR\_USERNAME/bulletproof-imap-migrator.git](https://github.com/YOUR\_USERNAME/bulletproof-imap-migrator.git)

cd bulletproof-imap-migrator

