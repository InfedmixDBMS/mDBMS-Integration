# mDBMS-Integration

Cara menjalankan
1. Sync submodules:
   ```bash
   git submodule update --init --recursive
   ```

2. Jalankan program utama:
   ```bash
   python main.py
   ```

3. Untuk menjalankan unit testing seluruh sistem:
   ```bash
   python test_system_integration.py
   ```

4. Untuk membersihkan data dan katalog storage manager:
   ```bash
   make clean-storage
   ```