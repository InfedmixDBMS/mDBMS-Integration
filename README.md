# mDBMS-Integration

Sistem manajemen database terdistribusi dengan arsitektur client-server.

## Setup

Clone repository dengan submodules:
```bash
git clone --recurse-submodules https://github.com/InfedmixDBMS/mDBMS-Integration.git
cd mDBMS-Integration
git submodule update --init --recursive
```

## Cara Menjalankan

### 1. Jalankan Server
```bash
python server.py
```

### 2. Jalankan Client
Di terminal lain:
```bash
python client.py
```

### 3. Gunakan SQL Commands
```sql
dbms> CREATE TABLE mahasiswa (nim INT, nama VARCHAR, ipk FLOAT)
dbms> INSERT INTO mahasiswa VALUES (123, 'Budi', 3.5)
dbms> SELECT * FROM mahasiswa
```

Ketik `help` untuk melihat perintah lengkap, `exit` untuk keluar.

## Testing

Jalankan server, lalu di terminal lain:
```bash
python test_client.py
```

## Utility

Bersihkan storage:
```bash
make clean-storage
```
