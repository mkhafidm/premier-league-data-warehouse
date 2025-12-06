import requests
from google.cloud import bigquery
import json
from datetime import datetime
from io import BytesIO


PROJECT_ID = "premier-league-analysis"
BIGQUERY_DATASET = "fpl_data"
client = bigquery.Client(project=PROJECT_ID)

STATIC_URL = "https://fantasy.premierleague.com/api/bootstrap-static/"
FIXTURES_URL = "https://fantasy.premierleague.com/api/fixtures/"
GAMEWEEK_URL = "https://fantasy.premierleague.com/api/event/{event_id}/live/"


def extract_fpl():
    run_timestamp = datetime.now().isoformat()

    extracted_data = {
        'static_data': None,
        'fixtures_data': None,
        'gameweek_data': [],
        'run_timestamp': run_timestamp
    }

    # Static Data
    try:
        response_static = requests.get(STATIC_URL, timeout=10)
        response_static.raise_for_status()
        response_data = response_static.json()
        extracted_data["static_data"] = response_data
        print("Success extracted static data")

    except requests.exceptions.RequestException as e:
        print(f"Fail extract static data: {e}")
        return None
    
    # Fixtures Data
    try:
        response_fixtures = requests.get(FIXTURES_URL, timeout=10)
        response_fixtures.raise_for_status()
        fixtures_data = response_fixtures.json()
        extracted_data["fixtures_data"] = fixtures_data
        print("Success extracted fixtures data")
    
    except requests.exceptions.RequestException as e:
        print(f"Fail extract fixtures data")
        return None
    
    # Gameweek Live
    latest_gw = get_latest_finished_gameweek(extracted_data["static_data"])
    if latest_gw > 0:
        print(f"Memulai loop Gameweek dari GW 1 hingga GW {latest_gw}")

        for gw_id in range(1, latest_gw + 1):
            try:
                url = GAMEWEEK_URL.format(event_id=gw_id)
                response_gameweek = requests.get(url, timeout=10)
                response_gameweek.raise_for_status()
                gameweek_data = response_gameweek.json()

                extracted_data["gameweek_data"].append({
                    "gw_id": gw_id,
                    "data": gameweek_data,
                    "extraction_timestamp": run_timestamp
                })
                
                print(f"Sukses extract dan simpan data GW {gw_id}")
                
            except requests.exceptions.HTTPError as e:
                print(f"ERROR HTTP: Gagal mengambil data GW {gw_id}. Detail: {e}")
            except requests.exceptions.RequestException as e:
                print(f"ERROR Koneksi: Gagal extract gameweek data {gw_id}: {e}")
            except json.JSONDecodeError as e:
                print(f"ERROR JSON: Gagal parsing data dari GW {gw_id}. Detail: {e}")

    return extracted_data


def get_latest_finished_gameweek(static_data):
    # Fungsi pembantu untuk menentukan GW terakhir yang selesai (dari data statis)
    latest_gw = 0
    if static_data and 'events' in static_data:
        for event in static_data['events']:
            if event.get('finished') is True:
                latest_gw = max(latest_gw, event.get('id', 0))
    return latest_gw



def flatten_live_data(gameweek_id, elements_list, run_timestamp):
    """
    Melakukan Transformasi Mini dengan secara dinamis meratakan SEMUA kunci 
    dari kamus 'stats' tanpa definisi manual.
    """
    flattened_data = []
    
    for element in elements_list:
        player_id = element.get('id')
        player_stats = element.get('stats', {}) 

        # 1. Inisialisasi baris data dengan kunci utama (Metadata/Foreign Keys)
        row = {
            'gameweek_id': gameweek_id,
            'player_id': player_id,
            'extraction_timestamp': run_timestamp 
        }
        
        # 2. Loop DINAMIS: Ambil SEMUA key-value dari player_stats dan gabungkan ke row
        # Menggunakan .items() untuk mendapatkan semua kunci dan nilai
        for key, value in player_stats.items():
            # Hindari menimpa kunci yang sudah ada (gameweek_id, player_id)
            if key not in row:
                row[key] = value
        
        flattened_data.append(row)
        
    return flattened_data


def load_data_to_bigquery(table_name, data_list):
    """
    Sementara write dispositionnya seadanya saja, terutama bagian weekly stats perlu diperhatikan
    karena masih berbeda dengan logika di fungsi sebelumnya yang extract seluruh data
    apabila diappend maka akan keliru yg bener harusnya ditimpa apabula load seluruh gameweek
    makanya kalo misalkan append per minggu fungsi sebelumnya harus hanya ambil data minggu itu saja
    Sementara pake timpa saja dulu gapapa, nanti perlu diubah
    """

    if not data_list:
        print(f"Peringatan: Tidak ada data untuk dimuat ke {table_name}.")
        return

    table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"
    
    # if 'raw_weekly_stats' in table_name:
    #     write_disposition = 'WRITE_APPEND'
    # else:
    #     write_disposition = 'WRITE_TRUNCATE'

    write_disposition = 'WRITE_TRUNCATE'

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True
    )

    print(f"Memuat {len(data_list)} baris ke {table_name} ({write_disposition})...")
    
    try:
        json_string = "\n".join([json.dumps(row) for row in data_list])

        data_as_bytes = json_string.encode('utf-8')
        data_file = BytesIO(data_as_bytes)

        load_job = client.load_table_from_file(
            data_file,
            table_id,
            job_config=job_config
        )
        load_job.result() 
        
        print(f"Sukses LOAD ke {table_name}. Job ID: {load_job.job_id}")
            
    except Exception as e:
        print(f"ERROR Fatal saat Load BigQuery ke {table_name}: {e}")



def run_full_pipeline():
    extracted_data = extract_fpl()
    
    if not extracted_data:
        print("\nFATAL: Pipeline berhenti karena kegagalan Ekstraksi.")
        return

    static_data = extracted_data.get('static_data')
    fixtures_data = extracted_data.get('fixtures_data')
    gameweek_data = extracted_data.get('gameweek_data')
    run_timestamp = extracted_data.get('run_timestamp', datetime.now().isoformat())
    
    ## Static Data
    # Tim
    teams_raw = static_data.get("teams", [])
    for t in teams_raw: 
        if 'extraction_timestamp' not in t: t['extraction_timestamp'] = datetime.now().isoformat()
    
    # Player
    players_raw = static_data.get('elements', [])
    for p in players_raw: 
        if 'extraction_timestamp' not in p: p['extraction_timestamp'] = datetime.now().isoformat()
    
    # Postition
    positions_raw = static_data.get('element_types', [])
    for po in positions_raw:
        if 'extraction_timestamp' not in po: po['extraction_timestamp'] = datetime.now().isoformat()
    
    # Gameweek
    EXCLUDE_GW_KEYS = ['overrides', 'chip_plays', 'h2h_matches']
    gameweek_raw = static_data.get('events', [])
    
    # --- Solusi Filter Eksklusi untuk Gameweek Live ---
    gameweek_safe_load = []
    
    for g in gameweek_raw:
        # 1. Buat copy dari dictionary mentah
        safe_row = g.copy()
        
        # 2. Hapus kunci bermasalah secara dinamis
        for key in EXCLUDE_GW_KEYS:
            if key in safe_row:
                del safe_row[key] # Hapus kunci bersarang yang menyebabkan error 400
                
        # 3. Tambahkan timestamp
        safe_row['extraction_timestamp'] = run_timestamp
        
        gameweek_safe_load.append(safe_row)
    # gameweek_raw = static_data.get('events', [])
    # for g in gameweek_raw:
    #     g['extraction_timestamp'] = extracted_data.get('run_timestamp')

    ## Fixtures Data
    fixtures_raw = fixtures_data
    for f in fixtures_raw: 
        if 'extraction_timestamp' not in f: f['extraction_timestamp'] = datetime.now().isoformat()
        

    ## Gameweek Live Data
    weekly_stats_for_load = []

    for gw_data_item in gameweek_data:
        gw_id = gw_data_item['gw_id']
        live_elements = gw_data_item['data'].get('elements', [])
        
        # Ambil timestamp dari wrapper data (sudah ditambahkan di fungsi extract_fpl)
        run_timestamp = gw_data_item['extraction_timestamp']
        flattened_stats = flatten_live_data(gw_id, live_elements, run_timestamp) 
        
        # Gabungkan hasil flattening ke list besar
        weekly_stats_for_load.extend(flattened_stats)
        
        print(f"Sukses flatten dan kumpulkan data GW {gw_id}. Baris terkumpul: {len(flattened_stats)}.")

    # LOAD 1: Pemain (WRITE_TRUNCATE)
    load_data_to_bigquery('raw_static_players', players_raw)
    
    # LOAD 2: Tim (WRITE_TRUNCATE)
    load_data_to_bigquery('raw_static_teams', teams_raw)
    
    # LOAD 3: Posisi (WRITE_TRUNCATE)
    load_data_to_bigquery('raw_static_positions', positions_raw) # Tambah tabel baru: raw_static_positions
    
    # LOAD 4: Gameweek (WRITE_TRUNCATE)
    load_data_to_bigquery('raw_static_gameweeks', gameweek_safe_load) # Tambah tabel baru: raw_static_gameweeks
    
    # LOAD 5: Fixtures (WRITE_TRUNCATE)
    load_data_to_bigquery('raw_fixtures', fixtures_raw)
    
    # LOAD 6: Skor Mingguan (WRITE_TRUNCATE)
    load_data_to_bigquery('raw_weekly_stats', weekly_stats_for_load)


if __name__ == "__main__":
    # Ini adalah entry point untuk menjalankan script
    run_full_pipeline()
